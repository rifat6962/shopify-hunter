from flask import Flask, render_template, request, jsonify, Response
import threading
import queue
import time
import json
import re
import random
import requests
from bs4 import BeautifulSoup
from groq import Groq
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import logging
import os

# Logging Setup
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

log_queue = queue.Queue()
automation_running = False
automation_thread = None
scheduler = BackgroundScheduler()
scheduler.start()

# Global Queue for AI Keywords
ai_keyword_pool = queue.Queue()

# --- Logging Function ---
def log(message, level="INFO"):
    entry = {
        'time': datetime.now().strftime('%H:%M:%S'),
        'level': level,
        'message': str(message)
    }
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

# --- Apps Script communication ---
def call_sheet(payload):
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url:
        return {'error': 'APPS_SCRIPT_URL not set'}
    
    for attempt in range(3):
        try:
            r = requests.post(script_url, json=payload, timeout=45,
                              headers={'Content-Type': 'application/json'})
            return r.json()
        except Exception as e:
            log(f"Sheet API error (Attempt {attempt+1}/3): {e}", "WARN")
            time.sleep(3)
    return {'error': 'Sheet API failed'}

# --- 1. AI KEYWORD EXPANSION (The "Magic" Part) ---
def generate_more_keywords(base_keyword, country, groq_key):
    """AI ব্যবহার করে একটি কিওয়ার্ড থেকে ২০টি নতুন প্রফেশনাল কিওয়ার্ড তৈরি করবে"""
    try:
        log(f"🧠 Asking AI to expand niche for: {base_keyword}...", "INFO")
        client = Groq(api_key=groq_key)
        prompt = f"""
        Act as an e-commerce expert. Based on the keyword '{base_keyword}', generate 20 highly specific niche keywords for Shopify stores in {country}.
        The keywords should be for stores that might have issues with their payment gateways.
        Return ONLY a comma-separated list of keywords. No numbers, no intro.
        Example: luxury leather bags, organic pet food, handmade jewelry, minimalist tech gear...
        """
        
        completion = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.8
        )
        raw_output = completion.choices[0].message.content.strip()
        new_keywords = [k.strip() for k in raw_output.split(',') if len(k.strip()) > 2]
        return new_keywords
    except Exception as e:
        log(f"AI Expansion Error: {e}", "WARN")
        return []

# --- 2. ADVANCED SCRAPING (URLScan.io + SerpAPI) ---
MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]).myshopify.com')

def find_shopify_stores(keyword, country, serpapi_key):
    all_urls = set()
    kw_clean = keyword.lower().replace(' ', '')

    # URLScan.io Logic
    try:
        urlscan_url = f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com AND {kw_clean}&size=100&sort=time"
        r = requests.get(urlscan_url, timeout=10)
        if r.status_code == 200:
            for result in r.json().get('results', []):
                page_url = result.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m: all_urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass

    # SerpAPI with "Past 7 Days" filter
    try:
        params = {
            'api_key': serpapi_key,
            'engine': 'google',
            'q': f'site:myshopify.com "{keyword}" {country}',
            'num': 50,
            'tbs': 'qdr:w'
        }
        res = requests.get('https://serpapi.com/search', params=params, timeout=15)
        if res.status_code == 200:
            for item in res.json().get('organic_results', []):
                m = MYSHOPIFY_RE.match(item.get('link', ''))
                if m: all_urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass

    return list(all_urls)

# --- 3. STRICT CHECKOUT TEST ---
def check_store_target(base_url, session):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0'}
    try:
        r = session.get(base_url, headers=headers, timeout=10, allow_redirects=True)
        html = r.text.lower()
        if 'shopify' not in html and 'cdn.shopify.com' not in html:
            return {"is_shopify": False, "is_lead": False}
        if '/password' in r.url or 'password-page' in html or 'opening soon' in html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected"}

        # Checkout Test Logic
        prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
        if prod_req.status_code == 200:
            prods = prod_req.json().get('products', [])
            if prods:
                variant_id = prods[0]['variants'][0]['id']
                session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers=headers)
                chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15)
                chk_html = chk_req.text.lower()
                
                # Payment Gateway Detection
                pay_keys = ['visa', 'mastercard', 'paypal', 'stripe', 'card number', 'klarna', 'shop pay']
                if any(pk in chk_html for pk in pay_keys):
                    return {"is_shopify": True, "is_lead": False, "reason": "Active Gateway Found"}
                
                if "isn't accepting payments" in chk_html or "not accepting payments" in chk_html or "checkout" in chk_html:
                    return {"is_shopify": True, "is_lead": True, "reason": "No Payment Gateway Found"}
                    
        return {"is_shopify": True, "is_lead": False, "reason": "No products to test"}
    except:
        return {"is_shopify": False, "is_lead": False}

# --- 4. DATA EXTRACTION ---
def get_store_info(base_url, session):
    info = {'store_name': base_url.split('//')[1].split('.')[0], 'email': None, 'phone': None}
    try:
        r = session.get(base_url, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        info['store_name'] = soup.title.text.strip()[:80] if soup.title else info['store_name']
        
        # Email Extraction
        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', r.text)
        for e in emails:
            if not any(x in e.lower() for x in ['shopify', 'sentry', 'example', 'png', 'jpg']):
                info['email'] = e.lower()
                break
    except: pass
    return info

# --- 5. AI EMAIL GENERATION ---
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"Write a short cold email for {lead['store_name']}. Problem: No payment gateway on checkout. Base template: {tpl_body}. Rules: 80 words max, JSON format: {{\"subject\": \"...\", \"body\": \"...\"}}"
        resp = client.chat.completions.create(model="llama-3.1-8b-instant", messages=[{"role": "user", "content": prompt}])
        data = json.loads(re.sub(r'```json|```', '', resp.choices[0].message.content).strip())
        return data.get('subject', tpl_subject), data.get('body', tpl_body)
    except:
        return tpl_subject, tpl_body

# --- 6. CORE AUTOMATION ENGINE ---
def run_automation():
    global automation_running
    automation_running = True
    try:
        # Load Config
        log("📋 Initializing Automation...", "INFO")
        cfg_resp = call_sheet({'action': 'get_config'})
        cfg = cfg_resp.get('config', {})
        groq_key = cfg.get('groq_api_key', '').strip()
        serpapi_key = cfg.get('serpapi_key', '').strip()
        min_leads = int(cfg.get('min_leads', 100))

        # Initial Keywords from Sheet
        kw_resp = call_sheet({'action': 'get_keywords'})
        ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
        
        for k in ready_kws:
            ai_keyword_pool.put(k)

        session = requests.Session()
        total_leads = 0

        while automation_running and total_leads < min_leads:
            if ai_keyword_pool.empty():
                log("⚠️ No more keywords in pool. Stopping.", "WARN")
                break

            current_obj = ai_keyword_pool.get()
            keyword = current_obj.get('keyword')
            country = current_obj.get('country', 'USA')
            kw_id = current_obj.get('id', '')

            log(f"🔍 SEARCHING: [{keyword}] in [{country}]", "INFO")
            stores = find_shopify_stores(keyword, country, serpapi_key)
            
            leads_from_this_kw = 0
            for url in stores:
                if not automation_running or total_leads >= min_leads: break
                
                target = check_store_target(url, session)
                if target.get('is_lead'):
                    info = get_store_info(url, session)
                    save_resp = call_sheet({
                        'action': 'save_lead',
                        'store_name': info['store_name'],
                        'url': url,
                        'email': info['email'] or '',
                        'country': country,
                        'keyword': keyword
                    })
                    
                    if save_resp.get('status') == 'ok':
                        total_leads += 1
                        leads_from_this_kw += 1
                        log(f"✅ LEAD #{total_leads}: {url}", "SUCCESS")
                
                time.sleep(1)

            # AI KEYWORD EXPANSION: যদি এই কিওয়ার্ড থেকে কোনো লিড আসে, তবে আরও ২০টি কিওয়ার্ড বানাও
            if leads_from_this_kw > 0 or random.random() > 0.5:
                new_kws = generate_more_keywords(keyword, country, groq_key)
                for nk in new_kws:
                    ai_keyword_pool.put({'keyword': nk, 'country': country})

            # Mark as used in sheet
            if kw_id:
                call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': leads_from_this_kw})

        log("🏁 Phase 1 Complete. Starting Email Phase...", "SUCCESS")
        # (Email Outreach logic remains same as Phase 2 in your original code)

    except Exception as e:
        log(f"💥 Error: {e}", "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation Stopped", "INFO")

# --- Flask Routes ---
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    return jsonify({'running': automation_running, 'pool_size': ai_keyword_pool.qsize()})

@app.route('/api/automation/start', methods=['POST'])
def api_start():
    global automation_thread
    if not automation_running:
        automation_thread = threading.Thread(target=run_automation, daemon=True)
        automation_thread.start()
        return jsonify({'status': 'started'})
    return jsonify({'status': 'already_running'})

@app.route('/api/automation/stop', methods=['POST'])
def api_stop():
    global automation_running
    automation_running = False
    return jsonify({'status': 'stopped'})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            msg = log_queue.get()
            yield f"data: {msg}\n\n"
    return Response(gen(), mimetype='text/event-stream')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
