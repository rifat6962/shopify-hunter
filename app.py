from flask import Flask, render_template, request, jsonify, Response
import threading
import queue
import time
import json
import re
import random
import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import logging
import os
import urllib.parse

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

log_queue = queue.Queue()
automation_running = False
automation_thread = None
scheduler = BackgroundScheduler()
scheduler.start()

# ── Apps Script communication ─────────────────────────────────────────────────
def call_sheet(payload):
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url:
        return {'error': 'APPS_SCRIPT_URL not set'}
    for attempt in range(3):
        try:
            r = requests.post(script_url, json=payload, timeout=30,
                              headers={'Content-Type': 'application/json'})
            try:
                return r.json()
            except:
                time.sleep(2); continue
        except requests.exceptions.Timeout:
            log(f"Sheet timeout ({attempt+1}/3)", "WARN"); time.sleep(2)
        except Exception as e:
            log(f"Sheet error ({attempt+1}/3): {e}", "WARN"); time.sleep(2)
    return {'error': 'Sheet API failed'}

def log(message, level="INFO"):
    entry = {'time': datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': str(message)}
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

# ── URL Cleaner ───────────────────────────────────────────────────────────────
def clean_shopify_url(raw_url):
    """লিংকের ভেতর থেকে আজেবাজে এনকোডিং (যেমন 2f) পরিষ্কার করবে"""
    decoded = urllib.parse.unquote(raw_url)
    match = re.search(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9]\.myshopify\.com)', decoded)
    if match:
        return f"https://{match.group(1)}"
    return None

# ─────────────────────────────────────────────────────────────────────────────
# AI KEYWORD GENERATOR
# ─────────────────────────────────────────────────────────────────────────────
def generate_ai_keywords(base_keyword, groq_key):
    log(f"🧠 AI: Generating sub-niches for '{base_keyword}'...", "INFO")
    try:
        prompt = f"Generate 50 specific sub-niche keywords for '{base_keyword}'. Return ONLY a JSON array of strings."
        headers = {"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"}
        payload = {"model": "llama-3.1-8b-instant", "messages":[{"role": "user", "content": prompt}], "max_tokens": 1000}
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload, timeout=20)
        if r.status_code == 200:
            raw = r.json()['choices'][0]['message']['content']
            return json.loads(re.sub(r'```(?:json)?|```', '', raw.strip()).strip())
    except: pass
    return []

# ─────────────────────────────────────────────────────────────────────────────
# NEW STORE DISCOVERY (SSL Logs + URLScan) - NO GOOGLE BLOCKING
# ─────────────────────────────────────────────────────────────────────────────
def get_new_stores_from_logs(keyword):
    """গুগল বাদ দিয়ে সরাসরি SSL Logs থেকে একদম নতুন স্টোর খুঁজবে"""
    urls = set()
    kw_clean = keyword.lower().replace(' ', '')
    
    # Source 1: crt.sh (SSL Certificate Logs - Finds stores created TODAY)
    log(f"🔍 Scanning SSL Logs for brand new '{keyword}' stores...", "INFO")
    try:
        r = requests.get(f"https://crt.sh/?q=%25{kw_clean}%25.myshopify.com&output=json", timeout=20)
        if r.status_code == 200:
            for cert in r.json():
                name = cert.get('common_name', '') or cert.get('name_value', '')
                for n in name.split('\n'):
                    cleaned = clean_shopify_url(n)
                    if cleaned: urls.add(cleaned)
    except: pass

    # Source 2: URLScan (Recently Scanned)
    try:
        r = requests.get(f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com AND {kw_clean}&size=100", timeout=15)
        if r.status_code == 200:
            for res in r.json().get('results', []):
                cleaned = clean_shopify_url(res.get('page', {}).get('url', ''))
                if cleaned: urls.add(cleaned)
    except: pass

    return list(urls)

# ─────────────────────────────────────────────────────────────────────────────
# DEEP HTML ANALYSIS (Slow & Accurate)
# ─────────────────────────────────────────────────────────────────────────────
def check_store_target(base_url, session):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Connection': 'close'}
    try:
        # ১. হোমপেজ চেক (Timeout 15s)
        r_home = session.get(base_url, headers=headers, timeout=15, allow_redirects=True)
        if r_home.status_code != 200: return {"is_lead": False, "reason": f"HTTP {r_home.status_code}"}
        
        home_html = r_home.text.lower()
        if '/password' in r_home.url or 'password-page' in home_html:
            return {"is_lead": False, "reason": "Password Protected"}

        # ২. চেকআউট চেক (প্রোডাক্ট কার্টে অ্যাড করে)
        chk_html = ""
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code == 200 and prod_req.json().get('products'):
                variant_id = prod_req.json()['products'][0]['variants'][0]['id']
                session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers={**headers, 'Content-Type': 'application/json'}, timeout=10)
                chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15, allow_redirects=True)
                chk_html = chk_req.text.lower()
        except: pass

        # ৩. পেমেন্ট গেটওয়ে অ্যানালাইসিস
        error_footprints = ["isn't accepting payments", "not accepting payments", "no payment methods", "checkout is disabled"]
        payment_kws = ['visa', 'mastercard', 'amex', 'paypal', 'credit card', 'stripe', 'shop pay', 'apple pay']
        
        is_broken = any(phrase in (home_html + chk_html) for phrase in error_footprints)
        found_pay = [kw for kw in payment_kws if kw in chk_html]

        if is_broken:
            reason = "Verified: No Payment Gateway"
        elif not found_pay and ('contact information' in chk_html or 'shipping address' in chk_html):
            reason = "Checkout OK, No Card Fields Found"
        else:
            return {"is_lead": False, "reason": f"Payment Found: {found_pay[:1]}" if found_pay else "Active Store"}

        # ৪. ইমেইল এক্সট্রাকশন
        email_re = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
        emails = email_re.findall(r_home.text)
        extracted_email = emails[0].lower() if emails else None

        return {"is_lead": True, "reason": reason, "email": extracted_email, "name": base_url.split('//')[-1].split('.')[0].title()}
    except:
        return {"is_lead": False, "reason": "Timeout"}

# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try: _run()
    except Exception as e: log(f"💥 FATAL: {e}", "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation stopped", "INFO")

def _run():
    cfg = call_sheet({'action': 'get_config'}).get('config', {})
    groq_key = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 5) or 5)
    
    ready_kws = [k for k in call_sheet({'action': 'get_keywords'}).get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws: log("❌ No READY keywords!"); return

    session = requests.Session()
    total_leads = 0

    for kw_row in ready_kws:
        if not automation_running or total_leads >= min_leads: break
        base_kw = kw_row.get('keyword', '')
        
        # AI দিয়ে সাব-কিওয়ার্ড জেনারেট
        sub_keywords = [base_kw] + generate_ai_keywords(base_kw, groq_key)
        
        for kw in sub_keywords:
            if not automation_running or total_leads >= min_leads: break
            log(f"\n🎯 Processing: [{kw}]", "INFO")
            
            stores = get_new_stores_from_logs(kw)
            log(f"   📦 Found {len(stores)} potential new stores. Starting Deep Check...", "INFO")
            
            for idx, url in enumerate(stores):
                if not automation_running or total_leads >= min_leads: break
                
                log(f"   🌐 [{idx+1}/{len(stores)}] Analyzing: {url}", "INFO")
                
                # আপনার কথামতো সময় নিয়ে চেক করবে (৫ সেকেন্ড বিরতি)
                time.sleep(5) 
                
                res = check_store_target(url, session)
                if res["is_lead"]:
                    log(f"      🎯 MATCH: {res['reason']}", "SUCCESS")
                    save_resp = call_sheet({'action': 'save_lead', 'store_name': res['name'], 'url': url, 'email': res['email'] or '', 'country': kw_row.get('country',''), 'keyword': base_kw})
                    if save_resp.get('status') != 'duplicate':
                        total_leads += 1
                        log(f"      ✅ LEAD #{total_leads} SAVED!", "SUCCESS")
                else:
                    log(f"      ↳ 🚫 SKIP: {res['reason']}", "WARN")

        call_sheet({'action': 'mark_keyword_used', 'id': kw_row.get('id', ''), 'leads_found': total_leads})

# Flask Routes
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    s_connected = bool(os.environ.get('APPS_SCRIPT_URL'))
    return jsonify({'running': automation_running, 'total_leads': 0, 'script_connected': s_connected})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try: yield f"data: {log_queue.get(timeout=25)}\n\n"
            except queue.Empty: yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/automation/start', methods=['POST'])
def api_start():
    global automation_running, automation_thread
    if not automation_running:
        automation_thread = threading.Thread(target=run_automation, daemon=True)
        automation_thread.start()
    return jsonify({'status': 'started'})

@app.route('/api/automation/stop', methods=['POST'])
def api_stop():
    global automation_running
    automation_running = False
    return jsonify({'status': 'stopped'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), threaded=True)
