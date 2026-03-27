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

MYSHOPIFY_RE = re.compile(r'([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

# ─────────────────────────────────────────────────────────────────────────────
# AI KEYWORD GENERATOR
# ─────────────────────────────────────────────────────────────────────────────
def generate_ai_keywords(base_keyword, groq_key):
    log(f"🧠 Using AI to generate related keywords for '{base_keyword}'...", "INFO")
    try:
        prompt = f"""Generate a list of 100 highly specific e-commerce niche keywords related to '{base_keyword}'. 
For example, if the keyword is 'shoes', return["sneakers", "leather boots", "running shoes", "high heels", "sports shoes"].
Return ONLY a valid JSON array of strings. No markdown formatting, no explanations, no extra text."""

        headers = {
            "Authorization": f"Bearer {groq_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "llama-3.1-8b-instant",
            "messages":[{"role": "user", "content": prompt}],
            "max_tokens": 1500,
            "temperature": 0.7
        }
        
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload, timeout=30)
        
        if r.status_code == 200:
            raw = r.json()['choices'][0]['message']['content']
            raw = re.sub(r'```(?:json)?|```', '', raw.strip()).strip()
            kw_list = json.loads(raw)
            
            if isinstance(kw_list, list) and len(kw_list) > 0:
                log(f"   ✅ AI successfully generated {len(kw_list)} related keywords in the backend!", "SUCCESS")
                return kw_list
    except Exception as e:
        log(f"   ⚠️ AI Error. Using base keyword only.", "WARN")
        
    return[]

# ─────────────────────────────────────────────────────────────────────────────
# MULTI-ENGINE SCRAPER (Unblockable)
# ─────────────────────────────────────────────────────────────────────────────
def get_stores_for_keyword(keyword, serpapi_key):
    urls = set()
    kw_clean = keyword.lower().replace(' ', '')
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    queries =[
        f'site:myshopify.com "{keyword}" "isn\'t accepting payments right now"',
        f'site:myshopify.com "{keyword}" "opening soon"',
        f'site:myshopify.com "{keyword}"'
    ]

    # 1. Yahoo Search (Pagination: 3 Pages)
    for q in queries:
        for b in[1, 11, 21]: 
            try:
                encoded_q = urllib.parse.quote_plus(q)
                r = requests.get(f"https://search.yahoo.com/search?p={encoded_q}&b={b}", headers=headers, timeout=15)
                soup = BeautifulSoup(r.text, 'html.parser')
                for a in soup.find_all('a', href=True):
                    m = MYSHOPIFY_RE.search(a['href'])
                    if m: urls.add(f"https://{m.group(1)}.myshopify.com")
            except: pass
            time.sleep(1)

    # 2. Bing Search (Pagination: 3 Pages)
    for q in queries:
        for first in[1, 11, 21]: 
            try:
                encoded_q = urllib.parse.quote_plus(q)
                r = requests.get(f"https://www.bing.com/search?q={encoded_q}&first={first}", headers=headers, timeout=15)
                soup = BeautifulSoup(r.text, 'html.parser')
                for a in soup.find_all('a', href=True):
                    m = MYSHOPIFY_RE.search(a['href'])
                    if m: urls.add(f"https://{m.group(1)}.myshopify.com")
            except: pass
            time.sleep(1)

    # 3. URLScan.io
    try:
        r = requests.get(f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com AND {kw_clean}&size=100", timeout=15)
        if r.status_code == 200:
            for res in r.json().get('results',[]):
                m = MYSHOPIFY_RE.search(res.get('page', {}).get('url', ''))
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass

    # 4. SerpAPI
    if serpapi_key:
        for q in queries:
            for start in[0, 100]: 
                try:
                    params = {'api_key': serpapi_key, 'engine': 'google', 'q': q, 'num': 100, 'start': start, 'tbs': 'qdr:m'}
                    res = requests.get('https://serpapi.com/search', params=params, timeout=20)
                    if res.status_code == 200:
                        for item in res.json().get('organic_results',[]):
                            m = MYSHOPIFY_RE.search(item.get('link', ''))
                            if m: urls.add(f"https://{m.group(1)}.myshopify.com")
                except: pass
                time.sleep(1)

    urls_list = list(urls)
    random.shuffle(urls_list)
    return urls_list

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL EXTRACTION HELPER
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL =['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']

def extract_email_from_html(html):
    for match in EMAIL_RE.findall(html):
        e = match.lower()
        if not any(d in e for d in SKIP_EMAIL): 
            return e
    return None

# ─────────────────────────────────────────────────────────────────────────────
# DEEP HTML ANALYSIS (No Rushing, Full Check, Anti-Hang)
# ─────────────────────────────────────────────────────────────────────────────
def check_store_target(base_url, session):
    ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    # Connection: close prevents the bot from hanging on slow servers
    headers = {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'close' 
    }

    try:
        # 1. Fetch Homepage HTML (Timeout 15s)
        r_home = session.get(base_url, headers=headers, timeout=15, allow_redirects=True)
        if r_home.status_code != 200: 
            return {"is_shopify": False, "is_lead": False, "reason": f"HTTP {r_home.status_code}"}
            
        home_html = r_home.text.lower()
        if 'shopify' not in home_html and 'cdn.shopify.com' not in home_html:
            return {"is_shopify": False, "is_lead": False, "reason": "Not a Shopify store"}

        # Password Check
        if '/password' in r_home.url or 'password-page' in home_html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected (No email available)"}

        # 2. Add to Cart (Timeout 15s)
        chk_html = ""
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=15)
            if prod_req.status_code == 200 and prod_req.json().get('products'):
                variant_id = prod_req.json()['products'][0]['variants'][0]['id']
                session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers={**headers, 'Content-Type': 'application/json'}, timeout=15)
                
                # 3. Fetch Checkout HTML (Timeout 20s)
                chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=20, allow_redirects=True)
                chk_html = chk_req.text.lower()
            else:
                return {"is_shopify": True, "is_lead": False, "reason": "No products to test checkout"}
        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": "Failed to load checkout page"}

        # 4. Combine Full HTML for Deep Analysis
        full_html = home_html + chk_html

        # 5. Analyze Full HTML for Payment Gateways
        error_footprints =["isn't accepting payments", "not accepting payments", "no payment methods", "payment provider hasn't been set up", "checkout is disabled"]
        is_broken = any(phrase in full_html for phrase in error_footprints)

        payment_kws =['visa', 'mastercard', 'amex', 'paypal', 'credit card', 'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay', 'google pay']
        found_pay =[kw for kw in payment_kws if kw in chk_html] 

        reason = ""
        is_lead = False

        if is_broken:
            is_lead = True
            reason = "CONFIRMED NO PAYMENT (Error found in HTML)"
        elif found_pay:
            return {"is_shopify": True, "is_lead": False, "reason": f"Payment Gateway Found: {found_pay[:2]}"}
        elif 'contact information' in chk_html or 'shipping address' in chk_html:
            is_lead = True
            reason = "Checkout OK, NO Payment options in HTML"
        else:
            return {"is_shopify": True, "is_lead": False, "reason": "Inconclusive Checkout"}

        # 6. If NO PAYMENT, extract email from FULL HTML
        extracted_email = None
        if is_lead:
            extracted_email = extract_email_from_html(r_home.text)
            if not extracted_email:
                try:
                    r_contact = session.get(base_url + '/pages/contact', headers=headers, timeout=10)
                    if r_contact.status_code == 200:
                        extracted_email = extract_email_from_html(r_contact.text)
                except: pass

        return {
            "is_shopify": True, 
            "is_lead": True, 
            "reason": reason, 
            "extracted_email": extracted_email,
            "store_name": base_url.replace('https://', '').split('.')[0].title()
        }

    except Exception as e:
        return {"is_shopify": False, "is_lead": False, "reason": "Connection Timeout/Error"}

# ── AI Email generation ───────────────────────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        prompt = f"""Write a short cold email to a Shopify store owner.
Store: {lead.get('store_name', 'the store')}
Country: {lead.get('country', '')}
Problem: NO payment gateway configured.
Base: Subject: {tpl_subject} | Body: {tpl_body}
Rules: 80-100 words, no spam words, mention store name once, 1 soft CTA, HTML <p> tags
Return ONLY valid JSON: {{"subject": "...", "body": "<p>...</p>"}}"""
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={"model": "llama-3.1-8b-instant",
                  "messages":[{"role": "user", "content": prompt}],
                  "max_tokens": 500, "temperature": 0.7},
            timeout=20)
        if r.status_code == 200:
            raw = r.json()['choices'][0]['message']['content']
            raw = re.sub(r'```(?:json)?|```', '', raw.strip()).strip()
            raw = raw.replace('\n', ' ').replace('\r', '')
            data = json.loads(raw, strict=False)
            return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception as e:
        pass
    return tpl_subject, f'<p>{tpl_body}</p>'

# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        log(f"💥 FATAL: {e}", "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation stopped", "INFO")

def _run():
    global automation_running
    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Apps Script: {cfg_resp['error']}", "ERROR"); return

    cfg = cfg_resp.get('config', {})
    serpapi_key = cfg.get('serpapi_key', '').strip()
    groq_key = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR"); return
    log(f"🗝️  {len(ready_kws)} base keywords ready", "INFO")

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates',[])
    if not templates:
        log("❌ No email template!", "ERROR"); return
    tpl = templates[0]
    log(f"📧 Template loaded: '{tpl['name']}'", "INFO")

    session = requests.Session()
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — AI KEYWORD MULTIPLIER & DEEP HTML ANALYSIS", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        base_keyword = kw_row.get('keyword', '')
        country      = kw_row.get('country', '')
        kw_id        = kw_row.get('id', '')
        kw_leads     = 0

        log(f"\n🎯 Base Keyword:[{base_keyword}] | Country: [{country}]", "INFO")

        # 1. Generate Keywords
        ai_keywords = generate_ai_keywords(base_keyword, groq_key)
        search_keywords = [base_keyword] + ai_keywords
        
        log(f"🚀 Starting sequential processing for {len(search_keywords)} keywords...", "INFO")

        # 2. Process ONE keyword at a time
        for sub_kw in search_keywords:
            if not automation_running or total_leads >= min_leads: break
            
            try:
                log(f"\n🔎 Processing Keyword:[{sub_kw}]", "INFO")
                
                store_urls = get_stores_for_keyword(sub_kw, serpapi_key)

                if not store_urls:
                    log(f"   ⚠️ No stores found for '{sub_kw}'. Moving to next...", "WARN")
                    continue

                log(f"   📦 Found {len(store_urls)} stores. Deep Analyzing HTML...", "INFO")

                # Verify Checkout HTML and Save Lead
                for idx, url in enumerate(store_urls):
                    if not automation_running or total_leads >= min_leads: break

                    try:
                        # 🔥 DETAILED LOGGING: Show exactly which URL is being checked 🔥
                        log(f"   🌐 [{idx+1}/{len(store_urls)}] Analyzing: {url}", "INFO")
                        
                        target_info = check_store_target(url, session)

                        if not target_info.get("is_shopify"):
                            log(f"      ↳ 🚫 SKIP: {target_info.get('reason', 'Not Shopify')}", "WARN")
                            continue 

                        if not target_info.get("is_lead"):
                            log(f"      ↳ 🚫 SKIP: {target_info.get('reason')}", "WARN")
                            continue

                        # ✅ LEAD FOUND!
                        log(f"      ↳ 🎯 100% MATCH: {target_info.get('reason')} — collecting info...", "SUCCESS")
                        
                        extracted_email = target_info.get("extracted_email") or ''
                        store_name = target_info.get("store_name") or ''
                        
                        save_resp = call_sheet({
                            'action': 'save_lead', 'store_name': store_name,
                            'url': url, 'email': extracted_email,
                            'phone': '', 'country': country, 'keyword': base_keyword
                        })
                        
                        if save_resp.get('error'):
                            continue
                        if save_resp.get('status') == 'duplicate':
                            log(f"      ↳ ⏭️ Duplicate in Sheet", "INFO"); continue

                        total_leads += 1; kw_leads += 1
                        email_str = f"📧 {extracted_email}" if extracted_email else "⚠ no email"
                        log(f"      ↳ ✅ LEAD #{total_leads} SAVED → {store_name} | {email_str}", "SUCCESS")
                        time.sleep(random.uniform(1.5, 3))

                    except Exception as e:
                        log(f"      ↳ ⚠️ Error processing URL: {e}", "ERROR")
                        continue
                
                log(f"   ✅ Finished processing '{sub_kw}'.", "SUCCESS")
                time.sleep(2) 
                
            except Exception as e:
                log(f"   ⚠️ Error processing keyword '{sub_kw}': {e}", "ERROR")
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ Base '{base_keyword}' done → {kw_leads} leads found", "SUCCESS")

    # ── PHASE 3: Email outreach ───────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 3 — EMAIL OUTREACH STARTING", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    time.sleep(10)
    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads',[]) if not leads_resp.get('error') else[]
    pending    =[l for l in all_leads if l.get('email') and '@' in str(l.get('email','')) and l.get('email_sent') != 'sent']
    
    log(f"📨 {len(pending)} leads with email addresses to contact", "INFO")

    if not pending:
        log("⚠️  No leads with emails found — check your collected leads", "WARN")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped during email phase", "WARN"); break
        email_to = lead['email']
        log(f"✉️  [{i+1}/{len(pending)}] Sending to {email_to}...", "INFO")
        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
        send_resp = call_sheet({
            'action': 'send_email', 'to': email_to,
            'subject': subject, 'body': body, 'lead_id': lead.get('id', '')
        })
        if send_resp.get('status') == 'ok':
            log(f"   ✅ Email sent to {email_to}", "SUCCESS")
        else:
            log(f"   ❌ Send failed: {send_resp.get('message', send_resp)}", "ERROR")
        delay = random.randint(90, 150)
        log(f"   ⏳ Waiting {delay}s before next email...", "INFO")
        time.sleep(delay)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE! Check your Google Sheet for leads.", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    total_leads = emails_sent = kw_total = kw_used = 0
    if script_url:
        try:
            lr = call_sheet({'action': 'get_leads'})
            if not lr.get('error'):
                leads = lr.get('leads',[])
                total_leads = len(leads)
                emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr = call_sheet({'action': 'get_keywords'})
            if not kr.get('error'):
                kws = kr.get('keywords',[])
                kw_total = len(kws)
                kw_used  = sum(1 for k in kws if k.get('status') == 'used')
        except: pass
    return jsonify({'running': automation_running, 'total_leads': total_leads,
                    'emails_sent': emails_sent, 'kw_total': kw_total,
                    'kw_used': kw_used, 'script_connected': bool(script_url)})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try: yield f"data: {log_queue.get(timeout=25)}\n\n"
            except queue.Empty: yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/sheet', methods=['POST'])
def api_sheet():
    if not os.environ.get('APPS_SCRIPT_URL', ''): return jsonify({'error': 'APPS_SCRIPT_URL not set'})
    return jsonify(call_sheet(request.json))

@app.route('/api/automation/start', methods=['POST'])
def api_start():
    global automation_running, automation_thread
    if automation_running: return jsonify({'status': 'already_running'})
    automation_thread = threading.Thread(target=run_automation, daemon=True)
    automation_thread.start()
    return jsonify({'status': 'started'})

@app.route('/api/automation/stop', methods=['POST'])
def api_stop():
    global automation_running
    automation_running = False
    return jsonify({'status': 'stopped'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
