from flask import Flask, render_template, request, jsonify, Response
import threading
import queue
import time
import json
import re
import random
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import logging
import os

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

# ─────────────────────────────────────────────────────────────────────────────
# THE PURE PYTHON "SHOPIFY SCRAPER WORKER" (No APIs Needed)
# ─────────────────────────────────────────────────────────────────────────────
class ShopifyScraperWorker:
    def __init__(self, keyword, country, min_leads):
        self.keyword = keyword.lower().strip()
        self.kw_clean = self.keyword.replace(' ', '').replace('-', '')
        self.country = country
        self.min_leads = min_leads
        self.found_leads = 0
        self.urls_to_test = set()
        self.session = requests.Session()
        
        # Headers to bypass basic bot protection
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8'
        }

    def generate_urls(self):
        """
        পাইথনের মাধ্যমে নিজে নিজে হাজার হাজার লজিক্যাল স্টোর URL জেনারেট করবে।
        """
        log(f"🚀 WORKER MODE: Generating massive URL list for '{self.keyword}'...", "INFO")
        
        prefixes = ['', 'my', 'the', 'shop', 'buy', 'best', 'new', 'official', 'top', 'pro', 'all', 'get', 'go', 'daily', 'super', 'premium', 'true', 'pure']
        suffixes = ['', 'shop', 'store', 'online', 'co', 'boutique', 'hub', 'spot', 'deals', 'mart', 'goods', 'market', 'cart', 'world', 'house']
        
        for p in prefixes:
            for s in suffixes:
                self.urls_to_test.add(f"https://{p}{self.kw_clean}{s}.myshopify.com")
                self.urls_to_test.add(f"https://{self.kw_clean}{p}{s}.myshopify.com")
                if p and s:
                    self.urls_to_test.add(f"https://{p}-{self.kw_clean}-{s}.myshopify.com")
                    
        urls_list = list(self.urls_to_test)
        random.shuffle(urls_list)
        log(f"📦 Worker generated {len(urls_list)} potential store URLs to scan!", "SUCCESS")
        return urls_list

    def check_single_store(self, base_url):
        """
        একটি সিঙ্গেল স্টোর চেক করার লজিক। এটি Worker Thread এর মাধ্যমে চলবে।
        """
        if not automation_running or self.found_leads >= self.min_leads:
            return None

        try:
            # 1. ALIVE CHECK (Fast Timeout)
            r = self.session.get(base_url, headers=self.headers, timeout=7, allow_redirects=True)
            if r.status_code != 200: return None
            
            html_lower = r.text.lower()
            if 'shopify' not in html_lower and 'cdn.shopify.com' not in html_lower:
                return None # Not a Shopify store

            # 2. NICHE CHECK
            if self.keyword not in html_lower and self.kw_clean not in base_url:
                return None # Keyword not found

            # 3. PASSWORD CHECK
            if '/password' in r.url or 'password-page' in html_lower or 'opening soon' in html_lower:
                return None # Password protected

            # 4. CHECKOUT TEST
            prod_req = self.session.get(f"{base_url}/products.json?limit=1", headers=self.headers, timeout=7)
            if prod_req.status_code != 200: return None
            
            products = prod_req.json().get('products', [])
            if not products: return None

            variant_id = products[0]['variants'][0]['id']
            self.session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers=self.headers, timeout=7)

            chk_req = self.session.get(f"{base_url}/checkout", headers=self.headers, timeout=10, allow_redirects=True)
            chk_html = chk_req.text.lower()

            # Explicit no-payment error
            error_footprints = [
                "isn't accepting payments", "not accepting payments", "no payment methods", 
                "payment provider hasn't been set up", "this store is unavailable", 
                "cannot accept payments", "can't accept payments", "checkout is disabled"
            ]
            
            is_lead = False
            reason = ""
            
            for phrase in error_footprints:
                if phrase in chk_html:
                    is_lead = True
                    reason = f"CONFIRMED NO PAYMENT: '{phrase}'"
                    break

            if not is_lead:
                payment_kws = ['visa', 'mastercard', 'amex', 'paypal', 'credit card', 'debit card', 'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay', 'google pay']
                if any(kw in chk_html for kw in payment_kws):
                    return None # Has payment
                
                if base_url.replace('https://', '') in chk_req.url and '/checkout' not in chk_req.url:
                    is_lead = True
                    reason = "Redirected from checkout = no payment"
                elif any(s in chk_html for s in ['contact information', 'shipping address', 'order summary']):
                    is_lead = True
                    reason = "Checkout OK, no payment options in HTML"

            if is_lead:
                # Extract Info
                store_name = base_url.replace('https://', '').split('.')[0].title()
                email = self.extract_email_deep(base_url)
                
                return {
                    'store_name': store_name,
                    'url': base_url,
                    'email': email or '',
                    'phone': '',
                    'reason': reason
                }
                
        except Exception:
            return None
            
    def extract_email_deep(self, base_url):
        email_re = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
        skip_email = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']
        
        pages = ['', '/pages/contact', '/contact', '/policies/refund-policy']
        for path in pages:
            try:
                r = self.session.get(base_url + path, headers=self.headers, timeout=7)
                if r.status_code == 200:
                    for match in email_re.findall(r.text):
                        e = match.lower()
                        if not any(d in e for d in skip_email):
                            return e
            except: continue
        return None

    def run_workers(self):
        """
        Multi-threading ব্যবহার করে একসাথে ২০টি URL চেক করবে।
        """
        urls = self.generate_urls()
        log(f"⚡ Starting 20 Concurrent Workers to scan stores...", "INFO")
        
        # Using ThreadPoolExecutor as the "Worker"
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            # Submit all URLs to the worker pool
            future_to_url = {executor.submit(self.check_single_store, url): url for url in urls}
            
            for idx, future in enumerate(concurrent.futures.as_completed(future_to_url)):
                if not automation_running or self.found_leads >= self.min_leads:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                    
                try:
                    result = future.result()
                    if result:
                        # ✅ LEAD FOUND BY WORKER!
                        log(f"   🎯 100% MATCH: {result['reason']} — {result['url']}", "SUCCESS")
                        
                        save_resp = call_sheet({
                            'action': 'save_lead', 'store_name': result['store_name'],
                            'url': result['url'], 'email': result['email'],
                            'phone': '', 'country': self.country, 'keyword': self.keyword
                        })
                        
                        if save_resp.get('status') != 'duplicate' and not save_resp.get('error'):
                            self.found_leads += 1
                            email_str = f"📧 {result['email']}" if result['email'] else "⚠ no email"
                            log(f"   ✅ LEAD #{self.found_leads} SAVED → {result['store_name']} | {email_str}", "SUCCESS")
                            
                except Exception as e:
                    pass
                    
                if idx > 0 and idx % 100 == 0:
                    log(f"   ⚙️ Workers scanned {idx} URLs so far...", "INFO")

        return self.found_leads

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
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 500, "temperature": 0.7},
            timeout=20)
        if r.status_code == 200:
            raw = r.json()['choices'][0]['message']['content']
            raw = re.sub(r'```(?:json)?|```', '', raw.strip()).strip()
            raw = raw.replace('\n', ' ').replace('\r', '')
            data = json.loads(raw, strict=False)
            return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception as e:
        log(f"Groq fallback: {e}", "WARN")
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
    groq_key = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR"); return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template!", "ERROR"); return
    tpl = templates[0]
    log(f"📧 Template loaded: '{tpl['name']}'", "INFO")

    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — PURE PYTHON WORKER SCRAPING", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads: break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        # 🔥 Initialize and Run the Python Worker 🔥
        worker = ShopifyScraperWorker(keyword, country, min_leads - total_leads)
        leads_found = worker.run_workers()
        
        total_leads += leads_found

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': leads_found})
        log(f"✅ '{keyword}' done → {leads_found} leads found", "SUCCESS")

    # ── PHASE 2: Email outreach ───────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH STARTING", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    time.sleep(10)
    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', []) if not leads_resp.get('error') else []
    pending    = [l for l in all_leads if l.get('email') and '@' in str(l.get('email','')) and l.get('email_sent') != 'sent']
    
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
                leads = lr.get('leads', [])
                total_leads = len(leads)
                emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr = call_sheet({'action': 'get_keywords'})
            if not kr.get('error'):
                kws = kr.get('keywords', [])
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
