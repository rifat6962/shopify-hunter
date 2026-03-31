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
from groq import Groq
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
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
            r = requests.post(script_url, json=payload, timeout=45,
                              headers={'Content-Type': 'application/json'})
            try:
                return r.json()
            except:
                time.sleep(2); continue
        except requests.exceptions.Timeout:
            log(f"Sheet API timeout (Attempt {attempt+1}/3). Retrying...", "WARN")
            time.sleep(3)
        except Exception as e:
            log(f"Sheet API error (Attempt {attempt+1}/3): {e}", "WARN")
            time.sleep(3)

    return {'error': 'Sheet API failed after 3 retries'}

def log(message, level="INFO"):
    entry = {
        'time':    datetime.now().strftime('%H:%M:%S'),
        'level':   level,
        'message': str(message),
    }
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

MYSHOPIFY_RE = re.compile(r'([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1: ANTI-BLOCK SCRAPER (CyberSec APIs + Smart Generator)
# ─────────────────────────────────────────────────────────────────────────────
def check_alive(url):
    """Fast ping to check if a generated URL is alive"""
    try:
        r = requests.head(url, timeout=3, allow_redirects=True)
        if r.status_code < 400:
            return url
    except:
        pass
    return None

def find_shopify_stores(keyword, serpapi_key):
    all_urls = set()
    kw_clean = keyword.lower().replace(' ', '').replace('-', '')
    
    log(f"🚀 ANTI-BLOCK MODE: Fetching stores for '{keyword}'...", "INFO")

    # ── SOURCE 1: URLScan.io (Recently scanned stores) ──
    log(f"   -> Checking URLScan (Recent Stores)...", "INFO")
    try:
        urlscan_url = f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com AND date:>now-7d&size=1000"
        r = requests.get(urlscan_url, timeout=15)
        if r.status_code == 200:
            for res in r.json().get('results', []):
                page_url = res.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m and kw_clean in m.group(1).lower():
                    all_urls.add(f"https://{m.group(1)}.myshopify.com")
    except Exception: pass

    # ── SOURCE 2: AlienVault OTX (Passive DNS) ──
    log(f"   -> Checking AlienVault OTX...", "INFO")
    try:
        r = requests.get("https://otx.alienvault.com/api/v1/indicators/domain/myshopify.com/passive_dns", timeout=15)
        if r.status_code == 200:
            for entry in r.json().get('passive_dns', []):
                h = entry.get('hostname', '').lower()
                if h.endswith('.myshopify.com') and kw_clean in h:
                    all_urls.add(f"https://{h}")
    except Exception: pass

    # ── SOURCE 3: CertSpotter (New SSLs) ──
    log(f"   -> Checking CertSpotter (New SSLs)...", "INFO")
    try:
        r = requests.get('https://api.certspotter.com/v1/issuances?domain=myshopify.com&include_subdomains=true&expand=dns_names&match_wildcards=false', timeout=15)
        if r.status_code == 200:
            for cert in r.json():
                for name in cert.get('dns_names', []):
                    if name.endswith('.myshopify.com') and kw_clean in name.lower():
                        all_urls.add(f"https://{name}")
    except Exception: pass

    # ── SOURCE 4: SerpAPI (If available, bypasses IP blocks) ──
    if serpapi_key:
        log(f"   -> Checking Google via SerpAPI (Past Week Filter)...", "INFO")
        queries = [
            f'site:myshopify.com "{keyword}" "opening soon"',
            f'site:myshopify.com "{keyword}" "isn\'t accepting payments right now"',
            f'site:myshopify.com "{keyword}"'
        ]
        for q in queries:
            if len(all_urls) > 500: break
            for start in [0, 100]:
                try:
                    params = {'api_key': serpapi_key, 'engine': 'google', 'q': q, 'num': 100, 'start': start, 'tbs': 'qdr:w'}
                    res = requests.get('https://serpapi.com/search', params=params, timeout=15)
                    if res.status_code == 200:
                        for item in res.json().get('organic_results', []):
                            m = MYSHOPIFY_RE.search(item.get('link', ''))
                            if m: all_urls.add(f"https://{m.group(1)}.myshopify.com")
                except Exception: pass
                time.sleep(1)

    # ── SOURCE 5: Smart Generator (Fast Ping) ──
    log(f"   -> Generating & Pinging targeted store links...", "INFO")
    prefixes = ['', 'my', 'the', 'shop', 'buy', 'best', 'new', 'official', 'top', 'pro', 'all']
    suffixes = ['', 'shop', 'store', 'online', 'co', 'boutique', 'hub', 'spot', 'deals', 'mart']
    generated = set()
    for p in prefixes:
        for s in suffixes:
            generated.add(f"https://{p}{kw_clean}{s}.myshopify.com")
            generated.add(f"https://{kw_clean}{p}{s}.myshopify.com")
            if p and s: generated.add(f"https://{p}-{kw_clean}-{s}.myshopify.com")

    # Fast ping using 50 concurrent workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        results = executor.map(check_alive, generated)
        for res in results:
            if res: all_urls.add(res)

    urls_list = list(all_urls)
    random.shuffle(urls_list)
    log(f"📦 Found {len(urls_list)} potential stores to test!", "SUCCESS")
    return urls_list

# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2: AGE VERIFICATION & CHECKOUT TEST
# ─────────────────────────────────────────────────────────────────────────────
def check_store_target(base_url, session, keyword):
    ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    headers = {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        r = session.get(base_url, headers=headers, timeout=10, allow_redirects=True)
        if r.status_code != 200:
            return {"is_shopify": False, "is_lead": False}
            
        html_lower = r.text.lower()
        if 'shopify' not in html_lower and 'cdn.shopify.com' not in html_lower:
            return {"is_shopify": False, "is_lead": False}

        # 🚨 NICHE CHECK
        kw_lower = keyword.lower().strip()
        if kw_lower and kw_lower not in html_lower and kw_lower not in base_url:
            return {"is_shopify": True, "is_lead": False, "reason": f"Keyword '{kw_lower}' not found"}

        # 🚨 PASSWORD CHECK
        if '/password' in r.url or 'password-page' in html_lower or 'opening soon' in html_lower:
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected (Skipping)"}

        # 🔥 AGE VERIFICATION (Is it a NEW store?) 🔥
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=5", headers=headers, timeout=10)
            if prod_req.status_code != 200:
                return {"is_shopify": True, "is_lead": False, "reason": "No products.json"}

            products = prod_req.json().get('products', [])
            if not products:
                return {"is_shopify": True, "is_lead": False, "reason": "0 products, cannot test checkout"}

            # Check the creation date of the oldest product in the list
            oldest_date_str = products[-1].get('created_at') or products[-1].get('published_at')
            if oldest_date_str:
                p_date = datetime.strptime(oldest_date_str[:10], '%Y-%m-%d')
                days_old = (datetime.now() - p_date).days
                
                # 🚨 STRICT 14-DAYS FILTER (If older than 14 days, REJECT)
                if days_old > 14:
                    return {"is_shopify": True, "is_lead": False, "reason": f"Old Store ({days_old} days old)"}
        except Exception:
            pass # If age check fails, proceed to checkout test anyway

        # 🔥 THE CHECKOUT TEST 🔥
        try:
            variant_id = products[0]['variants'][0]['id']
            session.post(f"{base_url}/cart/add.js",
                json={"id": variant_id, "quantity": 1},
                headers={**headers, 'Content-Type': 'application/json'}, timeout=10)

            chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15, allow_redirects=True)
            chk_html = chk_req.text.lower()

            # Explicit no-payment error
            error_footprints = [
                "isn't accepting payments", "not accepting payments", "no payment methods", 
                "payment provider hasn't been set up", "this store is unavailable", 
                "cannot accept payments", "can't accept payments", "checkout is disabled"
            ]
            for phrase in error_footprints:
                if phrase in chk_html:
                    return {"is_shopify": True, "is_lead": True, "reason": f"CONFIRMED NO PAYMENT: '{phrase}'"}

            # Payment keywords = HAS payment (REJECT)
            payment_kws = ['visa', 'mastercard', 'amex', 'american express', 'paypal', 'credit card', 'debit card', 'card number', 'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay', 'google pay']
            found_pay = [kw for kw in payment_kws if kw in chk_html]
            if found_pay:
                return {"is_shopify": True, "is_lead": False, "reason": f"Payment Gateway Found: {found_pay[:2]}"}

            if base_url.replace('https://', '') in chk_req.url and '/checkout' not in chk_req.url:
                return {"is_shopify": True, "is_lead": True, "reason": "Redirected from checkout = no payment"}

            if any(s in chk_html for s in ['contact information', 'shipping address', 'order summary']):
                return {"is_shopify": True, "is_lead": True, "reason": "Checkout OK, but NO Card/Payment options found!"}

            return {"is_shopify": True, "is_lead": False, "reason": "Inconclusive Checkout Page"}

        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": f"Checkout test failed"}
            
    except Exception:
        return {"is_shopify": False, "is_lead": False}

# ── Store info extraction ─────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')

def extract_email(html, soup):
    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            e = href[7:].split('?')[0].strip().lower()
            if '@' in e and not any(d in e for d in SKIP_EMAIL): return e
    for match in EMAIL_RE.findall(html):
        if not any(d in match.lower() for d in SKIP_EMAIL): return match.lower()
    return None

def extract_phone(html):
    m = PHONE_RE.search(html)
    return m.group(0).strip() if m else None

def get_store_info(base_url, session):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0'}
    result = {'store_name': base_url.replace('https://', '').split('.')[0], 'email': None, 'phone': None}
    pages = ['', '/pages/contact', '/contact', '/pages/about-us', '/policies/contact-information', '/policies/refund-policy']
    for path in pages:
        if result['email'] and result['phone']: break
        try:
            r = session.get(base_url + path, headers=headers, timeout=10)
            if r.status_code != 200: continue
            html = r.text
            soup = BeautifulSoup(html, 'html.parser')
            if path == '':
                title = soup.find('title')
                if title: result['store_name'] = title.text.strip()[:80]
            if not result['email']:
                e = extract_email(html, soup)
                if e: result['email'] = e
            if not result['phone']:
                result['phone'] = extract_phone(html)
        except: continue
    return result

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
    serpapi_key = cfg.get('serpapi_key', '').strip()
    groq_key = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing!", "ERROR"); return

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

    session = requests.Session()
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — ANTI-BLOCK SCRAPING & AGE VERIFICATION", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        # 1. Scrape stores (Bypassing Google Blocks)
        store_urls = find_shopify_stores(keyword, serpapi_key)

        if not store_urls:
            log("⚠️  No stores found. Moving to next...", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"🔍 Checking {len(store_urls)} stores: Age Check -> Add to Cart -> Checkout Test...", "INFO")

        # 2. Check Age, Checkout Page and Save Lead
        for idx, url in enumerate(store_urls):
            if not automation_running: break
            if total_leads >= min_leads: break

            try:
                target_info = check_store_target(url, session, keyword)

                if not target_info.get("is_shopify"):
                    continue 

                if not target_info.get("is_lead"):
                    reason = target_info.get('reason', '')
                    if "Keyword" not in reason:
                        log(f"   [{idx+1}/{len(store_urls)}] 🚫 SKIP ({reason}) — {url}", "WARN")
                    continue

                # ✅ LEAD FOUND!
                log(f"   [{idx+1}/{len(store_urls)}] 🎯 100% MATCH: {target_info.get('reason')} — collecting info...", "SUCCESS")
                
                info = get_store_info(url, session)
                
                save_resp = call_sheet({
                    'action': 'save_lead', 'store_name': info['store_name'],
                    'url': url, 'email': info['email'] or '',
                    'phone': info['phone'] or '', 'country': country, 'keyword': keyword
                })
                
                if save_resp.get('error'):
                    continue
                if save_resp.get('status') == 'duplicate':
                    log(f"   ⏭️  Duplicate", "INFO"); continue

                total_leads += 1; kw_leads += 1
                email_str = f"📧 {info['email']}" if info['email'] else "⚠ no email"
                log(f"   ✅ LEAD #{total_leads} SAVED → {info['store_name']} | {email_str}", "SUCCESS")
                time.sleep(random.uniform(1.5, 3))

            except Exception as e:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads found", "SUCCESS")

    # ── PHASE 3: Email outreach ───────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 3 — EMAIL OUTREACH STARTING", "INFO")
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
