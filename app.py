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
# AI KEYWORD GENERATOR (same as before - working fine)
# ─────────────────────────────────────────────────────────────────────────────
def generate_ai_keywords(base_keyword, groq_key):
    log(f"🧠 Generating 100+ related keywords for '{base_keyword}'...", "INFO")
    try:
        prompt = f"""Generate a list of 100 highly specific e-commerce niche keywords related to '{base_keyword}'. 
For example, if the keyword is 'shoes', return["sneakers", "leather boots", "running shoes", "high heels", "sports shoes"].
Return ONLY a valid JSON array of strings. No markdown formatting, no explanations, no extra text."""

        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={"model": "llama-3.1-8b-instant",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 1500, "temperature": 0.7},
            timeout=30)

        if r.status_code == 200:
            raw = r.json()['choices'][0]['message']['content']
            raw = re.sub(r'```(?:json)?|```', '', raw.strip()).strip()
            kw_list = json.loads(raw)
            if isinstance(kw_list, list) and len(kw_list) > 0:
                log(f"   ✅ Generated {len(kw_list)} keywords!", "SUCCESS")
                return kw_list
    except Exception as e:
        log(f"   ⚠️ AI Error: {e}", "WARN")
    return []

# ─────────────────────────────────────────────────────────────────────────────
# FIX 1: SCRAPER — Hard 8s timeout per request, no hang
# Sources: crt.sh (keyword SSL) + URLScan + Brute-force
# No Yahoo/Bing — those hang from Render
# ─────────────────────────────────────────────────────────────────────────────
def get_stores_for_keyword(keyword):
    urls = set()
    kw_clean = keyword.lower().replace(' ', '').replace('-', '')
    kw_words = keyword.lower().split()

    # ── Source 1: crt.sh SSL log (keyword in subdomain) ──────────────────────
    try:
        r = requests.get(
            f"https://crt.sh/?q=%25{kw_clean}%25.myshopify.com&output=json",
            timeout=8, headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            try:
                for cert in r.json():
                    name = cert.get('name_value', '')
                    for domain in name.split('\n'):
                        domain = domain.strip().replace('*.', '').lower()
                        if domain.endswith('.myshopify.com') and '*' not in domain:
                            urls.add(f"https://{domain}")
            except: pass
    except: pass

    # ── Source 2: URLScan ────────────────────────────────────────────────────
    try:
        r = requests.get(
            f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com+AND+{kw_clean}&size=200&sort=time",
            timeout=8, headers={'User-Agent': 'Mozilla/5.0'}
        )
        if r.status_code == 200:
            for res in r.json().get('results', []):
                m = MYSHOPIFY_RE.search(res.get('page', {}).get('url', ''))
                if m: urls.add(f"https://{m.group(1)}.myshopify.com")
    except: pass

    # ── Source 3: Brute-force name generation ────────────────────────────────
    prefixes = ['', 'my', 'the', 'shop', 'buy', 'best', 'new', 'get', 'pro', 'top']
    suffixes = ['', 'store', 'shop', 'co', 'hub', 'online', 'boutique', 'mart', 'goods']

    bases = [kw_clean] + kw_words
    for base in bases[:2]:  # limit to avoid too many
        for p in prefixes[:6]:
            for s in suffixes[:6]:
                if p or s:
                    urls.add(f"https://{p}{base}{s}.myshopify.com")
        for n in range(1, 10):
            urls.add(f"https://{base}{n}.myshopify.com")

    urls_list = list(urls)
    random.shuffle(urls_list)
    log(f"   📦 {len(urls_list)} candidate stores for '{keyword}'", "INFO")
    return urls_list

# ─────────────────────────────────────────────────────────────────────────────
# EMAIL EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']

def extract_email_from_html(html):
    for tag in BeautifulSoup(html, 'html.parser').find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            e = href[7:].split('?')[0].strip().lower()
            if '@' in e and not any(d in e for d in SKIP_EMAIL):
                return e
    for match in EMAIL_RE.findall(html):
        e = match.lower()
        if not any(d in e for d in SKIP_EMAIL):
            return e
    return None

# ─────────────────────────────────────────────────────────────────────────────
# FIX 2: DEEP HTML ANALYSIS — Hard timeouts, per-store logging
# ─────────────────────────────────────────────────────────────────────────────
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36'
HEADERS = {'User-Agent': UA, 'Accept': 'text/html,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}

NO_PAYMENT = [
    "isn't accepting payments", "is not accepting payments",
    "not accepting payments", "no payment methods",
    "payment provider hasn't been set up", "checkout is disabled",
    "this store is unavailable",
    # German
    "dieser shop kann zurzeit keine zahlungen",
    # French
    "n'accepte pas les paiements",
]
HAS_PAYMENT = [
    'visa', 'mastercard', 'paypal', 'credit card', 'card number',
    'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay', 'google pay',
]

def check_store_target(base_url, session, keyword, store_num, total_stores):
    """
    Full HTML analysis with per-store logging.
    Returns dict with is_shopify, is_lead, reason, extracted_email, store_name
    """
    kw_lower = keyword.lower().strip()

    try:
        # Step 1: Homepage — 7s timeout
        log(f"   [{store_num}/{total_stores}] Checking: {base_url[:55]}", "INFO")
        r = session.get(base_url, headers=HEADERS, timeout=7, allow_redirects=True)

        if r.status_code != 200:
            log(f"   [{store_num}/{total_stores}] ⏭ Dead (HTTP {r.status_code})", "INFO")
            return {"is_shopify": False, "is_lead": False}

        home_html = r.text
        home_lower = home_html.lower()

        if 'cdn.shopify.com' not in home_html and 'shopify' not in home_lower[:3000]:
            log(f"   [{store_num}/{total_stores}] ⏭ Not Shopify", "INFO")
            return {"is_shopify": False, "is_lead": False}

        # Niche check — keyword in URL or homepage
        if kw_lower and kw_lower not in home_lower and kw_lower not in base_url.lower():
            log(f"   [{store_num}/{total_stores}] ⏭ Wrong niche", "INFO")
            return {"is_shopify": True, "is_lead": False, "reason": "Wrong niche"}

        # Password protected
        if '/password' in r.url or 'password-page' in home_lower:
            log(f"   [{store_num}/{total_stores}] ⏭ Password protected", "INFO")
            return {"is_shopify": True, "is_lead": False, "reason": "Password protected"}

        log(f"   [{store_num}/{total_stores}] ✓ Shopify+niche confirmed → testing checkout...", "INFO")

        # Step 2: Get product → add to cart → checkout — all 7s timeout
        chk_html = ""
        try:
            pr = session.get(f"{base_url}/products.json?limit=1", headers=HEADERS, timeout=7)
            if pr.status_code == 200:
                products = pr.json().get('products', [])
                if products:
                    vid = products[0]['variants'][0]['id']
                    session.post(f"{base_url}/cart/add.js",
                                 json={"id": vid, "quantity": 1},
                                 headers={**HEADERS, 'Content-Type': 'application/json'},
                                 timeout=7)
                    cr = session.get(f"{base_url}/checkout", headers=HEADERS, timeout=10, allow_redirects=True)
                    chk_html = cr.text.lower()
                    log(f"   [{store_num}/{total_stores}] ✓ Checkout page loaded ({len(chk_html)} chars)", "INFO")
                else:
                    log(f"   [{store_num}/{total_stores}] ⏭ No products", "INFO")
                    return {"is_shopify": True, "is_lead": False, "reason": "No products"}
            else:
                log(f"   [{store_num}/{total_stores}] ⏭ No products.json", "INFO")
                return {"is_shopify": True, "is_lead": False, "reason": "No products.json"}
        except Exception as e:
            log(f"   [{store_num}/{total_stores}] ⚠ Checkout error: {e}", "WARN")

        # Step 3: Full HTML analysis
        full_html = home_lower + chk_html

        # Check for explicit no-payment message
        for phrase in NO_PAYMENT:
            if phrase in full_html:
                email = extract_email_from_html(home_html)
                # Try contact page too
                if not email:
                    try:
                        cr2 = session.get(base_url + '/pages/contact', headers=HEADERS, timeout=6)
                        if cr2.status_code == 200:
                            email = extract_email_from_html(cr2.text)
                    except: pass
                store_name = base_url.replace('https://', '').split('.')[0].title()
                log(f"   [{store_num}/{total_stores}] 🎯 NO PAYMENT CONFIRMED: '{phrase[:40]}'", "SUCCESS")
                return {"is_shopify": True, "is_lead": True, "reason": f"NO PAYMENT: {phrase}",
                        "extracted_email": email, "store_name": store_name}

        # Check for payment indicators in checkout
        for kw in HAS_PAYMENT:
            if kw in chk_html:
                log(f"   [{store_num}/{total_stores}] 💳 Has payment ({kw})", "INFO")
                return {"is_shopify": True, "is_lead": False, "reason": f"Has payment: {kw}"}

        # Reached checkout but no payment found
        if any(s in chk_html for s in ['contact information', 'shipping address', 'order summary']):
            email = extract_email_from_html(home_html)
            if not email:
                try:
                    cr2 = session.get(base_url + '/pages/contact', headers=HEADERS, timeout=6)
                    if cr2.status_code == 200:
                        email = extract_email_from_html(cr2.text)
                except: pass
            store_name = base_url.replace('https://', '').split('.')[0].title()
            log(f"   [{store_num}/{total_stores}] 🎯 NO PAYMENT: Checkout has no payment options", "SUCCESS")
            return {"is_shopify": True, "is_lead": True, "reason": "Checkout OK, no payment options",
                    "extracted_email": email, "store_name": store_name}

        log(f"   [{store_num}/{total_stores}] ⏭ Inconclusive", "INFO")
        return {"is_shopify": True, "is_lead": False, "reason": "Inconclusive"}

    except requests.exceptions.Timeout:
        log(f"   [{store_num}/{total_stores}] ⏭ Timeout — skipping", "WARN")
        return {"is_shopify": False, "is_lead": False}
    except Exception as e:
        log(f"   [{store_num}/{total_stores}] ⏭ Error: {e}", "WARN")
        return {"is_shopify": False, "is_lead": False}

# ─────────────────────────────────────────────────────────────────────────────
# AI EMAIL GENERATION (same as before)
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AUTOMATION
# ─────────────────────────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        import traceback
        log(f"💥 FATAL: {e}", "ERROR")
        log(traceback.format_exc()[:400], "ERROR")
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

    if not groq_key:
        log("❌ Groq API Key missing!", "ERROR"); return
    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR"); return
    log(f"🗝️  {len(ready_kws)} base keywords ready", "INFO")

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template!", "ERROR"); return
    tpl = templates[0]
    log(f"📧 Template: '{tpl['name']}'", "INFO")

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — AI KEYWORD MULTIPLIER + DEEP HTML ANALYSIS", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        base_keyword = kw_row.get('keyword', '')
        country      = kw_row.get('country', '')
        kw_id        = kw_row.get('id', '')
        kw_leads     = 0

        log(f"\n🎯 Base Keyword: [{base_keyword}] | Country: [{country}]", "INFO")

        # Generate 100+ sub-keywords with AI
        ai_keywords = generate_ai_keywords(base_keyword, groq_key)
        search_keywords = [base_keyword] + ai_keywords
        log(f"🚀 Processing {len(search_keywords)} keywords sequentially...", "INFO")

        for sub_kw in search_keywords:
            if not automation_running or total_leads >= min_leads: break

            try:
                log(f"\n🔎 Keyword: [{sub_kw}]", "INFO")

                # Get candidate stores (crt.sh + URLScan + brute-force)
                store_urls = get_stores_for_keyword(sub_kw)

                if not store_urls:
                    log(f"   ⚠ No candidates for '{sub_kw}'", "WARN")
                    continue

                log(f"   🔍 Analyzing {len(store_urls)} stores one by one...", "INFO")
                kw_checked = kw_found = kw_skipped = 0

                for idx, url in enumerate(store_urls):
                    if not automation_running or total_leads >= min_leads: break

                    try:
                        result = check_store_target(url, session, sub_kw, idx+1, len(store_urls))
                        kw_checked += 1

                        if not result.get("is_shopify"):
                            kw_skipped += 1
                            continue

                        if not result.get("is_lead"):
                            kw_skipped += 1
                            continue

                        # ✅ LEAD!
                        extracted_email = result.get("extracted_email") or ''
                        store_name = result.get("store_name") or url.replace('https://', '').split('.')[0].title()

                        save_resp = call_sheet({
                            'action': 'save_lead',
                            'store_name': store_name,
                            'url': url,
                            'email': extracted_email,
                            'phone': '',
                            'country': country,
                            'keyword': base_keyword
                        })

                        if save_resp.get('status') == 'duplicate':
                            log(f"   ⏭️  Duplicate", "INFO"); continue
                        if save_resp.get('error'):
                            log(f"   Sheet error: {save_resp['error']}", "WARN"); continue

                        total_leads += 1
                        kw_leads += 1
                        kw_found += 1
                        email_str = f"📧 {extracted_email}" if extracted_email else "⚠ no email"
                        log(f"   ✅ LEAD #{total_leads} → {store_name} | {email_str}", "SUCCESS")
                        time.sleep(random.uniform(1, 2))

                    except Exception as e:
                        log(f"   ⚠ Store error: {e}", "WARN")
                        continue

                log(f"   ✅ '{sub_kw}' done → checked:{kw_checked} leads:{kw_found} skipped:{kw_skipped}", "SUCCESS")
                time.sleep(1)

            except Exception as e:
                log(f"   ⚠ Keyword '{sub_kw}' error: {e}", "ERROR")
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{base_keyword}' complete → {kw_leads} total leads", "SUCCESS")

    # Phase 2: Email outreach
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads = leads_resp.get('leads', []) if not leads_resp.get('error') else []
    pending = [l for l in all_leads
               if l.get('email') and '@' in str(l.get('email', ''))
               and l.get('email_sent') != 'sent']
    log(f"📨 {len(pending)} leads to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running: break
        try:
            email_to = lead['email']
            log(f"✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")
            subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
            resp = call_sheet({'action': 'send_email', 'to': email_to,
                               'subject': subject, 'body': body, 'lead_id': lead.get('id', '')})
            if resp.get('status') == 'ok':
                log(f"   ✅ Sent!", "SUCCESS")
            else:
                log(f"   ❌ {resp.get('message', '')}", "ERROR")
            delay = random.randint(90, 150)
            log(f"   ⏳ Next in {delay}s...", "INFO")
            time.sleep(delay)
        except Exception as e:
            log(f"   Email error: {e}", "WARN")
            continue

    log("🎉 ALL DONE! Check Google Sheet.", "SUCCESS")

# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    total_leads = emails_sent = kw_total = kw_used = 0
    if os.environ.get('APPS_SCRIPT_URL'):
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
                kw_used = sum(1 for k in kws if k.get('status') == 'used')
        except: pass
    return jsonify({'running': automation_running, 'total_leads': total_leads,
                    'emails_sent': emails_sent, 'kw_total': kw_total,
                    'kw_used': kw_used, 'script_connected': bool(os.environ.get('APPS_SCRIPT_URL'))})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try: yield f"data: {log_queue.get(timeout=25)}\n\n"
            except queue.Empty: yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/sheet', methods=['POST'])
def api_sheet():
    if not os.environ.get('APPS_SCRIPT_URL'):
        return jsonify({'error': 'APPS_SCRIPT_URL not set'})
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
    log("⛔ Stopped by user", "WARN")
    return jsonify({'status': 'stopped'})

@app.route('/api/schedule', methods=['POST'])
def api_schedule():
    d = request.json
    try:
        run_time = datetime.fromisoformat(d.get('time', ''))
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger='date', run_date=run_time, id='scheduled_run', replace_existing=True)
        log(f"📅 Scheduled for {d.get('time')}", "INFO")
        return jsonify({'status': 'scheduled'})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
