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
            return r.json()
        except requests.exceptions.Timeout:
            log(f"Sheet API timeout ({attempt+1}/3)", "WARN")
            time.sleep(3)
        except Exception as e:
            log(f"Sheet API error ({attempt+1}/3): {e}", "WARN")
            time.sleep(3)
    return {'error': 'Sheet API failed after 3 retries'}

# ── Logging ───────────────────────────────────────────────────────────────────
def log(message, level="INFO"):
    entry = {
        'time': datetime.now().strftime('%H:%M:%S'),
        'level': level,
        'message': str(message)
    }
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

# ─────────────────────────────────────────────────────────────────────────────
# AI KEYWORD EXPANSION — 1 keyword → 200 variations using Groq
# ─────────────────────────────────────────────────────────────────────────────
def expand_keywords_with_ai(base_keyword, country, groq_key):
    """
    Use Groq AI to generate 200 keyword variations from one base keyword.
    These are used as Shopify subdomain search queries.
    Returns list of 200 keyword strings.
    """
    log(f"🤖 AI generating 200 keyword variations for '{base_keyword}'...", "INFO")
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""Generate exactly 200 keyword variations for finding Shopify stores in the '{base_keyword}' niche from {country}.

These will be used to search for myshopify.com stores. Include:
- Direct product terms (e.g., running shoes, leather shoes, kids shoes)
- Brand-style names (e.g., shoeco, footwear studio, shoe boutique)  
- Adjective combinations (e.g., luxury footwear, affordable shoes, handmade shoes)
- Target audience (e.g., womens shoes, mens footwear, baby shoes)
- Style variations (e.g., casual shoes, formal shoes, athletic footwear)
- Related products (e.g., sneakers, boots, sandals, heels)

Return ONLY a JSON array of 200 strings, nothing else. No explanation.
Example format: ["keyword1", "keyword2", "keyword3", ...]"""

        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=3000,
            temperature=0.8
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r'```(?:json)?|```', '', raw).strip()
        keywords = json.loads(raw)
        if isinstance(keywords, list):
            # Clean and deduplicate
            keywords = list(set([str(k).strip().lower() for k in keywords if k]))
            log(f"✅ AI generated {len(keywords)} keyword variations", "INFO")
            return keywords[:200]
    except Exception as e:
        log(f"AI keyword expansion error: {e} — using manual fallback", "WARN")

    # Fallback: manual variations if AI fails
    kw = base_keyword.lower()
    prefixes = ['best', 'top', 'cheap', 'luxury', 'premium', 'handmade',
                'custom', 'organic', 'modern', 'vintage', 'unique', 'cute',
                'trendy', 'affordable', 'quality', 'new', 'fresh', 'cool',
                'awesome', 'amazing', 'stylish', 'cute', 'pretty', 'nice']
    suffixes = ['shop', 'store', 'boutique', 'co', 'hub', 'mart', 'world',
                'zone', 'place', 'spot', 'depot', 'pro', 'plus', 'studio',
                'works', 'market', 'house', 'club', 'online', 'deals']
    fallback = [kw] + [f"{p} {kw}" for p in prefixes] + [f"{kw} {s}" for s in suffixes]
    return fallback[:200]


# ─────────────────────────────────────────────────────────────────────────────
# STORE DISCOVERY — keyword-based subdomain generation + URLScan
# ─────────────────────────────────────────────────────────────────────────────
def generate_subdomains_for_keyword(keyword):
    """Generate myshopify.com subdomain candidates for a keyword."""
    kw = keyword.lower().strip().replace(' ', '-')
    kw2 = keyword.lower().strip().replace(' ', '')
    year = str(datetime.now().year)

    suffixes = ['shop', 'store', 'co', 'hub', 'mart', 'boutique', 'studio',
                'place', 'zone', 'depot', 'hq', 'world', 'outlet', 'pro',
                'plus', 'market', 'house', 'club', 'online', 'direct']
    prefixes = ['the', 'my', 'best', 'top', 'get', 'buy', 'shop', 'go',
                'true', 'pure', 'fresh', 'smart', 'cool', 'nice', 'great']
    numbers = ['1', '2', '24', '25', year, year[2:], '101', '365']

    subs = set()
    subs.add(kw)
    subs.add(kw2)

    for s in suffixes:
        subs.add(f"{kw}-{s}")
        subs.add(f"{kw2}{s}")

    for p in prefixes:
        subs.add(f"{p}-{kw}")
        subs.add(f"{p}{kw2}")

    for n in numbers:
        subs.add(f"{kw}{n}")
        subs.add(f"{kw2}{n}")

    result = list(subs)
    random.shuffle(result)
    return result


def check_subdomain_exists(subdomain):
    """Quick check if myshopify subdomain is a real Shopify store."""
    url = f"https://{subdomain}.myshopify.com"
    try:
        r = requests.get(
            f"{url}/products.json?limit=1",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0"},
            timeout=5,
            allow_redirects=True
        )
        if r.status_code == 200 and 'products' in r.json():
            return url
    except:
        pass
    return None


def find_stores_for_keyword(keyword, country):
    """
    Find Shopify stores for a single keyword using:
    1. Subdomain generation + direct check
    2. URLScan.io
    Returns list of store URLs.
    """
    found = set()

    # Method 1: Subdomain brute-force
    subdomains = generate_subdomains_for_keyword(keyword)
    log(f"   Checking {len(subdomains)} subdomain candidates...", "INFO")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(check_subdomain_exists, sub): sub for sub in subdomains}
        for future in as_completed(futures):
            try:
                result = future.result(timeout=7)
                if result:
                    found.add(result)
            except:
                pass

    # Method 2: URLScan
    try:
        kw_clean = keyword.lower().replace(' ', '+')
        r = requests.get(
            f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com+AND+{kw_clean}&size=50&sort=time",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if r.status_code == 200:
            for result in r.json().get('results', []):
                page_url = result.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m:
                    found.add(f"https://{m.group(1)}.myshopify.com")
    except:
        pass

    return list(found)


# ─────────────────────────────────────────────────────────────────────────────
# PAYMENT CHECK — exact same logic, proven to work
# ─────────────────────────────────────────────────────────────────────────────
def check_store_target(base_url, session):
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
        html = r.text.lower()
        if 'shopify' not in html and 'cdn.shopify.com' not in html:
            return {"is_shopify": False, "is_lead": False}
        if '/password' in r.url or 'password-page' in html or 'opening soon' in html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected"}
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
                if 'products' in prod_data and len(prod_data['products']) > 0:
                    variant_id = prod_data['products'][0]['variants'][0]['id']
                    session.post(f"{base_url}/cart/add.js",
                                 json={"id": variant_id, "quantity": 1},
                                 headers=headers, timeout=10)
                    chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15)
                    chk_html = chk_req.text.lower()
                    if 'checkout' not in chk_html and 'contact information' not in chk_html and "isn't accepting payments" not in chk_html:
                        return {"is_shopify": True, "is_lead": False, "reason": "Could not reach checkout"}
                    payment_keywords = ['visa', 'mastercard', 'amex', 'paypal', 'credit card',
                                        'debit card', 'card number', 'stripe', 'klarna', 'afterpay',
                                        'shop pay', 'apple pay', 'google pay']
                    for pk in payment_keywords:
                        if pk in chk_html:
                            return {"is_shopify": True, "is_lead": False, "reason": f"Has payment ('{pk}')"}
                    if "isn't accepting payments" in chk_html or "not accepting payments" in chk_html:
                        return {"is_shopify": True, "is_lead": True, "reason": "Checkout Disabled — No Payment!"}
                    return {"is_shopify": True, "is_lead": True, "reason": "No Payment Options Found!"}
            return {"is_shopify": True, "is_lead": False, "reason": "No products to test"}
        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": "Checkout test failed"}
    except Exception as e:
        return {"is_shopify": False, "is_lead": False}


# ── Store info extraction ─────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS = ['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg',
                      '.svg', 'noreply', 'domain.com', 'no-reply']
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')

def extract_email(html, soup):
    for tag in soup.find_all('a', href=True):
        href = tag.get('href', '')
        if href.startswith('mailto:'):
            email = href[7:].split('?')[0].strip().lower()
            if '@' in email and not any(d in email for d in SKIP_EMAIL_DOMAINS):
                return email
    for match in EMAIL_RE.findall(html):
        m = match.lower()
        if not any(d in m for d in SKIP_EMAIL_DOMAINS):
            return m
    return None

def extract_phone(html):
    m = PHONE_RE.search(html)
    return m.group(0).strip() if m else None

def get_store_info(base_url, session):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0',
        'Accept': 'text/html,*/*;q=0.8',
    }
    result = {
        'store_name': base_url.replace('https://', '').split('.')[0],
        'email': None,
        'phone': None,
    }
    try:
        r = session.get(base_url, headers=headers, timeout=15)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title')
        if title:
            result['store_name'] = title.text.strip()[:80]
        result['email'] = extract_email(html, soup)
        result['phone'] = extract_phone(html)
        if not result['email']:
            for path in ['/pages/contact', '/contact', '/pages/about-us',
                         '/pages/contact-us', '/pages/about']:
                try:
                    pr = session.get(base_url + path, headers=headers, timeout=8)
                    if pr.status_code == 200:
                        ps = BeautifulSoup(pr.text, 'html.parser')
                        email = extract_email(pr.text, ps)
                        if email:
                            result['email'] = email
                            break
                        if not result['phone']:
                            result['phone'] = extract_phone(pr.text)
                except:
                    continue
    except Exception as e:
        pass
    return result


# ── AI Email generation ───────────────────────────────────────────────────────
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""Write a short cold email to a Shopify store owner.
Store: {lead.get('store_name', 'the store')}
Country: {lead.get('country', '')}
Problem: NO payment gateway — cannot accept payments.
Base subject: {tpl_subject}
Base body: {tpl_body}
Rules: 80-100 words, no spam words, mention store name once, 1 soft CTA, HTML <p> tags.
Return ONLY valid JSON: {{"subject": "...", "body": "<p>...</p>"}}"""
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500, temperature=0.7
        )
        raw = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception as e:
        log(f"Groq error: {e}", "WARN")
        return tpl_subject, f'<p>{tpl_body}</p>'


# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        import traceback
        log(f"💥 FATAL ERROR: {e}", "ERROR")
        log(traceback.format_exc()[:600], "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation stopped", "INFO")

def _run():
    global automation_running

    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR"); return

    cfg = cfg_resp.get('config', {})
    groq_key  = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing", "ERROR"); return
    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    # Load base keywords from sheet
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
    log("🚀 PHASE 1 — AI KEYWORD EXPANSION + STORE HUNTING", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for base_kw_row in ready_kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS"); break

        base_keyword = base_kw_row.get('keyword', '')
        country      = base_kw_row.get('country', '')
        kw_id        = base_kw_row.get('id', '')
        kw_leads     = 0

        log(f"\n🔑 Base Keyword: [{base_keyword}] | Country: [{country}]", "INFO")

        # ── STEP 1: AI generates 200 keyword variations ───────────────────────
        expanded_keywords = expand_keywords_with_ai(base_keyword, country, groq_key)
        log(f"📋 Processing {len(expanded_keywords)} AI-generated keyword variations...", "INFO")

        # ── STEP 2: Process each keyword one by one ───────────────────────────
        for kw_idx, keyword in enumerate(expanded_keywords):
            if not automation_running: break
            if total_leads >= min_leads: break

            log(f"\n   [{kw_idx+1}/{len(expanded_keywords)}] Searching: '{keyword}'", "INFO")

            store_urls = find_stores_for_keyword(keyword, country)

            if not store_urls:
                continue

            log(f"   Found {len(store_urls)} stores — checking payment...", "INFO")

            for url in store_urls:
                if not automation_running: break
                if total_leads >= min_leads: break

                try:
                    target_info = check_store_target(url, session)

                    if not target_info.get("is_shopify"):
                        continue

                    if not target_info.get("is_lead"):
                        continue  # Silent skip — has payment

                    # ✅ LEAD FOUND!
                    log(f"   🎯 {target_info.get('reason')} — collecting info...", "SUCCESS")
                    info = get_store_info(url, session)

                    save_resp = call_sheet({
                        'action': 'save_lead',
                        'store_name': info['store_name'],
                        'url': url,
                        'email': info['email'] or '',
                        'phone': info['phone'] or '',
                        'country': country,
                        'keyword': keyword
                    })

                    if save_resp.get('status') == 'duplicate':
                        log(f"   ⏭️  Duplicate", "INFO"); continue

                    total_leads += 1; kw_leads += 1
                    email_str = f"📧 {info['email']}" if info['email'] else "⚠ no email"
                    log(f"   ✅ LEAD #{total_leads} → {info['store_name']} | {email_str}", "SUCCESS")
                    time.sleep(random.uniform(1, 2))

                except Exception as e:
                    continue

        # Mark base keyword as used
        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"✅ '{base_keyword}' done → {kw_leads} leads found", "SUCCESS")

    # ── PHASE 2: Email outreach ───────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    time.sleep(5)
    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', [])
    pending    = [l for l in all_leads
                  if l.get('email') and '@' in str(l.get('email', ''))
                  and l.get('email_sent') != 'sent']

    log(f"📨 {len(pending)} leads to email", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped", "WARN"); break
        email_to = lead['email']
        log(f"✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")
        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)
        send_resp = call_sheet({
            'action': 'send_email', 'to': email_to,
            'subject': subject, 'body': body, 'lead_id': lead.get('id', '')
        })
        if send_resp.get('status') == 'ok':
            log(f"   ✅ Sent → {email_to}", "SUCCESS")
        else:
            log(f"   ❌ Failed: {send_resp.get('message', send_resp)}", "ERROR")
        delay = random.randint(90, 150)
        log(f"   ⏳ {delay}s...", "INFO")
        time.sleep(delay)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE! Check Google Sheet.", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")


# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    total_leads = emails_sent = kw_total = kw_used = 0
    if script_url:
        try:
            lr = call_sheet({'action': 'get_leads'})
            leads = lr.get('leads', [])
            total_leads = len(leads)
            emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr = call_sheet({'action': 'get_keywords'})
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
            try:
                msg = log_queue.get(timeout=25)
                yield f"data: {msg}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/sheet', methods=['POST'])
def api_sheet():
    if not os.environ.get('APPS_SCRIPT_URL', ''):
        return jsonify({'error': 'APPS_SCRIPT_URL not set'})
    return jsonify(call_sheet(request.json))

@app.route('/api/automation/start', methods=['POST'])
def api_start():
    global automation_running, automation_thread
    if automation_running:
        return jsonify({'status': 'already_running'})
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
    data = request.json
    try:
        run_time = datetime.fromisoformat(data.get('time', ''))
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger='date', run_date=run_time,
            id='scheduled_run', replace_existing=True
        )
        log(f"📅 Scheduled for {data['time']}", "INFO")
        return jsonify({'status': 'scheduled', 'time': data['time']})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
