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
            log(f"Sheet API timeout (Attempt {attempt+1}/3). Retrying...", "WARN")
            time.sleep(3)
        except Exception as e:
            log(f"Sheet API error (Attempt {attempt+1}/3): {e}", "WARN")
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

# ── Regex ─────────────────────────────────────────────────────────────────────
MYSHOPIFY_RE = re.compile(
    r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com'
)

# =============================================================================
# 1.  BRAVE SEARCH SCRAPER  (no API key — free, requests + BeautifulSoup only)
# =============================================================================
# Brave Search returns clean, uncluttered HTML — far easier to scrape than
# Google. We rotate User-Agents, scrape multiple pages per query, and combine
# results from 6 query patterns + URLScan.io to harvest ~200 store URLs.

_UA_POOL = [
    # Chrome / Windows
    ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
     'AppleWebKit/537.36 (KHTML, like Gecko) '
     'Chrome/123.0.0.0 Safari/537.36'),
    # Safari / macOS
    ('Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) '
     'AppleWebKit/605.1.15 (KHTML, like Gecko) '
     'Version/17.3 Safari/605.1.15'),
    # Chrome / Linux
    ('Mozilla/5.0 (X11; Linux x86_64) '
     'AppleWebKit/537.36 (KHTML, like Gecko) '
     'Chrome/122.0.0.0 Safari/537.36'),
    # Firefox / Windows
    ('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) '
     'Gecko/20100101 Firefox/124.0'),
]


def _make_headers():
    """Return a randomised header dict that looks like a real browser."""
    return {
        'User-Agent':      random.choice(_UA_POOL),
        'Accept':          'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': random.choice(['en-US,en;q=0.9', 'en-GB,en;q=0.8',
                                          'en-US,en;q=0.7']),
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT':             '1',
        'Connection':      'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }


def _brave_scrape_query(query: str, max_pages: int = 5) -> set:
    """
    Scrape Brave Search for `query`.
    Iterates through `max_pages` result pages (10 results each).
    Returns a raw set of href strings that contain 'myshopify.com'.
    """
    found   = set()
    base    = 'https://search.brave.com/search'
    session = requests.Session()   # keep cookies / connection alive per query

    for page in range(max_pages):
        offset = page * 10
        params = {'q': query, 'offset': offset, 'source': 'web'}
        try:
            resp = session.get(
                base, params=params,
                headers=_make_headers(),
                timeout=18, allow_redirects=True
            )
            if resp.status_code == 429:
                log(f"      ⚠️  Brave rate-limit hit — waiting 30s...", "WARN")
                time.sleep(30)
                continue
            if resp.status_code != 200:
                log(f"      Brave HTTP {resp.status_code} (page {page+1})", "WARN")
                break

            soup = BeautifulSoup(resp.text, 'html.parser')

            # --- Primary: result link anchors ---
            for a in soup.find_all('a', href=True):
                if 'myshopify.com' in a['href']:
                    found.add(a['href'])

            # --- Secondary: data-url attributes (Brave uses these too) ---
            for tag in soup.find_all(True, {'data-url': True}):
                if 'myshopify.com' in tag['data-url']:
                    found.add(tag['data-url'])

            # --- Tertiary: plain text that looks like a store URL ---
            for text in soup.stripped_strings:
                if 'myshopify.com' in text:
                    found.add(text.strip())

            # No more pages if Brave shows "no results"
            if 'no results' in resp.text.lower():
                break

        except requests.exceptions.Timeout:
            log(f"      Brave timeout (page {page+1})", "WARN")
            break
        except Exception as e:
            log(f"      Brave error (page {page+1}): {e}", "WARN")
            break

        # Polite delay between pages — critical to avoid soft-blocks
        time.sleep(random.uniform(2.0, 4.0))

    return found


def _urlscan_scrape(keyword: str) -> set:
    """
    URLScan.io free public API.
    Returns raw URLs of recently-scanned myshopify stores matching the keyword.
    """
    found    = set()
    kw_clean = keyword.lower().replace(' ', '+')
    try:
        url = (
            'https://urlscan.io/api/v1/search/'
            f'?q=domain:myshopify.com+AND+{kw_clean}&size=100&sort=time'
        )
        r = requests.get(url, timeout=12, headers=_make_headers())
        if r.status_code == 200:
            for result in r.json().get('results', []):
                page_url = result.get('page', {}).get('url', '')
                if 'myshopify.com' in page_url:
                    found.add(page_url)
    except Exception as e:
        log(f"      URLScan error: {e}", "WARN")
    return found


def find_shopify_stores(keyword: str, country: str) -> list:
    """
    Master harvester — combines:
      • URLScan.io (instant, no rate-limit)
      • Brave Search × 6 query patterns × 5 pages each

    All raw URLs are normalised to root myshopify store URLs and deduplicated.
    Target: ~200 unique store URLs per keyword.
    """
    raw_urls: set = set()

    # ── A. URLScan (fast warm-up) ─────────────────────────────────────────────
    log(f"   🛰️  URLScan.io crawl...", "INFO")
    raw_urls |= _urlscan_scrape(keyword)
    log(f"      → {len(raw_urls)} URLs so far", "INFO")

    # ── B. Brave Search — 6 targeted query patterns ───────────────────────────
    queries = [
        f'site:myshopify.com "{keyword}"',
        f'site:myshopify.com "{keyword}" {country}',
        f'site:myshopify.com "{keyword}" "welcome to our store"',
        f'site:myshopify.com "{keyword}" "free shipping"',
        f'site:myshopify.com intitle:"{keyword}"',
        f'site:myshopify.com "{keyword}" -password',
    ]

    for i, q in enumerate(queries):
        if not automation_running:
            break
        if len(raw_urls) >= 400:        # plenty of raw material — stop early
            break

        log(f"   🔍 Brave [{i+1}/{len(queries)}]: {q[:65]}...", "INFO")
        batch = _brave_scrape_query(q, max_pages=5)
        raw_urls |= batch
        log(f"      → +{len(batch)} new  |  total raw: {len(raw_urls)}", "INFO")

        # Gap between different queries (longer than between pages)
        time.sleep(random.uniform(3.0, 6.0))

    # ── C. Normalise → root store URLs ───────────────────────────────────────
    clean: set = set()
    for raw in raw_urls:
        m = MYSHOPIFY_RE.search(raw)
        if m:
            clean.add(f"https://{m.group(1)}.myshopify.com")

    result = list(clean)
    log(f"   📦 {len(result)} unique store URLs ready for checkout check", "INFO")
    return result


# =============================================================================
# 2.  STRICT CHECKOUT TEST  (unchanged — works perfectly)
# =============================================================================
def check_store_target(base_url, session):
    ua = (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    )
    headers = {
        'User-Agent':      ua,
        'Accept':          'text/html,application/xhtml+xml,*/*;q=0.8',
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
            return {"is_shopify": True, "is_lead": False,
                    "reason": "Password Protected (Skipping)"}

        try:
            prod_req = session.get(
                f"{base_url}/products.json?limit=1",
                headers=headers, timeout=10
            )
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
                if prod_data.get('products'):
                    variant_id = prod_data['products'][0]['variants'][0]['id']

                    session.post(
                        f"{base_url}/cart/add.js",
                        json={"id": variant_id, "quantity": 1},
                        headers=headers, timeout=10
                    )

                    chk_req  = session.get(
                        f"{base_url}/checkout", headers=headers, timeout=15
                    )
                    chk_html = chk_req.text.lower()

                    if (
                        'checkout'             not in chk_html
                        and 'contact information' not in chk_html
                        and "isn't accepting payments" not in chk_html
                    ):
                        return {"is_shopify": True, "is_lead": False,
                                "reason": "Could not reach valid checkout page"}

                    payment_keywords = [
                        'visa', 'mastercard', 'amex', 'paypal',
                        'credit card', 'debit card', 'card number',
                        'stripe', 'klarna', 'afterpay',
                        'shop pay', 'apple pay', 'google pay',
                    ]
                    for pk in payment_keywords:
                        if pk in chk_html:
                            return {"is_shopify": True, "is_lead": False,
                                    "reason": f"Active Checkout ('{pk}' found)"}

                    if (
                        "isn't accepting payments" in chk_html
                        or "not accepting payments" in chk_html
                    ):
                        return {"is_shopify": True, "is_lead": True,
                                "reason": "Live Store → Checkout Disabled (Explicit)!"}

                    return {"is_shopify": True, "is_lead": True,
                            "reason": "No Payment Options Found on Checkout!"}

            return {"is_shopify": True, "is_lead": False,
                    "reason": "Could not test checkout (no products)"}

        except Exception:
            return {"is_shopify": True, "is_lead": False,
                    "reason": "Checkout test failed"}

    except Exception:
        return {"is_shopify": False, "is_lead": False}


# =============================================================================
# 3.  STORE INFO EXTRACTION
# =============================================================================
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS = [
    'example', 'sentry', 'wixpress', 'shopify',
    '.png', '.jpg', '.svg', 'noreply', 'domain.com',
]
PHONE_RE = re.compile(
    r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})'
)


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
        'Accept':     'text/html,*/*;q=0.8',
    }
    result = {
        'store_name': base_url.replace('https://', '').split('.')[0],
        'email':      None,
        'phone':      None,
    }
    try:
        r    = session.get(base_url, headers=headers, timeout=15)
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title')
        if title:
            result['store_name'] = title.text.strip()[:80]
        result['email'] = extract_email(html, soup)
        result['phone'] = extract_phone(html)

        if not result['email']:
            for path in ['/pages/contact', '/contact', '/pages/about-us']:
                try:
                    pr = session.get(base_url + path, headers=headers, timeout=8)
                    if pr.status_code == 200:
                        ps    = BeautifulSoup(pr.text, 'html.parser')
                        email = extract_email(pr.text, ps)
                        if email:
                            result['email'] = email
                            break
                        if not result['phone']:
                            result['phone'] = extract_phone(pr.text)
                except Exception:
                    continue
    except Exception as e:
        log(f"Info extraction error: {e}", "WARN")
    return result


# =============================================================================
# 4.  AI EMAIL GENERATION
# =============================================================================
def generate_email(tpl_subject, tpl_body, lead, groq_key):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""You are writing a short cold email to a Shopify store owner.

Store: {lead.get('store_name', 'the store')}
URL: {lead.get('url', '')}
Country: {lead.get('country', '')}
Problem: This store has NO payment gateway — customers cannot pay!

Base template:
Subject: {tpl_subject}
Body: {tpl_body}

Rules:
- 80-100 words MAX
- Zero spam trigger words (FREE, GUARANTEED, ACT NOW, etc.)
- Mention store name once, naturally
- Helpful tone, not pushy
- End with ONE soft question
- Use HTML <p> tags

Respond ONLY with valid JSON, nothing else:
{{"subject": "...", "body": "<p>...</p><p>...</p>"}}"""

        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.7,
        )
        raw  = re.sub(r'```(?:json)?|```', '',
                      resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return (
            data.get('subject', tpl_subject),
            data.get('body', f'<p>{tpl_body}</p>')
        )
    except Exception as e:
        log(f"Groq error ({e}) — using template", "WARN")
        return tpl_subject, f'<p>{tpl_body}</p>'


# =============================================================================
# 5.  MAIN AUTOMATION
# =============================================================================
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

    # ── Load config ───────────────────────────────────────────────────────────
    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})
    if cfg_resp.get('error'):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR")
        return

    cfg       = cfg_resp.get('config', {})
    groq_key  = cfg.get('groq_api_key', '').strip()
    min_leads = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing — go to CFG screen → save", "ERROR")
        return

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    # ── Load keywords ──────────────────────────────────────────────────────────
    kw_resp   = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords! Add keywords or click Reset Used", "ERROR")
        return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    # ── Load email template ───────────────────────────────────────────────────
    tpl_resp  = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates', [])
    if not templates:
        log("❌ No email template! Add one in Email screen first", "ERROR")
        return
    tpl = templates[0]
    log(f"📧 Template loaded: '{tpl['name']}'", "INFO")

    # ── Phase 1 — Lead collection ─────────────────────────────────────────────
    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — BRAVE SEARCH SCRAPING + CHECKOUT FILTER", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads | Keywords: {len(ready_kws)}", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running:
            break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS")
            break

        keyword  = kw_row.get('keyword', '')
        country  = kw_row.get('country', '')
        kw_id    = kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        # Step 1 — Harvest store URLs (Brave + URLScan)
        try:
            store_urls = find_shopify_stores(keyword, country)
        except Exception as e:
            log(f"   Scraping failed: {e}", "WARN")
            store_urls = []

        if not store_urls:
            log("   ⚠️  No URLs found — moving to next keyword", "WARN")
            call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': 0})
            continue

        log(f"   🔍 Checking {len(store_urls)} stores for payment gateways...", "INFO")

        # Step 2 — Checkout filter
        for url in store_urls:
            if not automation_running:
                break
            if total_leads >= min_leads:
                break

            try:
                target_info = check_store_target(url, session)

                if not target_info.get("is_shopify"):
                    continue

                if not target_info.get("is_lead"):
                    log(f"      🚫 {target_info.get('reason')} — {url}", "WARN")
                    time.sleep(0.3)
                    continue

                # ✅ Lead confirmed — no payment gateway found
                log(f"      🎯 MATCH: {target_info.get('reason')}", "SUCCESS")

                # Step 3 — Extract contact info
                info = get_store_info(url, session)

                # Step 4 — Save to Google Sheet
                save_resp = call_sheet({
                    'action':     'save_lead',
                    'store_name': info['store_name'],
                    'url':        url,
                    'email':      info['email'] or '',
                    'phone':      info['phone'] or '',
                    'country':    country,
                    'keyword':    keyword,
                })

                if save_resp.get('status') == 'duplicate':
                    log("      ⏭️  Duplicate — skipped", "INFO")
                    continue

                total_leads += 1
                kw_leads    += 1
                email_label  = info['email'] or '⚠ no email found'
                log(
                    f"      ✅ LEAD #{total_leads} → "
                    f"{info['store_name']} | {email_label}",
                    "SUCCESS"
                )
                time.sleep(random.uniform(1.5, 3.0))

            except Exception:
                continue

        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})
        log(f"   ✅ '{keyword}' done → {kw_leads} leads", "SUCCESS")

    # ── Phase 2 — Email outreach ──────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({'action': 'get_leads'})
    all_leads  = leads_resp.get('leads', [])
    pending    = [
        l for l in all_leads
        if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent'
    ]
    log(f"📨 {len(pending)} leads to email", "INFO")

    if not pending:
        log("⚠️  No leads with emails found", "WARN")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped during email phase", "WARN")
            break

        email_to = lead['email']
        log(f"   ✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")

        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)

        send_resp = call_sheet({
            'action':  'send_email',
            'to':      email_to,
            'subject': subject,
            'body':    body,
            'lead_id': lead.get('id', ''),
        })

        if send_resp.get('status') == 'ok':
            log(f"      ✅ Sent to {email_to}", "SUCCESS")
        else:
            log(f"      ❌ Failed: {send_resp.get('message', send_resp)}", "ERROR")

        delay = random.randint(90, 150)
        log(f"      ⏳ Waiting {delay}s...", "INFO")
        time.sleep(delay)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🎉 ALL DONE! Check your Google Sheet.", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")


# =============================================================================
# 6.  FLASK ROUTES
# =============================================================================
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/status')
def api_status():
    script_url  = os.environ.get('APPS_SCRIPT_URL', '')
    total_leads = emails_sent = kw_total = kw_used = 0
    if script_url:
        try:
            lr          = call_sheet({'action': 'get_leads'})
            leads       = lr.get('leads', [])
            total_leads = len(leads)
            emails_sent = sum(1 for l in leads if l.get('email_sent') == 'sent')
            kr          = call_sheet({'action': 'get_keywords'})
            kws         = kr.get('keywords', [])
            kw_total    = len(kws)
            kw_used     = sum(1 for k in kws if k.get('status') == 'used')
        except Exception:
            pass
    return jsonify({
        'running':          automation_running,
        'total_leads':      total_leads,
        'emails_sent':      emails_sent,
        'kw_total':         kw_total,
        'kw_used':          kw_used,
        'script_connected': bool(script_url),
    })


@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try:
                msg = log_queue.get(timeout=25)
                yield f"data: {msg}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(
        gen(), mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
    )


@app.route('/api/sheet', methods=['POST'])
def api_sheet():
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url:
        return jsonify({'error': 'APPS_SCRIPT_URL not set in Render environment'})
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
    data         = request.json
    run_time_str = data.get('time', '')
    try:
        run_time = datetime.fromisoformat(run_time_str)
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger='date', run_date=run_time,
            id='scheduled_run', replace_existing=True,
        )
        log(f"📅 Scheduled for {run_time_str}", "INFO")
        return jsonify({'status': 'scheduled', 'time': run_time_str})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)}), 400


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
