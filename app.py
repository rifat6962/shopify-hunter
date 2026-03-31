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
import urllib.parse

# curl-cffi: Chrome TLS fingerprint impersonate করে Brave block bypass করে
try:
    from curl_cffi import requests as curl_requests
    CURL_AVAILABLE = True
except ImportError:
    CURL_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

log_queue          = queue.Queue()
automation_running = False
automation_thread  = None
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
        'time':    datetime.now().strftime('%H:%M:%S'),
        'level':   level,
        'message': str(message),
    }
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")


# ── Regex ─────────────────────────────────────────────────────────────────────
MYSHOPIFY_RE = re.compile(
    r'([a-zA-Z0-9][a-zA-Z0-9\-]{1,50}[a-zA-Z0-9])\.myshopify\.com'
)

SKIP_STORES = {
    'cdn', 'static', 'assets', 'media', 'images', 'files',
    'checkout', 'api', 'app', 'apps', 'partners', 'help',
    'community', 'polaris', 'shopifycloud', 'myshopify',
}

def extract_shopify_urls(text: str) -> set:
    found = set()
    for m in MYSHOPIFY_RE.finditer(text):
        store = m.group(1).lower()
        if store not in SKIP_STORES and len(store) > 3:
            found.add(f"https://{store}.myshopify.com")
    return found


# ═════════════════════════════════════════════════════════════════════════════
#  BRAVE SEARCH SCRAPER — curl-cffi দিয়ে Chrome TLS impersonate
#
#  কেন কাজ করে:
#   • সাধারণ requests library → Python এর TLS → Brave বুঝে ফেলে → 429 block
#   • curl-cffi → Chrome 120 এর exact TLS fingerprint → Brave মনে করে real browser
#   • কোনো API key লাগে না, সম্পূর্ণ free
#
#  Block এড়ানোর strategy:
#   1. Chrome TLS fingerprint (curl-cffi)
#   2. Realistic headers
#   3. Cookie session maintain
#   4. Random human-like delays
#   5. Offset pagination
# ═════════════════════════════════════════════════════════════════════════════

# Realistic Chrome headers pool
_CHROME_HEADERS = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    },
]

def _make_brave_session():
    """
    curl-cffi দিয়ে Chrome impersonate session তৈরি করে।
    Brave এর TLS fingerprint check bypass করে।
    """
    if CURL_AVAILABLE:
        # Chrome 120 impersonate — Brave এর bot detection বাইপাস করে
        session = curl_requests.Session(impersonate="chrome120")
        return session, True
    else:
        # Fallback: সাধারণ requests (কম কার্যকর)
        session = requests.Session()
        return session, False


def brave_scrape_stores(query: str, max_pages: int = 10) -> set:
    """
    Brave Search থেকে myshopify.com URLs scrape করে।
    curl-cffi দিয়ে Chrome TLS fingerprint use করে তাই block হয় না।
    """
    found   = set()
    headers = random.choice(_CHROME_HEADERS).copy()

    session, using_curl = _make_brave_session()

    if using_curl:
        log(f"      🔑 curl-cffi Chrome impersonation active", "INFO")
    else:
        log(f"      ⚠️  curl-cffi নেই, fallback mode (block হতে পারে)", "WARN")

    encoded_q = urllib.parse.quote_plus(query)

    # প্রথমে Brave homepage visit — natural cookie পাবে
    try:
        if using_curl:
            session.get("https://search.brave.com/", headers=headers, timeout=10)
        else:
            session.get("https://search.brave.com/", headers=headers, timeout=10)
        time.sleep(random.uniform(1.5, 3))
    except Exception:
        pass

    headers["Referer"] = "https://search.brave.com/"

    consecutive_empty = 0

    for page_num in range(max_pages):
        if not automation_running:
            break

        offset = page_num * 10
        url    = f"https://search.brave.com/search?q={encoded_q}&source=web&offset={offset}"

        try:
            if using_curl:
                resp = session.get(url, headers=headers, timeout=20)
            else:
                resp = session.get(url, headers=headers, timeout=20)

            status = resp.status_code

            if status == 200:
                html      = resp.text
                page_urls = extract_shopify_urls(html)

                # BeautifulSoup দিয়ে আরও precisely extract করো
                soup = BeautifulSoup(html, "lxml")

                # Brave result links থেকে
                for a in soup.select("a[href*='myshopify.com']"):
                    page_urls.update(extract_shopify_urls(a.get("href", "")))

                # cite elements (domain display)
                for cite in soup.select("cite, .result-url, span.netloc"):
                    page_urls.update(extract_shopify_urls(cite.get_text()))

                new_count = len(page_urls - found)
                found.update(page_urls)

                log(f"      📄 Page {page_num+1} (offset={offset}): "
                    f"+{new_count} new | total: {len(found)}", "INFO")

                if new_count == 0:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        log(f"      ℹ️  No new results — stopping at page {page_num+1}", "INFO")
                        break
                else:
                    consecutive_empty = 0

            elif status == 429:
                wait = random.randint(45, 90)
                log(f"      ⚠️  Rate limit (429) — waiting {wait}s...", "WARN")
                time.sleep(wait)
                # একই পেজ retry করো
                continue

            elif status in (403, 503):
                log(f"      ❌ Blocked ({status}) — changing session...", "WARN")
                # নতুন session তৈরি করো
                session, using_curl = _make_brave_session()
                headers = random.choice(_CHROME_HEADERS).copy()
                headers["Referer"] = "https://search.brave.com/"
                time.sleep(random.uniform(10, 20))
                continue

            else:
                log(f"      ⚠️  HTTP {status} on page {page_num+1}", "WARN")
                break

        except Exception as e:
            log(f"      ⚠️  Error: {e}", "WARN")
            break

        # Human-like delay between pages
        time.sleep(random.uniform(3, 7))

    return found


# ═════════════════════════════════════════════════════════════════════════════
#  URLSCAN.IO  (Free backup source — নতুন recently registered stores)
# ═════════════════════════════════════════════════════════════════════════════
def urlscan_search(keyword: str) -> set:
    found = set()
    kw    = urllib.parse.quote_plus(keyword.lower())
    endpoints = [
        f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com+AND+page.title:{kw}&size=100&sort=time",
        f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com+AND+{kw}&size=100&sort=time",
    ]
    for url in endpoints:
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                for result in r.json().get("results", []):
                    found.update(extract_shopify_urls(
                        result.get("page", {}).get("url", "")
                    ))
            time.sleep(1)
        except Exception as e:
            log(f"   URLScan error: {e}", "WARN")
    return found


# ═════════════════════════════════════════════════════════════════════════════
#  COMMONCRAWL  (Free — কোনো block নেই, সম্প্রতি crawl হওয়া stores)
# ═════════════════════════════════════════════════════════════════════════════
def commoncrawl_search(keyword: str) -> set:
    found = set()
    kw    = keyword.lower().replace(" ", "")
    for index in ["CC-MAIN-2025-08", "CC-MAIN-2024-51"]:
        if not automation_running:
            break
        try:
            url = (
                f"http://index.commoncrawl.org/{index}-index"
                f"?url=*.myshopify.com*&output=json&limit=200"
                f"&filter=urlkey:*{kw}*"
            )
            r = requests.get(url, timeout=25)
            if r.status_code == 200:
                for line in r.text.strip().split("\n"):
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        found.update(extract_shopify_urls(data.get("url", "")))
                    except Exception:
                        pass
        except Exception as e:
            log(f"   CommonCrawl error: {e}", "WARN")
    return found


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN STORE FINDER
# ═════════════════════════════════════════════════════════════════════════════
def find_shopify_stores(keyword: str, country: str) -> list:
    all_urls: set = set()

    log(f"   🔎 Finding new '{keyword}' stores from multiple sources...", "INFO")

    # ── SOURCE 1: Brave Search (curl-cffi Chrome impersonation) ──────────────
    log(f"   🦁 [1/3] Brave Search (Chrome TLS bypass)...", "INFO")

    # ৫টা query variation — বেশি results পাবে
    brave_queries = [
        f'site:myshopify.com "{keyword}"',
        f'site:myshopify.com "{keyword}" {country}' if country else f'site:myshopify.com "{keyword}" store',
        f'site:myshopify.com intitle:"{keyword}"',
        f'site:myshopify.com "{keyword}" -password',
        f'site:myshopify.com "{keyword}" "welcome to"',
    ]

    for i, q in enumerate(brave_queries, 1):
        if not automation_running:
            break
        log(f"   [{i}/5] Query: {q[:60]}", "INFO")
        urls = brave_scrape_stores(q, max_pages=10)
        new  = len(urls - all_urls)
        all_urls.update(urls)
        log(f"   → +{new} new stores (total: {len(all_urls)})", "INFO")
        # queries এর মধ্যে longer pause
        if i < len(brave_queries):
            time.sleep(random.uniform(6, 12))

    log(f"   🦁 Brave total: {len(all_urls)} URLs", "INFO")

    # ── SOURCE 2: URLScan.io ──────────────────────────────────────────────────
    log(f"   🔍 [2/3] URLScan.io...", "INFO")
    us_urls = urlscan_search(keyword)
    new_us  = len(us_urls - all_urls)
    all_urls.update(us_urls)
    log(f"   🔍 URLScan: +{new_us} new (total: {len(all_urls)})", "INFO")
    time.sleep(2)

    # ── SOURCE 3: CommonCrawl ─────────────────────────────────────────────────
    log(f"   🕸️  [3/3] Common Crawl...", "INFO")
    cc_urls = commoncrawl_search(keyword)
    new_cc  = len(cc_urls - all_urls)
    all_urls.update(cc_urls)
    log(f"   🕸️  CommonCrawl: +{new_cc} new (total: {len(all_urls)})", "INFO")

    result = list(all_urls)
    log(f"📦 Total unique stores to test: {len(result)}", "INFO")
    return result


# ── CHECKOUT TEST  (unchanged — working perfectly) ───────────────────────────
def check_store_target(base_url: str, session) -> dict:
    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    headers = {
        "User-Agent":      ua,
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        r = session.get(base_url, headers=headers, timeout=10, allow_redirects=True)
        if r.status_code != 200:
            return {"is_shopify": False, "is_lead": False}
        html = r.text.lower()
        if "shopify" not in html and "cdn.shopify.com" not in html:
            return {"is_shopify": False, "is_lead": False}
        if "/password" in r.url or "password-page" in html or "opening soon" in html:
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected"}
        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1",
                                   headers=headers, timeout=10)
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
                if prod_data.get("products"):
                    variant_id = prod_data["products"][0]["variants"][0]["id"]
                    session.post(f"{base_url}/cart/add.js",
                                 json={"id": variant_id, "quantity": 1},
                                 headers=headers, timeout=10)
                    chk_req  = session.get(f"{base_url}/checkout",
                                           headers=headers, timeout=15)
                    chk_html = chk_req.text.lower()
                    if ("checkout" not in chk_html
                            and "contact information" not in chk_html
                            and "isn't accepting payments" not in chk_html):
                        return {"is_shopify": True, "is_lead": False,
                                "reason": "Could not reach checkout"}
                    payment_keywords = [
                        "visa", "mastercard", "amex", "paypal", "credit card",
                        "debit card", "card number", "stripe", "klarna",
                        "afterpay", "shop pay", "apple pay", "google pay",
                    ]
                    for pk in payment_keywords:
                        if pk in chk_html:
                            return {"is_shopify": True, "is_lead": False,
                                    "reason": f"Active Checkout ('{pk}' found)"}
                    if ("isn't accepting payments" in chk_html
                            or "not accepting payments" in chk_html):
                        return {"is_shopify": True, "is_lead": True,
                                "reason": "Checkout Disabled (Explicit Error)!"}
                    return {"is_shopify": True, "is_lead": True,
                            "reason": "No Payment Options Found on Checkout!"}
            return {"is_shopify": True, "is_lead": False,
                    "reason": "No products to test"}
        except Exception:
            return {"is_shopify": True, "is_lead": False, "reason": "Checkout test failed"}
    except Exception:
        return {"is_shopify": False, "is_lead": False}


# ── Store info extraction ─────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
SKIP_EMAIL_DOMAINS = ["example", "sentry", "wixpress", "shopify",
                      ".png", ".jpg", ".svg", "noreply", "domain.com"]
PHONE_RE = re.compile(r"(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})")


def extract_email(html: str, soup) -> str | None:
    for tag in soup.find_all("a", href=True):
        href = tag.get("href", "")
        if href.startswith("mailto:"):
            email = href[7:].split("?")[0].strip().lower()
            if "@" in email and not any(d in email for d in SKIP_EMAIL_DOMAINS):
                return email
    for match in EMAIL_RE.findall(html):
        m = match.lower()
        if not any(d in m for d in SKIP_EMAIL_DOMAINS):
            return m
    return None


def extract_phone(html: str) -> str | None:
    m = PHONE_RE.search(html)
    return m.group(0).strip() if m else None


def get_store_info(base_url: str, session) -> dict:
    headers = {"User-Agent": "Mozilla/5.0 Chrome/122.0.0.0", "Accept": "text/html,*/*;q=0.8"}
    result  = {"store_name": base_url.replace("https://","").split(".")[0],
                "email": None, "phone": None}
    try:
        r    = session.get(base_url, headers=headers, timeout=15)
        html = r.text
        soup = BeautifulSoup(html, "lxml")
        title = soup.find("title")
        if title:
            result["store_name"] = title.text.strip()[:80]
        result["email"] = extract_email(html, soup)
        result["phone"] = extract_phone(html)
        if not result["email"]:
            for path in ["/pages/contact", "/contact", "/pages/about-us"]:
                try:
                    pr = session.get(base_url + path, headers=headers, timeout=8)
                    if pr.status_code == 200:
                        ps    = BeautifulSoup(pr.text, "lxml")
                        email = extract_email(pr.text, ps)
                        if email:
                            result["email"] = email
                            break
                        if not result["phone"]:
                            result["phone"] = extract_phone(pr.text)
                except Exception:
                    continue
    except Exception as e:
        log(f"Info extraction error: {e}", "WARN")
    return result


# ── AI Email generation ───────────────────────────────────────────────────────
def generate_email(tpl_subject: str, tpl_body: str, lead: dict, groq_key: str):
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""You are writing a short cold email to a Shopify store owner.
Store: {lead.get('store_name','the store')}
URL: {lead.get('url','')}
Country: {lead.get('country','')}
Problem: This store has NO payment gateway — customers cannot pay!
Base template - Subject: {tpl_subject} / Body: {tpl_body}
Rules: 80-100 words MAX, no spam words, helpful tone, end with one soft question, use HTML <p> tags.
Respond ONLY with valid JSON: {{"subject":"...","body":"<p>...</p>"}}"""
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500, temperature=0.7,
        )
        raw  = re.sub(r"```(?:json)?|```", "", resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return data.get("subject", tpl_subject), data.get("body", f"<p>{tpl_body}</p>")
    except Exception as e:
        log(f"Groq error ({e}) — using template", "WARN")
        return tpl_subject, f"<p>{tpl_body}</p>"


# ── Main automation ───────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        import traceback
        log(f"💥 FATAL ERROR: {e}", "ERROR")
        log(traceback.format_exc()[:800], "ERROR")
    finally:
        automation_running = False
        log("🔴 Automation stopped (All tasks finished)", "INFO")


def _run():
    global automation_running

    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({"action": "get_config"})
    if cfg_resp.get("error"):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR")
        return

    cfg       = cfg_resp.get("config", {})
    groq_key  = cfg.get("groq_api_key", "").strip()
    min_leads = int(cfg.get("min_leads", 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing", "ERROR")
        return

    if CURL_AVAILABLE:
        log("✅ curl-cffi loaded — Chrome TLS impersonation ACTIVE", "INFO")
    else:
        log("⚠️  curl-cffi not found — install করো: pip install curl-cffi", "WARN")

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    kw_resp   = call_sheet({"action": "get_keywords"})
    ready_kws = [k for k in kw_resp.get("keywords", []) if k.get("status") == "ready"]
    if not ready_kws:
        log("❌ No READY keywords!", "ERROR")
        return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    tpl_resp  = call_sheet({"action": "get_templates"})
    templates = tpl_resp.get("templates", [])
    if not templates:
        log("❌ No email template!", "ERROR")
        return
    tpl = templates[0]
    log(f"📧 Template: '{tpl['name']}'", "INFO")

    session     = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — BRAVE SEARCH + URLSCAN + COMMONCRAWL", "SUCCESS")
    log(f"🎯 Target: {min_leads} leads | Keywords: {len(ready_kws)}", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    for kw_row in ready_kws:
        if not automation_running:
            break
        if total_leads >= min_leads:
            log(f"🎯 Target reached! ({total_leads}/{min_leads})", "SUCCESS")
            break

        keyword  = kw_row.get("keyword", "")
        country  = kw_row.get("country", "")
        kw_id    = kw_row.get("id", "")
        kw_leads = 0

        log(f"\n🎯 Keyword: [{keyword}] | Country: [{country}]", "INFO")

        try:
            store_urls = find_shopify_stores(keyword, country)
        except Exception as e:
            log(f"Search failed: {e}", "WARN")
            store_urls = []

        if not store_urls:
            log("⚠️  No URLs found. Moving to next keyword...", "WARN")
            call_sheet({"action": "mark_keyword_used", "id": kw_id, "leads_found": 0})
            continue

        log(f"🔍 Checking {len(store_urls)} stores for payment gateways...", "INFO")

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
                    log(f"   🚫 {target_info.get('reason')} — {url}", "WARN")
                    time.sleep(0.3)
                    continue
                log(f"   🎯 MATCH: {target_info.get('reason')}", "SUCCESS")
                info = get_store_info(url, session)
                save_resp = call_sheet({
                    "action": "save_lead",
                    "store_name": info["store_name"],
                    "url": url,
                    "email": info["email"] or "",
                    "phone": info["phone"] or "",
                    "country": country,
                    "keyword": keyword,
                })
                if save_resp.get("status") == "duplicate":
                    log(f"   ⏭️  Duplicate — skipped", "INFO")
                    continue
                total_leads += 1
                kw_leads    += 1
                log(f"   ✅ LEAD #{total_leads} → {info['store_name']} | {info['email'] or '⚠ no email'}", "SUCCESS")
                time.sleep(random.uniform(1.5, 3))
            except Exception:
                continue

        call_sheet({"action": "mark_keyword_used", "id": kw_id, "leads_found": kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads", "SUCCESS")

    # ── Phase 2: Email outreach ───────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({"action": "get_leads"})
    all_leads  = leads_resp.get("leads", [])
    pending    = [l for l in all_leads
                  if l.get("email") and "@" in l["email"] and l.get("email_sent") != "sent"]

    log(f"📨 {len(pending)} leads to contact", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running:
            break
        email_to = lead["email"]
        log(f"✉️  [{i+1}/{len(pending)}] → {email_to}", "INFO")
        subject, body = generate_email(tpl["subject"], tpl["body"], lead, groq_key)
        send_resp = call_sheet({
            "action": "send_email", "to": email_to,
            "subject": subject, "body": body, "lead_id": lead.get("id", ""),
        })
        if send_resp.get("status") == "ok":
            log(f"   ✅ Sent to {email_to}", "SUCCESS")
        else:
            log(f"   ❌ Failed: {send_resp.get('message', send_resp)}", "ERROR")
        delay = random.randint(90, 150)
        log(f"   ⏳ Waiting {delay}s...", "INFO")
        time.sleep(delay)

    log("🎉 ALL DONE!", "SUCCESS")


# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/status")
def api_status():
    script_url  = os.environ.get("APPS_SCRIPT_URL", "")
    total_leads = emails_sent = kw_total = kw_used = 0
    if script_url:
        try:
            lr          = call_sheet({"action": "get_leads"})
            leads       = lr.get("leads", [])
            total_leads = len(leads)
            emails_sent = sum(1 for l in leads if l.get("email_sent") == "sent")
            kr       = call_sheet({"action": "get_keywords"})
            kws      = kr.get("keywords", [])
            kw_total = len(kws)
            kw_used  = sum(1 for k in kws if k.get("status") == "used")
        except Exception:
            pass
    return jsonify({"running": automation_running, "total_leads": total_leads,
                    "emails_sent": emails_sent, "kw_total": kw_total,
                    "kw_used": kw_used, "script_connected": bool(script_url)})

@app.route("/api/logs/stream")
def stream_logs():
    def gen():
        while True:
            try:
                msg = log_queue.get(timeout=25)
                yield f"data: {msg}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/api/sheet", methods=["POST"])
def api_sheet():
    script_url = os.environ.get("APPS_SCRIPT_URL", "")
    if not script_url:
        return jsonify({"error": "APPS_SCRIPT_URL not set"})
    return jsonify(call_sheet(request.json))

@app.route("/api/automation/start", methods=["POST"])
def api_start():
    global automation_running, automation_thread
    if automation_running:
        return jsonify({"status": "already_running"})
    automation_thread = threading.Thread(target=run_automation, daemon=True)
    automation_thread.start()
    return jsonify({"status": "started"})

@app.route("/api/automation/stop", methods=["POST"])
def api_stop():
    global automation_running
    automation_running = False
    log("⛔ Stopped by user", "WARN")
    return jsonify({"status": "stopped"})

@app.route("/api/schedule", methods=["POST"])
def api_schedule():
    data = request.json
    run_time_str = data.get("time", "")
    try:
        run_time = datetime.fromisoformat(run_time_str)
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger="date", run_date=run_time,
            id="scheduled_run", replace_existing=True,
        )
        log(f"📅 Scheduled for {run_time_str}", "INFO")
        return jsonify({"status": "scheduled", "time": run_time_str})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 400

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
