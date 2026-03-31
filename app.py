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

def extract_shopify_urls(text: str) -> set:
    found = set()
    for m in MYSHOPIFY_RE.finditer(text):
        store = m.group(1).lower()
        # filter out obvious false positives
        skip = ['cdn', 'static', 'assets', 'media', 'images', 'files',
                'checkout', 'api', 'app', 'apps', 'partners', 'help',
                'community', 'polaris', 'shopifycloud']
        if store not in skip:
            found.add(f"https://{store}.myshopify.com")
    return found


# ═════════════════════════════════════════════════════════════════════════════
#  SOURCE 1 — BRAVE SEARCH API  (Free tier: 2,000 queries/month)
#  Signup: https://api.search.brave.com/  → API Key পাবে
#  এটাই সবচেয়ে reliable — কোনো block নেই, real search results
# ═════════════════════════════════════════════════════════════════════════════
def brave_api_search(query: str, brave_api_key: str, count: int = 20) -> set:
    """
    Brave Search Official API দিয়ে search করে।
    Free tier: 2000 queries/month — rate limit নেই।
    """
    found = set()
    if not brave_api_key:
        return found

    headers = {
        "Accept":               "application/json",
        "Accept-Encoding":      "gzip",
        "X-Subscription-Token": brave_api_key,
    }

    # Brave API max 20 per request, offset দিয়ে paginate করি
    for offset in range(0, 100, 20):   # 5 pages × 20 = 100 results per query
        if not automation_running:
            break
        try:
            params = {
                "q":      query,
                "count":  20,
                "offset": offset,
                "search_lang": "en",
                "freshness": "pw",   # past week — নতুন stores!
            }
            r = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers=headers,
                params=params,
                timeout=15,
            )
            if r.status_code == 200:
                data = r.json()
                results = data.get("web", {}).get("results", [])
                if not results:
                    break
                for item in results:
                    for field in [item.get("url", ""), item.get("description", ""),
                                  item.get("extra_snippets", [""])]:
                        text = field if isinstance(field, str) else " ".join(field)
                        found.update(extract_shopify_urls(text))
            elif r.status_code == 429:
                log(f"   Brave API rate limited — waiting 10s", "WARN")
                time.sleep(10)
                break
            else:
                log(f"   Brave API: HTTP {r.status_code}", "WARN")
                break
        except Exception as e:
            log(f"   Brave API error: {e}", "WARN")
            break
        time.sleep(0.5)  # Brave API fair use delay

    return found


# ═════════════════════════════════════════════════════════════════════════════
#  SOURCE 2 — URLSCAN.IO  (Free, real-time newly scanned stores)
# ═════════════════════════════════════════════════════════════════════════════
def urlscan_search(keyword: str) -> set:
    found = set()
    kw = urllib.parse.quote_plus(keyword.lower())
    try:
        # keyword সম্পর্কিত নতুন myshopify stores
        url = (
            f"https://urlscan.io/api/v1/search/"
            f"?q=domain:myshopify.com+AND+page.title:{kw}&size=100&sort=time"
        )
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            for result in data.get("results", []):
                page_url = result.get("page", {}).get("url", "")
                found.update(extract_shopify_urls(page_url))
                # title থেকেও নাও
                title = result.get("page", {}).get("title", "")
                found.update(extract_shopify_urls(title))
        log(f"   URLScan (title): {len(found)} URLs", "INFO")

        # domain search দিয়েও
        url2 = (
            f"https://urlscan.io/api/v1/search/"
            f"?q=domain:myshopify.com+AND+{kw}&size=100&sort=time"
        )
        r2 = requests.get(url2, timeout=15)
        if r2.status_code == 200:
            for result in r2.json().get("results", []):
                page_url = result.get("page", {}).get("url", "")
                found.update(extract_shopify_urls(page_url))

    except Exception as e:
        log(f"   URLScan error: {e}", "WARN")
    return found


# ═════════════════════════════════════════════════════════════════════════════
#  SOURCE 3 — DUCKDUCKGO (duckduckgo-search library, smarter retry)
# ═════════════════════════════════════════════════════════════════════════════
def ddg_search(keyword: str, country: str) -> set:
    found = set()
    queries = [
        f'site:myshopify.com {keyword}',
        f'site:myshopify.com "{keyword}" shop',
        f'myshopify.com {keyword} store',
    ]
    try:
        from duckduckgo_search import DDGS
        for q in queries:
            if not automation_running:
                break
            try:
                with DDGS() as ddgs:
                    results = list(ddgs.text(q, max_results=30))
                for r in results:
                    for field in [r.get("href",""), r.get("body",""), r.get("title","")]:
                        found.update(extract_shopify_urls(field))
                log(f"   DDG '{q}': found {len(found)} total", "INFO")
                time.sleep(random.uniform(3, 6))  # DDG rate limit এড়াতে
            except Exception as e:
                if "Ratelimit" in str(e):
                    log(f"   DDG rate limited — sleeping 60s...", "WARN")
                    time.sleep(60)
                else:
                    log(f"   DDG query error: {e}", "WARN")
                time.sleep(5)
    except Exception as e:
        log(f"   DDG import error: {e}", "WARN")
    return found


# ═════════════════════════════════════════════════════════════════════════════
#  SOURCE 4 — COMMON CRAWL INDEX  (নতুন stores এর goldmine!)
#  কোনো API key লাগে না, rate limit নেই
# ═════════════════════════════════════════════════════════════════════════════
def commoncrawl_search(keyword: str) -> set:
    """
    Common Crawl CDX API দিয়ে সম্প্রতি crawl হওয়া myshopify stores খোঁজে।
    এটা free এবং block করে না।
    """
    found = set()
    kw = keyword.lower().replace(" ", "")
    try:
        # CDX API — myshopify.com এ keyword আছে এমন pages
        url = (
            f"http://index.commoncrawl.org/CC-MAIN-2024-51-index"
            f"?url=*.myshopify.com&output=json&limit=500&fl=url"
            f"&filter=urlkey:*{kw}*"
        )
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            for line in r.text.strip().split("\n"):
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    page_url = data.get("url", "")
                    found.update(extract_shopify_urls(page_url))
                except Exception:
                    pass
        log(f"   CommonCrawl: {len(found)} URLs", "INFO")
    except Exception as e:
        log(f"   CommonCrawl error: {e}", "WARN")
    return found


# ═════════════════════════════════════════════════════════════════════════════
#  SOURCE 5 — SHOPIFY SITEMAP DISCOVERY  (নিশ্চিত নতুন stores!)
#  Shopify এর public store discovery endpoints
# ═════════════════════════════════════════════════════════════════════════════
def shopify_discovery_search(keyword: str) -> set:
    """
    Shopify এর public APIs ব্যবহার করে keyword-related নতুন stores খোঁজে।
    """
    found = set()
    kw_encoded = urllib.parse.quote_plus(keyword)

    # Method 1: Shopify store search (public endpoint)
    try:
        r = requests.get(
            f"https://www.shopify.com/search?q={kw_encoded}",
            headers={"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"},
            timeout=15,
        )
        if r.status_code == 200:
            found.update(extract_shopify_urls(r.text))
            log(f"   Shopify.com search: {len(found)} URLs", "INFO")
    except Exception as e:
        log(f"   Shopify.com error: {e}", "WARN")

    # Method 2: Shopify Exchange marketplace (stores for sale = active stores)
    try:
        r2 = requests.get(
            f"https://exchangemarketplace.com/search?query={kw_encoded}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=15,
        )
        if r2.status_code == 200:
            found.update(extract_shopify_urls(r2.text))
    except Exception:
        pass

    # Method 3: MyIP.ms — shows recently registered myshopify domains
    try:
        r3 = requests.get(
            f"https://myip.ms/browse/sites/1/ipID/23.227.38.0/ipIDsubnet/24/offset/0",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=15,
        )
        if r3.status_code == 200:
            all_found = extract_shopify_urls(r3.text)
            # keyword filter
            kw_lower = keyword.lower()
            for u in all_found:
                store_name = u.replace("https://", "").replace(".myshopify.com", "")
                if any(part in store_name for part in kw_lower.split()):
                    found.add(u)
    except Exception:
        pass

    return found


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN STORE FINDER — সব sources একসাথে
# ═════════════════════════════════════════════════════════════════════════════
def find_shopify_stores(keyword: str, country: str, brave_api_key: str) -> list:
    all_urls: set = set()

    log(f"   🔎 Searching for NEW '{keyword}' stores from 5 sources...", "INFO")

    # ── SOURCE 1: Brave Search Official API ───────────────────────────────────
    if brave_api_key:
        log(f"   🦁 [1/5] Brave Search API...", "INFO")
        queries = [
            f'site:myshopify.com "{keyword}"',
            f'site:myshopify.com "{keyword}" {country}' if country else f'site:myshopify.com "{keyword}"',
            f'site:myshopify.com "{keyword}" -payment -checkout',
            f'myshopify.com "{keyword}" new store',
            f'site:myshopify.com intitle:"{keyword}"',
        ]
        for q in queries:
            if not automation_running:
                break
            urls = brave_api_search(q, brave_api_key)
            new  = len(urls - all_urls)
            all_urls.update(urls)
            log(f"      Query '{q[:50]}...': +{new} new (total: {len(all_urls)})", "INFO")
            time.sleep(1)
        log(f"   🦁 Brave API total: {len(all_urls)} URLs", "INFO")
    else:
        log(f"   ⚠️  [1/5] Brave API key not set — skipping (add BRAVE_API_KEY in env)", "WARN")

    # ── SOURCE 2: URLScan.io ──────────────────────────────────────────────────
    log(f"   🔍 [2/5] URLScan.io...", "INFO")
    us_urls = urlscan_search(keyword)
    new_us  = len(us_urls - all_urls)
    all_urls.update(us_urls)
    log(f"   🔍 URLScan: +{new_us} new (total: {len(all_urls)})", "INFO")
    time.sleep(2)

    # ── SOURCE 3: DuckDuckGo ──────────────────────────────────────────────────
    log(f"   🦆 [3/5] DuckDuckGo...", "INFO")
    ddg_urls = ddg_search(keyword, country)
    new_ddg  = len(ddg_urls - all_urls)
    all_urls.update(ddg_urls)
    log(f"   🦆 DDG: +{new_ddg} new (total: {len(all_urls)})", "INFO")

    # ── SOURCE 4: Common Crawl ────────────────────────────────────────────────
    log(f"   🕸️  [4/5] Common Crawl...", "INFO")
    cc_urls = commoncrawl_search(keyword)
    new_cc  = len(cc_urls - all_urls)
    all_urls.update(cc_urls)
    log(f"   🕸️  CommonCrawl: +{new_cc} new (total: {len(all_urls)})", "INFO")
    time.sleep(1)

    # ── SOURCE 5: Shopify Discovery ───────────────────────────────────────────
    log(f"   🛍️  [5/5] Shopify Discovery...", "INFO")
    sd_urls = shopify_discovery_search(keyword)
    new_sd  = len(sd_urls - all_urls)
    all_urls.update(sd_urls)
    log(f"   🛍️  Shopify Discovery: +{new_sd} new (total: {len(all_urls)})", "INFO")

    result = list(all_urls)
    log(f"📦 Grand total unique stores to test: {len(result)}", "INFO")
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
            return {"is_shopify": True, "is_lead": False,
                    "reason": "Password Protected (Skipping)"}

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
                                "reason": "Could not reach valid checkout page"}

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
                                "reason": "Live Store → Checkout Disabled (Explicit Error)!"}

                    return {"is_shopify": True, "is_lead": True,
                            "reason": "No Payment Options Found on Checkout!"}

            return {"is_shopify": True, "is_lead": False,
                    "reason": "Could not test checkout (No products)"}

        except Exception:
            return {"is_shopify": True, "is_lead": False,
                    "reason": "Checkout test failed"}

    except Exception:
        return {"is_shopify": False, "is_lead": False}


# ── Store info extraction ─────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
SKIP_EMAIL_DOMAINS = [
    "example", "sentry", "wixpress", "shopify",
    ".png", ".jpg", ".svg", "noreply", "domain.com",
]
PHONE_RE = re.compile(
    r"(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})"
)


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
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0",
        "Accept":     "text/html,*/*;q=0.8",
    }
    result = {
        "store_name": base_url.replace("https://", "").split(".")[0],
        "email":      None,
        "phone":      None,
    }
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
                        ps = BeautifulSoup(pr.text, "lxml")
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

Respond ONLY with valid JSON:
{{"subject": "...", "body": "<p>...</p>"}}"""

        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.7,
        )
        raw  = re.sub(r"```(?:json)?|```", "",
                      resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return (data.get("subject", tpl_subject),
                data.get("body", f"<p>{tpl_body}</p>"))
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

    # ── Load config ───────────────────────────────────────────────────────────
    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({"action": "get_config"})

    if cfg_resp.get("error"):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR")
        return

    cfg            = cfg_resp.get("config", {})
    groq_key       = cfg.get("groq_api_key", "").strip()
    min_leads      = int(cfg.get("min_leads", 50) or 50)
    # Brave API key — CFG sheet এ add করো অথবা Render env variable
    brave_api_key  = cfg.get("brave_api_key", "").strip() or \
                     os.environ.get("BRAVE_API_KEY", "").strip()

    if not groq_key:
        log("❌ Groq API Key missing — go to CFG screen → save", "ERROR")
        return

    if brave_api_key:
        log(f"✅ Brave Search API key found — primary source active", "INFO")
    else:
        log(f"⚠️  No Brave API key — add BRAVE_API_KEY to Render env for best results", "WARN")
        log(f"   Signup free at: https://api.search.brave.com/", "WARN")

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    # ── Load keywords ─────────────────────────────────────────────────────────
    kw_resp   = call_sheet({"action": "get_keywords"})
    ready_kws = [k for k in kw_resp.get("keywords", []) if k.get("status") == "ready"]
    if not ready_kws:
        log("❌ No READY keywords! Add keywords or click Reset Used", "ERROR")
        return
    log(f"🗝️  {len(ready_kws)} keywords ready", "INFO")

    # ── Load template ─────────────────────────────────────────────────────────
    tpl_resp  = call_sheet({"action": "get_templates"})
    templates = tpl_resp.get("templates", [])
    if not templates:
        log("❌ No email template! Add one in Email screen first", "ERROR")
        return
    tpl = templates[0]
    log(f"📧 Template loaded: '{tpl['name']}'", "INFO")

    # ── Phase 1: Lead collection ──────────────────────────────────────────────
    session     = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — MULTI-SOURCE SHOPIFY STORE DISCOVERY", "SUCCESS")
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
            store_urls = find_shopify_stores(keyword, country, brave_api_key)
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

                log(f"   🎯 MATCH: {target_info.get('reason')} — {url}", "SUCCESS")

                info = get_store_info(url, session)

                save_resp = call_sheet({
                    "action":     "save_lead",
                    "store_name": info["store_name"],
                    "url":        url,
                    "email":      info["email"] or "",
                    "phone":      info["phone"] or "",
                    "country":    country,
                    "keyword":    keyword,
                })

                if save_resp.get("status") == "duplicate":
                    log(f"   ⏭️  Duplicate — skipped", "INFO")
                    continue

                total_leads += 1
                kw_leads    += 1
                email_display = info["email"] or "⚠ no email found"
                log(f"   ✅ LEAD #{total_leads} → {info['store_name']} | {email_display}", "SUCCESS")
                time.sleep(random.uniform(1.5, 3))

            except Exception:
                continue

        call_sheet({"action": "mark_keyword_used", "id": kw_id, "leads_found": kw_leads})
        log(f"✅ '{keyword}' done → {kw_leads} leads found", "SUCCESS")

    # ── Phase 2: Email outreach ───────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log(f"📊 Scraping done! Total leads: {total_leads}", "SUCCESS")
    log("📧 PHASE 2 — EMAIL OUTREACH STARTING", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({"action": "get_leads"})
    all_leads  = leads_resp.get("leads", [])
    pending    = [
        l for l in all_leads
        if l.get("email") and "@" in l["email"] and l.get("email_sent") != "sent"
    ]

    log(f"📨 {len(pending)} leads with email to contact", "INFO")
    if not pending:
        log("⚠️  No leads with emails found — check your collected leads", "WARN")

    for i, lead in enumerate(pending):
        if not automation_running:
            log("⛔ Stopped during email phase", "WARN")
            break

        email_to = lead["email"]
        log(f"✉️  [{i+1}/{len(pending)}] Sending to {email_to}...", "INFO")

        subject, body = generate_email(tpl["subject"], tpl["body"], lead, groq_key)

        send_resp = call_sheet({
            "action":  "send_email",
            "to":      email_to,
            "subject": subject,
            "body":    body,
            "lead_id": lead.get("id", ""),
        })

        if send_resp.get("status") == "ok":
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
    return jsonify({
        "running":          automation_running,
        "total_leads":      total_leads,
        "emails_sent":      emails_sent,
        "kw_total":         kw_total,
        "kw_used":          kw_used,
        "script_connected": bool(script_url),
    })


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
        return jsonify({"error": "APPS_SCRIPT_URL not set in Render environment"})
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
    data         = request.json
    run_time_str = data.get("time", "")
    try:
        run_time = datetime.fromisoformat(run_time_str)
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation, daemon=True).start(),
            trigger="date",
            run_date=run_time,
            id="scheduled_run",
            replace_existing=True,
        )
        log(f"📅 Scheduled for {run_time_str}", "INFO")
        return jsonify({"status": "scheduled", "time": run_time_str})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 400


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
