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
    r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com'
)
MYSHOPIFY_PLAIN_RE = re.compile(
    r'([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com'
)

# ─────────────────────────────────────────────────────────────────────────────
#  BRAVE SEARCH SCRAPER  (requests + BeautifulSoup, no browser needed)
#
#  কীভাবে কাজ করে:
#   1.  search.brave.com এ query পাঠায় — human-like headers সহ
#   2.  HTML parse করে সব *.myshopify.com domain তোলে
#   3.  ?offset= parameter দিয়ে পরের পেজে যায় (Brave pagination)
#   4.  max_pages পেজ পর্যন্ত চলে → unique URLs এর set ফেরত দেয়
# ─────────────────────────────────────────────────────────────────────────────

# বিভিন্ন browser এর realistic headers pool
_HEADER_PROFILES = [
    {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest":  "document",
        "Sec-Fetch-Mode":  "navigate",
        "Sec-Fetch-Site":  "none",
        "Sec-CH-UA":       '"Chromium";v="123", "Google Chrome";v="123"',
        "Sec-CH-UA-Mobile":"?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "Cache-Control":   "max-age=0",
        "Connection":      "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest":  "document",
        "Sec-Fetch-Mode":  "navigate",
        "Sec-Fetch-Site":  "none",
        "Sec-CH-UA":       '"Chromium";v="122", "Google Chrome";v="122"',
        "Sec-CH-UA-Mobile":"?0",
        "Sec-CH-UA-Platform": '"macOS"',
        "Connection":      "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    },
]


def _extract_shopify_urls_from_html(html: str) -> set:
    """HTML থেকে সব myshopify.com URL বের করে।"""
    found = set()
    soup  = BeautifulSoup(html, "lxml")

    # 1. সব <a href> tag
    for a in soup.find_all("a", href=True):
        m = MYSHOPIFY_PLAIN_RE.search(a["href"])
        if m:
            found.add(f"https://{m.group(1)}.myshopify.com")

    # 2. Brave এর result URL cite/span elements
    for tag in soup.select("cite, .result-url, span[class*='url'], .snippet-url, .fz-13"):
        m = MYSHOPIFY_PLAIN_RE.search(tag.get_text())
        if m:
            found.add(f"https://{m.group(1)}.myshopify.com")

    # 3. পুরো raw HTML এ regex scan (সবচেয়ে aggressive)
    for m in MYSHOPIFY_PLAIN_RE.finditer(html):
        found.add(f"https://{m.group(1)}.myshopify.com")

    return found


def brave_scrape(query: str, max_pages: int = 10) -> set:
    """
    Brave Search থেকে requests দিয়ে myshopify.com URLs স্ক্র্যাপ করে।
    Brave pagination: ?offset=0, ?offset=10, ?offset=20 ...
    """
    found   = set()
    session = requests.Session()
    headers = random.choice(_HEADER_PROFILES).copy()

    # প্রথমে Brave homepage visit করি — natural cookie set করতে
    try:
        session.get("https://search.brave.com/", headers=headers, timeout=10)
        time.sleep(random.uniform(1, 2))
    except Exception:
        pass

    # Referer set করি
    headers["Referer"] = "https://search.brave.com/"

    encoded_q = urllib.parse.quote_plus(query)

    for page_num in range(max_pages):
        if not automation_running:
            break

        offset = page_num * 10
        url    = f"https://search.brave.com/search?q={encoded_q}&source=web&offset={offset}"

        try:
            resp = session.get(url, headers=headers, timeout=20)

            if resp.status_code == 200:
                page_urls = _extract_shopify_urls_from_html(resp.text)
                new_count = len(page_urls - found)
                found.update(page_urls)
                log(f"      📄 Page {page_num+1} (offset={offset}): "
                    f"+{new_count} new URLs (total: {len(found)})", "INFO")

                # যদি result না থাকে তাহলে আর পরের পেজ নেই
                if new_count == 0 and page_num > 0:
                    log(f"      ℹ️  No new results after page {page_num+1}, stopping.", "INFO")
                    break

            elif resp.status_code == 429:
                log(f"      ⚠️  Rate limited (429) — waiting 30s...", "WARN")
                time.sleep(30)
                continue

            elif resp.status_code in (403, 503):
                log(f"      ⚠️  Blocked ({resp.status_code}) on page {page_num+1}", "WARN")
                break

            else:
                log(f"      ⚠️  HTTP {resp.status_code} on page {page_num+1}", "WARN")

        except requests.exceptions.Timeout:
            log(f"      ⚠️  Timeout on page {page_num+1}", "WARN")
        except Exception as e:
            log(f"      ⚠️  Error on page {page_num+1}: {e}", "WARN")

        # Human-like delay between pages (3-7 seconds)
        time.sleep(random.uniform(3, 7))

    return found


# ─────────────────────────────────────────────────────────────────────────────
#  DUCKDUCKGO SCRAPER  (fallback — uses duckduckgo-search library)
# ─────────────────────────────────────────────────────────────────────────────
def ddg_scrape(query: str, max_results: int = 50) -> set:
    """DuckDuckGo থেকে myshopify.com URLs বের করে (Brave block হলে fallback)."""
    found = set()
    try:
        from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = ddgs.text(
                f'site:myshopify.com {query}',
                max_results=max_results
            )
            for r in results:
                for field in [r.get("href", ""), r.get("body", ""), r.get("title", "")]:
                    m = MYSHOPIFY_PLAIN_RE.search(field)
                    if m:
                        found.add(f"https://{m.group(1)}.myshopify.com")
    except Exception as e:
        log(f"   DDG error: {e}", "WARN")
    return found


# ─────────────────────────────────────────────────────────────────────────────
#  URLSCAN.IO  (free API — no browser, no block risk)
# ─────────────────────────────────────────────────────────────────────────────
def urlscan_scrape(keyword: str) -> set:
    found = set()
    kw    = urllib.parse.quote_plus(keyword.lower())
    try:
        url = (
            f"https://urlscan.io/api/v1/search/"
            f"?q=domain:myshopify.com+AND+{kw}&size=100&sort=time"
        )
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            for res in r.json().get("results", []):
                page_url = res.get("page", {}).get("url", "")
                m = MYSHOPIFY_RE.search(page_url)
                if m:
                    found.add(f"https://{m.group(1)}.myshopify.com")
    except Exception as e:
        log(f"   URLScan error: {e}", "WARN")
    return found


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN STORE FINDER
#
#  একটা keyword + country নিয়ে ৩টা source থেকে stores খোঁজে:
#   1. Brave Search  — ৫টা query variation × ১০ পেজ  (primary)
#   2. DuckDuckGo    — fallback যদি Brave block করে
#   3. URLScan.io    — secondary free API
#  → মোট ~200 unique myshopify URLs
# ─────────────────────────────────────────────────────────────────────────────
def find_shopify_stores(keyword: str, country: str) -> list:
    all_urls: set = set()

    # ৫টা query variation
    variations = list(dict.fromkeys([
        f'"{keyword}"',
        f'"{keyword}" {country}' if country else f'"{keyword}"',
        f'"{keyword}" shop',
        f'"{keyword}" store',
        f'"{keyword}" "free shipping"',
    ]))

    # ── SOURCE 1: Brave Search ────────────────────────────────────────────────
    log(f"   🦁 Brave Search: {len(variations)} queries × 10 pages...", "INFO")
    brave_total = 0
    for i, variant in enumerate(variations, 1):
        if not automation_running:
            break
        log(f"   [{i}/{len(variations)}] Query: site:myshopify.com {variant}", "INFO")
        urls = brave_scrape(f"site:myshopify.com {variant}", max_pages=10)
        new  = len(urls - all_urls)
        all_urls.update(urls)
        brave_total += new
        log(f"   → +{new} new (running total: {len(all_urls)})", "INFO")
        # queries এর মধ্যে longer pause — Brave rate limit এড়াতে
        time.sleep(random.uniform(5, 10))

    log(f"   🦁 Brave total: {brave_total} URLs", "INFO")

    # ── SOURCE 2: DuckDuckGo fallback ────────────────────────────────────────
    log(f"   🦆 DuckDuckGo fallback scan...", "INFO")
    ddg_urls = ddg_scrape(keyword, max_results=100)
    new_ddg  = len(ddg_urls - all_urls)
    all_urls.update(ddg_urls)
    log(f"   → DDG: +{new_ddg} new URLs", "INFO")
    time.sleep(random.uniform(2, 4))

    # ── SOURCE 3: URLScan.io ──────────────────────────────────────────────────
    log(f"   🔍 URLScan.io scan...", "INFO")
    us_urls  = urlscan_scrape(keyword)
    new_us   = len(us_urls - all_urls)
    all_urls.update(us_urls)
    log(f"   → URLScan: +{new_us} new URLs", "INFO")

    result = list(all_urls)
    log(f"📦 Total unique stores to check: {len(result)}", "INFO")
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

Respond ONLY with valid JSON, nothing else:
{{"subject": "...", "body": "<p>...</p><p>...</p>"}}"""

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
        log("👉 Make sure APPS_SCRIPT_URL is set in Render → Environment", "ERROR")
        return

    cfg       = cfg_resp.get("config", {})
    groq_key  = cfg.get("groq_api_key", "").strip()
    min_leads = int(cfg.get("min_leads", 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing — go to CFG screen → save", "ERROR")
        return

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
    log("🚀 PHASE 1 — BRAVE + DDG + URLSCAN SCRAPING", "SUCCESS")
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

        # STEP 1: Collect store URLs from all 3 sources
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

        # STEP 2: Checkout filter
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
                    log(f"   🚫 REJECTED: {target_info.get('reason')} — {url}", "WARN")
                    time.sleep(0.3)
                    continue

                # ✅ Lead confirmed!
                log(f"   🎯 MATCH: {target_info.get('reason')} — collecting info...", "SUCCESS")

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
