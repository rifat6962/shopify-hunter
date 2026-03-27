from flask import Flask, render_template, request, jsonify, Response
import threading, queue, time, json, re, random, requests, os, logging
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

log_queue      = queue.Queue()
automation_running = False
automation_thread  = None
scheduler = BackgroundScheduler(daemon=True)
scheduler.start()

last_status_fetch = 0
cached_status = {'total_leads':0,'emails_sent':0,'kw_total':0,'kw_used':0}
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36"

# ── Sheet bridge ──────────────────────────────────────────────────────────────
def call_sheet(payload, retries=3):
    url = os.environ.get('APPS_SCRIPT_URL','')
    if not url: return {'error':'APPS_SCRIPT_URL not set'}
    for i in range(retries):
        try:
            r = requests.post(url, json=payload, timeout=30,
                              headers={'Content-Type':'application/json'})
            return r.json()
        except Exception as e:
            log(f"Sheet ({i+1}/{retries}): {e}","WARN")
            time.sleep(2)
    return {'error':'Sheet API failed'}

def log(msg, level="INFO"):
    entry = {'time':datetime.now().strftime('%H:%M:%S'),'level':level,'message':str(msg)}
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {msg}")

MYSHOPIFY_RE = re.compile(r'([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

# ─────────────────────────────────────────────────────────────────────────────
# STORE DISCOVERY — keyword-based subdomain generation + live check
#
# HOW IT WORKS:
# Shopify store subdomains follow patterns like:
#   shoes-by-sarah, bestshoes2024, sarahshoesco, the-shoe-store, etc.
# We generate hundreds of likely subdomains for the keyword,
# then check each one to see if it's a real live Shopify store.
# New stores (< 7 days) won't be indexed by Google yet — this bypasses that.
# ─────────────────────────────────────────────────────────────────────────────

def generate_subdomains(keyword, count=500):
    """
    Generate likely myshopify.com subdomains for a keyword.
    Shopify subdomains are created by store owners so they follow
    human patterns: keyword + name/number/adjective combos.
    """
    kw = keyword.lower().replace(' ','-')
    kw2 = keyword.lower().replace(' ','')
    kw3 = keyword.lower().replace(' ','_')

    year = datetime.now().year
    years = [str(year), str(year-1), str(year)[2:]]

    prefixes = [
        'the','my','best','top','get','buy','shop','store','online',
        'official','your','our','new','just','try','go','pro','real',
        'true','pure','fresh','fast','smart','easy','quick','cool',
        'nice','great','good','super','ultra','mega','mini','daily',
        'local','global','prime','royal','elite','style','trend',
    ]
    suffixes = [
        'shop','store','co','hub','mart','world','zone','place',
        'spot','depot','hq','central','pro','plus','studio','works',
        'market','boutique','house','club','space','lab','base',
        'now','today','direct','official','online','deals','outlet',
    ]
    adjectives = [
        'modern','classic','vintage','premium','luxury','budget',
        'affordable','trendy','stylish','cute','cool','awesome',
        'amazing','perfect','unique','custom','handmade','artisan',
    ]
    names = [
        'sarah','emma','lily','grace','rose','claire','anne','kate',
        'jane','mia','ella','zoe','alice','luna','sky','nova','ivy',
        'jake','mike','john','tom','sam','alex','chris','ryan','adam',
        'james','ben','max','leo','jack','noah','ethan','luke','evan',
    ]
    numbers = ['1','2','23','24','25','2024','2025','101','365','247','co']
    locations = [
        'usa','uk','ca','au','ny','la','tx','fl','nj','wa',
        'london','boston','miami','dallas','seattle','chicago',
    ]

    subs = set()

    # Pattern 1: keyword alone
    subs.add(kw)
    subs.add(kw2)

    # Pattern 2: keyword + suffix
    for s in suffixes:
        subs.add(f"{kw}-{s}")
        subs.add(f"{kw2}{s}")
        subs.add(f"{kw}{s}")

    # Pattern 3: prefix + keyword
    for p in prefixes:
        subs.add(f"{p}-{kw}")
        subs.add(f"{p}{kw2}")

    # Pattern 4: keyword + year/number
    for n in numbers + years:
        subs.add(f"{kw}{n}")
        subs.add(f"{kw}-{n}")
        subs.add(f"{kw2}{n}")

    # Pattern 5: keyword + name
    for name in names:
        subs.add(f"{name}{kw2}")
        subs.add(f"{kw2}{name}")
        subs.add(f"{name}-{kw}")
        subs.add(f"{kw}-{name}")

    # Pattern 6: keyword + location
    for loc in locations:
        subs.add(f"{kw2}{loc}")
        subs.add(f"{kw}-{loc}")

    # Pattern 7: adjective + keyword
    for adj in adjectives:
        subs.add(f"{adj}-{kw}")
        subs.add(f"{adj}{kw2}")

    # Pattern 8: "the" + keyword + suffix
    for s in suffixes[:10]:
        subs.add(f"the-{kw}-{s}")
        subs.add(f"the{kw2}{s}")

    # Pattern 9: name + keyword + suffix
    for name in names[:10]:
        for s in suffixes[:5]:
            subs.add(f"{name}{kw2}{s}")

    result = list(subs)
    random.shuffle(result)
    return result[:count]


def check_store_exists(subdomain, session):
    """
    Quick check: does this myshopify.com subdomain exist and is it Shopify?
    Returns (url, True) if valid Shopify store, else (url, False)
    """
    url = f"https://{subdomain}.myshopify.com"
    try:
        r = session.get(
            f"{url}/products.json?limit=1",
            headers={"User-Agent": UA},
            timeout=6,
            allow_redirects=True
        )
        if r.status_code == 200:
            try:
                data = r.json()
                if "products" in data:
                    return url, True
            except:
                pass
        # Some stores redirect to custom domain — check homepage
        if r.status_code in [301, 302, 200]:
            r2 = session.get(url, headers={"User-Agent":UA}, timeout=5)
            if r2.status_code == 200 and "shopify" in r2.text.lower():
                return url, True
    except:
        pass
    return url, False


def discover_stores_by_keyword(keyword, max_workers=30, max_stores=200):
    """
    Generate subdomains for keyword, then check them concurrently.
    Returns list of verified Shopify store URLs.
    """
    subdomains = generate_subdomains(keyword, count=600)
    log(f"   Generated {len(subdomains)} subdomain candidates for '{keyword}'","INFO")

    found = []
    checked = 0

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=max_workers,
        pool_maxsize=max_workers,
        max_retries=0
    )
    session.mount('https://', adapter)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(check_store_exists, sub, session): sub
                   for sub in subdomains}
        for future in as_completed(futures):
            if len(found) >= max_stores:
                break
            try:
                url, exists = future.result(timeout=8)
                checked += 1
                if exists:
                    found.append(url)
                    log(f"   ✅ Found: {url}","INFO")
            except:
                checked += 1

    log(f"   Subdomain scan: checked {checked}, found {len(found)} stores","INFO")
    return found


# ─────────────────────────────────────────────────────────────────────────────
# SUPPLEMENT: crt.sh + URLScan (fast sources, run in parallel)
# ─────────────────────────────────────────────────────────────────────────────
def _src_crtsh(cutoff):
    found = set()
    try:
        r = requests.get("https://crt.sh/",
            params={"q":"%.myshopify.com","output":"json"},
            timeout=20, headers={"User-Agent":UA,"Accept":"application/json"})
        if r.status_code == 200:
            for cert in r.json():
                nb = cert.get("not_before","")
                if nb:
                    try:
                        if datetime.strptime(nb[:19],"%Y-%m-%dT%H:%M:%S") < cutoff:
                            continue
                    except: pass
                name = cert.get("common_name","") or cert.get("name_value","")
                for part in re.split(r'[\n\r,]+', name):
                    part = part.strip().lstrip("*.")
                    m = MYSHOPIFY_RE.match(part)
                    if m: found.add(f"https://{m.group(1)}.myshopify.com")
    except: pass
    return found

def _src_urlscan(cutoff):
    found = set()
    try:
        r = requests.get("https://urlscan.io/api/v1/search/",
            params={"q":"domain:myshopify.com","size":500,"sort":"time"},
            timeout=15, headers={"User-Agent":UA})
        if r.status_code == 200:
            for res in r.json().get("results",[]):
                t = res.get("task",{}).get("time","")
                if t:
                    try:
                        if datetime.strptime(t[:19],"%Y-%m-%dT%H:%M:%S") < cutoff:
                            continue
                    except: pass
                pu = res.get("page",{}).get("url","")
                m = MYSHOPIFY_RE.search(pu)
                if m: found.add(f"https://{m.group(1)}.myshopify.com")
    except: pass
    return found

def _src_commoncrawl(keyword):
    found = set()
    kw = keyword.replace(' ','-')
    for idx in ["CC-MAIN-2025-08","CC-MAIN-2024-51"]:
        try:
            r = requests.get(
                f"https://index.commoncrawl.org/{idx}-index",
                params={"url":f"*.myshopify.com/*{kw}*","output":"json",
                        "limit":500,"fl":"url"},
                timeout=15)
            if r.status_code == 200 and r.text.strip():
                for line in r.text.strip().split("\n"):
                    try:
                        m = MYSHOPIFY_RE.search(json.loads(line).get("url",""))
                        if m: found.add(f"https://{m.group(1)}.myshopify.com")
                    except: continue
                if found: break
        except: pass
    return found


def fetch_all_stores(keyword, country):
    """
    Combine all discovery methods:
    1. Subdomain brute-force (primary — works even when all APIs are down)
    2. crt.sh + URLScan + CommonCrawl (supplement)
    """
    all_stores = set()
    cutoff = datetime.utcnow() - timedelta(days=7)

    # Run external sources in parallel (with timeout protection)
    log("   Running external sources in parallel...","INFO")
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_crt = ex.submit(_src_crtsh, cutoff)
        f_url = ex.submit(_src_urlscan, cutoff)
        f_cc  = ex.submit(_src_commoncrawl, keyword)
        for fname, future in [("crt.sh",f_crt),("URLScan",f_url),("CommonCrawl",f_cc)]:
            try:
                result = future.result(timeout=25)
                log(f"   {fname}: +{len(result)} stores","INFO")
                all_stores.update(result)
            except Exception as e:
                log(f"   {fname}: timeout/error — skipping","WARN")

    # Primary method: subdomain brute-force for keyword
    log(f"   Running subdomain scan for '{keyword}'...","INFO")
    brute = discover_stores_by_keyword(keyword, max_workers=25, max_stores=150)
    log(f"   Subdomain scan: +{len(brute)} stores","INFO")
    all_stores.update(brute)

    total = list(all_stores)
    log(f"📦 Total: {len(total)} stores to test","SUCCESS")
    return total


# ─────────────────────────────────────────────────────────────────────────────
# PAYMENT CHECK
# ─────────────────────────────────────────────────────────────────────────────
PAID_KWS = ['visa','mastercard','amex','paypal','credit card','debit card',
            'card number','stripe','klarna','afterpay','shop pay','shoppay',
            'apple pay','google pay','discover','diners']
NO_PAY   = ["isn't accepting payments","not accepting payments",
            "no payment methods","payment provider hasn't been set up",
            "this store is unavailable","unable to process payment"]

def check_payment(base_url, session):
    hdrs = {"User-Agent":UA,"Accept":"text/html,*/*;q=0.8","Accept-Language":"en-US,en;q=0.9"}
    try:
        r = session.get(base_url, headers=hdrs, timeout=8, allow_redirects=True)
        if r.status_code != 200:
            return {"ok":False,"lead":False,"reason":"dead"}
        html = r.text.lower()
        if "shopify" not in html and "cdn.shopify.com" not in html:
            return {"ok":False,"lead":False,"reason":"not shopify"}

        pr = session.get(f"{base_url}/products.json?limit=1", headers=hdrs, timeout=8)
        if pr.status_code != 200:
            return {"ok":True,"lead":False,"reason":"no products.json"}

        products = pr.json().get("products",[])

        if not products:
            cr = session.get(f"{base_url}/checkout", headers=hdrs, timeout=10, allow_redirects=True)
            cl = cr.text.lower()
            for msg in NO_PAY:
                if msg in cl:
                    return {"ok":True,"lead":True,"reason":f"0 products + '{msg}'"}
            return {"ok":True,"lead":False,"reason":"0 products unclear"}

        vid = products[0]["variants"][0]["id"]
        session.post(f"{base_url}/cart/add.js",
                     json={"id":vid,"quantity":1},
                     headers={**hdrs,"Content-Type":"application/json"},
                     timeout=8)
        cr = session.get(f"{base_url}/checkout", headers=hdrs, timeout=12, allow_redirects=True)
        cl = cr.text.lower()

        for msg in NO_PAY:
            if msg in cl:
                return {"ok":True,"lead":True,"reason":f"confirmed: '{msg}'"}
        for kw in PAID_KWS:
            if kw in cl:
                return {"ok":True,"lead":False,"reason":f"has payment ({kw})"}
        if any(s in cl for s in ["contact information","shipping address","order summary"]):
            return {"ok":True,"lead":True,"reason":"checkout OK, no payment options"}

        return {"ok":True,"lead":False,"reason":"inconclusive"}
    except Exception as e:
        return {"ok":False,"lead":False,"reason":str(e)[:50]}


# ─────────────────────────────────────────────────────────────────────────────
# CONTACT INFO
# ─────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'(\+\d{1,3}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4})')
SKIP_EM  = ['example','sentry','wixpress','shopify','.png','.jpg','.svg',
            'noreply','no-reply','schema.org','domain.com','w3.org']

def vmail(e):
    e = e.lower()
    if any(s in e for s in SKIP_EM): return False
    p = e.split('@')
    if len(p)!=2 or not p[0] or '.' not in p[1]: return False
    return 2<=len(p[1].split('.')[-1])<=6

def scrape_email(html, soup):
    for a in soup.find_all('a', href=True):
        h = a['href']
        if h.startswith('mailto:'):
            e = h[7:].split('?')[0].strip().lower()
            if vmail(e): return e
    for m in EMAIL_RE.findall(html):
        if vmail(m): return m.lower()
    return None

def get_contact(base_url, session):
    hdrs = {"User-Agent":UA}
    info = {"store_name": base_url.replace("https://","").split(".")[0],
            "email":None,"phone":None}
    pages = ["","/pages/contact","/pages/contact-us","/contact",
             "/pages/about-us","/pages/about","/pages/faq",
             "/pages/help","/pages/support",
             "/policies/contact-information","/policies/refund-policy"]
    for path in pages:
        if info["email"] and info["phone"]: break
        try:
            r = session.get(base_url+path, headers=hdrs, timeout=9)
            if r.status_code != 200: continue
            html = r.text
            soup = BeautifulSoup(html,"html.parser")
            if path == "":
                t = soup.find("title")
                if t:
                    name = t.text.strip()
                    for sfx in [" – Shopify"," | Shopify"," - Powered by Shopify"," – Online Store"]:
                        name = name.replace(sfx,"")
                    info["store_name"] = name.strip()[:80]
            if not info["email"]:
                e = scrape_email(html, soup)
                if e:
                    info["email"] = e
                    log(f"   📧 {path or '/'}: {e}","INFO")
            if not info["phone"]:
                m = PHONE_RE.search(html)
                if m: info["phone"] = m.group(0).strip()
        except: continue

    if not info["email"]:
        try:
            r = session.get(base_url, headers=hdrs, timeout=9)
            soup = BeautifulSoup(r.text,"html.parser")
            for sc in soup.find_all("script", type="application/ld+json"):
                try:
                    d = json.loads(sc.string or "{}")
                    items = d if isinstance(d,list) else [d]
                    for item in items:
                        e = item.get("email","") or item.get("contactPoint",{}).get("email","")
                        if e and vmail(e):
                            info["email"] = e.lower()
                            log(f"   📧 JSON-LD: {e}","INFO")
                            break
                except: continue
        except: pass
    return info


# ─────────────────────────────────────────────────────────────────────────────
# AI EMAIL
# ─────────────────────────────────────────────────────────────────────────────
def gen_email(tpl, lead, groq_key):
    try:
        import groq as glib
        client = glib.Groq(api_key=groq_key)
        prompt = (f"Write a short cold email to a Shopify store owner.\n"
                  f"Store: {lead.get('store_name','the store')}\n"
                  f"Country: {lead.get('country','')}\n"
                  f"Problem: No payment gateway — cannot accept payments.\n"
                  f"Base subject: {tpl['subject']}\nBase body: {tpl['body']}\n"
                  f"Rules: 80-100 words, no spam, mention store name once, 1 soft CTA, HTML <p> tags.\n"
                  f'Return ONLY JSON: {{"subject":"...","body":"<p>...</p>"}}')
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role":"user","content":prompt}],
            max_tokens=500, temperature=0.7)
        raw  = re.sub(r'```(?:json)?|```','',resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw.replace('\n',' '))
        return data.get("subject",tpl["subject"]), data.get("body",tpl["body"])
    except Exception as e:
        log(f"Groq fallback: {e}","WARN")
        return tpl["subject"], f"<p>{tpl['body']}</p>"


# ─────────────────────────────────────────────────────────────────────────────
# MAIN AUTOMATION
# ─────────────────────────────────────────────────────────────────────────────
def run_automation():
    global automation_running
    automation_running = True
    try:
        _run()
    except Exception as e:
        log(f"💥 FATAL: {e}","ERROR")
        log(traceback.format_exc()[:800],"ERROR")
    finally:
        automation_running = False
        log("🔴 Automation stopped","INFO")

def _run():
    global automation_running

    log("📋 Loading config...","INFO")
    cfg = call_sheet({"action":"get_config"}).get("config",{})
    groq_key  = cfg.get("groq_api_key","").strip()
    min_leads = int(cfg.get("min_leads",50) or 50)
    if not groq_key:
        log("❌ Groq API Key missing","ERROR"); return
    log(f"✅ Config OK | Target: {min_leads} leads","INFO")

    kws = [k for k in call_sheet({"action":"get_keywords"}).get("keywords",[])
           if k.get("status")=="ready"]
    if not kws:
        log("❌ No READY keywords!","ERROR"); return
    log(f"🗝️  {len(kws)} keywords ready","INFO")

    tpls = call_sheet({"action":"get_templates"}).get("templates",[])
    if not tpls:
        log("❌ No email template!","ERROR"); return
    tpl = tpls[0]
    log(f"📧 Template: '{tpl['name']}'","INFO")

    session = requests.Session()
    session.max_redirects = 5
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━","INFO")
    log("🚀 PHASE 1+2 — Discover & Filter Stores","SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━","INFO")

    for kw_row in kws:
        if not automation_running: break
        if total_leads >= min_leads:
            log(f"🎯 Target reached ({total_leads}/{min_leads})","SUCCESS"); break

        keyword = kw_row.get("keyword","")
        country = kw_row.get("country","")
        kw_id   = kw_row.get("id","")
        kw_leads = has_pay = dead = 0

        log(f"\n🎯 [{keyword}] [{country}]","INFO")

        stores = fetch_all_stores(keyword, country)
        if not stores:
            log("⚠️  No stores found","WARN")
            call_sheet({"action":"mark_keyword_used","id":kw_id,"leads_found":0})
            continue

        log(f"🔍 Checking {len(stores)} stores for payment gateway...","INFO")

        for idx, url in enumerate(stores):
            if not automation_running: break
            if total_leads >= min_leads: break

            result = check_payment(url, session)

            if not result["ok"]:
                dead += 1
            elif not result["lead"]:
                has_pay += 1
            else:
                log(f"   🎯 {result['reason']}","SUCCESS")
                info = get_contact(url, session)
                save = call_sheet({
                    "action":"save_lead",
                    "store_name":info["store_name"],
                    "url":url,
                    "email":info["email"] or "",
                    "phone":info["phone"] or "",
                    "country":country,
                    "keyword":keyword
                })
                if save.get("status")=="duplicate":
                    log("   ⏭️  Duplicate","INFO"); continue
                total_leads += 1; kw_leads += 1
                em = f"📧 {info['email']}" if info["email"] else "⚠ no email"
                log(f"   ✅ LEAD #{total_leads} — {info['store_name']} | {em}","SUCCESS")
                time.sleep(random.uniform(1,2))
                continue

            if (idx+1) % 25 == 0:
                log(f"   [{idx+1}/{len(stores)}] leads:{kw_leads} paid:{has_pay} dead:{dead}","INFO")

        call_sheet({"action":"mark_keyword_used","id":kw_id,"leads_found":kw_leads})
        log(f"✅ [{keyword}] done — leads:{kw_leads} paid:{has_pay} dead:{dead}","SUCCESS")

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━","INFO")
    log(f"📊 Total: {total_leads} leads","SUCCESS")

    log("📧 PHASE 3 — Email outreach","INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━","INFO")
    time.sleep(5)
    all_leads = call_sheet({"action":"get_leads"}).get("leads",[])
    pending   = [l for l in all_leads
                 if l.get("email") and "@" in str(l["email"])
                 and l.get("email_sent")!="sent"]
    log(f"📨 {len(pending)} leads to email","INFO")

    for i, lead in enumerate(pending):
        if not automation_running: break
        email_to = lead["email"]
        log(f"✉️  [{i+1}/{len(pending)}] → {email_to}","INFO")
        subject, body = gen_email(tpl, lead, groq_key)
        resp = call_sheet({"action":"send_email","to":email_to,
                           "subject":subject,"body":body,"lead_id":lead.get("id","")})
        if resp.get("status")=="ok":
            log(f"   ✅ Sent","SUCCESS")
        else:
            log(f"   ❌ {resp.get('message','failed')}","ERROR")
        delay = random.randint(90,150)
        log(f"   ⏳ {delay}s...","INFO")
        time.sleep(delay)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━","INFO")
    log("🎉 ALL DONE!","SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━","INFO")


# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/') 
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    global last_status_fetch, cached_status
    url = os.environ.get('APPS_SCRIPT_URL','')
    if url and time.time()-last_status_fetch > 60:
        try:
            r1 = requests.post(url,json={'action':'get_leads'},timeout=15)
            if r1.status_code==200:
                leads=r1.json().get('leads',[])
                cached_status['total_leads']=len(leads)
                cached_status['emails_sent']=sum(1 for l in leads if l.get('email_sent')=='sent')
            r2 = requests.post(url,json={'action':'get_keywords'},timeout=15)
            if r2.status_code==200:
                kws=r2.json().get('keywords',[])
                cached_status['kw_total']=len(kws)
                cached_status['kw_used']=sum(1 for k in kws if k.get('status')=='used')
            last_status_fetch=time.time()
        except: pass
    return jsonify({'running':automation_running,**cached_status,'script_connected':bool(url)})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try: yield f"data: {log_queue.get(timeout=25)}\n\n"
            except queue.Empty: yield f"data: {json.dumps({'ping':True})}\n\n"
    return Response(gen(),mimetype='text/event-stream',
                    headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})

@app.route('/api/sheet',methods=['POST'])
def api_sheet():
    if not os.environ.get('APPS_SCRIPT_URL',''): return jsonify({'error':'APPS_SCRIPT_URL not set'})
    return jsonify(call_sheet(request.json))

@app.route('/api/automation/start',methods=['POST'])
def api_start():
    global automation_running, automation_thread
    if automation_running: return jsonify({'status':'already_running'})
    automation_thread=threading.Thread(target=run_automation,daemon=True)
    automation_thread.start()
    return jsonify({'status':'started'})

@app.route('/api/automation/stop',methods=['POST'])
def api_stop():
    global automation_running
    automation_running=False
    log("⛔ Stopped by user","WARN")
    return jsonify({'status':'stopped'})

@app.route('/api/schedule',methods=['POST'])
def api_schedule():
    d=request.json
    try:
        rt=datetime.fromisoformat(d.get('time',''))
        scheduler.add_job(
            func=lambda: threading.Thread(target=run_automation,daemon=True).start(),
            trigger='date',run_date=rt,id='sched',replace_existing=True)
        log(f"📅 Scheduled for {d['time']}","INFO")
        return jsonify({'status':'scheduled','time':d['time']})
    except Exception as e:
        return jsonify({'status':'error','msg':str(e)}),400

if __name__=='__main__':
    port=int(os.environ.get('PORT',5000))
    app.run(host='0.0.0.0',port=port,debug=False,threaded=True)
