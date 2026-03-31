from flask import Flask, render_template, request, jsonify, Response
import threading
import queue
import time
import json
import re
import random
import requests
import urllib.parse
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

# ── NEW: AI KEYWORD GENERATOR (1 -> 200) ────────────────────────────────────
def generate_related_keywords(base_keyword, groq_key, count=200):
    """
    Groq AI ব্যবহার করে একটি কিওয়ার্ড থেকে ২০০টি রেলিভেন্ট কিওয়ার্ড বানাবে।
    """
    try:
        client = Groq(api_key=groq_key)
        prompt = f"""Generate exactly {count} highly relevant, long-tail e-commerce search keywords related to '{base_keyword}'. 
        These should be phrases people use to find specific products or online stores (e.g. if base is 'clothing', output 'buy men winter clothing online', 'cheap vintage dresses', etc).
        
        Rules:
        - Return ONLY a valid JSON array of strings. 
        - Do not add markdown like ```json or any introductory text.
        - Just the array: ["keyword1", "keyword2", ...]"""
        
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2500,
            temperature=0.8
        )
        raw = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        kws = json.loads(raw)
        
        if isinstance(kws, list):
            clean_kws = list(set([str(k).strip() for k in kws if k]))
            return clean_kws
    except Exception as e:
        log(f"⚠️ AI Keyword generation failed for '{base_keyword}': {e}", "WARN")
    return []

# ── 1. ADVANCED SCRAPING (Brave Search + URLScan) ─────────────────────────────
MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.myshopify\.com')

def find_shopify_stores_brave(keyword):
    """
    SerpAPI/DuckDuckGo বাদ দিয়ে সরাসরি Brave Search এবং URLScan ব্যবহার করে স্ক্র্যাপিং।
    """
    all_urls = set()
    kw_clean = keyword.lower().replace(' ', '')

    # METHOD 1: URLScan.io (Real-time newly registered stores)
    try:
        urlscan_url = f"https://urlscan.io/api/v1/search/?q=domain:myshopify.com AND {kw_clean}&size=50&sort=time"
        r = requests.get(urlscan_url, timeout=10)
        if r.status_code == 200:
            for result in r.json().get('results', []):
                page_url = result.get('page', {}).get('url', '')
                m = MYSHOPIFY_RE.search(page_url)
                if m:
                    all_urls.add(f"https://{m.group(1)}.myshopify.com")
    except Exception:
        pass

    # METHOD 2: FREE BRAVE SEARCH HTML SCRAPING
    queries = [
        f'site:myshopify.com "{keyword}"',
        f'intitle:"{keyword}" site:myshopify.com'
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    }

    for q in queries:
        try:
            # tf=pm মানে Brave Search-এ গত ১ মাসের ফিল্টার (Past Month), এতে ফ্রেশ স্টোর পাওয়া যায়
            brave_url = f"https://search.brave.com/search?q={urllib.parse.quote(q)}&tf=pm"
            res = requests.get(brave_url, headers=headers, timeout=15)
            
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, 'html.parser')
                # Brave-এর সার্চ রেজাল্ট থেকে লিংকগুলো বের করা
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    m = MYSHOPIFY_RE.search(href)
                    if m:
                        all_urls.add(f"https://{m.group(1)}.myshopify.com")
        except Exception:
            pass
        # Brave Search যেন ব্লক না করে তাই একটু বিরতি
        time.sleep(1.5)

    return list(all_urls)

# ── 2. STRICT CHECKOUT TEST (No Password Pages, Only No-Payment) ──────────────
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
            return {"is_shopify": True, "is_lead": False, "reason": "Password Protected (Skipping as requested)"}

        try:
            prod_req = session.get(f"{base_url}/products.json?limit=1", headers=headers, timeout=10)
            if prod_req.status_code == 200:
                prod_data = prod_req.json()
                if 'products' in prod_data and len(prod_data['products']) > 0:
                    variant_id = prod_data['products'][0]['variants'][0]['id']
                    
                    session.post(f"{base_url}/cart/add.js", json={"id": variant_id, "quantity": 1}, headers=headers, timeout=10)
                    
                    chk_req = session.get(f"{base_url}/checkout", headers=headers, timeout=15)
                    chk_html = chk_req.text.lower()
                    
                    if 'checkout' not in chk_html and 'contact information' not in chk_html and "isn't accepting payments" not in chk_html:
                        return {"is_shopify": True, "is_lead": False, "reason": "Could not reach valid checkout page"}

                    payment_keywords =[
                        'visa', 'mastercard', 'amex', 'paypal', 'credit card', 
                        'debit card', 'card number', 'stripe', 'klarna', 'afterpay', 'shop pay', 'apple pay', 'google pay'
                    ]
                    
                    for pk in payment_keywords:
                        if pk in chk_html:
                            return {"is_shopify": True, "is_lead": False, "reason": f"Active Checkout ('{pk}' found)"}
                    
                    if "isn't accepting payments" in chk_html or "not accepting payments" in chk_html:
                        return {"is_shopify": True, "is_lead": True, "reason": "Live Store -> Checkout Disabled (Explicit Error)!"}
                    
                    return {"is_shopify": True, "is_lead": True, "reason": "No Payment Options Found on Checkout!"}
                    
            return {"is_shopify": True, "is_lead": False, "reason": "Could not test checkout (No products to add)"}
            
        except Exception as e:
            return {"is_shopify": True, "is_lead": False, "reason": "Checkout test failed"}
            
    except Exception as e:
        return {"is_shopify": False, "is_lead": False}

# ── Store info extraction ─────────────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
SKIP_EMAIL_DOMAINS =['example', 'sentry', 'wixpress', 'shopify', '.png', '.jpg', '.svg', 'noreply', 'domain.com']
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
            for path in['/pages/contact', '/contact', '/pages/about-us']:
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
        prompt = f"""You are writing a short cold email to a Shopify store owner.
Store: {lead.get('store_name', 'the store')} | URL: {lead.get('url', '')} | Country: {lead.get('country', '')}
Problem: This store has NO payment gateway — customers cannot pay!
Base template: Subject: {tpl_subject} | Body: {tpl_body}
Rules: 80-100 words MAX, Zero spam trigger words, Mention store name once naturally, End with ONE soft question, Use HTML <p> tags.
Respond ONLY with valid JSON: {{"subject": "...", "body": "<p>...</p><p>...</p>"}}"""
        resp = client.chat.completions.create(model="llama-3.1-8b-instant", messages=[{"role": "user", "content": prompt}], max_tokens=500, temperature=0.7)
        raw = re.sub(r'```(?:json)?|```', '', resp.choices[0].message.content.strip()).strip()
        data = json.loads(raw)
        return data.get('subject', tpl_subject), data.get('body', f'<p>{tpl_body}</p>')
    except Exception as e:
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
        log("🔴 Automation stopped (All tasks finished)", "INFO")

def _run():
    global automation_running

    log("📋 Loading config from Google Sheet...", "INFO")
    cfg_resp = call_sheet({'action': 'get_config'})

    if cfg_resp.get('error'):
        log(f"❌ Cannot reach Apps Script: {cfg_resp['error']}", "ERROR")
        return

    cfg = cfg_resp.get('config', {})
    groq_key    = cfg.get('groq_api_key', '').strip()
    min_leads   = int(cfg.get('min_leads', 50) or 50)

    if not groq_key:
        log("❌ Groq API Key missing — go to CFG screen → save", "ERROR")
        return

    log(f"✅ Config loaded | Target: {min_leads} leads", "INFO")

    kw_resp = call_sheet({'action': 'get_keywords'})
    ready_kws = [k for k in kw_resp.get('keywords', []) if k.get('status') == 'ready']
    if not ready_kws:
        log("❌ No READY keywords! Add keywords in Leads screen.", "ERROR")
        return
    log(f"🗝️  {len(ready_kws)} base keywords ready", "INFO")

    tpl_resp = call_sheet({'action': 'get_templates'})
    templates = tpl_resp.get('templates',[])
    if not templates:
        log("❌ No email template! Add one in Email screen first", "ERROR")
        return
    tpl = templates[0]

    session = requests.Session()
    session.max_redirects = 3
    total_leads = 0

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("🚀 PHASE 1 — BRAVE SEARCH SCRAPING & CHECKOUT CHECKING", "SUCCESS")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    # ── লুপ শুরু: মেইন কিওয়ার্ড -> ২০০ সাব-কিওয়ার্ড -> Brave Search ──
    for kw_row in ready_kws:
        if not automation_running or total_leads >= min_leads:
            break

        base_keyword = kw_row.get('keyword', '')
        country = kw_row.get('country', '')
        kw_id   = kw_row.get('id', '')
        kw_leads = 0

        log(f"\n🧠 Using AI to generate ~200 keywords from: [{base_keyword}]", "INFO")
        
        # AI দিয়ে কিওয়ার্ড বানানো
        ai_kws = generate_related_keywords(base_keyword, groq_key, count=200)
        all_kws = [base_keyword] + ai_kws
        log(f"✅ Created {len(all_kws)} unique keywords to scrape!", "SUCCESS")

        for index, current_kw in enumerate(all_kws):
            if not automation_running or total_leads >= min_leads:
                break

            log(f"🔍 [{index+1}/{len(all_kws)}] Scraping via Free Brave Search: '{current_kw}'", "INFO")
            
            # Brave Search দিয়ে URL বের করা
            store_urls = find_shopify_stores_brave(current_kw)

            if not store_urls:
                continue

            for url in store_urls:
                if not automation_running or total_leads >= min_leads:
                    break

                try:
                    # আপনার কড়া চেকআউট লজিক (হুবহু আগের মতোই)
                    target_info = check_store_target(url, session)

                    if not target_info.get("is_shopify") or not target_info.get("is_lead"):
                        continue # যেগুলো পাস করবে না সেগুলো স্কিপ হবে

                    log(f"   🎯 100% MATCH: ({target_info.get('reason')}) -> {url}", "SUCCESS")

                    info = get_store_info(url, session)
                    save_resp = call_sheet({
                        'action': 'save_lead',
                        'store_name': info['store_name'],
                        'url': url,
                        'email': info['email'] or '',
                        'phone': info['phone'] or '',
                        'country': country,
                        'keyword': current_kw
                    })

                    if save_resp.get('status') == 'duplicate':
                        continue

                    total_leads += 1
                    kw_leads += 1
                    email_display = info['email'] or '⚠ no email found'
                    log(f"   ✅ LEAD #{total_leads} → {info['store_name']} | {email_display}", "SUCCESS")
                    time.sleep(random.uniform(1.5, 3))

                except Exception as e:
                    continue

        # লুপ শেষ হলে কিওয়ার্ড 'Used' করে দেওয়া
        call_sheet({'action': 'mark_keyword_used', 'id': kw_id, 'leads_found': kw_leads})

    # ── Phase 2: Email outreach ───────────────────────────────────────────────
    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")
    log("📧 PHASE 2 — EMAIL OUTREACH STARTING", "INFO")
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "INFO")

    leads_resp = call_sheet({'action': 'get_leads'})
    pending = [l for l in leads_resp.get('leads', []) if l.get('email') and '@' in l['email'] and l.get('email_sent') != 'sent']

    log(f"📨 {len(pending)} leads with email addresses to contact", "INFO")

    for i, lead in enumerate(pending):
        if not automation_running: break

        email_to = lead['email']
        log(f"✉️[{i+1}/{len(pending)}] Sending to {email_to}...", "INFO")
        subject, body = generate_email(tpl['subject'], tpl['body'], lead, groq_key)

        send_resp = call_sheet({
            'action': 'send_email', 'to': email_to, 'subject': subject,
            'body': body, 'lead_id': lead.get('id', '')
        })

        if send_resp.get('status') == 'ok': log(f"   ✅ Email sent", "SUCCESS")
        
        delay = random.randint(90, 150)
        time.sleep(delay)

    log("\n🎉 ALL DONE! Check your Google Sheet for leads.", "SUCCESS")

# ── Flask routes ──────────────────────────────────────────────────────────────
@app.route('/')
def index(): return render_template('index.html')

@app.route('/api/status')
def api_status():
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    total_leads = emails_sent = kw_total = kw_used = 0
    if script_url:
        try:
            leads = call_sheet({'action': 'get_leads'}).get('leads',[])
            total_leads, emails_sent = len(leads), sum(1 for l in leads if l.get('email_sent') == 'sent')
            kws = call_sheet({'action': 'get_keywords'}).get('keywords',[])
            kw_total, kw_used = len(kws), sum(1 for k in kws if k.get('status') == 'used')
        except: pass
    return jsonify({'running': automation_running, 'total_leads': total_leads, 'emails_sent': emails_sent, 'kw_total': kw_total, 'kw_used': kw_used, 'script_connected': bool(script_url)})

@app.route('/api/logs/stream')
def stream_logs():
    def gen():
        while True:
            try: yield f"data: {log_queue.get(timeout=25)}\n\n"
            except queue.Empty: yield f"data: {json.dumps({'ping': True})}\n\n"
    return Response(gen(), mimetype='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

@app.route('/api/sheet', methods=['POST'])
def api_sheet(): return jsonify(call_sheet(request.json))

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
    data = request.json
    try:
        run_time = datetime.fromisoformat(data.get('time', ''))
        scheduler.add_job(func=lambda: threading.Thread(target=run_automation, daemon=True).start(), trigger='date', run_date=run_time, id='scheduled_run', replace_existing=True)
        return jsonify({'status': 'scheduled', 'time': data.get('time', '')})
    except Exception as e: return jsonify({'status': 'error', 'msg': str(e)}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, threaded=True)
