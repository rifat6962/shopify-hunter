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

# ── Logging ─────────────────────────────────────────────

def log(message, level="INFO"):
    entry = {
        'time': datetime.now().strftime('%H:%M:%S'),
        'level': level,
        'message': str(message)
    }
    log_queue.put(json.dumps(entry))
    print(f"[{level}] {message}")

# ── Apps Script ─────────────────────────────────────────

def call_sheet(payload):
    script_url = os.environ.get('APPS_SCRIPT_URL', '')
    if not script_url:
        return {'error': 'APPS_SCRIPT_URL not set'}

    for attempt in range(3):
        try:
            r = requests.post(script_url, json=payload, timeout=45)
            return r.json()
        except:
            time.sleep(2)

    return {'error': 'Sheet API failed'}

# ── KEYWORD GENERATOR (🔥 NEW) ─────────────────────────

def generate_keywords(seed_keyword, country, groq_key):
    try:
        client = Groq(api_key=groq_key)

        prompt = f"""
Generate 200 Shopify buyer-intent keywords.

Seed: {seed_keyword}
Country: {country}

Return JSON array only.
"""

        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=1500
        )

        raw = re.sub(r'```|json', '', resp.choices[0].message.content.strip())
        return list(set(json.loads(raw)))[:200]

    except Exception as e:
        log(f"Keyword generation failed: {e}", "WARN")
        return [seed_keyword]

# ── SCRAPER ────────────────────────────────────────────

MYSHOPIFY_RE = re.compile(r'https?://([a-zA-Z0-9-]+).myshopify.com')

def find_shopify_stores(keyword, country, serpapi_key):
    all_urls = set()

    # URLSCAN BOOST
    try:
        url = f"https://urlscan.io/api/v1/search/?q={keyword} domain:myshopify.com&size=500"
        r = requests.get(url, timeout=10)
        for res in r.json().get('results', []):
            m = MYSHOPIFY_RE.search(res.get('page', {}).get('url', ''))
            if m:
                all_urls.add(f"https://{m.group(1)}.myshopify.com")
    except:
        pass

    # SERPAPI PAGINATION
    queries = [
        f'site:myshopify.com "{keyword}"',
        f'site:myshopify.com "{keyword}" {country}'
    ]

    for q in queries:
        for page in range(3):
            try:
                params = {
                    'api_key': serpapi_key,
                    'engine': 'google',
                    'q': q,
                    'num': 50,
                    'start': page * 50,
                    'tbs': 'qdr:w'
                }
                res = requests.get('https://serpapi.com/search', params=params, timeout=10)

                for item in res.json().get('organic_results', []):
                    m = MYSHOPIFY_RE.match(item.get('link', ''))
                    if m:
                        all_urls.add(f"https://{m.group(1)}.myshopify.com")

            except:
                pass

            time.sleep(1)

    return list(all_urls)

# ── CHECKOUT CHECK (UNCHANGED) ─────────────────────────

def check_store_target(url, session):
    try:
        r = session.get(url, timeout=10)
        html = r.text.lower()

        if 'shopify' not in html:
            return {"is_shopify": False}

        if 'password' in html:
            return {"is_shopify": True, "is_lead": False}

        if "isn't accepting payments" in html:
            return {"is_shopify": True, "is_lead": True}

        return {"is_shopify": True, "is_lead": False}

    except:
        return {"is_shopify": False}

# ── INFO EXTRACT ───────────────────────────────────────

def get_store_info(url, session):
    try:
        r = session.get(url, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')

        title = soup.title.text if soup.title else url
        email = None

        for a in soup.find_all('a', href=True):
            if 'mailto:' in a['href']:
                email = a['href'].replace('mailto:', '')
                break

        return {
            "store_name": title[:60],
            "email": email
        }
    except:
        return {"store_name": url, "email": None}

# ── MAIN AUTOMATION (🔥 UPDATED LOOP) ──────────────────

def run_automation():
    global automation_running
    automation_running = True

    session = requests.Session()

    cfg = call_sheet({'action': 'get_config'}).get('config', {})
    groq_key = cfg.get('groq_api_key')
    serpapi_key = cfg.get('serpapi_key')

    kws = call_sheet({'action': 'get_keywords'}).get('keywords', [])

    total_leads = 0

    for row in kws:
        seed = row['keyword']
        country = row.get('country', '')

        log(f"Generating keywords for {seed}", "INFO")

        sub_keywords = generate_keywords(seed, country, groq_key)

        for keyword in sub_keywords:
            if not automation_running:
                break

            log(f"Scraping: {keyword}", "INFO")

            urls = find_shopify_stores(keyword, country, serpapi_key)

            for url in urls:
                if not automation_running:
                    break

                res = check_store_target(url, session)

                if not res.get("is_lead"):
                    continue

                info = get_store_info(url, session)

                call_sheet({
                    'action': 'save_lead',
                    'store_name': info['store_name'],
                    'url': url,
                    'email': info['email'] or '',
                    'country': country,
                    'keyword': keyword
                })

                total_leads += 1
                log(f"LEAD #{total_leads} → {info['store_name']}", "SUCCESS")

                time.sleep(random.uniform(1, 2))

    automation_running = False
    log("DONE", "SUCCESS")

# ── API ────────────────────────────────────────────────

@app.route('/api/start', methods=['POST'])
def start():
    global automation_thread
    automation_thread = threading.Thread(target=run_automation)
    automation_thread.start()
    return jsonify({"status": "started"})

@app.route('/api/stop', methods=['POST'])
def stop():
    global automation_running
    automation_running = False
    return jsonify({"status": "stopped"})

@app.route('/api/logs')
def logs():
    def gen():
        while True:
            try:
                msg = log_queue.get(timeout=10)
                yield f"data: {msg}\n\n"
            except:
                yield "data: {}\n\n"
    return Response(gen(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(port=5000)
