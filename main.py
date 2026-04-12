from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from bs4 import BeautifulSoup
import requests
import re

app = FastAPI()

headers = {"User-Agent": "Mozilla/5.0"}

# ------------------------
# SEARCH STORES
# ------------------------
def search_stores(keyword, location):
    query = f"{keyword} {location} site:myshopify.com"
    url = f"https://html.duckduckgo.com/html/?q={query}"

    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")

    sites = []

    for a in soup.find_all("a", href=True):
        link = a["href"]
        if "myshopify.com" in link:
            sites.append(link)

    return list(set(sites))


# ------------------------
# FILTERS
# ------------------------
def is_shopify(site):
    try:
        r = requests.get(site, timeout=8)
        return "cdn.shopify.com" in r.text.lower()
    except:
        return False


def product_count(site):
    try:
        r = requests.get(site + "/products.json", timeout=8)
        return len(r.json().get("products", []))
    except:
        return 0


def checkout_working(site):
    try:
        r = requests.get(site + "/checkout", timeout=8, allow_redirects=True)
        return "checkout" in r.url.lower()
    except:
        return False


def extract_email(site):
    try:
        r = requests.get(site, timeout=8)
        emails = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", r.text)
        return list(set(emails))
    except:
        return []


# ------------------------
# LEAD LOGIC
# ------------------------
def analyze(site):
    if not is_shopify(site):
        return None

    products = product_count(site)
    checkout_ok = checkout_working(site)

    # NEW + BROKEN CHECKOUT = BEST LEAD
    if products <= 10 and not checkout_ok:
        return {
            "site": site,
            "products": products,
            "emails": ", ".join(extract_email(site)),
            "status": "Hot Lead 🔥"
        }

    return None


# ------------------------
# UI (Dashboard)
# ------------------------
@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <h2>Shopify Lead Finder 🚀</h2>
    <form action="/search" method="post">
        Keyword: <input name="keyword"><br><br>
        Location: <input name="location"><br><br>
        <button type="submit">Find Leads</button>
    </form>
    """


@app.post("/search", response_class=HTMLResponse)
def search(request: Request, keyword: str = Form(...), location: str = Form(...)):
    sites = search_stores(keyword, location)

    leads = []

    for s in sites[:20]:  # limit (free safe)
        data = analyze(s)
        if data:
            leads.append(data)

    html = "<h2>Results</h2><a href='/'>Back</a><br><br>"

    for lead in leads:
        html += f"""
        <div style="border:1px solid #ccc; padding:10px; margin:10px;">
            <b>Site:</b> {lead['site']}<br>
            <b>Products:</b> {lead['products']}<br>
            <b>Email:</b> {lead['emails']}<br>
            <b>Status:</b> {lead['status']}
        </div>
        """

    if not leads:
        html += "No leads found 😢"

    return html
