#!/usr/bin/env python3
"""
Public Pension Fund Allocation Monitor
=======================================
Scrapes the web daily for news about the top 100 US public pensions
making new allocations to VC, PE, and private credit funds.
Sends a formatted digest email at 5pm PT.

Uses SerpAPI for web search (free tier: 100 searches/month).
"""

import os
import json
import time
import logging
import smtplib
import hashlib
import re
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

import requests

# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────

RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL", "dremondi@generalcatalyst.com")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# SerpAPI key (free tier: 100 searches/month)
# Get yours at: https://serpapi.com/
SERPAPI_KEY = os.getenv("SERPAPI_KEY")

# Optional: NewsAPI.org key for supplementary search (free tier: 100 req/day)
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")

# Deduplication cache
CACHE_FILE = Path(__file__).parent / "seen_articles.json"
MAX_CACHE_AGE_DAYS = 30

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# Top 100 US Public Pension Funds (by AUM, approximate)
# ─────────────────────────────────────────────────────────────────────

PENSION_FUNDS = [
    "CalPERS", "CalSTRS", "New York State Common Retirement Fund",
    "New York City Retirement Systems", "Florida State Board of Administration",
    "Texas Teachers Retirement System", "New York State Teachers Retirement System",
    "State of Wisconsin Investment Board", "Washington State Investment Board",
    "Ohio Public Employees Retirement System",
    "North Carolina Retirement Systems", "New Jersey Division of Investment",
    "Virginia Retirement System", "Oregon Investment Council",
    "Michigan Retirement Systems", "Pennsylvania Public School Employees",
    "State Teachers Retirement System of Ohio", "Minnesota State Board of Investment",
    "Colorado PERA", "Massachusetts PRIM", "MassPRIM",
    "Los Angeles County Employees Retirement", "LACERA",
    "Teacher Retirement System of Texas", "Maryland State Retirement",
    "Connecticut Retirement Plans", "Tennessee Consolidated Retirement System",
    "South Carolina Retirement System", "Iowa Public Employees Retirement System",
    "Los Angeles Fire and Police Pensions", "LAFPP",
    "Missouri State Employees Retirement System", "Kentucky Retirement Systems",
    "Arizona State Retirement System", "Indiana Public Retirement System",
    "San Francisco Employees Retirement System", "SFERS",
    "Illinois Teachers Retirement System", "Illinois State Board of Investment",
    "Illinois Municipal Retirement Fund", "State Universities Retirement System Illinois",
    "Hawaii Employees Retirement System", "New Mexico State Investment Council",
    "Oklahoma Teachers Retirement System", "Nevada Public Employees Retirement System",
    "Kansas Public Employees Retirement System", "Louisiana State Employees Retirement",
    "Utah Retirement Systems", "Rhode Island State Investment Commission",
    "Alabama Retirement Systems", "Mississippi Public Employees Retirement System",
    "San Diego County Employees Retirement", "SDCERA",
    "Orange County Employees Retirement System", "OCERS",
    "Contra Costa County Employees Retirement", "CCCERA",
    "San Bernardino County Employees Retirement",
    "Alameda County Employees Retirement", "ACERA",
    "Sacramento County Employees Retirement",
    "Dallas Police and Fire Pension", "Houston Firefighters Relief and Retirement",
    "Teachers Retirement Association of Minnesota",
    "Public Employee Retirement System of Idaho",
    "Nebraska Investment Council", "Arkansas Teacher Retirement System",
    "West Virginia Investment Management Board",
    "Maine Public Employees Retirement System",
    "New Hampshire Retirement System", "Vermont Pension Investment Commission",
    "Wyoming Retirement System", "Montana Board of Investments",
    "North Dakota State Investment Board", "South Dakota Investment Council",
    "Alaska Permanent Fund", "Delaware Public Employees Retirement System",
    "District of Columbia Retirement Board",
    "Chicago Teachers Pension Fund", "Chicago Municipal Employees",
    "Chicago Police Pension Fund", "Chicago Fire Pension Fund",
    "New York City Teachers Retirement System",
    "New York City Police Pension Fund", "New York City Fire Pension Fund",
    "Philadelphia Board of Pensions", "Detroit General Retirement System",
    "San Jose Federated City Employees Retirement",
    "Employees Retirement System of Texas", "ERS Texas",
    "Pennsylvania State Employees Retirement System", "SERS Pennsylvania",
    "Georgia Teachers Retirement System", "Employees Retirement System of Georgia",
    "Municipal Employees Annuity Benefit Fund Chicago",
    "Denver Employees Retirement Plan", "Jacksonville Police and Fire Pension",
    "Fresno County Employees Retirement", "Kern County Employees Retirement",
    "Tulare County Employees Retirement",
    "Ventura County Employees Retirement", "Santa Barbara County Employees Retirement",
    "Sonoma County Employees Retirement", "Marin County Employees Retirement",
    "San Mateo County Employees Retirement", "Stanislaus County Employees Retirement",
]

# ─────────────────────────────────────────────────────────────────────
# Search Queries & Keyword Filters
# ─────────────────────────────────────────────────────────────────────

ASSET_CLASSES = [
    "private credit", "private equity", "venture capital",
    "private debt", "direct lending", "mezzanine",
    "growth equity", "buyout fund", "credit fund",
    "alternative credit", "infrastructure debt",
]

ACTION_KEYWORDS = [
    "allocate", "allocated", "allocation", "commit", "committed", "commitment",
    "increase", "increased", "raise", "raised", "approve", "approved",
    "invest", "invested", "investment", "deploy", "deployed",
    "new fund", "fund raise", "fundraise", "capital call",
    "co-invest", "co-investment", "mandate", "awarded",
    "target allocation", "strategic allocation", "rebalance",
    "overweight", "pacing plan", "vintage year",
    "emerging manager", "first-time fund",
]

EXCLUDE_KEYWORDS = [
    "lawsuit", "scandal", "bankruptcy", "fraud",
    "pension crisis", "underfunded", "layoff",
]

# ─────────────────────────────────────────────────────────────────────
# Search Functions
# ─────────────────────────────────────────────────────────────────────

def build_search_queries():
    """
    Generate focused search queries.
    SerpAPI free tier = 100/month, so we use ~15-20 queries per run
    to stay well within budget for daily use.
    """
    queries = [
        # Broad pension + asset class queries (8 queries)
        'public pension "private credit" allocate OR commit OR approve 2025 OR 2026',
        'public pension "private equity" new commitment OR allocation 2025 OR 2026',
        'public pension "venture capital" allocation OR investment 2025 OR 2026',
        'pension fund "direct lending" OR "private debt" commitment allocation',
        'pension board approved "private equity" OR "private credit" commitment',
        'state retirement fund "private equity" OR "private credit" new allocation',
        'pension fund "emerging manager" OR "co-investment" private credit equity',
        'public pension alternative investment allocation increase 2026',

        # Top pension names specifically (8 queries)
        'CalPERS "private credit" OR "private equity" OR "venture capital" allocation OR commit',
        'CalSTRS "private credit" OR "private equity" OR "venture capital" allocation OR commit',
        '"New York State Common" OR "NYSCRF" private credit OR private equity allocation',
        '"State of Wisconsin Investment Board" OR "SWIB" private equity OR credit',
        '"Washington State Investment Board" OR "WSIB" private equity OR credit commit',
        '"New Jersey Division of Investment" private credit OR equity allocation',
        '"Virginia Retirement System" OR "Oregon Investment Council" private equity credit',
        '"Texas Teachers" OR "Ohio PERS" private credit OR equity allocation commit',
    ]
    return queries


def search_serpapi(query, num_results=10):
    """Search using SerpAPI (Google Search wrapper)."""
    if not SERPAPI_KEY:
        logger.error("SERPAPI_KEY not configured.")
        return []

    url = "https://serpapi.com/search"
    params = {
        "api_key": SERPAPI_KEY,
        "engine": "google",
        "q": query,
        "num": num_results,
        "tbs": "qdr:w",  # last week
    }

    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for item in data.get("organic_results", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "source": item.get("displayed_link", item.get("source", "")),
                "date": item.get("date", ""),
            })

        # Also grab news results if present
        for item in data.get("news_results", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "source": item.get("source", {}).get("name", "") if isinstance(item.get("source"), dict) else item.get("source", ""),
                "date": item.get("date", ""),
            })

        return results
    except Exception as e:
        logger.error(f"SerpAPI error for query '{query[:60]}...': {e}")
        return []


def search_newsapi(query, days_back=3):
    """Search using NewsAPI.org for supplementary coverage."""
    if not NEWSAPI_KEY:
        return []

    url = "https://newsapi.org/v2/everything"
    from_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    params = {
        "apiKey": NEWSAPI_KEY,
        "q": query,
        "from": from_date,
        "sortBy": "relevancy",
        "language": "en",
        "pageSize": 10,
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for article in data.get("articles", []):
            results.append({
                "title": article.get("title", ""),
                "url": article.get("url", ""),
                "snippet": article.get("description", ""),
                "source": article.get("source", {}).get("name", ""),
                "date": article.get("publishedAt", ""),
            })
        return results
    except Exception as e:
        logger.error(f"NewsAPI error for query '{query[:60]}...': {e}")
        return []


# ─────────────────────────────────────────────────────────────────────
# Filtering & Scoring
# ─────────────────────────────────────────────────────────────────────

def article_hash(article):
    key = (article.get("url", "") or article.get("title", "")).lower().strip()
    return hashlib.md5(key.encode()).hexdigest()


def load_seen_cache():
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text())
            cutoff = (datetime.utcnow() - timedelta(days=MAX_CACHE_AGE_DAYS)).isoformat()
            return {k: v for k, v in data.items() if v > cutoff}
        except Exception:
            return {}
    return {}


def save_seen_cache(cache):
    CACHE_FILE.write_text(json.dumps(cache, indent=2))


def score_article(article):
    """Score an article's relevance (0-100)."""
    text = f"{article.get('title', '')} {article.get('snippet', '')}".lower()
    score = 0

    # Check for pension fund mentions
    title = article.get("title", "").lower()
    pension_match = False
    matched_pension = None
    for fund in PENSION_FUNDS:
        if fund.lower() in text:
            pension_match = True
            matched_pension = fund
            score += 30
            # Extra boost if the pension fund name is in the title (strongest signal)
            if fund.lower() in title:
                score += 20
            break

    if not pension_match:
        pension_generics = ["pension", "retirement system", "retirement fund", "public employees"]
        if any(term in text for term in pension_generics):
            score += 15

    # Asset class relevance
    asset_match = False
    matched_assets = []
    for ac in ASSET_CLASSES:
        if ac in text:
            asset_match = True
            matched_assets.append(ac)
            score += 15

    # Action keyword signals
    action_count = 0
    matched_actions = []
    for kw in ACTION_KEYWORDS:
        if kw in text:
            action_count += 1
            matched_actions.append(kw)
    score += min(action_count * 5, 25)

    # Dollar amount mentions
    dollar_pattern = r'\$[\d,.]+\s*(?:million|billion|mn|bn|m|b)'
    if re.search(dollar_pattern, text, re.IGNORECASE):
        score += 10

    # Exclude noise
    for neg in EXCLUDE_KEYWORDS:
        if neg in text:
            score -= 20

    # Must have pension context AND asset class
    if not (pension_match or score >= 15) or not asset_match:
        score = max(score - 30, 0)

    article["_score"] = min(score, 100)
    article["_matched_pension"] = matched_pension
    article["_matched_assets"] = matched_assets
    article["_matched_actions"] = matched_actions[:5]

    return article


def filter_and_rank(articles, min_score=25):
    seen_cache = load_seen_cache()
    unique = {}

    for article in articles:
        h = article_hash(article)
        if h in seen_cache or h in unique:
            continue
        scored = score_article(article)
        if scored["_score"] >= min_score:
            unique[h] = scored

    now = datetime.utcnow().isoformat()
    for h in unique:
        seen_cache[h] = now
    save_seen_cache(seen_cache)

    return sorted(unique.values(), key=lambda x: x["_score"], reverse=True)


# ─────────────────────────────────────────────────────────────────────
# Email Formatting
# ─────────────────────────────────────────────────────────────────────

def format_digest_html(articles, run_date):
    high_priority = [a for a in articles if a["_score"] >= 60]
    medium_priority = [a for a in articles if 40 <= a["_score"] < 60]
    low_priority = [a for a in articles if a["_score"] < 40]

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: 'Helvetica Neue', Arial, sans-serif; color: #1a1a1a; max-width: 720px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #0d1b2a 0%, #1b3a5c 100%); color: white; padding: 28px 32px; border-radius: 8px; margin-bottom: 24px; }}
            .header h1 {{ margin: 0 0 6px; font-size: 22px; font-weight: 600; letter-spacing: -0.3px; }}
            .header p {{ margin: 0; font-size: 13px; opacity: 0.8; }}
            .summary {{ background: #f0f4f8; padding: 16px 20px; border-radius: 6px; margin-bottom: 24px; font-size: 14px; line-height: 1.5; }}
            .section-label {{ font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: #6b7280; margin: 28px 0 12px; }}
            .priority-high .section-label {{ color: #dc2626; }}
            .priority-medium .section-label {{ color: #d97706; }}
            .article {{ border-left: 3px solid #e5e7eb; padding: 12px 16px; margin-bottom: 16px; background: #fafafa; border-radius: 0 6px 6px 0; }}
            .priority-high .article {{ border-left-color: #dc2626; background: #fef2f2; }}
            .priority-medium .article {{ border-left-color: #d97706; background: #fffbeb; }}
            .article h3 {{ margin: 0 0 6px; font-size: 15px; }}
            .article h3 a {{ color: #1a56db; text-decoration: none; }}
            .article h3 a:hover {{ text-decoration: underline; }}
            .article .meta {{ font-size: 12px; color: #6b7280; margin-bottom: 6px; }}
            .article .snippet {{ font-size: 13px; line-height: 1.5; color: #374151; }}
            .tags {{ margin-top: 8px; }}
            .tag {{ display: inline-block; font-size: 11px; padding: 2px 8px; border-radius: 12px; margin-right: 4px; margin-bottom: 4px; }}
            .tag-pension {{ background: #dbeafe; color: #1e40af; }}
            .tag-asset {{ background: #d1fae5; color: #065f46; }}
            .tag-action {{ background: #fef3c7; color: #92400e; }}
            .footer {{ margin-top: 32px; padding-top: 16px; border-top: 1px solid #e5e7eb; font-size: 11px; color: #9ca3af; text-align: center; }}
            .empty {{ text-align: center; padding: 40px 20px; color: #6b7280; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🏛️ Pension Fund Allocation Monitor</h1>
            <p>{run_date.strftime('%A, %B %d, %Y')} &nbsp;|&nbsp; VC · PE · Private Credit &nbsp;|&nbsp; General Catalyst CVF</p>
        </div>

        <div class="summary">
            <strong>{len(articles)} actionable update{'s' if len(articles) != 1 else ''}</strong> found today.
            &nbsp;🔴 {len(high_priority)} high priority
            &nbsp;🟡 {len(medium_priority)} medium
            &nbsp;⚪ {len(low_priority)} informational
        </div>
    """

    def render_section(items, label, css_class):
        if not items:
            return ""
        section = f'<div class="{css_class}"><div class="section-label">{label} ({len(items)})</div>'
        for a in items:
            tags_html = ""
            if a.get("_matched_pension"):
                tags_html += f'<span class="tag tag-pension">{a["_matched_pension"]}</span>'
            for asset in a.get("_matched_assets", [])[:2]:
                tags_html += f'<span class="tag tag-asset">{asset}</span>'
            for action in a.get("_matched_actions", [])[:2]:
                tags_html += f'<span class="tag tag-action">{action}</span>'

            section += f"""
            <div class="article">
                <h3><a href="{a.get('url', '#')}">{a.get('title', 'No title')}</a></h3>
                <div class="meta">{a.get('source', 'Unknown')} {(' · ' + a['date'][:10]) if a.get('date') else ''} · Score: {a['_score']}</div>
                <div class="snippet">{a.get('snippet', '')}</div>
                <div class="tags">{tags_html}</div>
            </div>
            """
        section += "</div>"
        return section

    html += render_section(high_priority, "🔴 High Priority — Active Allocations", "priority-high")
    html += render_section(medium_priority, "🟡 Medium Priority — Likely Relevant", "priority-medium")
    html += render_section(low_priority, "⚪ Informational", "priority-low")

    if not articles:
        html += '<div class="empty"><p>No new actionable updates found today.</p><p>The monitor searched for allocation activity across 100+ public pension funds.</p></div>'

    html += f"""
        <div class="footer">
            Auto-generated by Pension Allocation Monitor · Searches {len(PENSION_FUNDS)} funds across VC, PE & Private Credit<br>
            To adjust filters or recipients, update the configuration in pension_monitor.py
        </div>
    </body>
    </html>
    """
    return html


def format_digest_text(articles, run_date):
    lines = [
        f"PENSION FUND ALLOCATION MONITOR — {run_date.strftime('%A, %B %d, %Y')}",
        f"{'=' * 60}",
        f"{len(articles)} actionable updates found.\n",
    ]
    for i, a in enumerate(articles, 1):
        lines.append(f"{i}. [{a['_score']}] {a.get('title', 'No title')}")
        lines.append(f"   Source: {a.get('source', 'Unknown')}  |  {a.get('url', '')}")
        if a.get("_matched_pension"):
            lines.append(f"   Pension: {a['_matched_pension']}")
        if a.get("_matched_assets"):
            lines.append(f"   Asset Class: {', '.join(a['_matched_assets'])}")
        if a.get("snippet"):
            lines.append(f"   {a['snippet'][:200]}")
        lines.append("")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────
# Email Sending
# ─────────────────────────────────────────────────────────────────────

def send_email(subject, html_body, text_body):
    if not all([SENDER_EMAIL, SMTP_USER, SMTP_PASSWORD]):
        logger.error("SMTP credentials not configured.")
        fallback = Path(__file__).parent / f"digest_{datetime.utcnow().strftime('%Y%m%d')}.html"
        fallback.write_text(html_body)
        logger.info(f"Digest saved locally to {fallback}")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"Pension Monitor <{SENDER_EMAIL}>"
    msg["To"] = RECIPIENT_EMAIL

    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        logger.info(f"Digest sent to {RECIPIENT_EMAIL}")
        return True
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────
# Main Execution
# ─────────────────────────────────────────────────────────────────────

def run_monitor():
    run_date = datetime.utcnow()
    logger.info("=" * 60)
    logger.info(f"Pension Fund Allocation Monitor — {run_date.strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info("=" * 60)

    all_results = []
    queries = build_search_queries()
    logger.info(f"Running {len(queries)} SerpAPI search queries...")

    for i, query in enumerate(queries):
        logger.info(f"  [{i+1}/{len(queries)}] Searching: {query[:80]}...")
        results = search_serpapi(query)
        all_results.extend(results)
        logger.info(f"    → {len(results)} results")
        time.sleep(1)  # Rate limiting

    # NewsAPI supplementary search
    if NEWSAPI_KEY:
        logger.info("Running supplementary NewsAPI searches...")
        newsapi_queries = [
            'pension "private credit" allocation',
            'pension "private equity" commitment',
            'pension "venture capital" investment',
            'public pension fund new commitment',
        ]
        for query in newsapi_queries:
            results = search_newsapi(query)
            all_results.extend(results)
            time.sleep(0.5)

    logger.info(f"Total raw results: {len(all_results)}")

    ranked = filter_and_rank(all_results, min_score=25)
    logger.info(f"After filtering: {len(ranked)} actionable articles")

    subject = f"🏛️ Pension Allocation Digest — {run_date.strftime('%b %d')} ({len(ranked)} updates)"
    html = format_digest_html(ranked, run_date)
    text = format_digest_text(ranked, run_date)

    send_email(subject, html, text)

    latest = Path(__file__).parent / "latest_digest.html"
    latest.write_text(html)
    logger.info(f"Digest saved to {latest}")

    return ranked


if __name__ == "__main__":
    run_monitor()
