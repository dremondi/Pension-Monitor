#!/usr/bin/env python3
"""
Public Pension Fund Allocation Monitor
=======================================
Scrapes the web daily for news about the top 100 US public pensions
making new allocations to VC, PE, and private credit funds.
Sends a formatted digest email at 5pm PT.

Author: Dan Remondi / General Catalyst CVF Team
"""

import os
import json
import time
import logging
import smtplib
import hashlib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────

RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL", "dremondi@generalcatalyst.com")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")          # e.g. monitor@yourdomain.com
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")         # App password (not regular pw)

# Google Custom Search API (free tier: 100 queries/day)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")

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
    # Mega plans (>$100B)
    "CalPERS", "CalSTRS", "New York State Common Retirement Fund",
    "New York City Retirement Systems", "Florida State Board of Administration",
    "Texas Teachers Retirement System", "New York State Teachers Retirement System",
    "State of Wisconsin Investment Board", "Washington State Investment Board",
    "Ohio Public Employees Retirement System",
    # Large plans ($50B-$100B)
    "North Carolina Retirement Systems", "New Jersey Division of Investment",
    "Virginia Retirement System", "Oregon Investment Council",
    "Michigan Retirement Systems", "Pennsylvania Public School Employees",
    "State Teachers Retirement System of Ohio", "Minnesota State Board of Investment",
    "Colorado PERA", "Massachusetts PRIM",
    # Mid-Large ($25B-$50B)
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
    # Mid-size ($10B-$25B)
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
    # Notable city/county plans
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

# Asset class terms to search for
ASSET_CLASSES = [
    "private credit", "private equity", "venture capital",
    "private debt", "direct lending", "mezzanine",
    "growth equity", "buyout fund", "credit fund",
    "alternative credit", "infrastructure debt",
]

# Action-oriented keywords (signals active allocation decisions)
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

# Negative keywords to exclude noise
EXCLUDE_KEYWORDS = [
    "lawsuit", "scandal", "bankruptcy", "fraud",
    "pension crisis", "underfunded", "layoff",
]

# ─────────────────────────────────────────────────────────────────────
# Search Functions
# ─────────────────────────────────────────────────────────────────────

def build_search_queries():
    """Generate focused search queries combining pension names + asset classes."""
    queries = []

    # Strategy 1: Broad asset-class searches with pension context
    broad_queries = [
        'public pension "private credit" allocate commit 2025 2026',
        'public pension "private equity" new fund commitment 2026',
        'public pension "venture capital" allocation increase 2026',
        'state pension "private credit" fund investment approved',
        'pension fund "direct lending" commitment allocation',
        'public retirement system "private equity" commit approved',
        'pension fund "alternative credit" new allocation',
        'state retirement "private debt" investment approve',
        'pension board approved "private equity" commitment',
        'pension board approved "private credit" allocation',
        'pension fund "emerging manager" private credit equity',
        'public pension "co-investment" private equity credit',
    ]
    queries.extend(broad_queries)

    # Strategy 2: Top pension names specifically (top ~30 by AUM)
    top_pensions = PENSION_FUNDS[:30]
    for pension in top_pensions:
        # Use shorter name variants for better search results
        short_name = pension.replace("Retirement System", "").replace("Retirement", "").strip()
        queries.append(f'"{short_name}" private credit OR private equity OR venture capital allocation OR commit')

    return queries


def search_google_cse(query, num_results=10):
    """Search using Google Custom Search Engine API."""
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        logger.warning("Google CSE credentials not configured, skipping.")
        return []

    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_API_KEY,
        "cx": GOOGLE_CSE_ID,
        "q": query,
        "num": min(num_results, 10),
        "dateRestrict": "d3",   # last 3 days
        "sort": "date",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for item in data.get("items", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "source": item.get("displayLink", ""),
                "date": item.get("pagemap", {}).get("metatags", [{}])[0].get("article:published_time", ""),
            })
        return results
    except Exception as e:
        logger.error(f"Google CSE error for query '{query[:50]}...': {e}")
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
        logger.error(f"NewsAPI error for query '{query[:50]}...': {e}")
        return []


# ─────────────────────────────────────────────────────────────────────
# Filtering & Scoring
# ─────────────────────────────────────────────────────────────────────

def article_hash(article):
    """Create a unique hash for deduplication."""
    key = (article.get("url", "") or article.get("title", "")).lower().strip()
    return hashlib.md5(key.encode()).hexdigest()


def load_seen_cache():
    """Load previously seen article hashes."""
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text())
            # Prune old entries
            cutoff = (datetime.utcnow() - timedelta(days=MAX_CACHE_AGE_DAYS)).isoformat()
            return {k: v for k, v in data.items() if v > cutoff}
        except Exception:
            return {}
    return {}


def save_seen_cache(cache):
    """Save seen article hashes."""
    CACHE_FILE.write_text(json.dumps(cache, indent=2))


def score_article(article):
    """
    Score an article's relevance (0-100).
    Higher scores = more actionable for outreach.
    """
    text = f"{article.get('title', '')} {article.get('snippet', '')}".lower()
    score = 0

    # Check for pension fund mentions
    pension_match = False
    matched_pension = None
    for fund in PENSION_FUNDS:
        if fund.lower() in text:
            pension_match = True
            matched_pension = fund
            score += 30
            break

    # Generic pension references
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
    score += min(action_count * 5, 25)  # Cap at 25 from actions

    # Dollar amount mentions (strong signal)
    import re
    dollar_pattern = r'\$[\d,.]+\s*(?:million|billion|mn|bn|m|b)'
    if re.search(dollar_pattern, text, re.IGNORECASE):
        score += 10

    # Exclude noise
    for neg in EXCLUDE_KEYWORDS:
        if neg in text:
            score -= 20

    # Must have BOTH pension context AND asset class to be relevant
    if not (pension_match or score >= 15) or not asset_match:
        score = max(score - 30, 0)

    article["_score"] = min(score, 100)
    article["_matched_pension"] = matched_pension
    article["_matched_assets"] = matched_assets
    article["_matched_actions"] = matched_actions[:5]

    return article


def filter_and_rank(articles, min_score=25):
    """Deduplicate, score, filter, and rank articles."""
    seen_cache = load_seen_cache()
    unique = {}

    for article in articles:
        h = article_hash(article)
        if h in seen_cache:
            continue
        if h in unique:
            continue
        scored = score_article(article)
        if scored["_score"] >= min_score:
            unique[h] = scored

    # Update cache
    now = datetime.utcnow().isoformat()
    for h in unique:
        seen_cache[h] = now
    save_seen_cache(seen_cache)

    # Sort by score descending
    ranked = sorted(unique.values(), key=lambda x: x["_score"], reverse=True)
    return ranked


# ─────────────────────────────────────────────────────────────────────
# Email Formatting
# ─────────────────────────────────────────────────────────────────────

def format_digest_html(articles, run_date):
    """Format articles into a professional HTML digest email."""

    # Categorize by priority
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
    """Plain-text fallback for the digest."""
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
    """Send the digest email via SMTP."""
    if not all([SENDER_EMAIL, SMTP_USER, SMTP_PASSWORD]):
        logger.error("SMTP credentials not configured. Set SENDER_EMAIL, SMTP_USER, SMTP_PASSWORD.")
        # Save to local file as fallback
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
    """Main monitor execution: search, filter, email."""
    run_date = datetime.utcnow()
    logger.info("=" * 60)
    logger.info(f"Pension Fund Allocation Monitor — {run_date.strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info("=" * 60)

    all_results = []
    queries = build_search_queries()
    logger.info(f"Running {len(queries)} search queries...")

    # Google Custom Search (primary)
    for i, query in enumerate(queries):
        logger.info(f"  [{i+1}/{len(queries)}] Searching: {query[:80]}...")
        results = search_google_cse(query)
        all_results.extend(results)

        # Rate limiting: ~1 req/sec to stay within quotas
        time.sleep(1.2)

        # Stop if we've used most of our daily quota (100 free queries)
        if i >= 90:
            logger.warning("Approaching Google CSE daily quota limit, stopping searches.")
            break

    # NewsAPI supplementary search
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

    # Filter and rank
    ranked = filter_and_rank(all_results, min_score=25)
    logger.info(f"After filtering: {len(ranked)} actionable articles")

    # Format and send
    subject = f"🏛️ Pension Allocation Digest — {run_date.strftime('%b %d')} ({len(ranked)} updates)"
    html = format_digest_html(ranked, run_date)
    text = format_digest_text(ranked, run_date)

    send_email(subject, html, text)

    # Also save latest digest locally
    latest = Path(__file__).parent / "latest_digest.html"
    latest.write_text(html)
    logger.info(f"Digest saved to {latest}")

    return ranked


if __name__ == "__main__":
    run_monitor()
