#!/usr/bin/env python3
"""
Burst Capital Portfolio Jobs Scraper
"""

import json, re, time, logging
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

OUTPUT_FILE = "jobs.json"
COMPANIES_FILE = "companies.json"
REQUEST_TIMEOUT = 15
DELAY_BETWEEN_COMPANIES = 1.5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

JOBS_PATHS = [
    "/careers", "/jobs", "/careers/jobs", "/careers/openings",
    "/careers/open-roles", "/careers/positions", "/careers/listings",
    "/about/careers", "/company/careers", "/join", "/join-us",
    "/open-roles", "/work-with-us", "/hiring", "/openings", "/career",
]

# Hardcoded overrides — ONLY used when auto-detection returns nothing.
# Key = exact company name (lowercase). Value = (ats, slug, jobs_url).
KNOWN_ATS = {
    "faire":                         ("greenhouse", "Faire",        "https://boards.greenhouse.io/faire"),
    "grammarly":                     ("greenhouse", "grammarly",    "https://boards.greenhouse.io/grammarly"),
    "instawork":                     ("greenhouse", "instawork",    "https://boards.greenhouse.io/instawork"),
    "handshake":                     ("ashby_embedded", None,       "https://joinhandshake.com/careers/"),
    "lattice":                       ("greenhouse", "lattice",      "https://boards.greenhouse.io/lattice"),
    "ada":                           ("greenhouse", "ada18",        "https://job-boards.greenhouse.io/ada18"),
    "adquick":                       ("greenhouse", "adquick",      "https://boards.greenhouse.io/adquick"),
    "cambly":                        ("greenhouse", "cambly",       "https://boards.greenhouse.io/cambly"),
    "curri":                         ("greenhouse", "curri",        "https://boards.greenhouse.io/curri"),
    "glossgenius":                   ("greenhouse", "glossgenius",  "https://boards.greenhouse.io/glossgenius"),
    "hipcamp":                       ("greenhouse", "hipcamp",      "https://boards.greenhouse.io/hipcamp"),
    "owner":                         ("lever",      "owner",         "https://jobs.lever.co/owner"),
    "bounce":                        ("ashby_embedded", None,        "https://jobs.ashbyhq.com/Bounce"),
    "lily":                          ("greenhouse", "lilyai",       "https://boards.greenhouse.io/lilyai"),
    "medely":                        ("greenhouse", "medely",       "https://boards.greenhouse.io/medely"),
    "padlet":                        ("greenhouse", "padlet",       "https://boards.greenhouse.io/padlet"),
    "peek":                          ("greenhouse", "peek",         "https://boards.greenhouse.io/peek"),
    "workstream":                    ("greenhouse", "workstream",   "https://boards.greenhouse.io/workstream"),
    "wonderschool":                  ("ashby",      "wonderschool", "https://jobs.ashbyhq.com/wonderschool"),
    "ava":                           ("ashby",      "ava",          "https://jobs.ashbyhq.com/ava"),
    "resortpass":                    ("lever",      "resortpass",   "https://jobs.lever.co/resortpass"),
    "overflow":                      ("lever",      "overflow",     "https://jobs.lever.co/overflow"),
    "huckleberry":                   ("lever",      "huckleberry",  "https://jobs.lever.co/huckleberry"),
    "goodtime":                      ("lever",      "goodtime",     "https://jobs.lever.co/goodtime"),
    "sourcegraph":                   ("lever",      "sourcegraph",  "https://jobs.lever.co/sourcegraph"),
    "liftoff":                       ("lever",      "liftoff",      "https://jobs.lever.co/liftoff"),
    "trendsi":                       ("lever",      "trendsi",      "https://jobs.lever.co/trendsi"),
    "lilo":                          ("lever",      "lilo",         "https://jobs.lever.co/lilo"),
    "willow":                        ("yc",         "willow",       "https://www.ycombinator.com/companies/willow/jobs"),
    "woz":                           ("yc",         "woz",          "https://www.ycombinator.com/companies/woz/jobs"),
    "namespace":                     ("yc",         "namespace",    "https://www.ycombinator.com/companies/namespace/jobs"),
    "sibli":                         ("yc",         "sibli",        "https://www.ycombinator.com/companies/sibli/jobs"),
    "allstripes (acquired by picnic health)": ("greenhouse", "picnichealth", "https://boards.greenhouse.io/picnichealth"),
    "northstar (acquired by nayya health)":   ("greenhouse", "nayya",        "https://boards.greenhouse.io/nayya"),
    "via (acquired by justworks)":            ("greenhouse", "justworks",    "https://boards.greenhouse.io/justworks"),
    "yik yak (acquired by sidechat)":         ("greenhouse", "sidechat",     "https://boards.greenhouse.io/sidechat"),
    "setter.com (acquired by thumbtack)":     ("greenhouse", "thumbtackjobs","https://boards.greenhouse.io/thumbtackjobs"),
    "assemble (acquired by deel)":            ("ashby",      "deel",         "https://jobs.ashbyhq.com/deel"),
    "carebrain":                              ("greenhouse", "carebrain",    "https://boards.greenhouse.io/carebrain"),
    "sprx technologies":                      ("greenhouse", "sprx",         "https://boards.greenhouse.io/sprx"),
    "resq":                                   ("ashby",      "ResQ",         "https://jobs.ashbyhq.com/ResQ"),
    "medely":                                 ("ashby",      "medely",       "https://jobs.ashbyhq.com/medely"),
    "standard fleet":                         ("ashby",      "StandardFleet","https://jobs.ashbyhq.com/StandardFleet"),
}


# ── HELPERS ───────────────────────────────────────────────────────────────────

def fetch_html(url, use_playwright=False):
    if use_playwright and HAS_PLAYWRIGHT:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(extra_http_headers=HEADERS)
                page.goto(url, wait_until="domcontentloaded", timeout=15000)
                page.wait_for_timeout(1000)
                html = page.content()
                browser.close()
                return html
        except Exception as e:
            log.warning(f"    Playwright failed: {e}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        log.warning(f"    requests failed: {e}")
    return None


def make_job(title, department, location, url, company, company_website=""):
    return {
        "title": title.strip(),
        "department": (department or "General").strip(),
        "location": (location or "Not specified").strip(),
        "url": url,
        "company": company,
        "company_website": company_website,
    }


# ── ATS DETECTION ─────────────────────────────────────────────────────────────

def detect_ats_from_html(html, page_url):
    if not html:
        return None, None
    if "ashby_jid=" in html:
        m = re.search(r'ashbyhq\.com/([a-zA-Z0-9_-]+)', html)
        return ("ashby", m.group(1)) if m else ("ashby_embedded", page_url)
    if "jobs.ashbyhq.com" in html:
        m = re.search(r'jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)', html)
        return ("ashby", m.group(1)) if m else (None, None)
    if "greenhouse.io" in html or "grnh.se" in html:
        m = (re.search(r'boards\.greenhouse\.io/([a-zA-Z0-9_-]+)', html) or
             re.search(r'job-boards\.greenhouse\.io/([a-zA-Z0-9_-]+)', html) or
             re.search(r'greenhouse\.io/embed/job_board\?for=([a-zA-Z0-9_-]+)', html))
        return ("greenhouse", m.group(1)) if m else (None, None)
    if "lever.co" in html:
        m = re.search(r'jobs\.lever\.co/([a-zA-Z0-9_-]+)', html)
        return ("lever", m.group(1)) if m else (None, None)
    if "apply.workable.com" in html or "workable.com" in html:
        m = re.search(r'apply\.workable\.com/([a-zA-Z0-9_-]+)', html)
        return ("workable", m.group(1)) if m else (None, None)
    if "ats.rippling.com" in html or "rippling-ats.com" in html:
        m = re.search(r'ats\.rippling\.com/([a-zA-Z0-9_-]+)', html)
        return ("rippling", m.group(1)) if m else (None, None)
    return None, None


def detect_ats_from_url(url):
    if not url:
        return None, None
    checks = [
        (r'boards\.greenhouse\.io/([a-zA-Z0-9_-]+)', "greenhouse"),
        (r'job-boards\.greenhouse\.io/([a-zA-Z0-9_-]+)', "greenhouse"),
        (r'jobs\.lever\.co/([a-zA-Z0-9_-]+)', "lever"),
        (r'jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)', "ashby"),
        (r'ashbyhq\.com/([a-zA-Z0-9_-]+)', "ashby"),
        (r'apply\.workable\.com/([a-zA-Z0-9_-]+)', "workable"),
        (r'ats\.rippling\.com/([a-zA-Z0-9_-]+)', "rippling"),
    ]
    for pattern, ats in checks:
        m = re.search(pattern, url)
        if m:
            return ats, m.group(1)
    return None, None


# ── FIND JOBS PAGE ────────────────────────────────────────────────────────────

def find_jobs_page(base):
    base = base.rstrip("/")

    # Keywords that signal a link leads to job listings
    CAREERS_KEYWORDS = ["career", "job", "hiring", "join us", "open role",
                        "we're hiring", "opening", "position", "work with us"]

    def check_page(url, html):
        """Check a page's HTML for ATS signals or links to a deeper listings page."""
        ats, slug = detect_ats_from_html(html, url)
        if ats:
            return url, ats, slug, html

        # Look for links that go deeper into a listings subpage
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            href_lower = href.lower()
            if any(kw in href_lower or kw in text for kw in CAREERS_KEYWORDS):
                full_url = urljoin(url, href)
                # Skip if it's the same page or an anchor
                if full_url.rstrip("/") == url.rstrip("/") or href.startswith("#"):
                    continue
                # Skip external links that aren't ATS
                ats, slug = detect_ats_from_url(full_url)
                if ats:
                    return full_url, ats, slug, None
                # Fetch the linked page and check it too
                sub_html = fetch_html(full_url)
                if sub_html:
                    ats, slug = detect_ats_from_html(sub_html, full_url)
                    if ats:
                        return full_url, ats, slug, sub_html
        return None, None, None, None

    # Step 1: check homepage
    html = fetch_html(base)
    if html:
        url, ats, slug, h = check_page(base, html)
        if ats:
            return url, ats, slug, h

    # Step 2: try common paths
    for path in JOBS_PATHS:
        url = base + path
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200 and len(r.text) > 500:
                final_url = r.url
                ats, slug = detect_ats_from_url(final_url)
                if ats:
                    return final_url, ats, slug, None
                found_url, ats, slug, h = check_page(final_url, r.text)
                if ats:
                    return found_url, ats, slug, h
                # No ATS found but page exists — return it for generic scraping
                return final_url, None, None, r.text
        except Exception:
            pass
        time.sleep(0.3)

    return None, None, None, None


# ── ATS SCRAPERS ──────────────────────────────────────────────────────────────

def scrape_greenhouse(slug, company, fallback_url):
    try:
        r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
                         headers=HEADERS, timeout=REQUEST_TIMEOUT)
        jobs = []
        for j in r.json().get("jobs", []):
            depts = j.get("departments", [{}])
            dept = depts[0]["name"] if depts else "General"
            loc = j.get("location", {}).get("name", "")
            jobs.append(make_job(j.get("title", ""), dept, loc,
                                 j.get("absolute_url", fallback_url), company))
        log.info(f"    Greenhouse API: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        log.warning(f"    Greenhouse API failed: {e}")
        return scrape_generic(fallback_url, company)


def scrape_lever(slug, company, fallback_url):
    try:
        r = requests.get(f"https://api.lever.co/v0/postings/{slug}?mode=json",
                         headers=HEADERS, timeout=REQUEST_TIMEOUT)
        data = r.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected Lever response: {type(data)}")
        jobs = []
        for j in data:
            cats = j.get("categories", {})
            dept = cats.get("team", cats.get("department", "General"))
            locs = cats.get("allLocations", [cats.get("location", "")])
            loc = locs[0] if locs else ""
            jobs.append(make_job(j.get("text", ""), dept, loc,
                                 j.get("hostedUrl", fallback_url), company))
        log.info(f"    Lever API: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        log.warning(f"    Lever API failed: {e}")
        return scrape_generic(fallback_url, company)


def scrape_ashby(slug, company, fallback_url):
    for try_slug in [slug, slug.lower(), slug.capitalize()]:
        try:
            r = requests.get(f"https://api.ashbyhq.com/posting-api/job-board/{try_slug}",
                             headers=HEADERS, timeout=REQUEST_TIMEOUT)
            data = r.json()
            # Ashby uses either 'jobPostings' or 'jobs' depending on the account
            raw = data.get("jobPostings") or data.get("jobs") or []
            jobs = []
            for j in raw:
                loc = (j.get("locationName") or j.get("location") or
                       ("Remote" if j.get("isRemote") else ""))
                dept = (j.get("departmentName") or j.get("department") or
                        j.get("team") or "General")
                url = (j.get("jobPostingUrl") or j.get("jobUrl") or fallback_url)
                jobs.append(make_job(j.get("title", ""), dept, loc, url, company))
            if jobs:
                log.info(f"    Ashby API ({try_slug}): {len(jobs)} jobs")
                return jobs
        except Exception:
            pass
    log.warning(f"    Ashby API failed for all slug variants, trying embedded parse")
    return scrape_ashby_embedded(fallback_url, company)


def scrape_ashby_embedded(jobs_url, company):
    html = fetch_html(jobs_url, use_playwright=True)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    jobs, seen = [], set()

    # Pattern 1: embedded on company site via ?ashby_jid= links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "ashby_jid=" not in href:
            continue
        title = re.sub(r'\s*View\s*$', '', a.get_text(strip=True)).strip()
        if not title or title in seen or len(title) < 3:
            continue
        seen.add(title)
        # Extract UUID and build direct Ashby app URL (stable, works without JS)
        uuid_m = re.search(r'ashby_jid=([0-9a-f-]{36})', href)
        job_url = f"https://app.ashbyhq.com/jobs/{uuid_m.group(1)}" if uuid_m else urljoin(jobs_url, href)
        parent = a.find_parent()
        text = parent.get_text(" ", strip=True) if parent else ""
        loc_m = re.search(r'\b(Remote|New York|San Francisco|Los Angeles|Austin|'
                           r'London|Chicago|Seattle|Boston|Denver|NYC|SF|'
                           r'Portland|Atlanta|Miami|Washington|Philadelphia|'
                           r'Toronto|Vancouver|Berlin|Paris|Amsterdam)\b', text, re.I)
        jobs.append(make_job(title, "General", loc_m.group(1) if loc_m else "",
                             job_url, company))

    # Pattern 2: jobs.ashbyhq.com/Slug/uuid links (Ashby-hosted board)
    if not jobs:
        slug_m = re.search(r'ashbyhq\.com/([^/]+)', jobs_url)
        slug = slug_m.group(1) if slug_m else None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(jobs_url, href)
            # Match /Slug/uuid-pattern links
            if slug and f"/{slug}/" not in full and f"ashbyhq.com/{slug}/" not in full:
                continue
            if not re.search(r'/[0-9a-f-]{36}', full):
                continue
            title = a.get_text(strip=True)
            if not title or title in seen or len(title) < 3:
                continue
            seen.add(title)
            parent = a.find_parent()
            text = parent.get_text(" ", strip=True) if parent else ""
            loc_m = re.search(r'\b(Remote|New York|San Francisco|Los Angeles|Austin|'
                               r'London|Chicago|Seattle|Boston|Denver|NYC|SF|Portugal|Tokyo)\b',
                               text, re.I)
            jobs.append(make_job(title, "General", loc_m.group(1) if loc_m else "",
                                 full, company))

    log.info(f"    Ashby embedded: {len(jobs)} jobs")
    return jobs


def scrape_workable(slug, company, fallback_url):
    try:
        r = requests.post(f"https://apply.workable.com/api/v3/accounts/{slug}/jobs",
                          json={"query": "", "location": [], "department": [], "worktype": [], "remote": []},
                          headers={**HEADERS, "Content-Type": "application/json"},
                          timeout=REQUEST_TIMEOUT)
        jobs = []
        for j in r.json().get("results", []):
            loc = j.get("location", {}).get("city", "") if j.get("location") else ""
            if j.get("remote"):
                loc = "Remote"
            jobs.append(make_job(j.get("title", ""), j.get("department", "General"),
                                 loc, f"https://apply.workable.com/{slug}/j/{j.get('shortcode', '')}/",
                                 company))
        log.info(f"    Workable API: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        log.warning(f"    Workable API failed: {e}")
        return scrape_generic(fallback_url, company)


def scrape_rippling(slug, company, fallback_url):
    """Scrape jobs from Rippling ATS API."""
    try:
        # Rippling's public jobs API
        api = f"https://ats.rippling.com/api/v1/{slug}/jobs?limit=200"
        r = requests.get(api, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        jobs = []
        data = r.json()
        items = data if isinstance(data, list) else data.get("jobs", data.get("results", []))
        for j in items:
            title = j.get("title", j.get("name", ""))
            dept = j.get("department", j.get("team", "General")) or "General"
            if isinstance(dept, dict):
                dept = dept.get("name", "General")
            loc = j.get("location", j.get("locationName", ""))
            if isinstance(loc, dict):
                loc = loc.get("city", loc.get("name", ""))
            if j.get("remote"):
                loc = "Remote"
            job_id = j.get("id", j.get("slug", ""))
            job_url = f"https://ats.rippling.com/{slug}/jobs/{job_id}" if job_id else fallback_url
            jobs.append(make_job(title, dept, loc, job_url, company))
        if jobs:
            log.info(f"    Rippling API: {len(jobs)} jobs")
            return jobs
    except Exception as e:
        log.warning(f"    Rippling API failed: {e}")

    # Fallback: scrape the Rippling jobs page HTML
    return scrape_generic(fallback_url or f"https://ats.rippling.com/{slug}/jobs", company)



def scrape_yc(slug, company, fallback_url):
    """Scrape jobs from YC's company page via their JSON API."""
    try:
        api = f"https://www.ycombinator.com/companies/{slug}/jobs.json"
        r = requests.get(api, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        jobs = []
        for j in r.json():
            loc = j.get("location", "San Francisco, CA")
            dept = j.get("subtype", j.get("type", "General"))
            url = f"https://www.ycombinator.com/companies/{slug}/jobs/{j.get('slug', '')}"
            jobs.append(make_job(j.get("title", ""), dept, loc, url, company))
        if jobs:
            log.info(f"    YC API: {len(jobs)} jobs")
            return jobs
    except Exception as e:
        log.warning(f"    YC API failed: {e}")

    # Fallback: scrape the YC jobs page HTML
    try:
        html = fetch_html(fallback_url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        jobs, seen = [], set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if f"/companies/{slug}/jobs/" not in href:
                continue
            title = a.get_text(strip=True)
            if not title or title in seen:
                continue
            seen.add(title)
            parent = a.find_parent()
            text = parent.get_text(" ", strip=True) if parent else ""
            loc_m = re.search(r'\b(Remote|San Francisco|New York|Austin|Seattle|Boston)\b', text, re.I)
            loc = loc_m.group(1) if loc_m else "San Francisco, CA"
            jobs.append(make_job(title, "General", loc,
                                 f"https://www.ycombinator.com{href}", company))
        log.info(f"    YC HTML: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        log.warning(f"    YC HTML scrape failed: {e}")
        return []


def scrape_generic(jobs_url, company):
    if not jobs_url:
        return []
    html = fetch_html(jobs_url, use_playwright=True)
    if not html:
        return []
    if "ashby_jid=" in html:
        return scrape_ashby_embedded(jobs_url, company)
    soup = BeautifulSoup(html, "html.parser")
    jobs, seen = [], set()
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            d = json.loads(script.string or "")
            for item in (d if isinstance(d, list) else [d]):
                if item.get("@type") == "JobPosting":
                    loc_obj = item.get("jobLocation") or {}
                    addr = loc_obj.get("address") or {}
                    loc = addr.get("addressLocality", "") if isinstance(addr, dict) else ""
                    jobs.append(make_job(item.get("title", ""),
                                        item.get("occupationalCategory", "General"),
                                        loc, item.get("url", jobs_url), company))
        except Exception:
            pass
    if jobs:
        log.info(f"    Schema.org: {len(jobs)} jobs")
        return jobs
    for container in soup.find_all(class_=re.compile(
            r"job|position|role|opening|listing|posting|career|vacancy", re.I))[:150]:
        a = container.find("a", href=True)
        if not a:
            continue
        title = a.get_text(strip=True)
        if not title or len(title) < 3 or title in seen:
            continue
        seen.add(title)
        text = container.get_text(" ", strip=True)
        loc_m = re.search(r'\b(Remote|New York|San Francisco|Los Angeles|Austin|'
                           r'London|Chicago|Seattle|Boston|Denver|[A-Z][a-z]+,\s*[A-Z]{2})\b', text)
        jobs.append(make_job(title, "General", loc_m.group(1) if loc_m else "",
                             urljoin(jobs_url, a["href"]), company))
    if jobs:
        log.info(f"    HTML heuristic: {len(jobs)} jobs")
        return jobs
    for a in soup.find_all("a", href=True):
        title = a.get_text(strip=True)
        if (5 < len(title) < 100 and
                re.search(r'(engineer|manager|designer|analyst|director|specialist|'
                           r'coordinator|lead|head of|vp |senior|junior|intern|'
                           r'developer|scientist|recruiter|executive|associate)', title, re.I)):
            full_url = urljoin(jobs_url, a["href"])
            if full_url not in seen:
                seen.add(full_url)
                jobs.append(make_job(title, "General", "", full_url, company))
    log.info(f"    Link scan: {len(jobs)} jobs")
    return jobs[:200]


def scrape_generic(jobs_url, company):
    if not jobs_url:
        return []
    html = fetch_html(jobs_url, use_playwright=True)
    if not html:
        return []
    if "ashby_jid=" in html:
        return scrape_ashby_embedded(jobs_url, company)
    soup = BeautifulSoup(html, "html.parser")
    jobs, seen = [], set()
    # schema.org
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            d = json.loads(script.string or "")
            for item in (d if isinstance(d, list) else [d]):
                if item.get("@type") == "JobPosting":
                    loc_obj = item.get("jobLocation") or {}
                    addr = loc_obj.get("address") or {}
                    loc = addr.get("addressLocality", "") if isinstance(addr, dict) else ""
                    jobs.append(make_job(item.get("title", ""),
                                        item.get("occupationalCategory", "General"),
                                        loc, item.get("url", jobs_url), company))
        except Exception:
            pass
    if jobs:
        log.info(f"    Schema.org: {len(jobs)} jobs")
        return jobs
    # CSS class heuristic
    for container in soup.find_all(class_=re.compile(
            r"job|position|role|opening|listing|posting|career|vacancy", re.I))[:150]:
        a = container.find("a", href=True)
        if not a:
            continue
        title = a.get_text(strip=True)
        if not title or len(title) < 3 or title in seen:
            continue
        seen.add(title)
        text = container.get_text(" ", strip=True)
        loc_m = re.search(r'\b(Remote|New York|San Francisco|Los Angeles|Austin|'
                           r'London|Chicago|Seattle|Boston|Denver|[A-Z][a-z]+,\s*[A-Z]{2})\b', text)
        jobs.append(make_job(title, "General", loc_m.group(1) if loc_m else "",
                             urljoin(jobs_url, a["href"]), company))
    if jobs:
        log.info(f"    HTML heuristic: {len(jobs)} jobs")
        return jobs
    # Last resort: keyword links
    for a in soup.find_all("a", href=True):
        title = a.get_text(strip=True)
        if (5 < len(title) < 100 and
                re.search(r'(engineer|manager|designer|analyst|director|specialist|'
                           r'coordinator|lead|head of|vp |senior|junior|intern|'
                           r'developer|scientist|recruiter|executive|associate)', title, re.I)):
            full_url = urljoin(jobs_url, a["href"])
            if full_url not in seen:
                seen.add(full_url)
                jobs.append(make_job(title, "General", "", full_url, company))
    log.info(f"    Link scan: {len(jobs)} jobs")
    return jobs[:200]


def route_to_scraper(ats, slug, name, jobs_url):
    if ats == "greenhouse" and slug:
        return scrape_greenhouse(slug, name, jobs_url)
    elif ats == "lever" and slug:
        return scrape_lever(slug, name, jobs_url)
    elif ats == "ashby" and slug:
        return scrape_ashby(slug, name, jobs_url)
    elif ats in ("ashby_embedded", "ashby") and not slug:
        return scrape_ashby_embedded(jobs_url, name)
    elif ats == "workable" and slug:
        return scrape_workable(slug, name, jobs_url)
    elif ats == "yc" and slug:
        return scrape_yc(slug, name, jobs_url)
    elif ats == "rippling" and slug:
        return scrape_rippling(slug, name, jobs_url)
    else:
        return scrape_generic(jobs_url, name)


# ── MAIN COMPANY SCRAPER ──────────────────────────────────────────────────────

def scrape_company(company):
    name = company["name"]
    website = company.get("website", "").rstrip("/")

    # Step 1: use hardcoded override if we have one — it's always more reliable
    override = KNOWN_ATS.get(name.lower())
    if override:
        ats, slug, jobs_url = override
        log.info(f"  → Override: {jobs_url} | ATS: {ats} | Slug: {slug}")
        jobs = route_to_scraper(ats, slug, name, jobs_url)
        if jobs:
            for j in jobs: j["company_website"] = website
            return jobs, None
        log.info(f"  → Override got 0 jobs, falling back to auto-detection...")

    # Step 2: auto-detection
    if not website:
        return [], "no website"

    jobs_url, ats, slug, html = find_jobs_page(website)
    if jobs_url and ats:
        log.info(f"  → Auto-detected: {jobs_url} | ATS: {ats} | Slug: {slug or 'n/a'}")
        jobs = route_to_scraper(ats, slug, name, jobs_url)
        for j in jobs: j["company_website"] = website
        return jobs, None

    if jobs_url:
        log.info(f"  → Auto-detected page (no ATS): {jobs_url}")
        jobs = scrape_generic(jobs_url, name)
        for j in jobs: j["company_website"] = website
        return jobs, None

    return [], "no jobs page found"


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info(f"Burst Capital Jobs Scraper — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    try:
        with open(COMPANIES_FILE) as f:
            companies = [c for c in json.load(f) if c.get("website") and not c.get("_note")]
        log.info(f"Loaded {len(companies)} companies from {COMPANIES_FILE}")
    except FileNotFoundError:
        log.error(f"{COMPANIES_FILE} not found.")
        return

    all_jobs, failed = [], []

    for i, company in enumerate(companies, 1):
        log.info(f"\n[{i}/{len(companies)}] {company['name']} — {company.get('website','')}")
        try:
            jobs, error = scrape_company(company)
            all_jobs.extend(jobs)
            if error:
                failed.append({"name": company["name"], "reason": error})
        except Exception as e:
            log.warning(f"  ✗ Error: {e}")
            failed.append({"name": company["name"], "reason": str(e)})
        time.sleep(DELAY_BETWEEN_COMPANIES)

    companies_with_jobs = len(set(j["company"] for j in all_jobs))
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_jobs": len(all_jobs),
        "total_companies": len(companies),
        "companies_with_jobs": companies_with_jobs,
        "failed_companies": failed,
        "jobs": all_jobs,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    log.info(f"\n{'=' * 60}")
    log.info(f"Done! {len(all_jobs)} jobs across {companies_with_jobs} companies")
    if failed:
        log.info(f"{len(failed)} companies had no listings or errors")


if __name__ == "__main__":
    main()
