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

# Companies to skip entirely — no current openings or retired
SKIP_COMPANIES = {
    "prenda",
    "optimoroute",
    "prairie health (acquired by carbon health)",
    "next insurance",
    "ergo next",
    "next insurance (acquired by munichre)",
}

# Hardcoded overrides — ONLY used when auto-detection returns nothing.
# Key = exact company name (lowercase). Value = (ats, slug, jobs_url).
KNOWN_ATS = {
    "faire":                         ("greenhouse", "Faire",        "https://boards.greenhouse.io/faire"),
    "superhuman":                    ("ashby", "Superhuman%20Platform%20Inc", "https://jobs.ashbyhq.com/Superhuman%20Platform%20Inc"),
    "instawork":                     ("greenhouse", "instawork",    "https://boards.greenhouse.io/instawork"),
    "stress-free auto care":         ("generic",    None,           "https://careers.stressfree.com"),
    "handshake":                     ("ashby_embedded", None,       "https://joinhandshake.com/careers/"),
    "lattice":                       ("greenhouse", "lattice",      "https://boards.greenhouse.io/lattice"),
    "ada":                           ("greenhouse", "ada18",        "https://job-boards.greenhouse.io/ada18"),
    "adquick":                       ("greenhouse", "adquick",      "https://boards.greenhouse.io/adquick"),
    "cambly":                        ("greenhouse", "cambly",       "https://boards.greenhouse.io/cambly"),
    "curri":                         ("ashby",      "curri",        "https://jobs.ashbyhq.com/curri"),
    "genius ai":                     ("greenhouse", "glossgenius",  "https://boards.greenhouse.io/glossgenius"),
    "hipcamp":                       ("greenhouse", "hipcamp",      "https://boards.greenhouse.io/hipcamp"),
    "owner":                         ("ashby",      "owner",        "https://jobs.ashbyhq.com/owner"),
    "bounce":                        ("ashby_embedded", None,       "https://jobs.ashbyhq.com/Bounce"),
    "lily":                          ("greenhouse", "lilyai",       "https://boards.greenhouse.io/lilyai"),
    "medely":                        ("ashby",      "medely",       "https://jobs.ashbyhq.com/medely"),
    "padlet":                        ("ashby_embedded", None,       "https://padlet.jobs/open-roles"),
    "workstream":                    ("greenhouse", "workstream",   "https://boards.greenhouse.io/workstream"),
    "wonderschool":                  ("ashby",      "wonderschool", "https://jobs.ashbyhq.com/wonderschool"),
    "ava":                           ("ashby",      "ava",          "https://jobs.ashbyhq.com/ava"),
    "resortpass":                    ("greenhouse", "resortpass",   "https://boards.greenhouse.io/resortpass"),
    "overflow":                      ("lever",      "overflow",     "https://jobs.lever.co/overflow"),
    "huckleberry":                   ("lever",      "huckleberry",  "https://jobs.lever.co/huckleberry"),
    "goodtime":                      ("lever",      "goodtime",     "https://jobs.lever.co/goodtime"),
    "sourcegraph":                   ("greenhouse", "sourcegraph91","https://boards.greenhouse.io/sourcegraph91"),
    "trendsi":                       ("lever",      "trendsi",      "https://jobs.lever.co/trendsi"),
    "lilo":                          ("lever",      "lilo",         "https://jobs.lever.co/lilo"),
    "cardinal":                      ("yc",         "trycardinal-ai", "https://www.ycombinator.com/companies/trycardinal-ai/jobs"),
    "onshore":                          ("generic",    None,           "https://careers.kula.ai/onshore"),
    "liftoff":                       ("generic",    None,           "https://www.onliftoff.com/about-us"),
    "willow":                        ("yc",         "willow",       "https://www.ycombinator.com/companies/willow/jobs"),
    "namespace":                     ("yc",         "namespace",    "https://www.ycombinator.com/companies/namespace/jobs"),
    "sibli":                         ("yc",         "sibli",        "https://www.ycombinator.com/companies/sibli/jobs"),
    "allstripes (acquired by picnic health)": ("greenhouse", "picnichealth",  "https://boards.greenhouse.io/picnichealth"),
    "northstar (acquired by nayya health)":   ("greenhouse", "nayya",         "https://boards.greenhouse.io/nayya"),
    "justworks": ("greenhouse", "justworks",     "https://boards.greenhouse.io/justworks"),
    "yik yak (acquired by sidechat)":         ("greenhouse", "sidechat",      "https://boards.greenhouse.io/sidechat"),
    "setter.com (acquired by thumbtack)":     ("greenhouse", "thumbtackjobs", "https://boards.greenhouse.io/thumbtackjobs"),
    "assemble (acquired by deel)":            ("ashby",      "deel",          "https://jobs.ashbyhq.com/deel"),
    "cut+dry":                               ("jazzhr",     "cutanddry",     "https://cutanddry.applytojob.com/apply"),
    "commenda":                              ("commenda",   None,            "https://www.commenda.io/careers"),
    "daisy":                                 ("careerplug", "daisy-co-careers", "https://daisy-co-careers.careerplug.com/jobs"),
    "carebrain":                             ("greenhouse", "carebrain",     "https://boards.greenhouse.io/carebrain"),
    "resq":                                  ("ashby",      "ResQ",          "https://jobs.ashbyhq.com/ResQ"),
    "standard fleet":                        ("ashby",      "StandardFleet", "https://jobs.ashbyhq.com/StandardFleet"),
    "float health":                          ("breezy",     "float-health",  "https://float-health.breezy.hr"),
    "canopy":                                ("ashby_embedded", None,        "https://www.canopyworks.com/about"),
    "cabana":                                ("generic",    None,           "https://cabanapools.com/jobs-careers/"),
    "honey health":                          ("notion",     None,           "https://cedar-brain-7ce.notion.site/25d95cfecb688015b117d3c625021a36?v=25d95cfecb68803ea201000cfecd578d"),
    "pickle":                                ("generic",    None,           "https://www.shoponpickle.com/careers"),
    "emanate":                               ("ashby",      "emanate",       "https://jobs.ashbyhq.com/emanate"),
    "mintmcp":                               ("ashby",      "mintmcp",       "https://jobs.ashbyhq.com/mintmcp"),
    "hostie":                                ("rippling",   "hostie",        "https://ats.rippling.com/hostie/jobs"),
}


# ── HELPERS ───────────────────────────────────────────────────────────────────

def fetch_html(url, use_playwright=False):
    if use_playwright and HAS_PLAYWRIGHT:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(extra_http_headers=HEADERS)
                page.goto(url, wait_until="domcontentloaded", timeout=15000)
                page.wait_for_timeout(2500)
                # Scroll and click "load/show more" so lazy-loaded roles all render
                last_h = 0
                for _ in range(15):
                    page.mouse.wheel(0, 4000)
                    page.wait_for_timeout(600)
                    for lbl in ("Load more", "Show more", "See more", "View more", "Show all", "View all"):
                        try:
                            btn = page.get_by_role("button", name=re.compile(lbl, re.I)).first
                            if btn.is_visible(timeout=200):
                                btn.click(timeout=1500)
                                page.wait_for_timeout(800)
                        except Exception:
                            pass
                    h = page.evaluate("document.body.scrollHeight")
                    if h == last_h:
                        break
                    last_h = h
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
    """Embedded Ashby board. Reads BOTH the raw HTML and the browser-rendered page
    and uses whichever contains more unique jobs (raw HTML often has the full list)."""
    UUID = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    LOC = re.compile(r'\b(Remote|New York|San Francisco|Los Angeles|Austin|London|Chicago|'
                     r'Seattle|Boston|Denver|NYC|SF|Portland|Atlanta|Miami|Washington|'
                     r'Philadelphia|Toronto|Vancouver|Berlin|Paris|Amsterdam|Portugal|Tokyo|'
                     r'Mountain View|Palo Alto|Dublin|Sydney|Bangalore)\b', re.I)

    def parse(html):
        soup = BeautifulSoup(html, "html.parser")
        found = {}
        for a in soup.find_all("a", href=True):
            m = re.search(r'(?:ashby_jid=|ashbyhq\.com/[^/]+/)(' + UUID + ')', a["href"])
            if not m or m.group(1) in found:
                continue
            if a["href"].rstrip("/").endswith("/application"):
                continue
            parts = [p for p in a.stripped_strings if p.lower() not in ("view", "apply", "apply now")]
            title = parts[0] if parts else ""
            if len(title) < 3:
                continue
            row = a.find_parent(["li", "tr", "div"]) or a
            text = row.get_text(" ", strip=True)
            loc = ", ".join(parts[1:]) if len(parts) > 1 else ""
            if not loc:
                lm = LOC.search(text.replace(title, "", 1))
                loc = lm.group(1) if lm else ""
            found[m.group(1)] = (title, loc)
        return found

    raw = None
    try:
        r = requests.get(jobs_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            raw = r.text
    except Exception as e:
        log.warning(f"    raw fetch failed: {e}")
    rendered = fetch_html(jobs_url, use_playwright=True)

    best, best_html = {}, raw or rendered or ""
    for h in (raw, rendered):
        if h:
            f = parse(h)
            if len(f) > len(best):
                best, best_html = f, h
    if not best:
        # Pattern 2: Ashby-hosted board links on the page itself
        soup = BeautifulSoup(best_html, "html.parser") if best_html else None
        slug_m = re.search(r'ashbyhq\.com/([^/]+)', jobs_url)
        slug = slug_m.group(1) if slug_m else None
        jobs, seen = [], set()
        if soup:
            for a in soup.find_all("a", href=True):
                full = urljoin(jobs_url, a["href"])
                if slug and f"ashbyhq.com/{slug}/" not in full:
                    continue
                if not re.search(r'/' + UUID, full):
                    continue
                title = a.get_text(strip=True)
                if len(title) < 3 or full in seen:
                    continue
                seen.add(full)
                lm = LOC.search(a.find_parent().get_text(" ", strip=True) if a.find_parent() else "")
                jobs.append(make_job(title, "General", lm.group(1) if lm else "", full, company))
        log.info(f"    Ashby embedded: {len(jobs)} jobs")
        return jobs

    slug_m = re.search(r'ashbyhq\.com/([A-Za-z0-9_%.-]+)/' + UUID, best_html)
    slug = slug_m.group(1) if slug_m else None
    jobs = []
    for uid, (title, loc) in best.items():
        url = f"https://jobs.ashbyhq.com/{slug}/{uid}" if slug else f"https://app.ashbyhq.com/jobs/{uid}"
        jobs.append(make_job(title, "General", loc, url, company))
    log.info(f"    Ashby embedded: {len(jobs)} jobs (slug={slug})")
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


def scrape_jazzhr(slug, company, fallback_url):
    """Scrape jobs from JazzHR (applytojob.com) via HTML parsing."""
    url = fallback_url or f"https://{slug}.applytojob.com/apply"
    try:
        html = fetch_html(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        jobs, seen = [], set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # JazzHR job links follow pattern: /apply/SLUG/Job-Title
            if "applytojob.com/apply/" not in href:
                continue
            parts = href.rstrip("/").split("/")
            if len(parts) < 6:  # needs at least the job slug segment
                continue
            title = a.get_text(strip=True)
            if not title or title in seen or len(title) < 3:
                continue
            seen.add(title)
            parent = a.find_parent()
            text = parent.get_text(" ", strip=True) if parent else ""
            loc_m = re.search(r'\b(Remote|New York|San Francisco|Los Angeles|Austin|'
                               r'London|Chicago|Seattle|Boston|Denver|NYC|SF)\b', text, re.I)
            jobs.append(make_job(title, "General", loc_m.group(1) if loc_m else "",
                                 href, company))
        log.info(f"    JazzHR: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        log.warning(f"    JazzHR failed: {e}")
        return []



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
        key = (title, a["href"])
        if not title or len(title) < 3 or key in seen:
            continue
        seen.add(key)
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


def scrape_commenda(jobs_url, company):
    """Scrape Commenda's custom careers page (open-roles section only)."""
    BAD_HOST = re.compile(r'linkedin|businessinsider|techcrunch|forbes|bloomberg|reuters|'
                          r'medium\.com|twitter|x\.com|youtube|press|news|blog|crunchbase', re.I)
    BAD_TITLE = re.compile(r'^(see|view|browse)\b.*\broles?\b|open roles|raises|\$\d|'
                           r'I left|Business Insider|LinkedIn', re.I)
    try:
        html = fetch_html(jobs_url, use_playwright=True)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        roots = [soup.find(id=re.compile(r'open-?roles', re.I))] + soup.find_all(class_="jobs-list")
        roots = [r for r in roots if r] or [soup]
        jobs, seen = [], set()
        for root in roots:
            for a in root.find_all("a", href=True):
                href = a["href"].strip()
                if not href or href.startswith(("#", "mailto:", "javascript:")):
                    continue
                full = urljoin(jobs_url, href)
                if BAD_HOST.search(full.split("?")[0]):
                    continue
                parts = [p for p in a.stripped_strings
                         if not re.fullmatch(r'[\u2192\u2197\u2198→↗➜>]+', p)]
                if not parts:
                    continue
                title = parts[0].strip()
                loc = ", ".join(parts[1:]).strip()
                if len(parts) == 1:
                    m = re.match(r'^(.+?[a-z\)])((?:[A-Z][a-z]+(?: [A-Z][a-z]+)*)'
                                 r'(?:,\s*[A-Z][a-z]+(?: [A-Z][a-z]+)*)*)$', title)
                    if m:
                        title, loc = m.group(1).strip(), m.group(2).strip()
                if loc.lower() in ("not specified", ""):
                    loc = ""
                if len(title) < 3 or BAD_TITLE.search(title):
                    continue
                key = (title, full)
                if key in seen:
                    continue
                seen.add(key)
                jobs.append(make_job(title, "General", loc, full, company))
        log.info(f"    Commenda custom: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        log.warning(f"    Commenda scraper failed: {e}")
        return []


def scrape_notion(jobs_url, company):
    """Scrape a public Notion database (table/list/gallery) used as a careers board."""
    if not HAS_PLAYWRIGHT:
        return []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(extra_http_headers=HEADERS, viewport={"width": 1400, "height": 1000})
            page.goto(jobs_url, wait_until="domcontentloaded", timeout=30000)
            try:
                page.wait_for_selector("a[href*='-'], .notion-collection-item, .notion-table-view", timeout=15000)
            except Exception:
                pass
            page.wait_for_timeout(4000)
            for _ in range(8):
                page.mouse.wheel(0, 3000)
                page.wait_for_timeout(500)
            rows = page.evaluate("""() => {
                const out = [];
                const root = new URL(location.href);
                const dbid = (root.pathname.match(/[0-9a-f]{32}/) || [''])[0];
                document.querySelectorAll('a[href]').forEach(a => {
                    const m = a.getAttribute('href').match(/[0-9a-f]{32}/);
                    if (!m || m[0] === dbid) return;
                    const row = a.closest('.notion-collection-item, [role=row], tr') || a.parentElement;
                    const cells = Array.from(row.querySelectorAll('[class*=cell], td, [role=gridcell]'))
                        .map(c => c.innerText.trim()).filter(Boolean);
                    out.push({href: a.href, title: a.innerText.trim(), cells: cells,
                              text: (row.innerText || '').trim()});
                });
                return out;
            }""")
            browser.close()
        jobs, seen = [], set()
        for r in rows:
            title = (r["title"] or "").split("\n")[0].strip()
            if len(title) < 3 or r["href"] in seen:
                continue
            seen.add(r["href"])
            others = [c for c in r["cells"] if c != title and c.split("\n")[0] != title]
            loc = ""
            for c in others:
                if re.search(r'remote|,\s*[A-Z]{2}\b|new york|san francisco|seattle|austin|boston|london|chicago|nyc|sf', c, re.I):
                    loc = c.replace("\n", ", ")
                    break
            jobs.append(make_job(title, "General", loc, r["href"], company))
        log.info(f"    Notion: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        log.warning(f"    Notion scraper failed: {e}")
        return []


def scrape_careerplug(slug, company, fallback_url):
    """Scrape CareerPlug jobs board via HTML — jobs are in static HTML with aria-labels."""
    url = fallback_url or f"https://{slug}.careerplug.com/jobs"
    try:
        html = fetch_html(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        jobs, seen = [], set()
        for a in soup.find_all("a", attrs={"aria-label": True}, href=True):
            label = a["aria-label"]  # format: "Job Title in City, ST"
            href = a["href"]
            if not href.startswith("/jobs/"):
                continue
            # Parse title and location from aria-label
            loc_m = re.search(r'\bin\s+(.+)$', label)
            if loc_m:
                title = label[:loc_m.start()].strip()
                loc = loc_m.group(1).strip()
            else:
                title = label.strip()
                loc = ""
            if not title or title in seen or len(title) < 3:
                continue
            seen.add(title + href)  # allow same title in different locations
            full_url = f"https://{slug}.careerplug.com{href}"
            jobs.append(make_job(title, "General", loc, full_url, company))
        log.info(f"    CareerPlug: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        log.warning(f"    CareerPlug failed: {e}")
        return []


def scrape_breezy(slug, company, fallback_url):
    """Scrape Breezy HR jobs via their API."""
    try:
        r = requests.get(f"https://{slug}.breezy.hr/json",
                         headers=HEADERS, timeout=REQUEST_TIMEOUT)
        data = r.json()
        positions = data if isinstance(data, list) else data.get("positions", [])
        jobs = []
        for j in positions:
            title = j.get("name", "")
            loc = j.get("location", {})
            if isinstance(loc, dict):
                loc = loc.get("name", loc.get("city", ""))
            dept = j.get("department", {})
            if isinstance(dept, dict):
                dept = dept.get("name", "General")
            job_url = f"https://{slug}.breezy.hr/p/{j.get('friendly_id', '')}"
            jobs.append(make_job(title, dept or "General", loc or "", job_url, company))
        log.info(f"    Breezy HR: {len(jobs)} jobs")
        return jobs
    except Exception as e:
        log.warning(f"    Breezy HR failed: {e}")
        return scrape_generic(fallback_url, company)


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
    elif ats == "jazzhr":
        return scrape_jazzhr(slug, name, jobs_url)
    elif ats == "notion":
        return scrape_notion(jobs_url, name)
    elif ats == "commenda":
        return scrape_commenda(jobs_url, name)
    elif ats == "careerplug":
        return scrape_careerplug(slug, name, jobs_url)
    elif ats == "breezy":
        return scrape_breezy(slug, name, jobs_url)
    else:
        return scrape_generic(jobs_url, name)


# ── MAIN COMPANY SCRAPER ──────────────────────────────────────────────────────

def scrape_company(company):
    name = company["name"]
    website = company.get("website", "").rstrip("/")

    # Skip companies with no current openings
    if name.lower() in SKIP_COMPANIES:
        log.info(f"  → Skipped (no openings)")
        return [], None

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
