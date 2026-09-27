import os
import re
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

from src.utils import clean_url, domain, root_domain, sleep_delay

UA = "remote4me-india-job-engine/5.1 (+https://remote4.me)"
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

ATS_DOMAINS = {
    "greenhouse": {"boards.greenhouse.io", "job-boards.greenhouse.io"},
    "lever": {"jobs.lever.co"},
    "ashby": {"jobs.ashbyhq.com"},
    "workable": {"apply.workable.com", "jobs.workable.com"},
}

CAREER_PATHS = (
    "/careers", "/career", "/jobs", "/join-us", "/joinus", "/work-with-us",
    "/opportunities", "/job-openings", "/open-positions", "/vacancies",
)


def _get(url):
    try:
        r = requests.get(
            url,
            headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/json"},
            timeout=TIMEOUT,
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return None
        return r
    except Exception:
        return None


def _ats_from_url(url):
    d = domain(url)
    p = urlparse(url)
    parts = [x for x in p.path.split("/") if x]
    for ats, domains in ATS_DOMAINS.items():
        if d in domains:
            return ats, parts[0] if parts else ""
    # Greenhouse custom job-board URLs occasionally appear under job-boards.
    if d.endswith("greenhouse.io") and parts:
        return "greenhouse", parts[0]
    return "", ""


def _extract_ats_links(base_url, html):
    soup = BeautifulSoup(html, "html.parser")
    found = []
    for a in soup.find_all("a", href=True):
        href = clean_url(urljoin(base_url, a.get("href", "")))
        if not href:
            continue
        ats, board = _ats_from_url(href)
        if ats and board:
            found.append((ats, board, href))
    # Some sites expose ATS URLs in script text rather than anchors.
    raw = html or ""
    patterns = [
        ("greenhouse", r"https?://(?:job-boards|boards)\.greenhouse\.io/([A-Za-z0-9_-]+)"),
        ("lever", r"https?://jobs\.lever\.co/([A-Za-z0-9_-]+)"),
        ("ashby", r"https?://jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)"),
        ("workable", r"https?://(?:apply|jobs)\.workable\.com/([A-Za-z0-9_-]+)"),
    ]
    for ats, pattern in patterns:
        for board in re.findall(pattern, raw, flags=re.I):
            found.append((ats, board, ""))
    return found


def detect_ats(url, html=None):
    url = clean_url(url)
    ats, board = _ats_from_url(url)
    if ats and board:
        return ats, board, url
    if html:
        links = _extract_ats_links(url, html)
        if links:
            ats, board, href = links[0]
            return ats, board, href
    return "", "", ""


def discover_career_url(company_url):
    company_url = clean_url(company_url)
    if not company_url:
        return ""
    r = _get(company_url)
    if r:
        ats, board, href = detect_ats(r.url, r.text)
        if ats:
            return href or r.url
        soup = BeautifulSoup(r.text, "html.parser")
        candidates = []
        for a in soup.find_all("a", href=True):
            href = clean_url(urljoin(r.url, a.get("href", "")))
            if not href:
                continue
            text = " ".join(a.stripped_strings).lower()
            low = href.lower()
            score = 0
            if any(k in text for k in ("careers", "career", "jobs", "join us", "work with us", "opportunities")):
                score += 5
            if any(k in low for k in CAREER_PATHS):
                score += 4
            if root_domain(href) != root_domain(r.url):
                score -= 2
            if score > 0:
                candidates.append((score, href))
        candidates.sort(reverse=True)
        if candidates:
            return candidates[0][1]
    parsed = urlparse(company_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    for path in CAREER_PATHS:
        candidate = origin + path
        rr = _get(candidate)
        if rr:
            return clean_url(rr.url)
        sleep_delay()
    return ""


def _job(title, location, description, job_url, apply_url, company, posted="", remote=False, country_code="", employment_type="", source="ats"):
    return {
        "url": clean_url(job_url or apply_url),
        "title": (title or "").strip(),
        "snippet": (description or "")[:1000],
        "description": description or "",
        "company": company or "",
        "location": location or "",
        "country_code": country_code or "",
        "remote": bool(remote),
        "work_model": "Remote" if remote else "",
        "posted": posted or "",
        "employment_type": employment_type or "",
        "salary": "",
        "company_website": "",
        "source": source,
        "source_url": clean_url(job_url or apply_url),
        "apply_url": clean_url(apply_url or job_url),
    }


def crawl_greenhouse(board, company):
    api = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
    r = _get(api)
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    out = []
    for j in data.get("jobs", []):
        loc = (j.get("location") or {}).get("name", "") if isinstance(j.get("location"), dict) else str(j.get("location") or "")
        content = j.get("content") or ""
        out.append(_job(j.get("title"), loc, BeautifulSoup(content, "html.parser").get_text(" ", strip=True),
                        j.get("absolute_url"), j.get("absolute_url"), company,
                        j.get("updated_at") or j.get("created_at"),
                        "remote" in (loc + " " + content).lower(),
                        employment_type=j.get("metadata") and "" or "", source="greenhouse"))
    return out


def crawl_lever(site, company):
    api = f"https://api.lever.co/v0/postings/{site}?mode=json"
    r = _get(api)
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    out = []
    for j in data if isinstance(data, list) else []:
        cats = j.get("categories") or {}
        loc = cats.get("location", "") if isinstance(cats, dict) else ""
        desc = BeautifulSoup(j.get("descriptionPlain") or j.get("description") or "", "html.parser").get_text(" ", strip=True)
        workplace = j.get("workplaceType") or ""
        out.append(_job(j.get("text"), loc, desc, j.get("hostedUrl"), j.get("applyUrl") or j.get("hostedUrl"), company,
                        j.get("createdAt") or j.get("updatedAt"),
                        str(workplace).lower() == "remote" or "remote" in (loc + " " + desc).lower(), source="lever"))
    return out


def crawl_ashby(board, company):
    api = f"https://api.ashbyhq.com/posting-api/job-board/{board}"
    r = _get(api)
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    out = []
    for j in data.get("jobs", []):
        loc = j.get("location", "") or ""
        secondary = " ".join(x.get("location", "") for x in (j.get("secondaryLocations") or []) if isinstance(x, dict))
        out.append(_job(j.get("title"), " | ".join(x for x in [loc, secondary] if x), "",
                        j.get("jobUrl"), j.get("applyUrl") or j.get("jobUrl"), company,
                        "", "remote" in (loc + " " + secondary).lower(), source="ashby"))
    return out


def crawl_workable(company_slug, company):
    # Workable exposes a public careers endpoint used by its hosted job boards.
    api = f"https://www.workable.com/api/accounts/{company_slug}?details=true"
    r = _get(api)
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    jobs = data.get("jobs", []) if isinstance(data, dict) else []
    out = []
    for j in jobs:
        loc = j.get("location") or {}
        if isinstance(loc, dict):
            loc_text = loc.get("location_str") or ", ".join(x for x in [loc.get("city"), loc.get("region"), loc.get("country")] if x)
            cc = loc.get("country_code") or ""
            workplace = loc.get("workplace_type") or ""
        else:
            loc_text, cc, workplace = str(loc), "", ""
        desc = BeautifulSoup(j.get("description") or j.get("description_html") or "", "html.parser").get_text(" ", strip=True)
        url = j.get("url") or j.get("shortlink") or ""
        apply = j.get("application_url") or j.get("shortlink") or url
        out.append(_job(j.get("title") or j.get("full_title"), loc_text, desc, url, apply, company,
                        j.get("created_at"), workplace.lower() == "remote" or bool(loc.get("telecommuting")) if isinstance(loc, dict) else False,
                        country_code=cc, employment_type="", source="workable"))
    return out


def crawl(ats, board, company):
    if not ats or not board:
        return []
    if ats == "greenhouse":
        return crawl_greenhouse(board, company)
    if ats == "lever":
        return crawl_lever(board, company)
    if ats == "ashby":
        return crawl_ashby(board, company)
    if ats == "workable":
        return crawl_workable(board, company)
    return []


def discover_and_crawl(company, domain_url, career_url="", ats="", board=""):
    # COMPANIES stores the reusable ATS board URL. Normalize it back to a board
    # slug before calling the public ATS endpoint on the next run.
    if board and str(board).startswith(("http://", "https://")):
        detected_ats, detected_board, detected_url = _ats_from_url(board)
        if detected_ats:
            ats, board = detected_ats, detected_board
            career_url = career_url or detected_url
    candidates = [x for x in [career_url, domain_url] if x]
    seen = set()
    detected = (ats, board, career_url)
    for candidate in candidates:
        candidate = clean_url(candidate)
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        a, b, href = detect_ats(candidate)
        if a:
            detected = (a, b, href or candidate)
            break
        r = _get(candidate)
        if r:
            a, b, href = detect_ats(r.url, r.text)
            if a:
                detected = (a, b, href or r.url)
                break
        sleep_delay()
    if not detected[0] and domain_url:
        found = discover_career_url(domain_url)
        if found:
            a, b, href = detect_ats(found)
            if a:
                detected = (a, b, href or found)
    a, b, href = detected
    if not a or not b:
        return detected, []
    jobs = crawl(a, b, company)
    return detected, jobs
