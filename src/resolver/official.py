import json
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from src.utils import clean_url, domain, root_domain

UA = "remote4me-india-job-engine/4.0 (+https://remote4.me)"
CAREER_HINTS = (
    "career", "careers", "jobs", "join-us", "joinus", "work-with-us",
    "opportunities", "vacancies", "job-openings", "open-positions", "apply",
)
ATS_HINTS = (
    "greenhouse", "lever", "ashby", "workable", "workday", "smartrecruiters",
    "icims", "successfactors", "jobvite", "teamtailor", "bamboohr", "recruitee",
    "jazzhr", "breezy", "pinpointhq", "personio", "oraclecloud", "myworkdayjobs",
)
DISCOVERY_DOMAINS = {
    "linkedin.com", "indeed.com", "naukri.com", "foundit.in", "shine.com", "timesjobs.com",
    "cutshort.io", "instahyre.com", "wellfound.com", "remoterocketship.com", "jobicy.com",
    "onjob.io", "devglobaljobs.com", "remotejobs.org", "yubhub.co", "weworkremotely.com",
    "remotive.com", "himalayas.app", "jobvetta.com", "glassdoor.co.in", "glassdoor.com",
    "ziprecruiter.com", "simplyhired.com", "jooble.org", "talent.com", "adzuna.com",
}


def fetch(url, timeout=20):
    try:
        r = requests.get(
            url,
            headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"},
            timeout=timeout,
            allow_redirects=True,
        )
        if r.status_code >= 400:
            return None
        return r
    except Exception:
        return None


def _is_discovery_domain(url):
    d = root_domain(url)
    return d in DISCOVERY_DOMAINS


def _candidate_score(href, text="", source_domain=""):
    href_low = href.lower()
    text_low = text.lower()
    score = 0
    if any(x in text_low for x in ("apply", "apply now", "view job", "job details")):
        score += 8
    if any(x in href_low for x in CAREER_HINTS):
        score += 5
    if any(x in href_low for x in ATS_HINTS):
        score += 6
    if source_domain and root_domain(href) != source_domain:
        score += 4
    if _is_discovery_domain(href):
        score -= 20
    if href_low.startswith("mailto:") or href_low.startswith("javascript:"):
        score -= 50
    return score


def _jsonld_urls(soup):
    urls = []
    for tag in soup.find_all("script", type="application/ld+json"):
        raw = tag.string or tag.get_text()
        try:
            data = json.loads(raw)
        except Exception:
            continue
        stack = data if isinstance(data, list) else [data]
        for item in stack:
            if not isinstance(item, dict):
                continue
            if item.get("@type") == "JobPosting" and item.get("url"):
                urls.append(item["url"])
            if item.get("mainEntity") and isinstance(item["mainEntity"], dict):
                u = item["mainEntity"].get("url")
                if u:
                    urls.append(u)
    return urls


def links_from_page(url, html):
    soup = BeautifulSoup(html, "html.parser")
    source_domain = root_domain(url)
    candidates = []

    for raw in _jsonld_urls(soup):
        href = clean_url(urljoin(url, raw))
        if href and not _is_discovery_domain(href):
            candidates.append((12, href))

    for a in soup.find_all("a", href=True):
        href = clean_url(urljoin(url, a.get("href")))
        if not href:
            continue
        text = " ".join(a.stripped_strings)
        score = _candidate_score(href, text, source_domain)
        if score > 0:
            candidates.append((score, href))

    candidates.sort(key=lambda x: x[0], reverse=True)
    return list(dict.fromkeys(href for _, href in candidates))[:60]


def likely_official(url):
    d = root_domain(url)
    return bool(d) and d not in DISCOVERY_DOMAINS


def resolve(discovery_url, company="", company_website=""):
    """Resolve a discovered listing to a current employer/ATS URL when possible.

    This only requests public pages and does not bypass login, CAPTCHA, robots blocks,
    paywalls, or anti-bot protections.
    """
    if not discovery_url:
        return None

    discovery_url = clean_url(discovery_url)
    if not discovery_url:
        return None

    # Already on an employer/ATS domain.
    if likely_official(discovery_url) and any(
        h in discovery_url.lower() for h in CAREER_HINTS + ATS_HINTS
    ):
        r = fetch(discovery_url)
        if r and r.url and likely_official(r.url):
            return clean_url(r.url)

    # Inspect the discovered job page for an external employer/ATS application link.
    r = fetch(discovery_url)
    if r:
        candidates = links_from_page(r.url, r.text)
        if candidates:
            return candidates[0]

    # If a structured company website was supplied by a source, inspect it for careers.
    if company_website:
        site = clean_url(company_website)
        if site and likely_official(site):
            rr = fetch(site)
            if rr:
                candidates = links_from_page(rr.url, rr.text)
                if candidates:
                    return candidates[0]

    return None
