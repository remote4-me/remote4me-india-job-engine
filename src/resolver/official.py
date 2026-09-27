import json
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from src.utils import clean_url, root_domain

UA = "remote4me-india-job-engine/6.0 (+https://remote4.me)"
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
ATS_DOMAINS = {
    "boards.greenhouse.io", "job-boards.greenhouse.io", "jobs.lever.co", "jobs.ashbyhq.com",
    "apply.workable.com", "jobs.workable.com", "myworkdayjobs.com", "smartrecruiters.com",
    "icims.com", "successfactors.com", "oraclecloud.com", "jobvite.com", "teamtailor.com",
    "bamboohr.com", "recruitee.com", "jazz.co", "breezy.hr", "pinpointhq.com", "personio.com",
}
BAD_PATHS = (
    "/privacy", "/privacy-policy", "/terms", "/terms-of-service", "/policy", "/policies",
    "/about", "/blog", "/news", "/press", "/addons", "/extensions", "/help", "/support",
    "/contact", "/legal", "/cookies", "/security", "/docs",
)


def fetch(url, timeout=20):
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"}, timeout=timeout, allow_redirects=True)
        if r.status_code >= 400:
            return None
        return r
    except Exception:
        return None


def _is_discovery_domain(url):
    return root_domain(url) in DISCOVERY_DOMAINS


def _is_ats_domain(url):
    d = root_domain(url)
    return d in ATS_DOMAINS or any(d.endswith("." + x) for x in ATS_DOMAINS)


def _career_like(url):
    low = clean_url(url).lower()
    if not low:
        return False
    path = urlparse(low).path
    if any(path.startswith(x) for x in BAD_PATHS):
        return False
    return _is_ats_domain(low) or any(x in path for x in CAREER_HINTS)


def _candidate_score(href, text="", source_domain=""):
    href_low = href.lower(); text_low = text.lower(); score = 0
    if any(x in text_low for x in ("apply", "apply now", "view job", "job details")): score += 8
    if any(x in href_low for x in CAREER_HINTS): score += 5
    if any(x in href_low for x in ATS_HINTS): score += 6
    if source_domain and root_domain(href) != source_domain: score += 4
    if _is_discovery_domain(href): score -= 20
    if not _career_like(href): score -= 5
    if href_low.startswith(("mailto:", "javascript:")): score -= 50
    return score


def _jsonld_urls(soup):
    urls=[]
    for tag in soup.find_all("script", type="application/ld+json"):
        try: data=json.loads(tag.string or tag.get_text())
        except Exception: continue
        stack=data if isinstance(data,list) else [data]
        for item in stack:
            if not isinstance(item,dict): continue
            if item.get("@type")=="JobPosting" and item.get("url"): urls.append(item["url"])
            me=item.get("mainEntity")
            if isinstance(me,dict) and me.get("url"): urls.append(me["url"])
    return urls


def links_from_page(url, html):
    soup=BeautifulSoup(html,"html.parser"); source_domain=root_domain(url); candidates=[]
    for raw in _jsonld_urls(soup):
        href=clean_url(urljoin(url,raw))
        if href and not _is_discovery_domain(href) and _career_like(href): candidates.append((12,href))
    for a in soup.find_all("a",href=True):
        href=clean_url(urljoin(url,a.get("href")))
        if not href: continue
        text=" ".join(a.stripped_strings)
        score=_candidate_score(href,text,source_domain)
        if score>0: candidates.append((score,href))
    candidates.sort(key=lambda x:x[0],reverse=True)
    return list(dict.fromkeys(href for _,href in candidates))[:60]


def likely_official(url, expected_domain=""):
    if not url: return False
    d=root_domain(url)
    if not d or d in DISCOVERY_DOMAINS: return False
    if _is_ats_domain(url): return True
    if expected_domain and d != root_domain(expected_domain): return False
    return _career_like(url)


def resolve(discovery_url, company="", company_website=""):
    if not discovery_url: return None
    discovery_url=clean_url(discovery_url)
    if not discovery_url: return None
    expected=root_domain(company_website) if company_website else ""
    if likely_official(discovery_url, expected):
        r=fetch(discovery_url)
        if r and likely_official(r.url, expected): return clean_url(r.url)
    r=fetch(discovery_url)
    if r:
        for candidate in links_from_page(r.url,r.text):
            if likely_official(candidate, expected): return candidate
    if company_website:
        site=clean_url(company_website)
        if site and root_domain(site) not in DISCOVERY_DOMAINS:
            rr=fetch(site)
            if rr:
                for candidate in links_from_page(rr.url,rr.text):
                    if likely_official(candidate, root_domain(site)): return candidate
    return None
