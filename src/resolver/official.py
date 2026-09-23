import requests, re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from src.utils import clean_url, domain, root_domain, sleep_delay

UA="remote4me-india-job-discovery/3.0 (+https://remote4.me)"
CAREER_HINTS=("career","careers","jobs","join-us","work-with-us","opportunities","vacancies","job-openings")
ATS_HINTS=("greenhouse","lever","ashby","workable","workday","smartrecruiters","icims","successfactors","jobvite","teamtailor","bamboohr","recruitee","jazz","breezy","pinpointhq","personio")

def fetch(url, timeout=15):
    try:
        r=requests.get(url,headers={"User-Agent":UA},timeout=timeout,allow_redirects=True)
        if r.status_code>=400: return None
        return r
    except Exception: return None

def links_from_page(url, html):
    soup=BeautifulSoup(html,"html.parser")
    out=[]
    for a in soup.find_all("a",href=True):
        href=clean_url(urljoin(url,a.get("href")))
        text=" ".join(a.stripped_strings).lower()
        if not href: continue
        low=href.lower()
        if any(h in text or h in low for h in CAREER_HINTS+ATS_HINTS):
            out.append(href)
    return list(dict.fromkeys(out))[:80]

def likely_official(url):
    d=domain(url)
    return d and not any(x in d for x in ("linkedin.com","indeed.com","naukri.com","foundit.in","shine.com","timesjobs.com","cutshort.io","instahyre.com","wellfound.com","remoterocketship.com"))

def resolve(discovery_url, company=""):
    """Return best current official URL. Never bypasses login/CAPTCHA."""
    if not discovery_url: return None
    d=domain(discovery_url)
    if likely_official(discovery_url) and any(h in discovery_url.lower() for h in CAREER_HINTS+ATS_HINTS):
        r=fetch(discovery_url)
        if r and r.url:
            return clean_url(r.url)
    # For ordinary company pages, inspect links on the current page.
    if likely_official(discovery_url):
        r=fetch(discovery_url)
        if r:
            candidates=links_from_page(r.url,r.text)
            if candidates:
                return candidates[0]
    return None
