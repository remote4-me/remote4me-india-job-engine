import requests, time, re
from src.utils import clean_url, safe_int

UA = "remote4me-india-job-discovery/3.0 (+https://remote4.me)"
CDX = "https://web.archive.org/cdx/search/cdx"

CAREER_TERMS = ("career","careers","jobs","job","work-with-us","join-us","greenhouse","lever","ashby","workable","workday","smartrecruiters","icims")

def cdx(url_pattern, limit=300):
    params={
        "url":url_pattern,"output":"json","fl":"timestamp,original,mimetype,statuscode",
        "filter":"statuscode:200","collapse":"urlkey","limit":min(limit,1000),"gzip":"false"
    }
    try:
        r=requests.get(CDX,params=params,headers={"User-Agent":UA},timeout=30)
        r.raise_for_status()
        data=r.json()
        if not data or len(data)<2: return []
        headers=data[0]
        rows=[]
        for row in data[1:]:
            if not row or len(row)<2: continue
            item=dict(zip(headers,row))
            u=clean_url(item.get("original",""))
            if u: rows.append(item)
        return rows
    except Exception as e:
        print("Wayback error:", e); return []

def discover_from_seed_domains(domains, per_domain=100):
    out=[]
    for d in domains:
        d=d.strip()
        if not d: continue
        rows=cdx(f"*.{d}/*", per_domain)
        for x in rows:
            u=x["original"]
            low=u.lower()
            if any(t in low for t in CAREER_TERMS):
                out.append({"url":u,"title":"","snippet":"Wayback career/ATS discovery","source":"wayback"})
        time.sleep(1.0)
    return out
