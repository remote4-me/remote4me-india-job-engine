import os, requests, re
from src.utils import safe_int, clean_url, sleep_delay

UA = "remote4me-india-job-discovery/3.0 (+https://remote4.me)"

def brave(query, count=20):
    key = os.getenv("BRAVE_SEARCH_API_KEY","").strip()
    if not key: return []
    url = "https://api.search.brave.com/res/v1/web/search"
    try:
        r = requests.get(url, params={"q":query,"count":min(count,20),"country":"IN","search_lang":"en"},
                         headers={"Accept":"application/json","X-Subscription-Token":key}, timeout=15)
        r.raise_for_status()
        out=[]
        for x in r.json().get("web",{}).get("results",[]):
            u=clean_url(x.get("url",""))
            if u: out.append({"url":u,"title":x.get("title",""),"snippet":x.get("description",""),"source":"search"})
        return out
    except Exception as e:
        print("Brave error:", e); return []

def serper(query, count=20):
    key = os.getenv("SERPER_API_KEY","").strip()
    if not key: return []
    try:
        r=requests.post("https://google.serper.dev/search",
            headers={"X-API-KEY":key,"Content-Type":"application/json"},
            json={"q":query,"gl":"in","hl":"en","num":min(count,100)},timeout=15)
        r.raise_for_status()
        out=[]
        for x in r.json().get("organic",[]):
            u=clean_url(x.get("link",""))
            if u: out.append({"url":u,"title":x.get("title",""),"snippet":x.get("snippet",""),"source":"search"})
        return out
    except Exception as e:
        print("Serper error:", e); return []

def search(query, count=20):
    # Prefer Brave; fall back to Serper.
    x=brave(query,count)
    if x: return x
    return serper(query,count)

def build_queries(keywords, cities, targets):
    queries = []
    core = keywords[:40]

    # Broad India + remote searches.
    for k in core:
        queries.append(f'"{k}" "remote" India jobs')
        queries.append(f'"{k}" "work from home" India jobs')

    # City expansion. The workflow caps total queries, so a large city file
    # increases geographic coverage across runs without creating an unbounded run.
    city_list = [c for c in cities if c.lower() not in {"india", "remote india", "remote - india", "work from home india"}]
    city_keywords = keywords[:12]
    for city in city_list:
        for k in city_keywords:
            queries.append(f'"{k}" "{city}" remote jobs')

    # Targeted portal/ATS searches.
    for host in targets:
        for k in keywords[:12]:
            queries.append(f'site:{host} "{k}" India remote')

    return list(dict.fromkeys(queries))
