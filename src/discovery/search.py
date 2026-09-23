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
    queries=[]
    # Broad India queries.
    for k in keywords[:24]:
        queries.append(f'"{k}" India jobs')
        queries.append(f'"{k}" "remote" India jobs')
    # City expansion, but capped so one run remains lightweight.
    for city in cities:
        if city.lower() in {"india","remote india","remote - india"}: continue
        for k in keywords[:10]:
            queries.append(f'"{k}" "{city}" jobs')
    # Search targeted portals and ATS.
    for host in targets:
        for k in keywords[:8]:
            queries.append(f'site:{host} "{k}" India')
    return list(dict.fromkeys(queries))
