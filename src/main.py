import argparse, json, os, re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from src.discovery.search import search, build_queries
from src.discovery.wayback import discover_from_seed_domains
from src.filters.india import is_india_job
from src.filters.support import relevance
from src.resolver.official import resolve
from src.storage.sheets import write
from src.utils import clean_url, domain, root_domain, fingerprint, safe_int

BASE=Path(__file__).resolve().parents[1]
KW=json.loads((BASE/"config/keywords.json").read_text())
CITIES=(BASE/"config/india_cities.txt").read_text().splitlines()
TARGETS=[x.strip() for x in (BASE/"config/search_targets.txt").read_text().splitlines() if x.strip()]
ATS=[x.strip() for x in (BASE/"config/ats_hosts.txt").read_text().splitlines() if x.strip()]

def extract_company(url,title=""):
    # Best-effort company signal from hostname. Search results are allowed to be company-unknown.
    d=domain(url)
    if not d: return ""
    bits=d.split(".")
    return bits[-2].replace("-"," ").title() if len(bits)>=2 else bits[0].title()

def parse_result(x):
    url=clean_url(x.get("url",""))
    title=x.get("title","")
    snippet=x.get("snippet","")
    company=extract_company(url,title)
    text=f"{title} {snippet}"
    # crude location extraction for sheet; official page verification happens later.
    loc="India"
    for c in CITIES:
        if c.lower() in text.lower():
            loc=c
            break
    return {"url":url,"title":title,"snippet":snippet,"company":company,"location":loc,"source":x.get("source","unknown")}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--dry-run",action="store_true")
    args=ap.parse_args()

    maxq=safe_int("MAX_SEARCH_QUERIES_PER_RUN",120)
    maxper=safe_int("MAX_RESULTS_PER_QUERY",20)
    maxverify=safe_int("MAX_OFFICIAL_CHECKS",250)
    now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    queries=build_queries(KW["primary"],CITIES,TARGETS)[:maxq]
    raw=[]
    errors=0
    for i,q in enumerate(queries,1):
        try:
            raw.extend(search(q,maxper))
        except Exception as e:
            errors+=1
            print("query error",q,e)
        if i%10==0: print(f"searches {i}/{len(queries)}; raw={len(raw)}")

    # Archive discovery: search a modest list of known Indian/company domains if supplied.
    seed_file=BASE/"data/seed_domains.txt"
    seed_domains=[]
    if seed_file.exists():
        seed_domains=[x.strip() for x in seed_file.read_text().splitlines() if x.strip() and not x.startswith("#")]
    if seed_domains:
        try: raw.extend(discover_from_seed_domains(seed_domains, per_domain=safe_int("WAYBACK_MAX_URLS_PER_QUERY",100)))
        except Exception as e:
            errors+=1; print("wayback error",e)

    unique={}
    for x in raw:
        p=parse_result(x)
        if not p["url"]: continue
        key=fingerprint(p["title"],p["company"],p["location"],p["url"])
        unique[key]=p

    india=[]
    support=[]
    for p in unique.values():
        if not is_india_job(p["title"],p["location"],p["snippet"]): continue
        india.append(p)
        score,hits=relevance(p["title"],p["snippet"],p["company"])
        if score>=4:
            p["score"]=score; p["hits"]=hits; support.append(p)

    # Prefer search results that look like direct job pages or career pages.
    support.sort(key=lambda x:(x["score"], "career" in x["url"].lower(), "jobs" in x["url"].lower()), reverse=True)
    to_verify=support[:maxverify]

    verified=[]; review=[]; discovered=[]; companies=[]
    seen_official=set()
    for p in to_verify:
        official=None
        try: official=resolve(p["url"],p["company"])
        except Exception as e:
            errors+=1
        discovered.append([now,p["title"],p["company"],p["location"],"",p["url"],official or "",p["source"],"verified" if official else "needs_review"])
        if official and official not in seen_official:
            seen_official.add(official)
            verified.append([now,p["title"],p["company"],p["location"],"Remote" if "remote" in (p["title"]+" "+p["snippet"]).lower() else "", "",official,p["source"],", ".join(p["hits"])])
            companies.append([p["company"],root_domain(official),official,"", "India",p["source"],now])
        else:
            review.append([now,p["title"],p["company"],p["location"],p["url"],"Official current career/ATS URL not resolved",p["source"]])

    result={"verified":verified,"discovered":discovered,"review":review,"companies":companies,
            "log":[now,len(unique),len(india),len(support),len(verified),len(review),errors]}
    print(json.dumps({"queries":len(queries),"raw":len(raw),"unique":len(unique),"india":len(india),"support":len(support),"verified":len(verified),"review":len(review),"errors":errors},indent=2))
    if not args.dry_run:
        write(result)

if __name__=="__main__":
    main()
