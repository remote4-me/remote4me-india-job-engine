import os, re, json, time, hashlib, sqlite3, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import gspread
from google.oauth2.service_account import Credentials

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
os.makedirs(DATA, exist_ok=True)
BOARDS_FILE = os.path.join(DATA, "boards.json")
META_FILE = os.path.join(DATA, "boards_meta.json")
DB_FILE = os.path.join(DATA, "jobs.db")

with open(os.path.join(ROOT, "config", "keywords.json"), encoding="utf-8") as f:
    KW = json.load(f)

MAX_AGE_DAYS = int(os.getenv("JOB_MAX_AGE_DAYS", "7"))
DISCOVERY_DAYS = int(os.getenv("ATS_REFRESH_DAYS", "7"))
MAX_WORKERS = int(os.getenv("ATS_WORKERS", "8"))
DISCOVERY_LIMIT = int(os.getenv("WAYBACK_LIMIT", "50000"))
MAX_VALIDATION_PER_PLATFORM = int(os.getenv("MAX_VALIDATION_PER_PLATFORM", "6000"))

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Remote4me-India-Job-Engine/4.0 (+https://remote4.me)"
})
TIMEOUT = 18

# Public ATS board endpoints. Discovery is performed from public web archives,
# not paid search APIs. These endpoints are intended to expose published jobs.
ATS = {
    "greenhouse": {"archive": "boards.greenhouse.io/*", "validate": "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true", "host": "boards.greenhouse.io"},
    "lever": {"archive": "jobs.lever.co/*", "validate": "https://api.lever.co/v0/postings/{slug}?mode=json", "host": "jobs.lever.co"},
    "ashby": {"archive": "jobs.ashbyhq.com/*", "validate": "https://api.ashbyhq.com/posting-api/job-board/{slug}", "host": "jobs.ashbyhq.com"},
    "smartrecruiters": {"archive": "jobs.smartrecruiters.com/*", "validate": "https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=100", "host": "jobs.smartrecruiters.com"},
    "recruitee": {"archive": "*.recruitee.com/*", "validate": "https://{slug}.recruitee.com/api/offers/", "host": "recruitee.com"},
    "breezy": {"archive": "*.breezy.hr/*", "validate": "https://{slug}.breezy.hr/json", "host": "breezy.hr"},
    "teamtailor": {"archive": "*.teamtailor.com/jobs*", "validate": "https://{slug}.teamtailor.com/jobs.json", "host": "teamtailor.com"},
    "workable": {"archive": "apply.workable.com/*", "validate": "https://apply.workable.com/api/v1/widget/accounts/{slug}", "host": "apply.workable.com"},
}


def clean(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()


def norm(s):
    return clean(s).lower()


def get(url, **kwargs):
    try:
        r = SESSION.get(url, timeout=TIMEOUT, **kwargs)
        if r.status_code == 200:
            return r
    except requests.RequestException:
        pass
    return None


def head_ok(url):
    try:
        r = SESSION.head(url, timeout=TIMEOUT, allow_redirects=True)
        return r.status_code == 200
    except requests.RequestException:
        return False


def job_key(url, company="", title=""):
    raw = (url or "").strip().lower() or f"{norm(company)}|{norm(title)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def parse_dt(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        # Lever commonly uses epoch milliseconds.
        if v > 10_000_000_000:
            v = v / 1000
        try:
            return datetime.fromtimestamp(v, timezone.utc)
        except Exception:
            return None
    s = str(v).strip()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def age_ok(date_value):
    dt = parse_dt(date_value)
    if not dt:
        return True
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt >= datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)


def role_score(title, description="", department=""):
    t = norm(title)
    d = norm(description)[:20000]
    dep = norm(department)
    score = 0
    matched = []
    for x in KW["role_phrases"]:
        if x in t:
            score += 55
            matched.append(x)
        elif x in dep:
            score += 25
            matched.append(x)
        elif x in d:
            score += 12
            matched.append(x)
    for x in KW["support_terms"]:
        if x in d:
            score += 4
            matched.append(x)
    bad = ["software engineer", "frontend engineer", "backend engineer", "data scientist",
           "devops engineer", "account executive", "sales development representative",
           "recruiter", "marketing manager", "graphic designer"]
    if any(x in t for x in bad) and not any(x in t for x in ["support", "success", "customer"]):
        score -= 60
    return score, sorted(set(matched))


def classify_workplace(title, location, description):
    text = norm(" ".join([title, location, description]))
    negatives = [x for x in KW["remote_negative"] if x in text]
    positives = [x for x in KW["remote_positive"] if x in text]
    if negatives and not positives:
        return None
    if any(x in text for x in ["hybrid", "remote/hybrid", "hybrid/remote"]):
        return "Hybrid"
    if positives:
        return "Remote"
    return None


def india_match(location, description="", title=""):
    # Prefer location metadata. Only fall back to description/title if location is missing.
    loc = norm(location)
    if loc:
        if any(x in loc for x in KW["remote_negative"]):
            # A location such as "Bangalore - Hybrid" is still India eligible; workplace is handled separately.
            pass
        india_city = any(x in loc for x in KW["india_terms"])
        if india_city:
            return True
        # Explicit country restriction can be phrased as "IN" or "India".
        if re.search(r"(^|[^a-z])in([^a-z]|$)", loc) and "india" in norm(description):
            return True
        return False
    return any(x in norm(description + " " + title) for x in KW["india_terms"])


def make_row(title, company, location, posted, url, source, description="", department=""):
    title, company, location, url = map(clean, [title, company, location, url])
    score, matched = role_score(title, description, department)
    workplace = classify_workplace(title, location, description)
    if not india_match(location, description, title):
        return None
    if workplace not in ("Remote", "Hybrid"):
        return None
    if not age_ok(posted):
        return None
    if score < 22:
        return None
    return {
        "added": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "title": title,
        "company": company,
        "location": location or "India",
        "remote": workplace,
        "posted": clean(posted),
        "job_url": url,
        "source": source,
        "score": score,
        "matched": ", ".join(matched[:15]),
    }

# ---------------- Public API sources ----------------

def api_himalayas():
    out=[]; cursor=None
    for _ in range(40):
        params={"limit":50}
        if cursor: params["cursor"]=cursor
        r=get("https://himalayas.app/jobs/api", params=params)
        if not r: break
        try: data=r.json()
        except Exception: break
        for j in data.get("jobs",[]):
            loc="; ".join(j.get("locationRestrictions") or [])
            rr=make_row(j.get("title"),j.get("companyName"),loc,j.get("pubDate"),j.get("applicationLink"),"Himalayas",j.get("description",""))
            if rr: out.append(rr)
        cursor=data.get("nextCursor")
        if not cursor: break
    return out


def api_jobicy():
    out=[]
    for tag in list(dict.fromkeys(KW["role_phrases"]))[:45]:
        r=get("https://jobicy.com/api/v2/remote-jobs", params={"count":200,"tag":tag})
        if not r: continue
        try: data=r.json()
        except Exception: continue
        for j in data.get("jobs",[]):
            loc=j.get("jobGeo") or ""
            desc=j.get("jobDescription","")
            rr=make_row(j.get("jobTitle"),j.get("companyName"),loc,j.get("pubDate"),j.get("url"),"Jobicy",desc)
            if rr: out.append(rr)
    return out


def api_remoteok():
    out=[]
    r=get("https://remoteok.com/api")
    if not r: return out
    try: data=r.json()
    except Exception: return out
    for j in data if isinstance(data,list) else []:
        tags=" ".join([str(x) for x in (j.get("tags") or []) if x is not None])
        rr=make_row(j.get("position") or j.get("title"),j.get("company"),j.get("location") or "Remote",
                    j.get("date"),j.get("url"),"Remote OK",j.get("description","")+" "+tags)
        if rr: out.append(rr)
    return out

# ---------------- ATS discovery ----------------

def extract_slug(ats, original):
    try:
        p=urlparse(original)
        host=p.netloc.lower()
        path=p.path.strip("/").split("/")
        if ats in ("greenhouse","lever","ashby","smartrecruiters","workable"):
            return path[0] if path else ""
        if ats in ("recruitee","breezy","teamtailor"):
            parts=host.split(".")
            if len(parts) >= 3:
                return parts[0]
    except Exception:
        pass
    return ""


def wayback_discover(ats, pattern):
    url="https://web.archive.org/cdx/search/cdx"
    params={
        "url": pattern,
        "output":"json",
        "fl":"original",
        "filter":"statuscode:200",
        "collapse":"urlkey",
        "limit":str(DISCOVERY_LIMIT),
    }
    r=get(url, params=params)
    if not r: return []
    try:
        data=r.json()
        originals=[]
        if data and isinstance(data,list):
            start=1 if isinstance(data[0],list) else 0
            for item in data[start:]:
                if isinstance(item,list) and item:
                    originals.append(item[0])
                elif isinstance(item,str):
                    originals.append(item)
        return originals
    except Exception:
        return []


def commoncrawl_discover(pattern):
    # Fallback only. Uses the latest public Common Crawl index, no API key.
    idx=get("https://index.commoncrawl.org/collinfo.json")
    if not idx: return []
    try:
        latest=idx.json()[0]["id"]
    except Exception:
        return []
    endpoint=f"https://index.commoncrawl.org/{latest}-index"
    r=get(endpoint, params={"url":pattern,"output":"json","filter":"status:200","collapse":"urlkey","fl":"url","limit":DISCOVERY_LIMIT})
    if not r: return []
    out=[]
    for line in r.text.splitlines():
        try:
            obj=json.loads(line)
            if obj.get("url"): out.append(obj["url"])
        except Exception:
            continue
    return out


def load_boards():
    if not os.path.exists(BOARDS_FILE):
        return {k:[] for k in ATS}
    try:
        with open(BOARDS_FILE,encoding="utf-8") as f: return json.load(f)
    except Exception: return {k:[] for k in ATS}


def save_boards(boards):
    tmp=BOARDS_FILE+".tmp"
    with open(tmp,"w",encoding="utf-8") as f: json.dump(boards,f,indent=2,sort_keys=True)
    os.replace(tmp,BOARDS_FILE)
    with open(META_FILE,"w",encoding="utf-8") as f:
        json.dump({"last_refresh":datetime.now(timezone.utc).isoformat()},f,indent=2)


def should_refresh():
    if os.getenv("FORCE_ATS_REFRESH","false").lower()=="true": return True
    if not os.path.exists(META_FILE): return True
    try:
        meta=json.load(open(META_FILE,encoding="utf-8"))
        dt=parse_dt(meta.get("last_refresh"))
        return not dt or dt < datetime.now(timezone.utc)-timedelta(days=DISCOVERY_DAYS)
    except Exception:
        return True


def validate_board(ats, slug):
    cfg=ATS[ats]
    url=cfg["validate"].format(slug=slug)
    # HEAD is cheap for the three main ATS; GET for the rest.
    if ats in ("greenhouse","lever","ashby") and not head_ok(url):
        return False
    r=get(url)
    if not r: return False
    try:
        if ats=="greenhouse": return isinstance(r.json().get("jobs"),list)
        if ats=="lever": return isinstance(r.json(),list)
        if ats=="ashby": return isinstance(r.json().get("jobs"),list)
        if ats=="smartrecruiters": return isinstance(r.json().get("content"),list)
        if ats=="recruitee": return isinstance(r.json(),dict) or isinstance(r.json(),list)
        if ats=="breezy": return isinstance(r.json(),list)
        if ats=="teamtailor": return isinstance(r.json().get("jobs"),list)
        if ats=="workable": return isinstance(r.json(),dict)
    except Exception: return False
    return False


def refresh_board_registry():
    boards=load_boards()
    for ats,cfg in ATS.items():
        originals=wayback_discover(ats,cfg["archive"])
        if not originals:
            originals=commoncrawl_discover(cfg["archive"])
        slugs=[]
        for u in originals:
            s=extract_slug(ats,u)
            if s and re.match(r"^[a-z0-9][a-z0-9._-]{1,80}$",s.lower()):
                slugs.append(s.lower())
        # preserve old boards, prioritize new unique candidates.
        candidates=list(dict.fromkeys(boards.get(ats,[])+slugs))[:MAX_VALIDATION_PER_PLATFORM]
        live=[]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            fut={ex.submit(validate_board,ats,s):s for s in candidates}
            for f in as_completed(fut):
                s=fut[f]
                try:
                    if f.result(): live.append(s)
                except Exception: pass
        boards[ats]=sorted(set(live))
        print(f"ATS registry {ats}: {len(boards[ats])} live boards")
    save_boards(boards)
    return boards


def normalize_ats_job(ats, company_slug, j):
    if ats=="greenhouse":
        loc=(j.get("location") or {}).get("name") if isinstance(j.get("location"),dict) else j.get("location")
        return make_row(j.get("title"),company_slug,loc,j.get("first_published") or j.get("updated_at"),j.get("absolute_url"),"Greenhouse",j.get("content", ""), "; ".join([x.get("name","") for x in j.get("departments",[]) if isinstance(x,dict)]))
    if ats=="lever":
        cat=j.get("categories") or {}
        loc=cat.get("location") or ""
        workplace=j.get("workplaceType") or ""
        if workplace: loc=f"{loc} {workplace}"
        return make_row(j.get("text"),company_slug,loc,j.get("createdAt"),j.get("hostedUrl") or j.get("applyUrl"),"Lever",j.get("descriptionPlain") or j.get("description"),cat.get("team") or cat.get("department"))
    if ats=="ashby":
        loc=j.get("location") or ""
        if j.get("workplaceType"): loc=f"{loc} {j.get('workplaceType')}"
        return make_row(j.get("title"),j.get("companyName") or company_slug,loc,j.get("publishedAt"),j.get("jobUrl"),"Ashby",j.get("description"),j.get("department"))
    if ats=="smartrecruiters":
        loc=j.get("location") or {}
        loctext=" ".join(str(loc.get(k,"")) for k in ["city","region","country"])
        return make_row(j.get("name"),j.get("company",{}).get("name") or company_slug,loctext,j.get("releasedDate"),j.get("refNumber") and f"https://jobs.smartrecruiters.com/{company_slug}/{j.get('id')}","SmartRecruiters",j.get("jobAd",{}).get("sections",{}).get("jobDescription",{}).get("text", ""),j.get("department",{}).get("label", ""))
    if ats=="recruitee":
        return make_row(j.get("title"),j.get("company_name") or company_slug,j.get("location"),j.get("created_at"),j.get("careers_url") or j.get("url"),"Recruitee",j.get("description"),j.get("department"))
    if ats=="breezy":
        return make_row(j.get("name") or j.get("title"),j.get("company_name") or company_slug,j.get("location"),j.get("published_date"),j.get("url"),"Breezy",j.get("description"),j.get("department"))
    if ats=="teamtailor":
        return make_row(j.get("title"),j.get("company_name") or company_slug,j.get("location"),j.get("created_at"),j.get("url"),"Teamtailor",j.get("body"),j.get("department", ""))
    if ats=="workable":
        return make_row(j.get("title"),j.get("company") or company_slug,j.get("location") or j.get("city"),j.get("created_at"),j.get("url"),"Workable",j.get("description"),j.get("department"))
    return None


def fetch_board(ats, slug):
    url=ATS[ats]["validate"].format(slug=slug)
    r=get(url)
    if not r: return []
    try: data=r.json()
    except Exception: return []
    if ats=="greenhouse": jobs=data.get("jobs",[])
    elif ats=="lever": jobs=data if isinstance(data,list) else []
    elif ats=="ashby": jobs=data.get("jobs",[])
    elif ats=="smartrecruiters": jobs=data.get("content",[])
    elif ats=="recruitee": jobs=data.get("offers",[]) if isinstance(data,dict) else data
    elif ats=="breezy": jobs=data if isinstance(data,list) else []
    elif ats=="teamtailor": jobs=data.get("jobs",[])
    elif ats=="workable": jobs=data.get("jobs",[]) if isinstance(data,dict) else []
    else: jobs=[]
    out=[]
    for j in jobs:
        rr=normalize_ats_job(ats,slug,j)
        if rr: out.append(rr)
    return out


def crawl_ats():
    boards=load_boards()
    if should_refresh():
        boards=refresh_board_registry()
    total=[]
    tasks=[]
    for ats,slugs in boards.items():
        for slug in slugs:
            tasks.append((ats,slug))
    print(f"ATS boards to crawl: {len(tasks)}")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fut={ex.submit(fetch_board,a,s):(a,s) for a,s in tasks}
        done=0
        for f in as_completed(fut):
            done+=1
            a,s=fut[f]
            try: total.extend(f.result())
            except Exception as e: print(f"ATS board error {a}/{s}: {e}")
            if done % 250 == 0: print(f"ATS progress: {done}/{len(tasks)}")
    return total

# ---------------- Google Sheets ----------------

def sheet_client():
    raw=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip(); sid=os.getenv("GOOGLE_SHEET_ID","").strip()
    if not raw or not sid: raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SHEET_ID")
    info=json.loads(raw)
    creds=Credentials.from_service_account_info(info,scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(sid)


def ensure_ws(sh,title,headers):
    try: ws=sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws=sh.add_worksheet(title=title,rows=3000,cols=len(headers)); ws.append_row(headers)
    return ws


def write_results(rows):
    sh=sheet_client()
    headers=["Added","Job Title","Company","Location","Work Type","Posted","Job URL","Source","Score","Matched Keywords"]
    ws=ensure_ws(sh,"JOBS",headers)
    existing=ws.col_values(7)[1:]
    keys={job_key(x) for x in existing if x}
    new=[]
    for r in rows:
        k=job_key(r["job_url"],r["company"],r["title"])
        if k in keys: continue
        keys.add(k)
        new.append([r["added"],r["title"],r["company"],r["location"],r["remote"],r["posted"],r["job_url"],r["source"],r["score"],r["matched"]])
    if new: ws.append_rows(new,value_input_option="RAW")
    return len(new)


def main():
    rows=[]
    for name,fn in [("Himalayas",api_himalayas),("Jobicy",api_jobicy),("Remote OK",api_remoteok)]:
        try:
            got=fn(); print(f"{name}: {len(got)} accepted"); rows.extend(got)
        except Exception as e: print(f"{name}: ERROR {e}")
    try:
        got=crawl_ats(); print(f"ATS discovery/crawl: {len(got)} accepted"); rows.extend(got)
    except Exception as e: print(f"ATS: ERROR {e}")
    unique={job_key(r["job_url"],r["company"],r["title"]):r for r in rows if r.get("job_url")}
    rows=sorted(unique.values(),key=lambda x:(-int(x["score"]),x["company"],x["title"]))
    print(f"TOTAL UNIQUE ACCEPTED: {len(rows)}")
    if os.getenv("DRY_RUN","false").lower()=="true":
        for r in rows[:100]: print(r["source"],"|",r["title"],"|",r["company"],"|",r["location"],"|",r["job_url"])
        return
    print(f"NEW ROWS WRITTEN: {write_results(rows)}")

if __name__=="__main__": main()
