import os, re, json, time, hashlib
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, quote_plus
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

ROOT = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(ROOT, "config", "keywords.json"), encoding="utf-8") as f:
    KW = json.load(f)
with open(os.path.join(ROOT, "config", "ats_registry.json"), encoding="utf-8") as f:
    ATS = json.load(f)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Remote4me-India-Remote-Job-Research/1.0"})
TIMEOUT = 20
MAX_AGE_DAYS = int(os.getenv("JOB_MAX_AGE_DAYS", "7"))
MAX_DISCOVERY_QUERIES = int(os.getenv("MAX_DISCOVERY_QUERIES", "80"))
MAX_SEARCH_RESULTS = int(os.getenv("MAX_SEARCH_RESULTS", "10"))

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

def job_key(url, company="", title=""):
    raw = (url or "").strip().lower()
    if not raw:
        raw = f"{norm(company)}|{norm(title)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]

def age_ok(date_text):
    if not date_text:
        return True
    try:
        dt = datetime.fromisoformat(str(date_text).replace("Z","+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    except Exception:
        return True

def remote_status(text):
    t = norm(text)
    if any(x in t for x in KW["remote_negative"]):
        return False
    return any(x in t for x in KW["remote_positive"])

def india_status(text):
    t = norm(text)
    return any(x in t for x in KW["india_terms"])

def role_score(title, description=""):
    t = norm(title)
    d = norm(description)[:15000]
    score = 0
    matched = []
    for x in KW["role_phrases"]:
        if x in t:
            score += 50
            matched.append(x)
        elif x in d:
            score += 18
            matched.append(x)
    for x in KW["support_terms"]:
        if x in d:
            score += 5
    # Explicitly reject obvious non-support families when they dominate the title.
    bad = ["software engineer","frontend engineer","backend engineer","data scientist",
           "devops engineer","account executive","sales development representative",
           "recruiter","marketing manager","graphic designer"]
    if any(x in t for x in bad) and not any(x in t for x in ["support","success","customer"]):
        score -= 45
    return score, sorted(set(matched))

def accept_job(title, location="", description="", url="", date_text=""):
    text = " ".join([title, location, description])
    score, matched = role_score(title, description)
    india = india_status(text)
    remote = remote_status(text)
    fresh = age_ok(date_text)
    # Worldwide jobs are not automatically India eligible unless explicitly stated.
    if not india or not remote or not fresh or score < 22:
        return False, score, matched
    return True, score, matched

def row(title, company, location, posted, url, source, description=""):
    ok, score, matched = accept_job(title, location, description, url, posted)
    if not ok:
        return None
    return {
        "added": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "title": clean(title),
        "company": clean(company),
        "location": clean(location) or "India",
        "remote": "Remote",
        "posted": clean(posted),
        "job_url": url.strip(),
        "source": source,
        "score": score,
        "matched": ", ".join(matched[:12])
    }

def api_himalayas():
    out=[]
    cursor=None
    for _ in range(40):
        params={"limit":20}
        if cursor: params["cursor"]=cursor
        r=get("https://himalayas.app/jobs/api", params=params)
        if not r: break
        data=r.json()
        for j in data.get("jobs",[]):
            desc=j.get("description","")
            loc="; ".join(j.get("locationRestrictions") or []) or "Remote"
            rr=row(j.get("title"),j.get("companyName"),loc,j.get("pubDate"),j.get("applicationLink"),"Himalayas",desc)
            if rr: out.append(rr)
        cursor=data.get("nextCursor")
        if not cursor: break
        time.sleep(.2)
    return out

def api_jobicy():
    out=[]
    for tag in list(dict.fromkeys(KW["role_phrases"]))[:35]:
        r=get("https://jobicy.com/api/v2/remote-jobs", params={"count":200,"tag":tag})
        if not r: continue
        for j in r.json().get("jobs",[]):
            loc=j.get("jobGeo") or j.get("jobExcerpt") or ""
            desc=j.get("jobDescription","")
            rr=row(j.get("jobTitle"),j.get("companyName"),loc,j.get("pubDate"),j.get("url"),"Jobicy",desc)
            if rr: out.append(rr)
        time.sleep(.15)
    return out

def api_remoteok():
    out=[]
    r=get("https://remoteok.com/api")
    if not r: return out
    data=r.json()
    for j in data if isinstance(data,list) else []:
        title=j.get("position") or j.get("title")
        desc=j.get("description","")
        loc=j.get("location") or ""
        tags=" ".join(j.get("tags") or [])
        rr=row(title,j.get("company"),loc,j.get("date"),j.get("url"),"Remote OK",desc+" "+tags)
        if rr: out.append(rr)
    return out

def api_remotive():
    out=[]
    r=get("https://remotive.com/api/remote-jobs", params={"limit":500})
    if not r: return out
    for j in r.json().get("jobs",[]):
        rr=row(j.get("title"),j.get("company_name"),j.get("candidate_required_location"),
               j.get("publication_date"),j.get("url"),"Remotive",j.get("description",""))
        if rr: out.append(rr)
    return out

def api_jobspipe():
    key=os.getenv("JOBSPIPE_API_KEY","").strip()
    if not key:
        return []
    out=[]
    titles=KW["role_phrases"][:40]
    payload={
        "job_title_or": titles,
        "job_country_code_or":["IN"],
        "remote":True,
        "posted_at_max_age_days":MAX_AGE_DAYS,
        "limit":25,
        "include_total_results":False
    }
    r=get("https://api.jobspipe.dev/v1/jobs/search",
          headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},
          json=payload)
    if not r: return out
    for j in r.json().get("data",[]):
        rr=row(j.get("job_title"),j.get("company"),j.get("location"),
               j.get("posted_at"),j.get("apply_url"),"JobsPipe",j.get("description",""))
        if rr: out.append(rr)
    return out

def detect_ats(url):
    host=norm(urlparse(url).netloc)
    path=norm(urlparse(url).path)
    for a in ATS:
        if any(h in host for h in a["hosts"]):
            return a["name"], a["type"]
    # URL hints for common embedded ATS paths.
    hints=[
        ("greenhouse","greenhouse"),("lever","lever"),("ashby","ashby"),
        ("workable","workable"),("smartrecruiters","smartrecruiters"),
        ("teamtailor","teamtailor"),("recruitee","recruitee")
    ]
    for h,t in hints:
        if h in host or h in path:
            return h.title(),t
    return "", ""

def search_brave(query):
    key=os.getenv("BRAVE_SEARCH_API_KEY","").strip()
    if not key: return []
    r=get("https://api.search.brave.com/res/v1/web/search",
          headers={"Accept":"application/json","X-Subscription-Token":key},
          params={"q":query,"count":MAX_SEARCH_RESULTS})
    if not r: return []
    return [{"title":x.get("title",""),"url":x.get("url",""),"description":x.get("description","")}
            for x in r.json().get("web",{}).get("results",[])]

def search_serper(query):
    key=os.getenv("SERPER_API_KEY","").strip()
    if not key: return []
    r=get("https://google.serper.dev/search",
          headers={"X-API-KEY":key,"Content-Type":"application/json"},
          json={"q":query,"num":MAX_SEARCH_RESULTS})
    if not r: return []
    return [{"title":x.get("title",""),"url":x.get("link",""),"description":x.get("snippet","")}
            for x in r.json().get("organic",[])]

def discovery_queries():
    roles=[
        "customer support","customer success","technical support","product support",
        "customer experience","customer onboarding","application support","helpdesk",
        "service desk","SaaS support","customer care","client success","user support"
    ]
    ats_domains=[
        "boards.greenhouse.io","jobs.lever.co","jobs.ashbyhq.com","apply.workable.com",
        "jobs.smartrecruiters.com","recruitee.com","breezy.hr","teamtailor.com",
        "myworkdayjobs.com","jobs.jobvite.com","pinpointhq.com","bamboohr.com"
    ]
    queries=[]
    for role in roles:
        queries.append(f'"{role}" "India" "remote" careers')
        for host in ats_domains[:10]:
            queries.append(f'site:{host} "{role}" "India" remote')
    # Company/career discovery queries without a fixed ATS.
    queries += [
        '"customer support" "India" "remote" "careers"',
        '"customer success" "India" "remote" "careers"',
        '"technical support" "India" "remote" "careers"',
        '"product support" "India" "remote" "careers"',
        '"customer onboarding" "India" "remote" "careers"',
        '"SaaS support" "India" remote jobs',
        '"customer experience" "India" remote careers'
    ]
    return list(dict.fromkeys(queries))[:MAX_DISCOVERY_QUERIES]

def discover_ats():
    boards={}
    for q in discovery_queries():
        results=search_brave(q)+search_serper(q)
        for x in results:
            u=x.get("url","")
            name,typ=detect_ats(u)
            if not typ: continue
            boards[u]=(name,typ,x.get("title",""))
        time.sleep(.1)
    return boards

def parse_generic_ats_page(url, source):
    # Lightweight fallback for ATS pages discovered by search.
    r=get(url)
    if not r: return []
    soup=BeautifulSoup(r.text,"html.parser")
    text=clean(soup.get_text(" ",strip=True))
    title=clean(soup.title.get_text()) if soup.title else ""
    # Search-page parsing is intentionally conservative: only emit if the page itself
    # looks like a single job and has an apply/job link.
    links=[]
    for a in soup.find_all("a",href=True):
        href=a["href"]
        if href.startswith("/"):
            href=urlparse(url)._replace(path=href,query="",fragment="").geturl()
        if href.startswith("http"):
            links.append((clean(a.get_text(" ",strip=True)),href))
    candidates=[]
    for anchor,href in links:
        if any(k in norm(anchor) for k in ["apply","apply now","view job","job details"]):
            candidates.append(href)
    if not candidates:
        candidates=[url]
    for u in candidates[:5]:
        # Use page text as evidence; title fallback comes from page title.
        rr=row(title.split("|")[0], "", text[:400], "", u, source, text[:12000])
        if rr: return [rr]
    return []

def discover_from_search():
    out=[]
    boards=discover_ats()
    for url,(name,typ,_) in boards.items():
        # We intentionally don't aggressively crawl unknown ATS pages.
        # Known direct job URLs discovered by search are still checked.
        if any(k in url for k in ["greenhouse","lever","ashby","workable","smartrecruiters","recruitee","teamtailor"]):
            out.extend(parse_generic_ats_page(url,name))
    return out

def sheet_client():
    raw=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    sid=os.getenv("GOOGLE_SHEET_ID","").strip()
    if not raw or not sid:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SHEET_ID")
    info=json.loads(raw)
    creds=Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc=gspread.authorize(creds)
    sh=gc.open_by_key(sid)
    return sh

def ensure_ws(sh, title, headers):
    try:
        ws=sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws=sh.add_worksheet(title=title, rows=2000, cols=len(headers))
        ws.append_row(headers)
    return ws

def write_results(rows):
    sh=sheet_client()
    headers=["Added","Job Title","Company","Location","Work Type","Posted","Job URL","Source","Score","Matched Keywords"]
    ws=ensure_ws(sh,"JOBS",headers)
    existing=ws.col_values(7)[1:]
    existing_keys={job_key(x) for x in existing if x}
    new=[]
    for r in rows:
        k=job_key(r["job_url"],r["company"],r["title"])
        if k in existing_keys: continue
        existing_keys.add(k)
        new.append([
            r["added"],r["title"],r["company"],r["location"],r["remote"],
            r["posted"],r["job_url"],r["source"],r["score"],r["matched"]
        ])
    if new:
        ws.append_rows(new, value_input_option="RAW")
    # Company memory sheet.
    c_headers=["Company","Domain","Career/ATS URL","ATS","Last Checked","Jobs Found","Status"]
    cws=ensure_ws(sh,"COMPANIES",c_headers)
    company_map={}
    for r in rows:
        company=r["company"]
        if not company: continue
        domain=urlparse(r["job_url"]).netloc
        company_map[company]=(domain,r["job_url"],r["source"])
    existing_companies=set(cws.col_values(1)[1:])
    for company,(domain,url,source) in company_map.items():
        if company in existing_companies: continue
        ats_name,_=detect_ats(url)
        cws.append_row([company,domain,url,ats_name or source,
                        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        1,"ACTIVE"])
    return len(new)

def main():
    all_rows=[]
    sources=[
        ("Himalayas",api_himalayas),
        ("Jobicy",api_jobicy),
        ("Remote OK",api_remoteok),
        ("JobsPipe",api_jobspipe),
    ]
    for name,fn in sources:
        try:
            got=fn()
            print(f"{name}: {len(got)} accepted")
            all_rows.extend(got)
        except Exception as e:
            print(f"{name}: ERROR {e}")
    try:
        discovered=discover_from_search()
        print(f"ATS discovery: {len(discovered)} accepted")
        all_rows.extend(discovered)
    except Exception as e:
        print(f"ATS discovery: ERROR {e}")
    # Deduplicate in memory.
    unique={}
    for r in all_rows:
        unique[job_key(r["job_url"],r["company"],r["title"])]=r
    rows=list(unique.values())
    rows.sort(key=lambda x:(-int(x["score"]),x["company"],x["title"]))
    print(f"TOTAL UNIQUE ACCEPTED: {len(rows)}")
    if os.getenv("DRY_RUN","false").lower()=="true":
        for r in rows[:100]:
            print(r["title"],"|",r["company"],"|",r["location"],"|",r["job_url"])
        return
    inserted=write_results(rows)
    print(f"NEW ROWS WRITTEN: {inserted}")

if __name__=="__main__":
    main()
