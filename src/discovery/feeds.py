import feedparser, requests
from src.utils import clean_url

UA="remote4me-india-job-discovery/3.0 (+https://remote4.me)"

def read_feed(url, limit=100):
    try:
        r=requests.get(url,headers={"User-Agent":UA},timeout=15)
        r.raise_for_status()
        f=feedparser.parse(r.content)
        out=[]
        for e in f.entries[:limit]:
            u=clean_url(getattr(e,"link",""))
            if not u: continue
            out.append({"url":u,"title":getattr(e,"title",""),"snippet":getattr(e,"summary",""),"source":"feed"})
        return out
    except Exception as e:
        print("Feed error:",e); return []
