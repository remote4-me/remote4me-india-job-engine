import hashlib, os, re, time
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

TRACKING_PARAMS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","gclid","fbclid","source"}

def clean_url(url):
    if not url: return ""
    try:
        p = urlparse(url.strip())
        if p.scheme not in ("http","https") or not p.netloc: return ""
        q = [(k,v) for k,v in parse_qsl(p.query, keep_blank_values=True) if k.lower() not in TRACKING_PARAMS]
        path = re.sub(r"/{2,}", "/", p.path or "/")
        return urlunparse((p.scheme.lower(), p.netloc.lower(), path.rstrip("/") or "/", "", urlencode(q), ""))
    except Exception:
        return ""

def domain(url):
    try: return urlparse(url).netloc.lower().split(":")[0]
    except Exception: return ""

def root_domain(url):
    d = domain(url)
    parts = d.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else d

def norm_text(s):
    return re.sub(r"\s+", " ", (s or "")).strip().lower()

def fingerprint(*parts):
    raw = "||".join(norm_text(x) for x in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]

def sleep_delay():
    try: time.sleep(float(os.getenv("CRAWL_DELAY","0.8")))
    except Exception: pass

def safe_int(name, default):
    try: return int(os.getenv(name, str(default)))
    except Exception: return default
