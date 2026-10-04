import os
import re
import json
import time
import hashlib
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin, quote, unquote

import requests
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup


# ============================================================
# REMOTE4.ME JOB CRAWLER V5
# INDIA-FIRST / NO PAID SEARCH API
# ============================================================
#
# Goal:
#   Find fresh India-only Remote + Hybrid Customer Support,
#   Customer Success, Technical Support and closely related jobs.
#
# Discovery strategy:
#   1) Public job APIs/feeds (Himalayas + Remotive + Arbeitnow + Working Nomads + Jobicy + Remote OK + Startup Jobs + WWR RSS)
#   2) Wellfound public India role pages -> company/job seeds
#   3) Cutshort public India role pages -> company/job seeds
#   4) Internet Archive + Common Crawl -> ATS board discovery
#   5) Company website -> career page -> ATS/job link hunter
#   6) Direct ATS public feeds/APIs
#
# No Brave / Serper / JobsPipe / paid search API required.
#
# IMPORTANT:
#   - LinkedIn and Indeed are NOT scraped directly. Their public/archived
#     pages may be used as historical discovery hints via Common Crawl,
#     but the crawler does not log in, bypass blocks, or scrape private pages.
#   - A job is accepted only when India + Remote/Hybrid is supported by
#     structured location/workplace data or strong location wording.
#
# Google Sheets:
#   JOBS      -> daily job rows
#   COMPANIES -> persistent company/ATS registry
#
# GitHub Actions:
#   19:00 IST = 13:30 UTC
# ============================================================


# ============================================================
# CONFIG
# ============================================================

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")
STARTUP_JOBS_API_KEY = os.environ.get("STARTUP_JOBS_API_KEY", "").strip()
STARTUP_JOBS_MAX_PAGES = int(os.environ.get("STARTUP_JOBS_MAX_PAGES", "9"))
HIMALAYAS_MAX_PAGES = int(os.environ.get("HIMALAYAS_MAX_PAGES", "3"))
ARBEITNOW_MAX_PAGES = int(os.environ.get("ARBEITNOW_MAX_PAGES", "5"))

MAX_JOB_AGE_DAYS = int(os.environ.get("JOB_MAX_AGE_DAYS", "7"))
CONCURRENCY = int(os.environ.get("ATS_WORKERS", "18"))
REQUEST_TIMEOUT = 18
HTML_TIMEOUT = 18

# Archive discovery is deliberately capped. The persistent Companies sheet
# keeps validated boards between runs, so we don't need to rediscover
# everything from zero every day.
ARCHIVE_CANDIDATE_CAP = 3000
MAX_NEW_VALIDATIONS_PER_ATS = 900
MAX_COMPANY_SEEDS = 500
MAX_WELLFOUND_PAGES_PER_ROLE = 6
MAX_CUTSHORT_PAGES_PER_ROLE = 5
MAX_EXTERNAL_JOB_PAGES = 180

USER_AGENT = (
    "Remote4.me Job Crawler/7.0 "
    "(public job discovery; contact via remote4.me)"
)

RETRY_CODES = {408, 425, 429, 500, 502, 503, 504}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "application/json,text/html,application/xhtml+xml,text/plain,*/*",
})


# ============================================================
# ROLE KEYWORDS
# ============================================================

TARGET_TITLE_PATTERNS = ['\\bcustomer support\\b', '\\bcustomer support specialist\\b', '\\bcustomer support associate\\b', '\\bcustomer support representative\\b', '\\bcustomer support agent\\b', '\\bcustomer support advisor\\b', '\\bcustomer support consultant\\b', '\\bcustomer support coordinator\\b', '\\bcustomer support analyst\\b', '\\bcustomer support executive\\b', '\\bcustomer support manager\\b', '\\bcustomer support lead\\b', '\\bcustomer support supervisor\\b', '\\bcustomer support director\\b', '\\bhead of customer support\\b', '\\bglobal customer support\\b', '\\binternational customer support\\b', '\\bcustomer technical support\\b', '\\bcustomer service\\b', '\\bcustomer service specialist\\b', '\\bcustomer service associate\\b', '\\bcustomer service representative\\b', '\\bcustomer service agent\\b', '\\bcustomer service advisor\\b', '\\bcustomer service consultant\\b', '\\bcustomer service coordinator\\b', '\\bcustomer service analyst\\b', '\\bcustomer service executive\\b', '\\bcustomer service manager\\b', '\\bcustomer service lead\\b', '\\bcustomer service supervisor\\b', '\\bcustomer service director\\b', '\\bhead of customer service\\b', '\\bcustomer care\\b', '\\bcustomer care specialist\\b', '\\bcustomer care associate\\b', '\\bcustomer care representative\\b', '\\bcustomer care agent\\b', '\\bcustomer care advisor\\b', '\\bcustomer care manager\\b', '\\bcustomer care lead\\b', '\\bcustomer experience\\b', '\\bcustomer experience specialist\\b', '\\bcustomer experience associate\\b', '\\bcustomer experience representative\\b', '\\bcustomer experience advisor\\b', '\\bcustomer experience manager\\b', '\\bcustomer experience lead\\b', '\\bcustomer experience analyst\\b', '\\bcustomer experience operations\\b', '\\bhead of customer experience\\b', '\\bcustomer experience director\\b', '\\bclient support\\b', '\\bclient support specialist\\b', '\\bclient support associate\\b', '\\bclient support representative\\b', '\\bclient support agent\\b', '\\bclient support manager\\b', '\\bclient service\\b', '\\bclient services\\b', '\\bclient services specialist\\b', '\\bclient services associate\\b', '\\bclient services representative\\b', '\\bclient services manager\\b', '\\buser support\\b', '\\buser support specialist\\b', '\\buser support associate\\b', '\\buser support representative\\b', '\\buser support agent\\b', '\\buser support manager\\b', '\\buser services\\b', '\\buser services specialist\\b', '\\bmember support\\b', '\\bmember support specialist\\b', '\\bmember support associate\\b', '\\bmember services\\b', '\\bmember services specialist\\b', '\\bcommunity support\\b', '\\bcommunity support specialist\\b', '\\bcommunity support manager\\b', '\\bcommunity operations\\b', '\\bpartner support\\b', '\\bpartner support specialist\\b', '\\bpartner support manager\\b', '\\bpartner services\\b', '\\bmerchant support\\b', '\\bmerchant support specialist\\b', '\\bmerchant support representative\\b', '\\bmerchant success\\b', '\\bseller support\\b', '\\bseller support specialist\\b', '\\bvendor support\\b', '\\bvendor support specialist\\b', '\\bprovider support\\b', '\\bprovider support specialist\\b', '\\bcustomer support engineer\\b', '\\bcustomer support technician\\b', '\\bsupport engineer\\b', '\\bsupport technician\\b', '\\btechnical support technician\\b', '\\bproduct support technician\\b', '\\bapplication support technician\\b', '\\bplatform support technician\\b', '\\bsupport specialist\\b', '\\bsupport associate\\b', '\\bsupport representative\\b', '\\bsupport agent\\b', '\\bsupport advisor\\b', '\\bsupport consultant\\b', '\\bsupport coordinator\\b', '\\bsupport analyst\\b', '\\bsupport executive\\b', '\\bsupport administrator\\b', '\\bsupport lead\\b', '\\bsupport manager\\b', '\\bsupport supervisor\\b', '\\bsupport operations\\b', '\\bsupport operations specialist\\b', '\\bsupport operations associate\\b', '\\bsupport operations analyst\\b', '\\bsupport operations manager\\b', '\\bsupport program manager\\b', '\\bsupport enablement\\b', '\\bsupport quality\\b', '\\bsupport quality analyst\\b', '\\bsupport quality specialist\\b', '\\bsupport trainer\\b', '\\bsupport training specialist\\b', '\\bsupport workforce\\b', '\\bsupport workforce analyst\\b', '\\bsupport workforce manager\\b', '\\bescalation support\\b', '\\bescalations specialist\\b', '\\bescalations manager\\b', '\\bescalation specialist\\b', '\\bescalation manager\\b', '\\btechnical escalation\\b', '\\bincident support\\b', '\\btechnical support\\b', '\\btechnical support specialist\\b', '\\btechnical support associate\\b', '\\btechnical support representative\\b', '\\btechnical support agent\\b', '\\btechnical support advisor\\b', '\\btechnical support consultant\\b', '\\btechnical support coordinator\\b', '\\btechnical support analyst\\b', '\\btechnical support engineer\\b', '\\btechnical support lead\\b', '\\btechnical support manager\\b', '\\btechnical support supervisor\\b', '\\btechnical support director\\b', '\\bhead of technical support\\b', '\\btechnical customer support\\b', '\\btechnical customer service\\b', '\\btechnical assistance specialist\\b', '\\btechnical assistance\\b', '\\btechnical helpdesk\\b', '\\btechnical help desk\\b', '\\btechnical service desk\\b', '\\bproduct support\\b', '\\bproduct support specialist\\b', '\\bproduct support associate\\b', '\\bproduct support representative\\b', '\\bproduct support agent\\b', '\\bproduct support advisor\\b', '\\bproduct support analyst\\b', '\\bproduct support engineer\\b', '\\bproduct support consultant\\b', '\\bproduct support manager\\b', '\\bproduct support lead\\b', '\\bproduct support operations\\b', '\\bsoftware support\\b', '\\bsoftware support specialist\\b', '\\bsoftware support associate\\b', '\\bsoftware support representative\\b', '\\bsoftware support engineer\\b', '\\bsoftware support analyst\\b', '\\bsoftware support manager\\b', '\\bapplication support\\b', '\\bapplication support specialist\\b', '\\bapplication support associate\\b', '\\bapplication support analyst\\b', '\\bapplication support engineer\\b', '\\bapplication support manager\\b', '\\bplatform support\\b', '\\bplatform support specialist\\b', '\\bplatform support associate\\b', '\\bplatform support engineer\\b', '\\bplatform support analyst\\b', '\\bplatform support manager\\b', '\\bapi support\\b', '\\bapi support specialist\\b', '\\bapi support engineer\\b', '\\bintegration support\\b', '\\bintegration support specialist\\b', '\\bintegration support engineer\\b', '\\bdeveloper support\\b', '\\bdeveloper support specialist\\b', '\\bdeveloper support engineer\\b', '\\bdeveloper experience support\\b', '\\bit support\\b', '\\bit support specialist\\b', '\\bit support associate\\b', '\\bit support representative\\b', '\\bit support engineer\\b', '\\bit support analyst\\b', '\\bit support technician\\b', '\\bit support manager\\b', '\\bit help desk\\b', '\\bhelp desk\\b', '\\bhelpdesk\\b', '\\bhelp desk specialist\\b', '\\bhelp desk analyst\\b', '\\bhelp desk technician\\b', '\\bhelp desk engineer\\b', '\\bhelp desk manager\\b', '\\bservice desk\\b', '\\bservice desk specialist\\b', '\\bservice desk analyst\\b', '\\bservice desk technician\\b', '\\bservice desk engineer\\b', '\\bservice desk manager\\b', '\\bit service desk\\b', '\\btechnical account support\\b', '\\btechnical account specialist\\b', '\\btechnical account associate\\b', '\\btechnical account manager\\b', '\\btechnical account executive\\b', '\\btechnical success\\b', '\\btechnical success specialist\\b', '\\btechnical success manager\\b', '\\btechnical customer success\\b', '\\bcustomer technical success\\b', '\\btechnical client success\\b', '\\bcustomer success\\b', '\\bcustomer success specialist\\b', '\\bcustomer success associate\\b', '\\bcustomer success representative\\b', '\\bcustomer success advisor\\b', '\\bcustomer success consultant\\b', '\\bcustomer success analyst\\b', '\\bcustomer success manager\\b', '\\bcustomer success lead\\b', '\\bcustomer success director\\b', '\\bhead of customer success\\b', '\\bvp customer success\\b', '\\bvice president customer success\\b', '\\bcustomer success operations\\b', '\\bcustomer success operations specialist\\b', '\\bcustomer success operations manager\\b', '\\bcustomer success enablement\\b', '\\bcustomer success strategy\\b', '\\bclient success\\b', '\\bclient success specialist\\b', '\\bclient success associate\\b', '\\bclient success representative\\b', '\\bclient success advisor\\b', '\\bclient success consultant\\b', '\\bclient success analyst\\b', '\\bclient success manager\\b', '\\bclient success lead\\b', '\\bclient success director\\b', '\\bhead of client success\\b', '\\bcustomer onboarding\\b', '\\bcustomer onboarding specialist\\b', '\\bcustomer onboarding associate\\b', '\\bcustomer onboarding representative\\b', '\\bcustomer onboarding manager\\b', '\\bcustomer onboarding lead\\b', '\\bclient onboarding\\b', '\\bclient onboarding specialist\\b', '\\bclient onboarding associate\\b', '\\bclient onboarding manager\\b', '\\bonboarding specialist\\b', '\\bonboarding associate\\b', '\\bonboarding representative\\b', '\\bonboarding advisor\\b', '\\bonboarding consultant\\b', '\\bonboarding manager\\b', '\\bonboarding lead\\b', '\\bonboarding operations\\b', '\\bcustomer implementation\\b', '\\bcustomer implementation specialist\\b', '\\bcustomer implementation manager\\b', '\\bclient implementation\\b', '\\bclient implementation specialist\\b', '\\bclient implementation manager\\b', '\\bimplementation specialist\\b', '\\bimplementation associate\\b', '\\bimplementation consultant\\b', '\\bimplementation analyst\\b', '\\bimplementation manager\\b', '\\bimplementation lead\\b', '\\bimplementation director\\b', '\\bimplementation operations\\b', '\\bsolutions implementation\\b', '\\bsolution implementation specialist\\b', '\\bcustomer enablement\\b', '\\bcustomer enablement specialist\\b', '\\bcustomer enablement manager\\b', '\\bclient enablement\\b', '\\bclient enablement specialist\\b', '\\bcustomer education\\b', '\\bcustomer education specialist\\b', '\\bcustomer education manager\\b', '\\bclient education\\b', '\\bclient education specialist\\b', '\\bcustomer adoption\\b', '\\bcustomer adoption specialist\\b', '\\bcustomer adoption manager\\b', '\\bcustomer retention\\b', '\\bcustomer retention specialist\\b', '\\bcustomer retention manager\\b', '\\bcustomer lifecycle\\b', '\\bcustomer lifecycle specialist\\b', '\\bcustomer lifecycle manager\\b', '\\bcustomer engagement\\b', '\\bcustomer engagement specialist\\b', '\\bcustomer engagement manager\\b', '\\bcustomer advocacy\\b', '\\bcustomer advocacy specialist\\b', '\\bcustomer advocacy manager\\b', '\\bcustomer operations\\b', '\\bcustomer operations specialist\\b', '\\bcustomer operations associate\\b', '\\bcustomer operations representative\\b', '\\bcustomer operations analyst\\b', '\\bcustomer operations manager\\b', '\\bclient operations\\b', '\\bclient operations specialist\\b', '\\bclient operations associate\\b', '\\bclient operations analyst\\b', '\\bclient operations manager\\b', '\\buser operations\\b', '\\buser operations specialist\\b', '\\buser operations associate\\b', '\\buser operations manager\\b', '\\bcx specialist\\b', '\\bcx associate\\b', '\\bcx representative\\b', '\\bcx advisor\\b', '\\bcx manager\\b', '\\bcx lead\\b', '\\bcustomer relations specialist\\b', '\\bcustomer relations representative\\b', '\\bclient relations specialist\\b', '\\bclient relations representative\\b', '\\btrust and safety specialist\\b', '\\btrust & safety specialist\\b', '\\btrust and safety operations\\b', '\\btrust & safety operations\\b']
EXCLUDED_TITLE_PATTERNS = ['\\bsales support\\b', '\\bsales development\\b', '\\bsales operations\\b', '\\bmarketing support\\b', '\\bmarketing operations\\b', '\\brecruiting support\\b', '\\brecruitment support\\b', '\\bexecutive assistant\\b', '\\badministrative assistant\\b', '\\bpersonal assistant\\b', '\\btechnical recruiter\\b', '\\bsupporting actor\\b']

TITLE_REGEX = re.compile(
    "|".join(TARGET_TITLE_PATTERNS), re.I
)
EXCLUDED_TITLE_REGEX = re.compile(
    "|".join(EXCLUDED_TITLE_PATTERNS), re.I
)

# Extra high-signal phrases. These are used for scoring/description matching,
# not as a replacement for the title filter.
SUPPORT_CONTEXT_TERMS = [
    "zendesk", "intercom", "freshdesk", "freshservice", "gorgias",
    "salesforce service cloud", "servicenow", "customer.io", "helpscout",
    "kustomer", "dixa", "front app", "hubspot service hub",
    "ticketing", "support tickets", "support queue", "customer tickets",
    "customer queries", "customer issues", "customer escalations",
    "technical troubleshooting", "troubleshooting", "bug reproduction",
    "incident management", "incident response", "l1 support", "l2 support",
    "l3 support", "tier 1", "tier 2", "tier 3", "help desk", "service desk",
    "knowledge base", "help center", "csat", "nps", "customer satisfaction",
    "customer onboarding", "customer adoption", "customer retention",
    "customer lifecycle", "customer education", "customer enablement",
    "customer advocacy", "renewals", "expansion", "churn", "implementation",
    "go live", "go-live", "product training", "client onboarding",
    "technical account", "customer success platform", "saas support",
    "product support", "application support", "platform support",
    "api support", "developer support", "integration support",
    "trust and safety", "content moderation", "user operations",
    "customer operations", "client operations", "member support",
    "merchant support", "partner support", "seller support",
    "customer care", "client services", "customer relations",
]

# These prevent broad words in descriptions from making unrelated jobs pass.
BAD_TITLE_PATTERNS = [
    r"\bsales\s+(?:support|operations|development)\b",
    r"\bmarketing\s+(?:support|operations)\b",
    r"\brecruit(?:er|ing|ment)\s+support\b",
    r"\bexecutive assistant\b",
    r"\badministrative assistant\b",
    r"\bpersonal assistant\b",
    r"\btechnical recruiter\b",
]


# ============================================================
# INDIA LOCATION
# ============================================================

INDIA_STATES = ['andhra pradesh', 'arunachal pradesh', 'assam', 'bihar', 'chhattisgarh', 'goa', 'gujarat', 'haryana', 'himachal pradesh', 'jharkhand', 'karnataka', 'kerala', 'madhya pradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha', 'orissa', 'punjab', 'rajasthan', 'sikkim', 'tamil nadu', 'telangana', 'tripura', 'uttar pradesh', 'uttarakhand', 'west bengal', 'andaman and nicobar', 'chandigarh', 'dadra and nagar haveli', 'daman and diu', 'delhi', 'jammu and kashmir', 'ladakh', 'lakshadweep', 'puducherry']
INDIA_CITIES = [
    # Major metros / common aliases
    "ahmedabad", "amritsar", "aurangabad", "sambhajinagar", "bengaluru", "bangalore",
    "bhilai", "bhubaneswar", "bikaner", "bokaro", "chandigarh", "chennai", "coimbatore",
    "cochin", "kochi", "cuttack", "dehradun", "delhi", "new delhi", "faridabad",
    "gandhinagar", "ghaziabad", "goa", "gurgaon", "gurugram", "guwahati", "gwalior",
    "howrah", "hubli", "hubballi", "hyderabad", "indore", "jaipur", "jalandhar", "jammu",
    "jamshedpur", "kanpur", "kolkata", "kota", "lucknow", "ludhiana", "madurai",
    "mangalore", "mangaluru", "meerut", "mohali", "mumbai", "bombay", "mysore", "mysuru",
    "nagpur", "nashik", "navi mumbai", "noida", "patna", "pondicherry", "puducherry",
    "prayagraj", "allahabad", "pune", "raipur", "rajkot", "ranchi", "salem", "surat",
    "thane", "thiruvananthapuram", "trivandrum", "udaipur", "vadodara", "baroda", "varanasi",
    "vijayawada", "visakhapatnam", "vizag", "warangal",

    # Andhra Pradesh
    "amaravati", "anantapur", "anantapuramu", "bhimavaram", "chittoor", "eluru", "guntur",
    "kadapa", "kakinada", "kurnool", "machilipatnam", "nellore", "ongole", "rajahmundry",
    "srikakulam", "tirupati", "tuni", "vijayanagaram", "tenali", "proddatur", "adoni",
    "hindupur", "madanapalle", "narasaraopet", "tadepalligudem", "srikalahasti",

    # Arunachal Pradesh
    "itanagar", "naharlagun", "pasighat", "tawang", "bomdila", "ziro", "along", "aalo",
    "tezu", "namsai", "roing", "seppa", "changlang", "khonsa", "daporijo",

    # Assam
    "guwahati", "silchar", "dibrugarh", "jorhat", "nagaon", "tinsukia", "tezpur", "sivasagar",
    "barpeta", "bongaigaon", "dhubri", "goalpara", "golaghat", "karimganj", "lakhimpur",
    "north lakhimpur", "mangaldoi", "diphu", "haflong", "kokrajhar", "nalbari", "morigaon",

    # Bihar
    "patna", "gaya", "bhagalpur", "muzaffarpur", "purnia", "darbhanga", "arrah", "ara",
    "begusarai", "katihar", "chapra", "chhapra", "motihari", "bettiah", "munger", "saharsa",
    "samastipur", "hajipur", "bihar sharif", "sasaram", "dehri", "siwan", "sitamarhi",
    "jehanabad", "aurangabad", "buxar", "jamui", "kishanganj", "lakhisarai", "madhepura",
    "nawada", "supaul", "vaishali", "sheikhpura", "sheohar", "araria", "bankaa",

    # Chhattisgarh
    "raipur", "bhilai", "durg", "bilaspur", "korba", "rajnandgaon", "jagdalpur", "ambikapur",
    "raigarh", "dhamtari", "kanker", "kawardha", "kondagaon", "mahasamund", "mungeli",
    "narayanpur", "sukma", "bijapur", "balod", "baloda bazar", "bemetara", "janjgir",

    # Goa
    "panaji", "panjim", "margao", "mormugao", "vasco da gama", "mapusa", "ponda", "calangute",

    # Gujarat
    "ahmedabad", "surat", "vadodara", "baroda", "rajkot", "bhavnagar", "jamnagar", "gandhinagar",
    "junagadh", "anand", "bharuch", "bhuj", "godhra", "mehsana", "morbi", "nadiad",
    "navsari", "palanpur", "patan", "porbandar", "surendranagar", "vapi", "valsad", "veraval",
    "amreli", "botad", "dahod", "himmatnagar", "modasa", "deesa", "jetpur", "kalol",

    # Haryana
    "gurugram", "gurgaon", "faridabad", "panipat", "ambala", "yamunanagar", "rohtak", "hisar",
    "karnal", "sonipat", "panchkula", "bhiwani", "sirsa", "rewari", "palwal", "jind",
    "kaithal", "thanesar", "kurukshetra", "fatehabad", "mahendragarh", "nuh", "narnaul",

    # Himachal Pradesh
    "shimla", "dharamshala", "dharamsala", "solan", "mandi", "kullu", "manali", "hamirpur",
    "una", "bilaspur", "chamba", "kangra", "kinnaur", "nahan", "sirmaur", "palampur",

    # Jharkhand
    "ranchi", "jamshedpur", "dhanbad", "bokaro", "deoghar", "hazaribagh", "giridih", "ramgarh",
    "chaibasa", "dumka", "medininagar", "daltonganj", "garhwa", "godda", "gumla", "koderma",
    "latehar", "lohardaga", "pakur", "sahebganj", "simdega", "saraikela", "chirkunda",

    # Karnataka
    "bengaluru", "bangalore", "mysuru", "mysore", "mangaluru", "mangalore", "hubballi", "hubli",
    "dharwad", "belagavi", "belgaum", "kalaburagi", "gulbarga", "shivamogga", "shimoga",
    "tumakuru", "tumkur", "davangere", "ballari", "bellary", "vijayapura", "bijapur",
    "udupi", "chikkamagaluru", "chikmagalur", "hassan", "mandya", "raichur", "bidar",
    "kolar", "chitradurga", "gadag", "haveri", "bagalkot", "yadgir", "kodagu", "madikeri",
    "karwar", "sirsi", "hosapete", "hospet", "ramanagara", "channapatna", "doddaballapur",

    # Kerala
    "thiruvananthapuram", "trivandrum", "kochi", "cochin", "kozhikode", "calicut", "kollam",
    "thrissur", "alappuzha", "alleppey", "palakkad", "kannur", "kasaragod", "kottayam",
    "malappuram", "pathanamthitta", "idukki", "manjeri", "tirur", "kottakkal", "thalassery",

    # Madhya Pradesh
    "bhopal", "indore", "jabalpur", "gwalior", "ujjain", "sagar", "dewas", "satna", "ratlam",
    "rewa", "murwara", "katni", "singrauli", "burhanpur", "khandwa", "bhind", "morena",
    "chhindwara", "vidisha", "shivpuri", "damoh", "mandsaur", "neemuch", "guna", "seoni",
    "hoshangabad", "narmadapuram", "betul", "balaghat", "khargone", "barwani", "mandla",
    "pithampur", "sanchi", "chhatarpur", "tikamgarh", "datia", "shahdol", "anupur",

    # Maharashtra
    "mumbai", "bombay", "pune", "nagpur", "nashik", "navi mumbai", "thane", "aurangabad",
    "sambhajinagar", "solapur", "kolhapur", "amravati", "nanded", "sangli", "jalgaon", "akola",
    "latur", "dhule", "ahmednagar", "satara", "ratnagiri", "chandrapur", "parbhani", "beed",
    "yavatmal", "buldhana", "washim", "gondia", "bhandara", "wardha", "hingoli", "palghar",
    "vasai", "virar", "kalyan", "dombivli", "panvel", "ulhasnagar", "badlapur", "baramati",
    "ichalkaranji", "malegaon", "manmad", "miraj", "jalna", "osmanabad", "dharashiv",

    # Manipur
    "imphal", "thoubal", "bishnupur", "churachandpur", "ukhrul", "senapati", "tamenglong",
    "chandel", "jiribam", "kakching", "kangpokpi", "noney", "pherzawl",

    # Meghalaya
    "shillong", "tura", "jowai", "nongpoh", "williamnagar", "baghmara", "nongsain",

    # Mizoram
    "aizawl", "lunglei", "champhai", "kolasib", "lawngtlai", "mamit", "serchhip", "saitual",

    # Nagaland
    "kohima", "dimapur", "mokokchung", "tuensang", "wokha", "mon", "zunheboto", "phek",
    "kiphire", "longleng", "peren", "chumoukedima", "niuland", "shamator",

    # Odisha
    "bhubaneswar", "cuttack", "rourkela", "berhampur", "brahmapur", "sambalpur", "puri",
    "balasore", "balasor", "baripada", "bhadrak", "jharsuguda", "jeypore", "koraput",
    "angul", "dhenkanal", "bargarh", "boudh", "deogarh", "gajapati", "jagatsinghpur",
    "jajpur", "kalahandi", "kendrapara", "kendujhar", "keonjhar", "khordha", "malkangiri",
    "mayurbhanj", "nabarangpur", "nayagarh", "nuapada", "rayagada", "sundargarh",

    # Punjab
    "amritsar", "ludhiana", "jalandhar", "patiala", "bathinda", "mohali", "sahibzada ajit singh nagar",
    "pathankot", "batala", "hoshiarpur", "moga", "abohar", "malerkotla", "kapurthala",
    "phagwara", "firozpur", "ferozepur", "muktsar", "barnala", "sangrur", "rupnagar",
    "ropar", "fazilka", "tarn taran", "gurdaspur", "nawanshahr", "shaheed bhagat singh nagar",

    # Rajasthan
    "jaipur", "jodhpur", "udaipur", "kota", "bikaner", "ajmer", "bhilwara", "alwar", "bharatpur",
    "sikar", "pali", "shri ganganagar", "ganganagar", "hanumangarh", "churu", "jhunjhunu",
    "barmer", "jaisalmer", "nagaur", "tonk", "sawai madhopur", "bundi", "chittorgarh",
    "dungarpur", "banswara", "pratapgarh", "raj samand", "rajsamand", "dausa", "dholpur",
    "karauli", "beawar", "kishangarh", "makrana", "sujangarh", "sri madhopur", "mount abu",

    # Sikkim
    "gangtok", "namchi", "goyaltok", "mangan", "singtam", "rangpo", "jorethang",

    # Tamil Nadu
    "chennai", "coimbatore", "madurai", "salem", "tiruchirappalli", "trichy", "tiruppur",
    "erode", "vellore", "thoothukudi", "tuticorin", "tirunelveli", "dindigul", "thanjavur",
    "nagercoil", "kanchipuram", "chengalpattu", "hosur", "karur", "namakkal", "cuddalore",
    "villupuram", "virudhunagar", "sivakasi", "ramanathapuram", "sivaganga", "pudukkottai",
    "nagapattinam", "mayiladuthurai", "tiruvallur", "tiruvannamalai", "dharmapuri", "krishnagiri",
    "ariyalur", "perambalur", "ooty", "udhagamandalam", "pollachi", "karaikudi", "kumbakonam",

    # Telangana
    "hyderabad", "warangal", "hanamkonda", "nizamabad", "karimnagar", "khammam", "ramagundam",
    "mahbubnagar", "mahabubnagar", "nalgonda", "adilabad", "siddipet", "suryapet", "jagtial",
    "kamareddy", "mancherial", "nirmal", "sangareddy", "vikarabad", "wanaparthy", "bhupalpally",
    "kothagudem", "narayanpet", "yadadri bhuvanagiri", "medak", "jangaon",

    # Tripura
    "agartala", "udaipur", "dharmanagar", "kailashahar", "belonia", "ambassa", "khowai",

    # Uttar Pradesh
    "lucknow", "kanpur", "ghaziabad", "agra", "varanasi", "prayagraj", "allahabad", "meerut",
    "noida", "greater noida", "bareilly", "aligarh", "moradabad", "saharanpur", "gorakhpur",
    "mathura", "firozabad", "jhansi", "muzaffarnagar", "rampur", "shahjahanpur", "hapur",
    "ayodhya", "faizabad", "mirzapur", "sultanpur", "basti", "deoria", "azamgarh", "mau",
    "ballia", "jaunpur", "ghazipur", "sitapur", "lakhimpur kheri", "bahraich", "gonda",
    "barabanki", "rae bareli", "raebareli", "unnao", "hardoi", "farrukhabad", "etawah",
    "mainpuri", "budaun", "pilibhit", "bulandshahr", "bijnor", "amroha", "sambhal", "kasganj",
    "etah", "fatehpur", "kaushambi", "pratapgarh", "chitrakoot", "banda", "mahoba", "hamirpur",
    "lalitpur", "mirzapur", "sonbhadra", "robertsganj", "shamli", "orai", "sitapur",

    # Uttarakhand
    "dehradun", "haridwar", "roorkee", "haldwani", "nainital", "rudrapur", "kashipur",
    "rishikesh", "mussoorie", "almora", "pithoragarh", "bageshwar", "chamoli", "gopeshwar",
    "pauri", "srinagar", "tehri", "uttarkashi", "kotdwar", "ramnagar", "khatima",

    # West Bengal
    "kolkata", "calcutta", "howrah", "siliguri", "durgapur", "asansol", "haldia", "kharagpur",
    "malda", "english bazar", "berhampore", "baharampur", "jalpaiguri", "cooch behar", "alipurduar",
    "raiganj", "balurghat", "krishnanagar", "burdwan", "bardhaman", "bankura", "purulia",
    "medinipur", "tamluk", "diamond harbour", "barrackpore", "barasat", "habra", "chandannagar",
    "serampore", "rishra", "kalyani", "dankuni", "bolpur", "santiniketan",

    # Union territories / island territories
    "srinagar", "jammu", "leh", "kargil", "port blair", "sri vijaya puram", "silvassa",
    "daman", "diu", "kavaratti", "agatti", "pondicherry", "puducherry", "karaikal", "mahe", "yanam"
]
# Keep the regex compact and deterministic while removing duplicates.
INDIA_CITIES = sorted(set(x.strip().lower() for x in INDIA_CITIES if x.strip()))

INDIA_COUNTRY_RE = re.compile(
    r"\b(?:india|republic of india|indian)\b", re.I
)

INDIA_REGEX = re.compile(
    "|".join(
        [r"\b" + re.escape(x) + r"\b" for x in INDIA_STATES + INDIA_CITIES]
        + [r"\bindia\b", r"\brepublic of india\b"]
    ),
    re.I,
)

REMOTE_RE = re.compile(
    r"\b(?:remote|fully remote|100% remote|work from home|work-from-home|"
    r"home based|home-based|distributed)\b",
    re.I,
)

HYBRID_RE = re.compile(
    r"\b(?:hybrid|partially remote|remote hybrid)\b",
    re.I,
)

INDIA_REMOTE_CONTEXT_RE = re.compile(
    r"(?:"
    r"remote(?:\s+(?:role|position|job))?\s*(?:in|from|within)\s+india"
    r"|india\s*[-–—,:()]?\s*remote"
    r"|remote\s*[-–—,:()]?\s*india"
    r"|india[-\s]+based"
    r"|based\s+in\s+india"
    r"|located\s+in\s+india"
    r"|work\s+(?:remotely|remote)\s+from\s+india"
    r"|must\s+be\s+based\s+in\s+india"
    r"|hiring\s+in\s+india"
    r")",
    re.I,
)


# ============================================================
# ATS DEFINITIONS
# ============================================================

ATS_DOMAINS = {
    "Greenhouse": [
        "boards.greenhouse.io",
        "job-boards.greenhouse.io",
    ],
    "Lever": [
        "jobs.lever.co",
        "jobs.eu.lever.co",
    ],
    "Ashby": [
        "jobs.ashbyhq.com",
    ],
    "Workable": [
        "apply.workable.com",
        "workable.com",
    ],
    "SmartRecruiters": [
        "careers.smartrecruiters.com",
        "jobs.smartrecruiters.com",
    ],
    "Recruitee": [
        "recruitee.com",
    ],
    "Breezy": [
        "breezy.hr",
    ],
    "Teamtailor": [
        "teamtailor.com",
    ],
    "BambooHR": [
        "bamboohr.com",
    ],
    "Personio": [
        "jobs.personio.de",
        "jobs.personio.com",
    ],
    "Workday": [
        "myworkdayjobs.com",
    ],
    "iCIMS": [
        "icims.com",
    ],
    "Taleo": [
        "taleo.net",
    ],
    "ADP": [
        "workforcenow.adp.com",
        "adp.com",
    ],
    "SuccessFactors": [
        "successfactors.com",
    ],
}

ARCHIVE_DOMAINS = {
    "Greenhouse": ["boards.greenhouse.io", "job-boards.greenhouse.io"],
    "Lever": ["jobs.lever.co", "jobs.eu.lever.co"],
    "Ashby": ["jobs.ashbyhq.com"],
    "Workable": ["apply.workable.com"],
    "SmartRecruiters": ["careers.smartrecruiters.com"],
    "Recruitee": ["*.recruitee.com"],
    "Breezy": ["*.breezy.hr"],
    "Teamtailor": ["*.teamtailor.com"],
    "BambooHR": ["*.bamboohr.com"],
    "Personio": ["*.jobs.personio.de", "*.jobs.personio.com"],
    "Workday": ["*.myworkdayjobs.com"],
}


# ============================================================
# HTTP HELPERS
# ============================================================

def clean_text(value):
    if value is None:
        return ""
    s = str(value)
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def get_json(url, params=None, method="GET", json_body=None, timeout=REQUEST_TIMEOUT):
    for attempt in range(3):
        try:
            if method == "POST":
                r = SESSION.post(url, params=params, json=json_body, timeout=timeout)
            else:
                r = SESSION.get(url, params=params, timeout=timeout)

            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    return None

            if r.status_code in RETRY_CODES and attempt < 2:
                time.sleep(1.0 + attempt)
                continue

            return None
        except requests.RequestException:
            if attempt < 2:
                time.sleep(1.0 + attempt)
            else:
                return None
    return None


def get_html(url, timeout=HTML_TIMEOUT):
    for attempt in range(2):
        try:
            r = SESSION.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r.text, r.url
            if r.status_code in RETRY_CODES and attempt == 0:
                time.sleep(1)
                continue
            return "", r.url
        except requests.RequestException:
            if attempt == 0:
                time.sleep(1)
            else:
                return "", url
    return "", url


def normalize_url(url):
    if not url:
        return ""
    try:
        p = urlparse(str(url).strip())
        if p.scheme not in {"http", "https"} or not p.netloc:
            return ""
        return f"{p.scheme.lower()}://{p.netloc.lower()}{p.path}".rstrip("/")
    except Exception:
        return str(url).strip().rstrip("/")


def same_or_subdomain(host, root):
    host = host.lower().split(":")[0]
    root = root.lower().split(":")[0]
    return host == root or host.endswith("." + root)


def slugify_company(name):
    s = clean_text(name).lower()
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s[:100]


def humanize_slug(slug):
    s = unquote(clean_text(slug))
    s = re.sub(r"[_-]+", " ", s).strip()
    if not s:
        return ""
    return " ".join(
        p if p.isupper() else p[:1].upper() + p[1:]
        for p in s.split()
    )


def parse_date(value):
    if value is None or value == "":
        return ""

    # Unix seconds / milliseconds (used by some public job APIs).
    if isinstance(value, (int, float)):
        try:
            ts = float(value)
            if ts > 10_000_000_000:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        except Exception:
            pass

    s = str(value).strip()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        pass

    # RSS feeds commonly use RFC 2822 dates, e.g. Wed, 01 Oct 2026 12:00:00 GMT.
    try:
        return parsedate_to_datetime(s).date().isoformat()
    except Exception:
        pass

    m = re.search(r"(20\d{2}-\d{2}-\d{2})", s)
    return m.group(1) if m else ""


def recent_enough(date_string):
    if not date_string:
        return True
    try:
        d = datetime.strptime(date_string, "%Y-%m-%d").date()
        return d >= datetime.now(timezone.utc).date() - timedelta(days=MAX_JOB_AGE_DAYS)
    except Exception:
        return True


def title_relevant(title):
    t = clean_text(title)
    if not t or EXCLUDED_TITLE_REGEX.search(t):
        return False
    if any(re.search(p, t, re.I) for p in BAD_TITLE_PATTERNS):
        return False
    return bool(TITLE_REGEX.search(t))


def score_job(title, description="", location=""):
    t = clean_text(title).lower()
    d = clean_text(description).lower()
    l = clean_text(location).lower()
    score = 0
    matches = []

    # Strong title match.
    for p in TARGET_TITLE_PATTERNS:
        m = re.search(p, t, re.I)
        if m:
            phrase = m.group(0).lower()
            if phrase not in matches:
                matches.append(phrase)
            score += 60
            break

    for term in SUPPORT_CONTEXT_TERMS:
        if term in d:
            score += 2
            if term not in matches and len(matches) < 6:
                matches.append(term)

    if "india" in l:
        score += 15
    if REMOTE_RE.search(l):
        score += 15
    if HYBRID_RE.search(l):
        score += 12

    return score, ", ".join(matches[:6])


def classify_workplace(location="", workplace_type="", remote=False, structured=None, description=""):
    def norm(v):
        s = clean_text(v).lower().replace("_", " ").replace("-", " ")
        if s in {"remote", "fully remote", "100% remote", "work from home", "home based"}:
            return "Remote"
        if s in {"hybrid", "remote hybrid", "partially remote"}:
            return "Hybrid"
        if s in {"on site", "onsite", "office", "in office"}:
            return "On-site"
        return ""

    x = norm(workplace_type)
    if x:
        return x

    if isinstance(remote, bool) and remote:
        return "Remote"
    if isinstance(remote, str) and remote.lower() in {"true", "yes", "1", "remote"}:
        return "Remote"

    if isinstance(structured, dict):
        # Explicit workplace/remote keys.
        for key in (
            "workplaceType", "workplace_type", "workplace", "workMode",
            "work_mode", "workArrangement", "work_arrangement",
        ):
            if key in structured:
                x = norm(structured.get(key))
                if x:
                    return x
        for key in ("isRemote", "is_remote", "remote", "telecommuting"):
            if key in structured and structured.get(key) is True:
                return "Remote"

    if HYBRID_RE.search(clean_text(location)):
        return "Hybrid"
    if REMOTE_RE.search(clean_text(location)):
        return "Remote"

    # Only a strong explicit remote/hybrid sentence may rescue a generic
    # location such as "India" or "Remote".
    text = clean_text(description)[:30000]
    if INDIA_REMOTE_CONTEXT_RE.search(text):
        return "Remote"

    return ""


def classify_india(location="", country_code="", address_country="", description="", workplace=""):
    code = clean_text(country_code).lower()
    addr = clean_text(address_country).lower()
    loc = clean_text(location)

    if code in {"in", "ind", "india"} or addr in {"in", "ind", "india"}:
        return True

    if INDIA_REGEX.search(loc):
        return True

    if workplace in {"Remote", "Hybrid"} and INDIA_REMOTE_CONTEXT_RE.search(clean_text(description)[:30000]):
        return True

    return False


def allowed_india(location, country_code="", address_country="", description="", workplace=""):
    if workplace not in {"Remote", "Hybrid"}:
        return False
    return classify_india(
        location, country_code, address_country, description, workplace
    )


def job_key(job):
    url = normalize_url(job.get("job_url", ""))
    if url:
        return ("url", url)
    raw = "|".join([
        clean_text(job.get("source", "")).lower(),
        clean_text(job.get("company", "")).lower(),
        clean_text(job.get("title", "")).lower(),
        clean_text(job.get("posted_date", "")),
    ])
    return ("hash", hashlib.sha256(raw.encode()).hexdigest())


# ============================================================
# PUBLIC API SOURCES
# ============================================================

def fetch_jobicy():
    jobs = []
    data = get_json("https://jobicy.com/api/v2/remote-jobs", params={"count": 200})
    if not isinstance(data, dict):
        return jobs
    items = data.get("jobs") or data.get("data") or []
    for j in items:
        title = clean_text(j.get("jobTitle") or j.get("title"))
        if not title_relevant(title):
            continue
        loc = clean_text(j.get("jobGeo") or j.get("location") or j.get("geo"))
        desc = clean_text(j.get("jobDescription") or j.get("description"))
        workplace = classify_workplace(
            loc,
            j.get("workplaceType") or j.get("jobGeo"),
            j.get("isRemote", False),
            j,
            desc,
        )
        if not allowed_india(loc, j.get("country"), "", desc, workplace):
            continue
        posted = parse_date(j.get("pubDate") or j.get("datePosted") or j.get("publishedAt"))
        if not recent_enough(posted):
            continue
        link = clean_text(j.get("url") or j.get("jobUrl") or j.get("applyUrl"))
        if not link:
            continue
        score, matched = score_job(title, desc, loc)
        jobs.append({
            "added": datetime.now(timezone.utc).date().isoformat(),
            "title": title,
            "company": clean_text(j.get("companyName") or j.get("company")),
            "location": loc or "India",
            "workplace": workplace,
            "posted_date": posted,
            "job_url": link,
            "source": "Jobicy",
            "score": score,
            "matched": matched,
        })
    return jobs


def fetch_remoteok():
    jobs = []
    data = get_json("https://remoteok.com/api")
    if not isinstance(data, list):
        return jobs
    for j in data:
        if not isinstance(j, dict) or not j.get("position"):
            continue
        title = clean_text(j.get("position"))
        if not title_relevant(title):
            continue
        loc = clean_text(j.get("location"))
        tags = clean_text(" ".join(j.get("tags") or []))
        desc = clean_text(j.get("description"))
        combined = " ".join([loc, tags, desc])
        # Remote OK is a remote feed; India eligibility still must be explicit.
        workplace = "Remote"
        if not classify_india(loc, "", "", combined, workplace):
            continue
        posted = parse_date(j.get("date"))
        if not recent_enough(posted):
            continue
        link = clean_text(j.get("url") or j.get("apply_url"))
        if not link:
            continue
        score, matched = score_job(title, desc, loc)
        jobs.append({
            "added": datetime.now(timezone.utc).date().isoformat(),
            "title": title,
            "company": clean_text(j.get("company")),
            "location": loc or "India / Remote",
            "workplace": "Remote",
            "posted_date": posted,
            "job_url": link,
            "source": "Remote OK",
            "score": score,
            "matched": matched,
        })
    return jobs


def _append_public_job(jobs, title, company, location, workplace, posted, link, source, description=""):
    """Shared validation/normalization for public feed/API records."""
    title = clean_text(title)
    if not title_relevant(title):
        return
    desc = clean_text(description)
    loc = clean_text(location)
    workplace = clean_text(workplace) or "Remote"
    if workplace.lower() == "remote":
        workplace = "Remote"
    elif workplace.lower() == "hybrid":
        workplace = "Hybrid"
    else:
        workplace = workplace.title()
    if not allowed_india(loc, "", "", desc, workplace):
        return
    posted = parse_date(posted)
    if not recent_enough(posted):
        return
    link = clean_text(link)
    if not link:
        return
    score, matched = score_job(title, desc, loc)
    jobs.append({
        "added": datetime.now(timezone.utc).date().isoformat(),
        "title": title,
        "company": clean_text(company),
        "location": loc or "India / Remote",
        "workplace": workplace,
        "posted_date": posted,
        "job_url": link,
        "source": source,
        "score": score,
        "matched": matched,
    })


def fetch_himalayas():
    """Use Himalayas' public search API as a discovery source.

    We query India + support-related terms rather than walking the entire
    100k+ global feed. Results remain subject to Remote4.me's 7-day and role
    filters. The original Himalayas URL is retained for review; publishing
    decisions remain manual.
    """
    jobs = []
    seen = set()
    terms = [
        "customer support",
        "customer success",
        "technical support",
        "product support",
    ]
    total_requests = 0
    for term in terms:
        for page in range(1, HIMALAYAS_MAX_PAGES + 1):
            params = {
                "q": term,
                "country": "IN",
                "sort": "recent",
                "page": page,
            }
            data = get_json("https://himalayas.app/jobs/api/search", params=params, timeout=25)
            total_requests += 1
            if not isinstance(data, dict):
                break
            items = data.get("jobs") or []
            if not items:
                break
            for j in items:
                if not isinstance(j, dict):
                    continue
                jid = clean_text(j.get("guid") or j.get("applicationLink"))
                if jid and jid in seen:
                    continue
                if jid:
                    seen.add(jid)
                restrictions = j.get("locationRestrictions") or []
                names = []
                for r in restrictions:
                    if isinstance(r, dict):
                        names.extend([clean_text(r.get("alpha2")), clean_text(r.get("name"))])
                loc = ", ".join([x for x in names if x]) or "India / Remote"
                desc = clean_text(j.get("description") or j.get("excerpt"))
                link = clean_text(j.get("applicationLink"))
                _append_public_job(
                    jobs,
                    j.get("title"),
                    j.get("companyName"),
                    loc,
                    "Remote",
                    j.get("pubDate"),
                    link,
                    "Himalayas",
                    desc,
                )
    print(f"Himalayas: {len(jobs)} accepted from {total_requests} API request(s)")
    return jobs


def fetch_remotive():
    jobs = []
    data = get_json("https://remotive.com/api/remote-jobs", params={"limit": 100})
    if not isinstance(data, dict):
        print("Remotive: no JSON response")
        return jobs
    for j in data.get("jobs") or []:
        if not isinstance(j, dict):
            continue
        loc = clean_text(j.get("candidate_required_location") or j.get("location"))
        desc = clean_text(j.get("description") or j.get("job_description"))
        # Remotive feed is remote by definition; India eligibility must still
        # be explicit in candidate-required location or description.
        if not classify_india(loc, "", "", desc, "Remote"):
            continue
        _append_public_job(
            jobs,
            j.get("title"),
            j.get("company_name") or j.get("company"),
            loc or "India / Remote",
            "Remote",
            j.get("publication_date") or j.get("date"),
            j.get("url") or j.get("job_url"),
            "Remotive",
            desc,
        )
    return jobs


def fetch_arbeitnow():
    jobs = []
    seen = set()
    for page in range(1, ARBEITNOW_MAX_PAGES + 1):
        data = get_json("https://www.arbeitnow.com/api/job-board-api", params={"page": page})
        if not isinstance(data, dict):
            break
        items = data.get("data") or []
        if not items:
            break
        for j in items:
            if not isinstance(j, dict):
                continue
            link = clean_text(j.get("url"))
            if link in seen:
                continue
            seen.add(link)
            loc = clean_text(j.get("location"))
            desc = clean_text(j.get("description"))
            remote = j.get("remote")
            workplace = "Remote" if remote is True else classify_workplace(loc, "", remote, j, desc)
            _append_public_job(
                jobs,
                j.get("title"),
                j.get("company_name") or j.get("company"),
                loc,
                workplace,
                j.get("created_at") or j.get("date"),
                link,
                "Arbeitnow",
                desc,
            )
    return jobs


def fetch_working_nomads():
    jobs = []
    data = get_json("https://www.workingnomads.com/api/exposed_jobs/", timeout=25)
    if not isinstance(data, list):
        print("Working Nomads: unexpected API response")
        return jobs
    for j in data:
        if not isinstance(j, dict):
            continue
        loc = clean_text(j.get("location"))
        desc = clean_text(j.get("description"))
        # Public feed is remote-only. India must be explicit.
        if not classify_india(loc, "", "", desc, "Remote"):
            continue
        _append_public_job(
            jobs,
            j.get("title"),
            j.get("company_name"),
            loc,
            "Remote",
            j.get("pub_date"),
            j.get("url"),
            "Working Nomads",
            desc,
        )
    return jobs


def fetch_wwr_rss():
    """Parse WWR's public RSS feed without scraping its search UI."""
    jobs = []
    try:
        r = SESSION.get(
            "https://weworkremotely.com/remote-jobs.rss",
            timeout=25,
            headers={"Accept": "application/rss+xml,application/xml,text/xml,*/*"},
        )
        if r.status_code != 200:
            print(f"We Work Remotely RSS: HTTP {r.status_code}")
            return jobs
        soup = BeautifulSoup(r.text, "xml")
        for item in soup.find_all("item"):
            title = clean_text(item.find("title").get_text(" ", strip=True) if item.find("title") else "")
            desc = clean_text(item.find("description").get_text(" ", strip=True) if item.find("description") else "")
            link = clean_text(item.find("link").get_text(" ", strip=True) if item.find("link") else "")
            pub = clean_text(item.find("pubDate").get_text(" ", strip=True) if item.find("pubDate") else "")
            # RSS title is commonly "Role: Company".
            company = ""
            if ":" in title:
                title, company = [clean_text(x) for x in title.split(":", 1)]
            combined = " ".join([desc, title, company])
            if not re.search(r"\bindia\b|\bindian\b", combined, re.I):
                continue
            _append_public_job(
                jobs,
                title,
                company,
                "India / Remote",
                "Remote",
                pub,
                link,
                "We Work Remotely",
                desc,
            )
    except requests.RequestException as exc:
        print(f"We Work Remotely RSS: {exc}")
    return jobs


def fetch_startup_jobs():
    """Fetch India remote/hybrid support jobs from Startup Jobs.

    Startup Jobs' free API is limited to the last 14 days and 20 requests/minute.
    We paginate the remote and hybrid India result sets, then apply Remote4.me's
    stricter 7-day + support-role filters locally. The API key is read only from
    the STARTUP_JOBS_API_KEY environment variable and is never hard-coded.
    """
    if not STARTUP_JOBS_API_KEY:
        print("Startup Jobs: STARTUP_JOBS_API_KEY not configured; skipping")
        return []

    endpoint = "https://api.startup.jobs/v1/jobs"
    jobs = []
    seen_ids = set()
    request_count = 0

    def fetch_pages(workplace_type):
        nonlocal request_count
        cursor = None
        pages = 0
        while pages < STARTUP_JOBS_MAX_PAGES:
            params = {
                "country": "IN",
                "workplace_type": workplace_type,
                "limit": 50,
            }
            if cursor is not None:
                params["starting_after"] = cursor

            # Stay below the documented 20 requests/minute free-tier limit.
            if request_count:
                time.sleep(3.2)

            try:
                r = SESSION.get(
                    endpoint,
                    params=params,
                    headers={"Authorization": f"Bearer {STARTUP_JOBS_API_KEY}"},
                    timeout=25,
                )
                request_count += 1
            except requests.RequestException as exc:
                print(f"Startup Jobs {workplace_type}: request error: {exc}")
                break

            if r.status_code == 429:
                print(f"Startup Jobs {workplace_type}: rate limited; stopping this stream")
                break
            if r.status_code != 200:
                print(f"Startup Jobs {workplace_type}: HTTP {r.status_code}: {clean_text(r.text)[:180]}")
                break

            try:
                payload = r.json()
            except ValueError:
                print(f"Startup Jobs {workplace_type}: invalid JSON")
                break

            for j in payload.get("data") or []:
                if not isinstance(j, dict):
                    continue
                jid = str(j.get("id") or j.get("url") or "")
                if jid and jid in seen_ids:
                    continue
                if jid:
                    seen_ids.add(jid)

                title = clean_text(j.get("title"))
                if not title_relevant(title):
                    continue

                loc_obj = j.get("location") or {}
                city = clean_text(loc_obj.get("city"))
                state = clean_text(loc_obj.get("state"))
                country = clean_text(loc_obj.get("country"))
                country_code = clean_text(loc_obj.get("country_code"))
                loc_parts = [x for x in [city, state, country] if x]
                loc = ", ".join(loc_parts) or "India"

                desc = clean_text(j.get("description_html"))
                workplace = clean_text(j.get("workplace_type"))
                workplace = {"remote": "Remote", "hybrid": "Hybrid", "on-site": "On-site"}.get(
                    workplace.lower(), workplace
                )

                if not allowed_india(loc, country_code, country, desc, workplace):
                    continue

                posted = parse_date(j.get("published_at"))
                if not recent_enough(posted):
                    continue

                link = clean_text(j.get("url"))
                if not link:
                    continue

                company_obj = j.get("company") or {}
                company = clean_text(company_obj.get("name")) or "Startup Jobs Company"
                score, matched = score_job(title, desc, loc)
                # Keep the source-required Startup Jobs URL; their API terms
                # require a dofollow link back to the original listing.
                jobs.append({
                    "added": datetime.now(timezone.utc).date().isoformat(),
                    "title": title,
                    "company": company,
                    "location": loc,
                    "workplace": workplace,
                    "posted_date": posted,
                    "job_url": link,
                    "source": "Startup Jobs",
                    "score": score,
                    "matched": matched,
                })

            if not payload.get("has_more"):
                break
            cursor = payload.get("next_cursor")
            if cursor is None:
                break
            pages += 1

        print(f"Startup Jobs {workplace_type}: scanned {pages + 1 if request_count else 0} page(s)")

    fetch_pages("remote")
    fetch_pages("hybrid")
    return jobs


# ============================================================
# ATS NORMALIZERS
# ============================================================

def fetch_greenhouse(slug):
    url = f"https://boards-api.greenhouse.io/v1/boards/{quote(slug, safe='')}/jobs"
    data = get_json(url, params={"content": "true"}, timeout=25)
    if not isinstance(data, dict):
        return []
    out = []
    for j in data.get("jobs") or []:
        title = clean_text(j.get("title"))
        if not title_relevant(title):
            continue
        loc = clean_text((j.get("location") or {}).get("name"))
        desc = clean_text(j.get("content"))
        workplace = classify_workplace(loc, "", False, j, desc)
        if not allowed_india(loc, "", "", desc, workplace):
            # Greenhouse often lacks explicit workplace metadata. The job
            # detail content is used only for strong India remote wording.
            continue
        posted = parse_date(j.get("updated_at") or j.get("first_published"))
        if not recent_enough(posted):
            continue
        link = clean_text(j.get("absolute_url"))
        if not link:
            continue
        score, matched = score_job(title, desc, loc)
        out.append({
            "title": title, "company": humanize_slug(slug), "location": loc or "India",
            "workplace": workplace, "posted_date": posted, "job_url": link,
            "source": "Greenhouse", "job_id": str(j.get("id") or ""),
            "score": score, "matched": matched,
        })
    return out


def fetch_lever(slug, eu=False):
    base = "https://api.eu.lever.co" if eu else "https://api.lever.co"
    url = f"{base}/v0/postings/{quote(slug, safe='')}?mode=json"
    data = get_json(url, timeout=25)
    if not isinstance(data, list):
        return []
    out = []
    for j in data:
        title = clean_text(j.get("text"))
        if not title_relevant(title):
            continue
        cats = j.get("categories") or {}
        loc = clean_text(cats.get("location"))
        desc = clean_text(j.get("descriptionPlain") or j.get("description"))
        workplace = classify_workplace(loc, j.get("workplaceType"), False, j, desc)
        country_code = clean_text(j.get("country"))
        if not allowed_india(loc, country_code, "", desc, workplace):
            continue
        posted = parse_date(j.get("createdAt"))
        if not recent_enough(posted):
            continue
        urls = j.get("urls") or {}
        link = clean_text(urls.get("show") or j.get("hostedUrl") or j.get("applyUrl"))
        if not link:
            continue
        score, matched = score_job(title, desc, loc)
        out.append({
            "title": title, "company": humanize_slug(slug), "location": loc or "India",
            "workplace": workplace, "posted_date": posted, "job_url": link,
            "source": "Lever", "job_id": str(j.get("id") or ""),
            "score": score, "matched": matched,
        })
    return out


def fetch_ashby(slug):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{quote(slug, safe='')}"
    data = get_json(url, timeout=25)
    if not isinstance(data, dict):
        return []
    out = []
    for j in data.get("jobs") or []:
        title = clean_text(j.get("title"))
        if not title_relevant(title):
            continue
        locations = []
        if j.get("location"):
            locations.append(clean_text(j.get("location")))
        for x in j.get("secondaryLocations") or []:
            if isinstance(x, dict):
                locations.append(clean_text(x.get("location")))
        loc = ", ".join(dict.fromkeys(x for x in locations if x))
        desc = clean_text(j.get("descriptionPlain") or j.get("descriptionHtml"))
        workplace = classify_workplace(
            loc, j.get("workplaceType"), j.get("isRemote", False), j, desc
        )
        country = clean_text(j.get("country"))
        if not allowed_india(loc, country, "", desc, workplace):
            continue
        posted = parse_date(j.get("publishedAt") or j.get("createdAt"))
        if not recent_enough(posted):
            continue
        link = clean_text(j.get("jobUrl") or j.get("applyUrl"))
        if not link:
            continue
        score, matched = score_job(title, desc, loc)
        out.append({
            "title": title, "company": humanize_slug(slug), "location": loc or "India",
            "workplace": workplace, "posted_date": posted, "job_url": link,
            "source": "Ashby", "job_id": str(j.get("id") or j.get("jobId") or ""),
            "score": score, "matched": matched,
        })
    return out


def fetch_workable(slug):
    url = f"https://apply.workable.com/api/v1/widget/accounts/{quote(slug, safe='')}"
    data = get_json(url, params={"details": "true"}, timeout=25)
    if not isinstance(data, dict):
        return []
    out = []
    company = clean_text(data.get("name")) or humanize_slug(slug)
    for j in data.get("jobs") or []:
        title = clean_text(j.get("title"))
        if not title_relevant(title):
            continue
        loc = ", ".join(
            x for x in [
                clean_text(j.get("city")),
                clean_text(j.get("state")),
                clean_text(j.get("country")),
            ] if x
        )
        desc = clean_text(j.get("description") or j.get("full_description"))
        workplace = classify_workplace(
            loc, j.get("workplace_type"), j.get("telecommuting", False), j, desc
        )
        if not allowed_india(loc, j.get("country"), "", desc, workplace):
            continue
        posted = parse_date(j.get("published_on") or j.get("created_at"))
        if not recent_enough(posted):
            continue
        link = clean_text(j.get("application_url") or j.get("url") or j.get("shortlink"))
        if not link:
            continue
        score, matched = score_job(title, desc, loc)
        out.append({
            "title": title, "company": company, "location": loc or "India",
            "workplace": workplace, "posted_date": posted, "job_url": link,
            "source": "Workable", "job_id": str(j.get("shortcode") or j.get("code") or ""),
            "score": score, "matched": matched,
        })
    return out


def fetch_smartrecruiters(slug):
    out = []
    offset = 0
    for _ in range(4):
        url = f"https://api.smartrecruiters.com/v1/companies/{quote(slug, safe='')}/postings"
        data = get_json(url, params={"limit": 100, "offset": offset}, timeout=25)
        if not isinstance(data, dict):
            break
        items = data.get("content") or []
        for j in items:
            title = clean_text(j.get("name"))
            if not title_relevant(title):
                continue
            loc_obj = j.get("location") or {}
            loc = ", ".join(
                x for x in [
                    clean_text(loc_obj.get("city")),
                    clean_text(loc_obj.get("region")),
                    clean_text(loc_obj.get("country")),
                ] if x
            )
            pid = str(j.get("id") or j.get("uuid") or "")
            detail = {}
            if pid:
                detail = get_json(
                    f"https://api.smartrecruiters.com/v1/companies/{quote(slug, safe='')}/postings/{pid}",
                    timeout=20,
                ) or {}
            detail_loc = detail.get("location") if isinstance(detail, dict) else {}
            if isinstance(detail_loc, dict):
                loc = ", ".join(
                    x for x in [
                        clean_text(detail_loc.get("city")),
                        clean_text(detail_loc.get("region")),
                        clean_text(detail_loc.get("country")),
                    ] if x
                ) or loc
                if detail_loc.get("remote") is True:
                    loc = (loc + " Remote").strip()

            sections = ((detail.get("jobAd") or {}).get("sections") or {}) if isinstance(detail, dict) else {}
            desc = clean_text(" ".join(
                clean_text(v.get("text") if isinstance(v, dict) else v)
                for v in sections.values()
            ))
            workplace = "Remote" if (loc_obj.get("remote") is True or detail_loc.get("remote") is True) else ""
            if not workplace:
                workplace = classify_workplace(loc, "", False, detail or j, desc)
            if not allowed_india(loc, loc_obj.get("country"), detail_loc.get("country") if isinstance(detail_loc, dict) else "", desc, workplace):
                continue
            posted = parse_date(j.get("releasedDate") or j.get("createdDate") or detail.get("releasedDate"))
            if not recent_enough(posted):
                continue
            link = clean_text(
                detail.get("postingUrl")
                or detail.get("applyUrl")
                or (f"https://jobs.smartrecruiters.com/{slug}/{pid}" if pid else "")
            )
            if not link:
                continue
            score, matched = score_job(title, desc, loc)
            out.append({
                "title": title, "company": clean_text((j.get("company") or {}).get("name")) or humanize_slug(slug),
                "location": loc or "India", "workplace": workplace, "posted_date": posted,
                "job_url": link, "source": "SmartRecruiters", "job_id": pid,
                "score": score, "matched": matched,
            })
        if len(items) < 100:
            break
        offset += 100
    return out


def fetch_recruitee(slug):
    # Public endpoint is still usable during the current transition period.
    url = f"https://{slug}.recruitee.com/api/offers/"
    data = get_json(url, timeout=25)
    if not isinstance(data, dict):
        return []
    items = data.get("offers") or data.get("results") or []
    if not isinstance(items, list):
        return []
    out = []
    for j in items:
        title = clean_text(j.get("title") or j.get("name"))
        if not title_relevant(title):
            continue
        locations = j.get("locations") or []
        loc_parts = []
        for x in locations:
            if isinstance(x, dict):
                loc_parts.extend([
                    clean_text(x.get("city")),
                    clean_text(x.get("country")),
                ])
            else:
                loc_parts.append(clean_text(x))
        loc = ", ".join(dict.fromkeys(x for x in loc_parts if x))
        desc = clean_text(j.get("description"))
        workplace = classify_workplace(loc, j.get("workplace_type"), False, j, desc)
        if not allowed_india(loc, j.get("country"), "", desc, workplace):
            continue
        posted = parse_date(j.get("published_at") or j.get("created_at"))
        if not recent_enough(posted):
            continue
        link = clean_text(j.get("careers_url") or j.get("url") or j.get("apply_url"))
        if not link:
            continue
        score, matched = score_job(title, desc, loc)
        out.append({
            "title": title, "company": humanize_slug(slug), "location": loc or "India",
            "workplace": workplace, "posted_date": posted, "job_url": link,
            "source": "Recruitee", "job_id": str(j.get("id") or ""),
            "score": score, "matched": matched,
        })
    return out


def fetch_breezy(slug):
    # Public careers JSON used by Breezy-hosted boards.
    url = f"https://{slug}.breezy.hr/json"
    data = get_json(url, timeout=25)
    if not isinstance(data, (dict, list)):
        return []
    if isinstance(data, dict):
        items = data.get("positions") or data.get("jobs") or data.get("results") or []
        company = clean_text(data.get("company") or data.get("name")) or humanize_slug(slug)
    else:
        items = data
        company = humanize_slug(slug)
    if not isinstance(items, list):
        return []
    out = []
    for j in items:
        if not isinstance(j, dict):
            continue
        title = clean_text(j.get("name") or j.get("title"))
        if not title_relevant(title):
            continue
        loc_obj = j.get("location") if isinstance(j.get("location"), dict) else {}
        loc = ", ".join(
            x for x in [
                clean_text(loc_obj.get("city")),
                clean_text(loc_obj.get("state")),
                clean_text(loc_obj.get("country")),
                clean_text(j.get("location") if isinstance(j.get("location"), str) else ""),
            ] if x
        )
        desc = clean_text(j.get("description"))
        workplace = classify_workplace(
            loc, j.get("workplace_type"), j.get("is_remote", False), j, desc
        )
        if not allowed_india(loc, loc_obj.get("country") or j.get("country"), "", desc, workplace):
            continue
        posted = parse_date(j.get("published_date") or j.get("published_at") or j.get("created_at"))
        if not recent_enough(posted):
            continue
        link = clean_text(j.get("url") or j.get("job_url") or j.get("apply_url"))
        if not link:
            continue
        score, matched = score_job(title, desc, loc)
        out.append({
            "title": title, "company": company, "location": loc or "India",
            "workplace": workplace, "posted_date": posted, "job_url": link,
            "source": "Breezy", "job_id": str(j.get("id") or j.get("_id") or ""),
            "score": score, "matched": matched,
        })
    return out


def fetch_teamtailor(slug):
    # Public jobs.json is a common Teamtailor career-site surface.
    url = f"https://{slug}.teamtailor.com/jobs.json"
    data = get_json(url, timeout=25)
    if not isinstance(data, dict):
        return []
    items = data.get("jobs") or data.get("items") or []
    out = []
    for j in items:
        if not isinstance(j, dict):
            continue
        title = clean_text(j.get("title") or j.get("name"))
        if not title_relevant(title):
            continue
        jp = j.get("_jobposting") or {}
        locations = jp.get("jobLocation") or []
        locs = []
        for x in locations:
            if not isinstance(x, dict):
                continue
            addr = x.get("address") or {}
            locs.extend([
                clean_text(addr.get("addressLocality")),
                clean_text(addr.get("addressRegion")),
                clean_text(addr.get("addressCountry")),
            ])
        loc = ", ".join(dict.fromkeys(x for x in locs if x))
        desc = clean_text(j.get("content_html") or jp.get("description"))
        workplace = classify_workplace(loc, "", False, j, desc)
        if not allowed_india(loc, "", "", desc, workplace):
            continue
        posted = parse_date(j.get("date_published"))
        if not recent_enough(posted):
            continue
        link = clean_text(j.get("url"))
        if not link:
            continue
        score, matched = score_job(title, desc, loc)
        out.append({
            "title": title, "company": humanize_slug(slug), "location": loc or "India",
            "workplace": workplace, "posted_date": posted, "job_url": link,
            "source": "Teamtailor", "job_id": str(j.get("id") or ""),
            "score": score, "matched": matched,
        })
    return out


def fetch_bamboohr(slug):
    url = f"https://{slug}.bamboohr.com/careers/list"
    data = get_json(url, timeout=25)
    if not isinstance(data, dict):
        return []
    items = data.get("result") or []
    out = []
    for j in items:
        title = clean_text(j.get("jobOpeningName") or j.get("title"))
        if not title_relevant(title):
            continue
        loc = " ".join(
            x for x in [
                clean_text((j.get("location") or {}).get("city")),
                clean_text((j.get("location") or {}).get("state")),
                clean_text((j.get("atsLocation") or {}).get("country")),
            ] if x
        )
        workplace = "Remote" if j.get("isRemote") is True else classify_workplace(loc, "", False, j, "")
        if not allowed_india(loc, (j.get("atsLocation") or {}).get("country"), "", "", workplace):
            continue
        # Bamboo's public list often lacks date and description; accept when current.
        jid = str(j.get("id") or "")
        link = f"https://{slug}.bamboohr.com/careers/{jid}" if jid else f"https://{slug}.bamboohr.com/careers"
        score, matched = score_job(title, "", loc)
        out.append({
            "title": title, "company": humanize_slug(slug), "location": loc or "India",
            "workplace": workplace, "posted_date": "", "job_url": link,
            "source": "BambooHR", "job_id": jid,
            "score": score, "matched": matched,
        })
    return out


def fetch_personio(slug):
    import xml.etree.ElementTree as ET
    url = f"https://{slug}.jobs.personio.de/xml"
    try:
        r = SESSION.get(url, params={"language": "en"}, timeout=25)
        if r.status_code != 200:
            # Try .com host if .de failed.
            r = SESSION.get(f"https://{slug}.jobs.personio.com/xml", params={"language": "en"}, timeout=25)
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.text)
    except Exception:
        return []
    out = []
    for p in root.findall(".//position"):
        title = clean_text(p.findtext("name"))
        if not title_relevant(title):
            continue
        office = clean_text(p.findtext("office"))
        desc = clean_text(" ".join(x.text or "" for x in p.findall(".//jobDescriptions//value")))
        loc = office
        workplace = classify_workplace(loc, "", False, {}, desc)
        if not allowed_india(loc, "", "", desc, workplace):
            continue
        posted = parse_date(p.findtext("createdAt"))
        if not recent_enough(posted):
            continue
        pid = clean_text(p.findtext("id"))
        link = f"https://{slug}.jobs.personio.de/job/{pid}" if pid else f"https://{slug}.jobs.personio.de"
        score, matched = score_job(title, desc, loc)
        out.append({
            "title": title, "company": humanize_slug(slug), "location": loc or "India",
            "workplace": workplace, "posted_date": posted, "job_url": link,
            "source": "Personio", "job_id": pid,
            "score": score, "matched": matched,
        })
    return out


FETCHERS = {
    "Greenhouse": fetch_greenhouse,
    "Lever": fetch_lever,
    "Ashby": fetch_ashby,
    "Workable": fetch_workable,
    "SmartRecruiters": fetch_smartrecruiters,
    "Recruitee": fetch_recruitee,
    "Breezy": fetch_breezy,
    "Teamtailor": fetch_teamtailor,
    "BambooHR": fetch_bamboohr,
    "Personio": fetch_personio,
}



def public_ats_board_url(ats, slug):
    q = quote(str(slug), safe="")
    if ats == "Greenhouse":
        return f"https://job-boards.greenhouse.io/{q}"
    if ats == "Lever":
        return f"https://jobs.lever.co/{q}"
    if ats == "Ashby":
        return f"https://jobs.ashbyhq.com/{q}"
    if ats == "Workable":
        return f"https://apply.workable.com/{q}/"
    if ats == "SmartRecruiters":
        return f"https://careers.smartrecruiters.com/{q}"
    if ats == "Recruitee":
        return f"https://{q}.recruitee.com/"
    if ats == "Breezy":
        return f"https://{q}.breezy.hr/"
    if ats == "Teamtailor":
        return f"https://{q}.teamtailor.com/jobs"
    if ats == "BambooHR":
        return f"https://{q}.bamboohr.com/careers"
    if ats == "Personio":
        return f"https://{q}.jobs.personio.de/"
    return ""

def probe_public_board(ats, slug):
    url = public_ats_board_url(ats, slug)
    if not url:
        return False
    try:
        r = SESSION.get(url, timeout=12, allow_redirects=True)
        return r.status_code == 200
    except requests.RequestException:
        return False

# ============================================================
# ATS DISCOVERY
# ============================================================

def ats_from_url(url):
    try:
        host = urlparse(url).netloc.lower().split(":")[0]
    except Exception:
        return ""
    for ats, domains in ATS_DOMAINS.items():
        for d in domains:
            d = d.replace("*.", "")
            if same_or_subdomain(host, d):
                return ats
    return ""


def extract_ats_slug(ats, url):
    try:
        p = urlparse(url)
        path = p.path.strip("/")
        host = p.netloc.lower()
    except Exception:
        return ""

    parts = [unquote(x) for x in path.split("/") if x]

    blocked = {
        "jobs", "job", "careers", "career", "apply", "search",
        "privacy", "terms", "about", "login", "en-us", "en-gb",
        "api", "v1", "v2", "v3", "xml",
    }

    if ats == "Greenhouse":
        # /company or /company/jobs/id-title
        return parts[0] if parts and parts[0].lower() not in blocked else ""

    if ats == "Lever":
        return parts[0] if parts and parts[0].lower() not in blocked else ""

    if ats == "Ashby":
        return parts[0] if parts and parts[0].lower() not in blocked else ""

    if ats == "Workable":
        if host == "apply.workable.com":
            return parts[0] if parts else ""
        return host.split(".")[0]

    if ats == "SmartRecruiters":
        if "careers.smartrecruiters.com" in host:
            return parts[0] if parts else ""
        if "jobs.smartrecruiters.com" in host:
            return parts[0] if parts else ""
        return ""

    if ats == "Recruitee":
        return host.split(".")[0]

    if ats == "Breezy":
        return host.split(".")[0]

    if ats == "Teamtailor":
        return host.split(".")[0]

    if ats == "BambooHR":
        return host.split(".")[0]

    if ats == "Personio":
        return host.split(".")[0]

    return ""


def archive_query_url(index_url, pattern):
    return (
        f"{index_url}?url={quote(pattern, safe=':/?*')}"
        "&output=json&filter=status:200&collapse=urlkey"
        "&pageSize=100000"
    )


def latest_commoncrawl_index():
    data = get_json("https://index.commoncrawl.org/collinfo.json", timeout=30)
    if not isinstance(data, list) or not data:
        return ""
    # Current first entry is normally the newest collection.
    return clean_text(data[0].get("cdx-api"))


def discover_archive_urls(pattern, limit=ARCHIVE_CANDIDATE_CAP):
    urls = set()

    # Wayback first.
    try:
        u = (
            "https://web.archive.org/cdx/search/cdx"
            f"?url={quote(pattern, safe=':/?*')}"
            "&output=json&fl=original"
            "&filter=statuscode:200&collapse=urlkey"
            f"&limit={limit}"
        )
        r = SESSION.get(u, timeout=60)
        if r.status_code == 200:
            data = r.json()
            for row in data:
                original = row[0] if isinstance(row, list) and row else row
                if original:
                    urls.add(str(original))
    except Exception:
        pass

    # Common Crawl fallback / expansion.
    try:
        idx = latest_commoncrawl_index()
        if idx:
            r = SESSION.get(
                archive_query_url(idx, pattern),
                timeout=90,
            )
            if r.status_code == 200:
                for line in r.text.splitlines():
                    try:
                        obj = json.loads(line)
                        u = obj.get("url")
                        if u:
                            urls.add(u)
                    except Exception:
                        continue
    except Exception:
        pass

    return list(urls)[:limit]


def discover_ats_boards_from_archives():
    candidates = {ats: set() for ats in FETCHERS}
    for ats, domains in ARCHIVE_DOMAINS.items():
        for domain in domains:
            pattern = domain
            if "*" in pattern:
                # Common Crawl/Wayback wildcard pattern.
                query_pattern = pattern.replace("*", "*")
            else:
                query_pattern = pattern + "/*"

            urls = discover_archive_urls(query_pattern)
            for u in urls:
                found_ats = ats_from_url(u)
                if found_ats != ats:
                    continue
                slug = extract_ats_slug(ats, u)
                if slug:
                    candidates[ats].add(slug)
            print(f"ARCHIVE {ats}: {len(candidates[ats])} candidates")
    return candidates


# ============================================================
# PUBLIC COMPANY / JOB DIRECTORY SEEDS
# ============================================================

WELLFOUND_ROLE_SLUGS = [
    "customer-support",
    "technical-support",
    "customer-success",
    "customer-success-specialist",
    "client-success-manager",
    "customer-service",
    "help-desk-support",
    "product-support",
    "customer-experience",
    "client-services",
    "onboarding",
    "implementation",
    "technical-account-manager",
    "customer-operations",
    "support-engineer",
]

CUTSHORT_ROLE_PAGES = [
    "customer-support-jobs",
    "customer-service-jobs",
    "customer-success-jobs",
    "technical-support-jobs",
    "tech-support-jobs",
    "remote-support-jobs",
    "help-desk-jobs",
    "client-servicing-jobs",
    "saas-jobs",
]


def parent_text(anchor, levels=4):
    node = anchor
    for _ in range(levels):
        if node.parent:
            node = node.parent
    return clean_text(node.get_text(" ", strip=True))


def extract_wellfound_seeds():
    companies = {}
    jobs = []

    for role in WELLFOUND_ROLE_SLUGS:
        for page in range(1, MAX_WELLFOUND_PAGES_PER_ROLE + 1):
            url = f"https://wellfound.com/role/l/{role}/india"
            if page > 1:
                url += f"?page={page}"

            html, final_url = get_html(url, timeout=25)
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")
            page_text = clean_text(soup.get_text(" ", strip=True))

            # Company profile links.
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if href.startswith("/company/"):
                    name = clean_text(a.get_text(" ", strip=True))
                    if name:
                        companies.setdefault(
                            name.lower(),
                            {
                                "company": name,
                                "wellfound": urljoin("https://wellfound.com", href),
                            },
                        )

            # Candidate job links. Only keep cards whose nearby text says
            # India + Remote/Hybrid/Onsite-or-remote.
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if not href.startswith("/jobs/"):
                    continue
                context = parent_text(a, 4).lower()
                if "india" not in context:
                    continue
                if not any(x in context for x in ["remote", "hybrid", "onsite or remote", "on-site or remote"]):
                    continue
                title = clean_text(a.get_text(" ", strip=True))
                if title and len(title) < 180:
                    jobs.append(urljoin("https://wellfound.com", href))

            # If the page has no India-specific job content, stop paging this role.
            if "results total" in page_text and page > 1 and len(soup.find_all("a", href=re.compile(r"^/jobs/"))) == 0:
                break

    # Keep bounded unique sets.
    companies = list(companies.values())[:MAX_COMPANY_SEEDS]
    jobs = list(dict.fromkeys(jobs))[:MAX_EXTERNAL_JOB_PAGES]
    print(f"WELLFOUND: {len(companies)} companies, {len(jobs)} job pages seeded")
    return companies, jobs


def extract_cutshort_seeds():
    companies = {}
    jobs = []

    for role_page in CUTSHORT_ROLE_PAGES:
        for page in range(1, MAX_CUTSHORT_PAGES_PER_ROLE + 1):
            url = f"https://cutshort.io/jobs/{role_page}"
            if page > 1:
                url += f"?page={page}"

            html, final_url = get_html(url, timeout=25)
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")

            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "/company/" in href:
                    name = clean_text(a.get_text(" ", strip=True))
                    if name:
                        companies.setdefault(
                            name.lower(),
                            {
                                "company": name,
                                "cutshort": urljoin("https://cutshort.io", href),
                            },
                        )

                if "/job/" in href or "/jobs/" in href:
                    context = parent_text(a, 4).lower()
                    if "remote" in context or "hybrid" in context:
                        jobs.append(urljoin("https://cutshort.io", href))

    companies = list(companies.values())[:MAX_COMPANY_SEEDS]
    jobs = list(dict.fromkeys(jobs))[:MAX_EXTERNAL_JOB_PAGES]
    print(f"CUTSHORT: {len(companies)} companies, {len(jobs)} job pages seeded")
    return companies, jobs


def extract_external_apply_from_job_page(url):
    html, final_url = get_html(url, timeout=20)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    candidates = []
    for a in soup.find_all("a", href=True):
        href = urljoin(final_url, a.get("href", ""))
        if not href.startswith(("http://", "https://")):
            continue
        host = urlparse(href).netloc.lower()
        if "wellfound.com" in host or "cutshort.io" in host:
            continue

        text = clean_text(a.get_text(" ", strip=True)).lower()
        ats = ats_from_url(href)

        if ats or any(k in text for k in ["apply", "career", "jobs", "job", "hiring"]):
            candidates.append(href)

    # Prefer ATS/career links.
    candidates.sort(
        key=lambda x: (
            0 if ats_from_url(x) else 1,
            0 if re.search(r"/apply(?:/|$)", x, re.I) else 1,
            len(x),
        )
    )

    return candidates[0] if candidates else None


def extract_company_website(company_url):
    html, final_url = get_html(company_url, timeout=20)
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = urljoin(final_url, a.get("href", ""))
        host = urlparse(href).netloc.lower()
        if not host or "wellfound.com" in host or "cutshort.io" in host:
            continue
        text = clean_text(a.get_text(" ", strip=True)).lower()
        if "website" in text or text in {"visit website", "website"}:
            return href
    # Fallback: first external http link that looks like a company domain.
    for a in soup.find_all("a", href=True):
        href = urljoin(final_url, a.get("href", ""))
        host = urlparse(href).netloc.lower()
        if host and "wellfound.com" not in host and "cutshort.io" not in host:
            if not any(x in host for x in ["facebook.", "linkedin.", "twitter.", "x.com", "instagram."]):
                return href
    return ""


def crawl_company_career_page(company_name, website):
    results = []
    if not website:
        return results

    html, final_url = get_html(website, timeout=20)
    if not html:
        return results

    soup = BeautifulSoup(html, "html.parser")
    links = []

    # ATS links anywhere on homepage.
    for a in soup.find_all("a", href=True):
        href = urljoin(final_url, a.get("href", ""))
        if ats_from_url(href):
            links.append(href)

    # Career/job links.
    for a in soup.find_all("a", href=True):
        href = urljoin(final_url, a.get("href", ""))
        text = clean_text(a.get_text(" ", strip=True)).lower()
        href_l = href.lower()
        if any(k in text or k in href_l for k in [
            "careers", "career", "jobs", "join us", "join-us",
            "work with us", "work-with-us", "vacancies", "hiring",
        ]):
            links.append(href)

    links = list(dict.fromkeys(links))[:5]

    for link in links:
        ats = ats_from_url(link)
        if ats:
            results.append({
                "company": company_name,
                "ats": ats,
                "ats_url": link,
                "website": website,
            })
            continue

        page_html, page_url = get_html(link, timeout=18)
        if not page_html:
            continue
        ps = BeautifulSoup(page_html, "html.parser")
        for a in ps.find_all("a", href=True):
            href = urljoin(page_url, a.get("href", ""))
            a_ats = ats_from_url(href)
            if a_ats:
                results.append({
                    "company": company_name,
                    "ats": a_ats,
                    "ats_url": href,
                    "website": website,
                })
            # Direct generic job links are also useful even without known ATS.
            elif re.search(r"/(?:jobs?|careers?)/[^/?#]{2,}", urlparse(href).path, re.I):
                results.append({
                    "company": company_name,
                    "ats": "Company Career",
                    "ats_url": href,
                    "website": website,
                })

    return results


# ============================================================
# COMPANY -> ATS SLUG GUESSING
# ============================================================

def guessed_board_candidates(company_name):
    base = slugify_company(company_name)
    variants = {base}

    # Common Indian brand suffix cleanup.
    for suffix in [
        "-private-limited", "-pvt-ltd", "-pvt-limited", "-limited",
        "-ltd", "-inc", "-llc", "-technologies", "-technology",
        "-solutions", "-services", "-labs", "-ai",
    ]:
        if base.endswith(suffix):
            variants.add(base[: -len(suffix)].strip("-"))

    return sorted(v for v in variants if len(v) >= 2)


def validate_company_slug(ats, slug):
    fetcher = FETCHERS.get(ats)
    if not fetcher:
        return False, []
    try:
        jobs = fetcher(slug)
        return True, jobs
    except Exception as exc:
        # A broken board must never stop the entire crawler.
        return False, []


def discover_seeded_company_ats(companies):
    discovered = {ats: set() for ats in FETCHERS}
    metadata = {}

    tasks = []
    for item in companies:
        name = item.get("company", "")
        if not name:
            continue
        for slug in guessed_board_candidates(name):
            for ats in [
                "Greenhouse", "Lever", "Ashby", "Workable",
                "SmartRecruiters", "Recruitee", "Breezy",
                "Teamtailor", "BambooHR", "Personio",
            ]:
                tasks.append((name, ats, slug))

    # Avoid huge request bursts.
    tasks = list(dict.fromkeys(tasks))[:MAX_COMPANY_SEEDS * 10]

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {
            ex.submit(validate_company_slug, ats, slug): (name, ats, slug)
            for name, ats, slug in tasks
        }
        for future in as_completed(futures):
            name, ats, slug = futures[future]
            try:
                ok, jobs = future.result()
            except Exception:
                ok, jobs = False, []

            if ok:
                # The fetcher returns [] both for a live empty board and for
                # some errors, so confirm with a lightweight board URL check
                # where possible before storing it.
                discovered[ats].add(slug)
                metadata[(ats, slug)] = {
                    "company": name,
                    "jobs": len(jobs),
                }

    total = sum(len(x) for x in discovered.values())
    print(f"COMPANY->ATS GUESSING: {total} candidate boards found")
    return discovered, metadata


# ============================================================
# GENERIC ARCHIVE ATS BOARD VALIDATION
# ============================================================

def validate_discovered_board(ats, slug):
    fetcher = FETCHERS.get(ats)
    if not fetcher:
        return False, 0, ""

    try:
        jobs = fetcher(slug)
        if jobs or probe_public_board(ats, slug):
            return True, len(jobs), humanize_slug(slug)
        return False, 0, ""
    except Exception:
        return False, 0, ""


def validate_archive_candidates(candidates):
    valid = {ats: set() for ats in FETCHERS}
    metadata = {}

    tasks = []
    for ats, slugs in candidates.items():
        for slug in sorted(slugs, key=str.lower)[:MAX_NEW_VALIDATIONS_PER_ATS]:
            tasks.append((ats, slug))

    print(f"ATS VALIDATION TASKS: {len(tasks)}")

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {
            ex.submit(validate_discovered_board, ats, slug): (ats, slug)
            for ats, slug in tasks
        }
        done = 0
        for future in as_completed(futures):
            ats, slug = futures[future]
            try:
                ok, count, company = future.result()
            except Exception:
                ok, count, company = False, 0, ""

            if ok:
                valid[ats].add(slug)
                metadata[(ats, slug)] = {
                    "company": company,
                    "jobs": count,
                }

            done += 1
            if done % 250 == 0:
                print(f"ATS VALIDATED: {done}/{len(tasks)}")

    return valid, metadata



def crawl_generic_career_records(records, limit=160):
    """
    Fallback for Workday/iCIMS/Taleo/ADP/SuccessFactors and company-owned
    careers pages where a public normalized JSON feed is not available.

    This is intentionally shallow: fetch the public careers page, collect
    obvious job links, and inspect only a bounded number of job pages.
    """
    out = []
    seen = set()

    candidates = []
    for rec in records:
        ats = rec.get("ats", "")
        url = rec.get("ats_url", "")
        if not url:
            continue
        if ats in FETCHERS:
            continue
        candidates.append((rec.get("company", ""), url, ats))

    candidates = candidates[:limit]

    for company, url, ats in candidates:
        html, final_url = get_html(url, timeout=18)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        links = [final_url]

        for a in soup.find_all("a", href=True):
            href = urljoin(final_url, a.get("href", ""))
            if not href.startswith(("http://", "https://")):
                continue
            path = urlparse(href).path.lower()
            text = clean_text(a.get_text(" ", strip=True))
            if (
                re.search(r"/(?:job|jobs|career|careers)/", path)
                or re.search(r"\b(?:apply|customer support|customer success|technical support)\b", text, re.I)
            ):
                links.append(href)

        for job_url in list(dict.fromkeys(links))[:12]:
            if job_url in seen:
                continue
            seen.add(job_url)

            page_html, page_final = get_html(job_url, timeout=16)
            if not page_html:
                continue

            ps = BeautifulSoup(page_html, "html.parser")
            title = ""
            meta = ps.find("meta", attrs={"property": "og:title"})
            if meta:
                title = clean_text(meta.get("content"))
            if not title and ps.title:
                title = clean_text(ps.title.get_text(" "))

            if not title_relevant(title):
                continue

            body = clean_text(ps.get_text(" ", strip=True))[:35000]
            # Generic pages need explicit India + remote/hybrid wording.
            if "india" not in body.lower():
                continue

            workplace = "Hybrid" if HYBRID_RE.search(body) else (
                "Remote" if REMOTE_RE.search(body) else ""
            )

            if not allowed_india(body, "", "", body, workplace):
                continue

            score, matched = score_job(title, body, "India")
            out.append({
                "added": datetime.now(timezone.utc).date().isoformat(),
                "title": title,
                "company": company,
                "location": "India",
                "workplace": workplace,
                "posted_date": "",
                "job_url": page_final,
                "source": f"{ats or 'Career'}->Direct",
                "score": score,
                "matched": matched,
            })

    return out

# ============================================================
# DIRECT ATS CRAWL
# ============================================================

def crawl_ats_boards(board_sets):
    jobs = []
    tasks = []
    for ats, slugs in board_sets.items():
        for slug in slugs:
            if ats in FETCHERS:
                tasks.append((ats, slug))

    print(f"ATS BOARDS TO CRAWL: {len(tasks)}")

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {
            ex.submit(FETCHERS[ats], slug): (ats, slug)
            for ats, slug in tasks
        }
        done = 0
        for future in as_completed(futures):
            ats, slug = futures[future]
            try:
                result = future.result()
                if result:
                    jobs.extend(result)
            except Exception as exc:
                print(f"ATS ERROR {ats}/{slug}: {type(exc).__name__}: {exc}")
            done += 1
            if done % 100 == 0:
                print(f"ATS PROGRESS: {done}/{len(tasks)}")

    return jobs


# ============================================================
# SHEETS
# ============================================================

JOBS_HEADERS = [
    "Added",
    "Job Title",
    "Company",
    "Location",
    "Work Type",
    "Posted",
    "Job URL",
    "Source",
    "Score",
    "Matched Keywords",
]

COMPANY_HEADERS = [
    "Company",
    "Domain",
    "Career/ATS URL",
    "ATS",
    "Last Checked",
    "Jobs Found",
    "Status",
]


def get_spreadsheet():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not GOOGLE_CREDENTIALS:
        raise RuntimeError("Missing GOOGLE_CREDENTIALS")

    info = json.loads(GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)


def get_ws(spreadsheet, name, rows=2000, cols=12):
    try:
        return spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)


def ensure_headers(ws, headers):
    current = ws.row_values(1)
    if current[:len(headers)] != headers:
        ws.update("A1", [headers])


def load_job_keys(ws):
    rows = ws.get_all_values()
    if not rows:
        return set()
    headers = rows[0]
    idx = {h: i for i, h in enumerate(headers)}
    out = set()
    for row in rows[1:]:
        url = row[idx["Job URL"]] if "Job URL" in idx and len(row) > idx["Job URL"] else ""
        if url:
            out.add(("url", normalize_url(url)))
    return out


def append_jobs(ws, jobs):
    existing = load_job_keys(ws)
    unique = {}

    for j in jobs:
        k = job_key(j)
        if k in unique:
            continue
        unique[k] = j

    rows = []
    for j in unique.values():
        url_key = ("url", normalize_url(j.get("job_url", "")))
        if url_key in existing:
            continue
        rows.append([
            j.get("added") or datetime.now(timezone.utc).date().isoformat(),
            j.get("title", ""),
            j.get("company", ""),
            j.get("location", ""),
            j.get("workplace", ""),
            j.get("posted_date", ""),
            j.get("job_url", ""),
            j.get("source", ""),
            j.get("score", 0),
            j.get("matched", ""),
        ])

    if rows:
        ws.append_rows(rows, value_input_option="RAW")
    return len(rows)


def load_companies(ws):
    rows = ws.get_all_values()
    out = {}
    if not rows:
        return out
    headers = rows[0]
    idx = {h: i for i, h in enumerate(headers)}
    for row in rows[1:]:
        company = row[idx["Company"]] if "Company" in idx and len(row) > idx["Company"] else ""
        url = row[idx["Career/ATS URL"]] if "Career/ATS URL" in idx and len(row) > idx["Career/ATS URL"] else ""
        ats = row[idx["ATS"]] if "ATS" in idx and len(row) > idx["ATS"] else ""
        if company and url:
            out[(company.lower().strip(), normalize_url(url))] = {
                "company": company,
                "domain": row[idx["Domain"]] if "Domain" in idx and len(row) > idx["Domain"] else "",
                "url": url,
                "ats": ats,
            }
    return out


def upsert_companies(ws, records):
    existing = load_companies(ws)
    today = datetime.now(timezone.utc).date().isoformat()

    # Read current rows and update in place where possible.
    rows = ws.get_all_values()
    headers = rows[0] if rows else COMPANY_HEADERS
    idx = {h: i for i, h in enumerate(headers)}

    updates = {}
    additions = []

    for rec in records:
        url = normalize_url(rec.get("url", ""))
        if not url:
            continue
        key = (clean_text(rec.get("company", "")).lower(), url)
        rownum = None
        for i, row in enumerate(rows[1:], start=2):
            c = clean_text(row[idx["Company"]] if "Company" in idx and len(row) > idx["Company"] else "").lower()
            u = normalize_url(row[idx["Career/ATS URL"]] if "Career/ATS URL" in idx and len(row) > idx["Career/ATS URL"] else "")
            if c == key[0] and u == key[1]:
                rownum = i
                break

        values = [
            rec.get("company", ""),
            rec.get("domain", ""),
            rec.get("url", ""),
            rec.get("ats", ""),
            today,
            str(rec.get("jobs_found", 0)),
            rec.get("status", "ACTIVE"),
        ]

        if rownum:
            updates[rownum] = values
        else:
            additions.append(values)

    for rownum, values in updates.items():
        ws.update(f"A{rownum}", [values])

    if additions:
        ws.append_rows(additions, value_input_option="RAW")


# ============================================================
# WELLFOUND/CUTSHORT DIRECT JOB EXTRACTION
# ============================================================

def collect_direct_external_jobs(urls, source_name):
    out = []
    urls = list(dict.fromkeys(urls))[:MAX_EXTERNAL_JOB_PAGES]

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(extract_external_apply_from_job_page, u): u for u in urls}
        for future in as_completed(futures):
            source_job_url = futures[future]
            try:
                external = future.result()
            except Exception:
                external = None
            if not external:
                continue

            # Follow the actual external application/career page if possible.
            html, final_url = get_html(external, timeout=18)
            if not html:
                final_url = external
                html = ""

            ats = ats_from_url(final_url)
            soup = BeautifulSoup(html, "html.parser") if html else None

            title = ""
            location = ""
            desc = ""
            if soup:
                title = clean_text(
                    (soup.find("meta", attrs={"property": "og:title"}) or {}).get("content")
                    if soup.find("meta", attrs={"property": "og:title"})
                    else ""
                )
                if not title:
                    title = clean_text(soup.title.get_text()) if soup.title else ""
                desc = clean_text(soup.get_text(" ", strip=True))[:30000]

            # If the ATS page is directly identifiable, let the ATS feed be
            # the canonical source and only use this path as a fallback.
            if ats and title_relevant(title):
                workplace = classify_workplace(desc, "", False, {}, desc)
                if not workplace:
                    # Search visible page text for explicit India remote/hybrid.
                    workplace = "Remote" if re.search(r"\bremote\b", desc, re.I) else (
                        "Hybrid" if re.search(r"\bhybrid\b", desc, re.I) else ""
                    )
                if allowed_india(desc, "", "", desc, workplace):
                    score, matched = score_job(title, desc, desc)
                    out.append({
                        "added": datetime.now(timezone.utc).date().isoformat(),
                        "title": title,
                        "company": "",
                        "location": "India",
                        "workplace": workplace,
                        "posted_date": "",
                        "job_url": final_url,
                        "source": f"{source_name}->ATS",
                        "score": score,
                        "matched": matched,
                    })
            elif final_url and title_relevant(title):
                # Company career page can still be a direct apply URL.
                if any(x in final_url.lower() for x in ["/careers/", "/jobs/", "/job/"]):
                    if "india" in desc.lower() and ("remote" in desc.lower() or "hybrid" in desc.lower()):
                        workplace = "Hybrid" if "hybrid" in desc.lower() else "Remote"
                        score, matched = score_job(title, desc, "India")
                        out.append({
                            "added": datetime.now(timezone.utc).date().isoformat(),
                            "title": title,
                            "company": "",
                            "location": "India",
                            "workplace": workplace,
                            "posted_date": "",
                            "job_url": final_url,
                            "source": f"{source_name}->Career",
                            "score": score,
                            "matched": matched,
                        })

    return out


# ============================================================
# MAIN
# ============================================================

def main():
    started = datetime.now(timezone.utc)
    print("================================================")
    print("REMOTE4.ME JOB CRAWLER V7 — INDIA FIRST")
    print("================================================")
    print(f"Started UTC: {started.isoformat()}")
    print("No Brave / Serper / JobsPipe required")
    print(f"Startup Jobs API: {'configured' if STARTUP_JOBS_API_KEY else 'NOT configured'}")
    print("India: Remote + Hybrid")
    print(f"Job age: {MAX_JOB_AGE_DAYS} days")

    spreadsheet = get_spreadsheet()
    jobs_ws = get_ws(spreadsheet, "JOBS", rows=5000, cols=len(JOBS_HEADERS))
    companies_ws = get_ws(spreadsheet, "COMPANIES", rows=5000, cols=len(COMPANY_HEADERS))
    ensure_headers(jobs_ws, JOBS_HEADERS)
    ensure_headers(companies_ws, COMPANY_HEADERS)

    # --------------------------------------------------------
    # 1. Public feeds
    # --------------------------------------------------------
    all_jobs = []

    for name, fn in [
        ("Himalayas", fetch_himalayas),
        ("Remotive", fetch_remotive),
        ("Arbeitnow", fetch_arbeitnow),
        ("Working Nomads", fetch_working_nomads),
        ("We Work Remotely", fetch_wwr_rss),
        ("Jobicy", fetch_jobicy),
        ("Remote OK", fetch_remoteok),
        ("Startup Jobs", fetch_startup_jobs),
    ]:
        try:
            result = fn()
            print(f"{name}: {len(result)} accepted")
            all_jobs.extend(result)
        except Exception as exc:
            print(f"{name}: ERROR {type(exc).__name__}: {exc}")

    # --------------------------------------------------------
    # 2. India-focused public directories
    # --------------------------------------------------------
    wf_companies, wf_jobs = extract_wellfound_seeds()
    cs_companies, cs_jobs = extract_cutshort_seeds()

    all_companies = []
    seen_company = set()
    for item in wf_companies + cs_companies:
        key = clean_text(item.get("company", "")).lower()
        if key and key not in seen_company:
            seen_company.add(key)
            all_companies.append(item)

    all_companies = all_companies[:MAX_COMPANY_SEEDS]

    direct_jobs = []
    try:
        direct_jobs.extend(collect_direct_external_jobs(wf_jobs, "Wellfound"))
    except Exception as exc:
        print(f"Wellfound direct extraction ERROR: {exc}")

    try:
        direct_jobs.extend(collect_direct_external_jobs(cs_jobs, "Cutshort"))
    except Exception as exc:
        print(f"Cutshort direct extraction ERROR: {exc}")

    print(f"Directory direct jobs accepted: {len(direct_jobs)}")
    all_jobs.extend(direct_jobs)

    # --------------------------------------------------------
    # 3. Company website -> ATS discovery
    # --------------------------------------------------------
    career_records = []
    company_ats_candidates = []

    for item in all_companies[:300]:
        company = item.get("company", "")
        wf_url = item.get("wellfound", "")
        cs_url = item.get("cutshort", "")
        profile_url = wf_url or cs_url
        website = extract_company_website(profile_url) if profile_url else ""

        if website:
            found = crawl_company_career_page(company, website)
            career_records.extend(found)
            for f in found:
                company_ats_candidates.append((f["company"], f["ats"], f["ats_url"]))

    print(f"Company website career/ATS links found: {len(career_records)}")

    generic_jobs = []
    try:
        generic_jobs = crawl_generic_career_records(career_records, limit=160)
    except Exception as exc:
        print(f"Generic career fallback ERROR: {type(exc).__name__}: {exc}")
    print(f"Generic career jobs accepted: {len(generic_jobs)}")
    all_jobs.extend(generic_jobs)

    # --------------------------------------------------------
    # 4. Seeded company -> guessed ATS board slugs
    # --------------------------------------------------------
    seeded_boards, seeded_meta = discover_seeded_company_ats(all_companies)
    for ats, slugs in seeded_boards.items():
        print(f"SEED ATS {ats}: {len(slugs)} boards")

    # --------------------------------------------------------
    # 5. Archive discovery
    # --------------------------------------------------------
    archive_candidates = discover_ats_boards_from_archives()
    archive_valid, archive_meta = validate_archive_candidates(archive_candidates)

    for ats, slugs in archive_valid.items():
        print(f"ARCHIVE LIVE {ats}: {len(slugs)}")

    # --------------------------------------------------------
    # 6. Merge all known boards with persistent Companies sheet
    # --------------------------------------------------------
    board_sets = {ats: set() for ats in FETCHERS}

    for source in [seeded_boards, archive_valid]:
        for ats, slugs in source.items():
            board_sets.setdefault(ats, set()).update(slugs)

    # Add exact ATS links discovered from company career pages.
    for rec in career_records:
        ats = rec.get("ats", "")
        url = rec.get("ats_url", "")
        if ats in FETCHERS and url:
            slug = extract_ats_slug(ats, url)
            if slug:
                board_sets[ats].add(slug)

    # Recover previously known ATS slugs from Companies sheet.
    existing_company_rows = companies_ws.get_all_values()
    if existing_company_rows:
        hdr = existing_company_rows[0]
        ix = {h: i for i, h in enumerate(hdr)}
        for row in existing_company_rows[1:]:
            ats = clean_text(row[ix["ATS"]] if "ATS" in ix and len(row) > ix["ATS"] else "")
            url = clean_text(row[ix["Career/ATS URL"]] if "Career/ATS URL" in ix and len(row) > ix["Career/ATS URL"] else "")
            if ats in FETCHERS and url:
                slug = extract_ats_slug(ats, url)
                if slug:
                    board_sets[ats].add(slug)

    total_boards = sum(len(x) for x in board_sets.values())
    print(f"TOTAL ATS BOARDS TO CRAWL: {total_boards}")

    # --------------------------------------------------------
    # 7. Crawl ATS
    # --------------------------------------------------------
    ats_jobs = crawl_ats_boards(board_sets)
    print(f"ATS jobs accepted: {len(ats_jobs)}")
    all_jobs.extend(ats_jobs)

    # --------------------------------------------------------
    # 8. Deduplicate + sort by relevance
    # --------------------------------------------------------
    unique = {}
    for j in all_jobs:
        if not j.get("job_url"):
            continue
        key = job_key(j)
        if key not in unique or j.get("score", 0) > unique[key].get("score", 0):
            unique[key] = j

    final_jobs = sorted(
        unique.values(),
        key=lambda x: (x.get("score", 0), x.get("posted_date", "")),
        reverse=True,
    )

    # --------------------------------------------------------
    # 9. Write Jobs
    # --------------------------------------------------------
    added = append_jobs(jobs_ws, final_jobs)
    print(f"FINAL UNIQUE JOBS: {len(final_jobs)}")
    print(f"NEW ROWS WRITTEN: {added}")

    # --------------------------------------------------------
    # 10. Write Companies registry
    # --------------------------------------------------------
    company_records = []

    # ATS boards.
    for ats, slugs in board_sets.items():
        for slug in sorted(slugs, key=str.lower):
            if ats == "Greenhouse":
                url = f"https://job-boards.greenhouse.io/{quote(slug, safe='')}"
            elif ats == "Lever":
                url = f"https://jobs.lever.co/{quote(slug, safe='')}"
            elif ats == "Ashby":
                url = f"https://jobs.ashbyhq.com/{quote(slug, safe='')}"
            elif ats == "Workable":
                url = f"https://apply.workable.com/{quote(slug, safe='')}/"
            elif ats == "SmartRecruiters":
                url = f"https://careers.smartrecruiters.com/{quote(slug, safe='')}"
            elif ats == "Recruitee":
                url = f"https://{quote(slug, safe='')}.recruitee.com/"
            elif ats == "Breezy":
                url = f"https://{quote(slug, safe='')}.breezy.hr/"
            elif ats == "Teamtailor":
                url = f"https://{quote(slug, safe='')}.teamtailor.com/jobs"
            elif ats == "BambooHR":
                url = f"https://{quote(slug, safe='')}.bamboohr.com/careers"
            elif ats == "Personio":
                url = f"https://{quote(slug, safe='')}.jobs.personio.de/"
            else:
                url = ""

            meta = (
                seeded_meta.get((ats, slug))
                or archive_meta.get((ats, slug))
                or {}
            )

            company = meta.get("company") or humanize_slug(slug)
            domain = urlparse(url).netloc if url else ""
            count = meta.get("jobs", 0)

            company_records.append({
                "company": company,
                "domain": domain,
                "url": url,
                "ats": ats,
                "jobs_found": count,
                "status": "ACTIVE",
            })

    # Website-discovered exact career/ATS links.
    for r in career_records:
        url = r.get("ats_url", "")
        company_records.append({
            "company": r.get("company", ""),
            "domain": urlparse(url).netloc,
            "url": url,
            "ats": r.get("ats", "Company Career"),
            "jobs_found": 0,
            "status": "ACTIVE",
        })

    # Deduplicate company registry records.
    dedup = {}
    for r in company_records:
        k = (clean_text(r.get("company", "")).lower(), normalize_url(r.get("url", "")))
        if k[0] and k[1]:
            dedup[k] = r

    upsert_companies(companies_ws, list(dedup.values()))

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"COMPANIES UPDATED: {len(dedup)}")
    print(f"RUNTIME SECONDS: {int(elapsed)}")
    print("REMOTE4.ME CRAWLER COMPLETE")


if __name__ == "__main__":
    main()
