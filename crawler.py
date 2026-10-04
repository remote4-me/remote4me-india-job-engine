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
# REMOTE4.ME JOB CRAWLER V11
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
    "Remote4.me Job Crawler/11.0 "
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

    if workplace in {"Remote", "Hybrid"}:
        text = clean_text(description)[:30000]
        # Many ATS feeds say only "Remote" in the location and mention
        # "India" elsewhere in the job eligibility text. The old matcher
        # required one very specific sentence shape and discarded those jobs.
        # For Remote4.me review, accept when India and remote/hybrid evidence
        # both exist in the posting text.
        if INDIA_COUNTRY_RE.search(text) and REMOTE_RE.search(text):
            return True
        if INDIA_COUNTRY_RE.search(text) and HYBRID_RE.search(text):
            return True
        if INDIA_REMOTE_CONTEXT_RE.search(text):
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
        soup = BeautifulSoup(r.text, "html.parser")
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
        posted = parse_date(j.get("first_published") or j.get("updated_at"))
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
    candidates = {ats: set() for ats in ARCHIVE_DOMAINS}
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
            print(f"ARCHIVE {ats}: {len(candidates.get(ats, set()))} candidates")
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

    # Wellfound/Cutshort sometimes expose the company URL only in JSON-LD.
    for script in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
        raw = script.string or script.get_text(" ", strip=True)
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        objs = obj if isinstance(obj, list) else [obj]
        for item in objs:
            if not isinstance(item, dict):
                continue
            candidate = item.get("url") or item.get("sameAs")
            values = candidate if isinstance(candidate, list) else [candidate]
            for href in values:
                if not isinstance(href, str):
                    continue
                href = urljoin(final_url, href)
                host = urlparse(href).netloc.lower()
                if host and "wellfound.com" not in host and "cutshort.io" not in host:
                    if not any(x in host for x in ["facebook.", "linkedin.", "twitter.", "x.com", "instagram."]):
                        return href

    for a in soup.find_all("a", href=True):
        href = urljoin(final_url, a.get("href", ""))
        host = urlparse(href).netloc.lower()
        if not host or "wellfound.com" in host or "cutshort.io" in host:
            continue
        text = clean_text(a.get_text(" ", strip=True)).lower()
        if "website" in text or text in {"visit website", "website"}:
            return href

    external = []
    for a in soup.find_all("a", href=True):
        href = urljoin(final_url, a.get("href", ""))
        host = urlparse(href).netloc.lower()
        if host and "wellfound.com" not in host and "cutshort.io" not in host:
            if not any(x in host for x in ["facebook.", "linkedin.", "twitter.", "x.com", "instagram."]):
                external.append(href)
    return external[0] if external else ""


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

    # Also probe a small set of conventional career paths. This catches
    # companies whose navigation is icon-only or whose career link is rendered
    # without useful anchor text.
    root = f"{urlparse(final_url).scheme}://{urlparse(final_url).netloc}"
    for path in ["/careers", "/jobs", "/join-us", "/work-with-us", "/hiring"]:
        links.append(root + path)

    links = list(dict.fromkeys(links))[:10]

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

            if ok and jobs:
                # Do not pollute the registry with every guessed slug. A board
                # is useful to the daily crawler only when its public feed
                # actually returned at least one relevant India job.
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
        # A public board URL returning HTTP 200 is not enough. The previous
        # version stored thousands of empty archive/guessed boards, then spent
        # 20+ minutes crawling them. Keep only boards that currently yield at
        # least one relevant India Remote/Hybrid job. Archive discovery runs
        # again on the next daily run, so newly active boards can still appear.
        if jobs:
            return True, len(jobs), humanize_slug(slug)
        return False, 0, ""
    except Exception:
        return False, 0, ""


def validate_archive_candidates(candidates):
    valid = {ats: set() for ats in FETCHERS}
    metadata = {}

    tasks = []
    for ats, slugs in candidates.items():
        # Some archive-only ATS domains (e.g. Workday/iCIMS/Taleo) do not
        # have a normalized public fetcher yet. Discover them for visibility,
        # but do not send them into the normalized ATS validator. They are
        # handled through company career-page discovery instead.
        if ats not in FETCHERS:
            continue
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
        ws.update([headers], "A1")


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
        ws.update([values], f"A{rownum}")

    if additions:
        ws.append_rows(additions, value_input_option="RAW")


# ============================================================
# WELLFOUND/CUTSHORT DIRECT JOB EXTRACTION
# ============================================================

def collect_direct_external_jobs(urls, source_name):
    """Follow Wellfound/Cutshort job pages to the real application URL.

    The old version inspected only the final external page. Many ATS/career
    pages block bots, render with JS, or omit the title/location from HTML, so
    that caused almost every direct application to be discarded. The source
    directory page already gives us the India + Remote/Hybrid context; use it
    as evidence and keep the final company/ATS application URL for review.
    """
    out = []
    urls = list(dict.fromkeys(urls))[:MAX_EXTERNAL_JOB_PAGES]

    def extract_source_context(source_url):
        html, final_url = get_html(source_url, timeout=20)
        if not html:
            return "", "", ""
        soup = BeautifulSoup(html, "html.parser")
        title = ""
        meta = soup.find("meta", attrs={"property": "og:title"})
        if meta:
            title = clean_text(meta.get("content"))
        if not title and soup.title:
            title = clean_text(soup.title.get_text(" "))
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = clean_text(h1.get_text(" ", strip=True))
        body = clean_text(soup.get_text(" ", strip=True))[:40000]
        return title, body, final_url

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

            source_title, source_body, _ = extract_source_context(source_job_url)

            html, final_url = get_html(external, timeout=18)
            if not html:
                final_url = external
                html = ""

            ats = ats_from_url(final_url)
            soup = BeautifulSoup(html, "html.parser") if html else None
            external_title = ""
            external_desc = ""
            if soup:
                meta = soup.find("meta", attrs={"property": "og:title"})
                if meta:
                    external_title = clean_text(meta.get("content"))
                if not external_title and soup.title:
                    external_title = clean_text(soup.title.get_text(" "))
                external_desc = clean_text(soup.get_text(" ", strip=True))[:30000]

            title = external_title if title_relevant(external_title) else source_title
            if not title_relevant(title):
                continue

            combined = " ".join([source_body, external_desc])
            workplace = classify_workplace(combined, "", False, {}, combined)
            if not workplace:
                if HYBRID_RE.search(combined):
                    workplace = "Hybrid"
                elif REMOTE_RE.search(combined):
                    workplace = "Remote"

            # Source pages were already seeded only for India + Remote/Hybrid.
            # Use the source page as additional evidence when the final ATS
            # page is JS-only or bot-blocked.
            if not allowed_india(combined, "", "", combined, workplace):
                continue

            score, matched = score_job(title, combined, "India")
            company = ""
            if ats:
                source = f"{source_name}->ATS"
            else:
                source = f"{source_name}->Company Career"

            out.append({
                "added": datetime.now(timezone.utc).date().isoformat(),
                "title": title,
                "company": company,
                "location": "India",
                "workplace": workplace,
                "posted_date": "",
                "job_url": normalize_url(final_url) or final_url,
                "source": source,
                "score": score,
                "matched": matched,
            })

    return out


# ============================================================
# MAIN
# ============================================================


V11_COMPANY_DATA_URL = (
    "https://raw.githubusercontent.com/outscal/OpenJobs/main/"
    "data/companies_v2.json"
)
V11_CAREER_DIRECTORY_URL = (
    "https://raw.githubusercontent.com/CSwala/awesome-career-pages/main/Portal.json"
)
V11_INDIA_ATS_URL = (
    "https://raw.githubusercontent.com/AnojSKunte/career-ops-india/main/portals/india.yml"
)

V11_COMPANY_BUDGET = int(os.environ.get("COMPANY_CRAWL_BUDGET", "900"))
V11_CAREER_BUDGET = int(os.environ.get("CAREER_CRAWL_BUDGET", "750"))
V11_GENERIC_JOB_PAGE_BUDGET = int(os.environ.get("GENERIC_JOB_PAGE_BUDGET", "2200"))
V11_ATS_BOARD_BUDGET = int(os.environ.get("ATS_BOARD_BUDGET", "1800"))
V11_ARCHIVE_VALIDATION_BUDGET = int(os.environ.get("ARCHIVE_VALIDATION_BUDGET", "250"))
V11_ARCHIVE_WEEKLY = os.environ.get("ARCHIVE_WEEKLY", "1").lower() not in {"0", "false", "no"}
V11_REQUEST_TIMEOUT = int(os.environ.get("V11_REQUEST_TIMEOUT", "14"))
V11_HTML_TIMEOUT = int(os.environ.get("V11_HTML_TIMEOUT", "14"))
V11_WORKERS = int(os.environ.get("V11_WORKERS", os.environ.get("ATS_WORKERS", "24")))

# A wide ATS map. Only ATSs with a reliable public normalized fetcher are sent
# to FETCHERS; the rest are handled through generic public career-page parsing.
V11_ATS_EXTRA_DOMAINS = {
    "Workday": ["myworkdayjobs.com", "myworkday.com"],
    "iCIMS": ["icims.com"],
    "Taleo": ["taleo.net"],
    "ADP": ["workforcenow.adp.com", "adp.com"],
    "SuccessFactors": ["successfactors.com", "successfactors.eu"],
    "Rippling": ["ats.rippling.com"],
    "Jobvite": ["jobs.jobvite.com", "jobs2.jobvite.com"],
    "JazzHR": ["applytojob.com", "app.jazz.co", "jazz.co"],
    "Comeet": ["comeet.com"],
    "Pinpoint": ["pinpointhq.com"],
    "Manatal": ["jobs.manatal.com", "manatal.com"],
    "Paylocity": ["recruiting2.ultipro.com", "ultipro.com", "paylocity.com"],
    "UKG": ["recruiting.ultipro.com", "ultipro.com"],
    "Freshteam": ["freshteam.com", "jobs.freshteam.com"],
    "Bullhorn": ["bullhorn.com"],
    "Fountain": ["fountain.com", "apply.fountain.com"],
    "Phenom": ["phenompeople.com", "careers.phenom.com"],
    "Jobylon": ["jobylon.com"],
    "Homerun": ["homerun.co"],
    "JobScore": ["jobscore.com"],
    "TalentLyft": ["talentlyft.com"],
    "Crelate": ["crelate.com"],
    "iSmartRecruit": ["ismartrecruit.com"],
    "Recruiterflow": ["recruiterflow.com"],
    "InHire": ["inhire.io"],
    "Zoho Recruit": ["zohorecruit.com"],
    "Darwinbox": ["darwinbox.com", "darwinbox.in"],
    "Keka": ["keka.com"],
    "Zwayam": ["zwayam.com", "careers.zwayam.com"],
    "PeopleStrong": ["peoplestrong.com"],
    "HROne": ["hrone.cloud", "hrone.com"],
    "Oracle Recruiting": ["oraclecloud.com", "ocs.oraclecloud.com"],
    "Join.com": ["join.com"],
}

for _ats, _domains in V11_ATS_EXTRA_DOMAINS.items():
    existing = ATS_DOMAINS.setdefault(_ats, [])
    for _d in _domains:
        if _d not in existing:
            existing.append(_d)

# Larger contextual vocabulary for scoring and rescue of good support roles.
V11_SUPPORT_CONTEXT_TERMS = [
    "customer support", "customer service", "customer care", "client support",
    "client services", "user support", "member support", "merchant support",
    "seller support", "partner support", "provider support", "vendor support",
    "community support", "consumer support", "player support", "driver support",
    "rider support", "patient support", "student support", "developer support",
    "technical support", "product support", "application support", "platform support",
    "software support", "cloud support", "database support", "api support",
    "integration support", "implementation support", "service desk", "help desk",
    "helpdesk", "technical account", "technical account manager", "customer success",
    "client success", "customer experience", "customer operations", "client operations",
    "user operations", "customer lifecycle", "customer retention", "customer adoption",
    "customer engagement", "customer advocacy", "customer education", "customer enablement",
    "customer onboarding", "client onboarding", "implementation", "onboarding",
    "professional services", "service delivery", "customer relations", "client relations",
    "post-sales", "after sales", "renewals", "expansion", "churn", "escalation",
    "escalations", "incident response", "incident management", "case management",
    "ticketing", "support tickets", "support queue", "customer tickets", "customer cases",
    "zendesk", "intercom", "freshdesk", "freshservice", "gorgias", "kustomer",
    "helpscout", "salesforce service cloud", "service cloud", "servicenow",
    "zoho desk", "hubspot service hub", "front", "gladly", "talkdesk", "genesys",
    "nice cxone", "dixa", "sprinklr", "jira service management", "confluence",
    "knowledge base", "help center", "help centre", "csat", "nps", "sla", "contact center",
    "contact centre", "omnichannel", "live chat", "email support", "phone support",
    "voice support", "chat support", "case resolution", "bug triage", "bug reproduction",
    "technical troubleshooting", "troubleshooting", "diagnostics", "root cause analysis",
    "customer journey", "customer health", "customer portfolio", "book of business",
    "customer advocacy", "voice of customer", "voc", "service quality", "support quality",
    "support training", "support enablement", "support workforce", "workforce management",
    "quality analyst", "quality assurance", "trust and safety", "content moderation",
    "fraud support", "payments support", "billing support", "order support", "delivery support",
    "merchant operations", "seller operations", "partner operations", "customer operations",
    "customer success operations", "cx operations", "customer care operations",
]
for _term in V11_SUPPORT_CONTEXT_TERMS:
    if _term not in SUPPORT_CONTEXT_TERMS:
        SUPPORT_CONTEXT_TERMS.append(_term)

# Title variants not reliably covered by a single phrase in the older list.
V11_EXTRA_TITLE_PATTERNS = [
    r"\bcustomer\s+(?:support|service|care|success|experience|operations|relations)\b",
    r"\bclient\s+(?:support|service|services|success|experience|operations|relations)\b",
    r"\b(?:user|member|merchant|seller|vendor|partner|provider|consumer|player|driver|rider)\s+(?:support|services?|success|operations?)\b",
    r"\b(?:support|service)\s+(?:specialist|associate|representative|agent|advisor|consultant|coordinator|analyst|executive|administrator|lead|manager|supervisor|director|head)\b",
    r"\b(?:technical|product|application|platform|software|cloud|database|api|integration|developer)\s+support\b",
    r"\b(?:technical|product|application|platform|software|cloud)\s+support\s+(?:engineer|specialist|analyst|associate|manager|lead|consultant|representative)\b",
    r"\b(?:support|service)\s+(?:engineer|technician|developer|consultant|analyst)\b",
    r"\b(?:customer|client)\s+success\s+(?:manager|specialist|associate|analyst|partner|advisor|lead|director|executive|operations)\b",
    r"\b(?:customer|client)\s+(?:onboarding|implementation|adoption|retention|lifecycle|enablement|education)\b",
    r"\b(?:implementation|onboarding|adoption|retention|renewal|enablement|education)\s+(?:specialist|manager|lead|consultant|associate|analyst|coordinator)\b",
    r"\btechnical\s+account\s+(?:manager|specialist|executive|lead|director)\b",
    r"\bTAM\b",
    r"\b(?:helpdesk|help\s+desk|service\s+desk|IT\s+service\s+desk)\b",
    r"\b(?:escalation|escalations)\s+(?:specialist|manager|lead|analyst|engineer)\b",
    r"\bsupport\s+(?:quality|training|enablement|workforce|operations|program)\b",
    r"\b(?:trust\s*(?:and|&)\s*safety|content\s+moderation)\b",
    r"\bcustomer\s+advocacy\b",
    r"\b(?:customer|client)\s+portfolio\b",
    r"\b(?:service|support)\s+delivery\b",
    r"\b(?:customer|client)\s+account\s+manager\b",
    r"\baccount\s+manager\b.*\b(?:customer|client|success|retention|renewal)\b",
    r"\bcustomer\s+(?:journey|health|lifecycle)\b",
    r"\bvoice\s+of\s+customer\b",
]
for _pattern in V11_EXTRA_TITLE_PATTERNS:
    if _pattern not in TARGET_TITLE_PATTERNS:
        TARGET_TITLE_PATTERNS.append(_pattern)
TITLE_REGEX = re.compile("|".join(TARGET_TITLE_PATTERNS), re.I)

# Strong explicit India phrases and all common state/UT aliases.
V11_INDIA_STATES = [
    "andhra pradesh", "arunachal pradesh", "assam", "bihar", "chhattisgarh", "goa",
    "gujarat", "haryana", "himachal pradesh", "jharkhand", "karnataka", "kerala",
    "madhya pradesh", "maharashtra", "manipur", "meghalaya", "mizoram", "nagaland",
    "odisha", "orissa", "punjab", "rajasthan", "sikkim", "tamil nadu", "tamilnadu",
    "telangana", "tripura", "uttar pradesh", "uttarakhand", "west bengal", "westbengal",
    "andaman and nicobar islands", "andaman and nicobar", "chandigarh",
    "dadra and nagar haveli", "dadra and nagar haveli and daman and diu", "daman and diu",
    "delhi", "nct of delhi", "jammu and kashmir", "jammu & kashmir", "ladakh", "lakshadweep",
    "puducherry", "pondicherry",
]
for _x in V11_INDIA_STATES:
    if _x not in INDIA_STATES:
        INDIA_STATES.append(_x)

V11_CITY_BLOCK = """
Visakhapatnam Vizag Vijayawada Guntur Tirupati Nellore Kakinada Rajahmundry Kurnool Anantapur
Anantapuramu Kadapa Proddatur Ongole Eluru Bhimavaram Machilipatnam Chittoor Srikakulam
Vizianagaram Amaravati Tenali Hindupur Madanapalle Tadepalligudem Narasaraopet Adoni
Itanagar Naharlagun Pasighat Tawang Bomdila Ziro Aalo Along Tezu Namsai Roing Seppa
Changlang Khonsa Daporijo Guwahati Silchar Dibrugarh Jorhat Nagaon Tinsukia Tezpur Sivasagar
Barpeta Bongaigaon Dhubri Goalpara Golaghat Karimganj Lakhimpur Diphu Haflong Kokrajhar
Nalbari Morigaon Patna Gaya Bhagalpur Muzaffarpur Purnia Darbhanga Arrah Ara Begusarai
Katihar Chapra Chhapra Motihari Bettiah Munger Saharsa Samastipur Hajipur Sasaram Dehri
Siwan Sitamarhi Jehanabad Buxar Jamui Kishanganj Lakhisarai Madhepura Nawada Supaul Araria
Bihar Sharif Chhapra Durg Raipur Bhilai Bilaspur Korba Rajnandgaon Jagdalpur Ambikapur
Raigarh Dhamtari Kanker Kawardha Kondagaon Mahasamund Mungeli Narayanpur Sukma Bijapur Balod
Bemetara Janjgir Panaji Panjim Margao Mormugao Vasco da Gama Mapusa Ponda Calangute
Ahmedabad Surat Vadodara Baroda Rajkot Bhavnagar Jamnagar Gandhinagar Junagadh Anand Bharuch
Bhuj Godhra Mehsana Morbi Nadiad Navsari Palanpur Patan Porbandar Surendranagar Vapi Valsad
Veraval Amreli Botad Dahod Himmatnagar Modasa Deesa Jetpur Kalol Gurugram Gurgaon Faridabad
Panipat Ambala Yamunanagar Rohtak Hisar Karnal Sonipat Panchkula Bhiwani Sirsa Rewari Palwal
Jind Kaithal Thanesar Kurukshetra Fatehabad Narnaul Mahendragarh Nuh Shimla Dharamshala
Dharamsala Solan Mandi Kullu Manali Hamirpur Una Chamba Kangra Nahan Palampur Ranchi
Jamshedpur Dhanbad Bokaro Deoghar Hazaribagh Giridih Ramgarh Chaibasa Dumka Medininagar
Daltonganj Garhwa Godda Gumla Koderma Latehar Lohardaga Pakur Sahebganj Simdega Saraikela
Bengaluru Bangalore Mysuru Mysore Mangaluru Mangalore Hubballi Hubli Dharwad Belagavi Belgaum
Kalaburagi Gulbarga Shivamogga Shimoga Tumakuru Tumkur Davangere Ballari Bellary Vijayapura
Bijapur Udupi Chikkamagaluru Chikmagalur Hassan Mandya Raichur Bidar Kolar Chitradurga Gadag
Haveri Bagalkot Yadgir Kodagu Madikeri Karwar Sirsi Hosapete Hospet Ramanagara Channapatna
Doddaballapur Kochi Cochin Thiruvananthapuram Trivandrum Kozhikode Calicut Kannur Kollam
Quilon Thrissur Alappuzha Alleppey Kottayam Palakkad Kasaragod Malappuram Manjeri Perinthalmanna
Mumbai Bombay Pune Thane Navi Mumbai Panvel Nashik Nasik Nagpur Chhatrapati Sambhajinagar
Aurangabad Kolhapur Solapur Amravati Nanded Sangli Satara Jalgaon Akola Latur Dhule Ratnagiri
Sangamner Ahmednagar Ahilyanagar Beed Bhiwandi Vasai Virar Kalyan Dombivli Jalna Wardha
Chandrapur Gondia Bhandara Yavatmal Barshi Bhopal Indore Gwalior Jabalpur Ujjain Sagar
Rewa Satna Ratlam Dewas Khandwa Burhanpur Chhindwara Morena Bhind Shivpuri Vidisha Sehore
Mandsaur Neemuch Itarsi Singrauli Katni Raipur Ranchi Patna Panaji Gangtok Namchi Mangan Singtam
Kohima Dimapur Mokokchung Tuensang Wokha Mon Zunheboto Phek Kiphire Longleng Peren
Bhubaneswar Cuttack Rourkela Berhampur Brahmapur Sambalpur Puri Balasore Baripada Bhadrak
Jharsuguda Jeypore Koraput Angul Dhenkanal Bargarh Jagatsinghpur Jajpur Kalahandi Kendrapara
Keonjhar Khordha Malkangiri Mayurbhanj Nabarangpur Nayagarh Nuapada Rayagada Sundargarh
Amritsar Ludhiana Jalandhar Patiala Bathinda Mohali Sahibzada Ajit Singh Nagar Pathankot Batala
Hoshiarpur Moga Abohar Malerkotla Kapurthala Phagwara Firozpur Muktsar Barnala Sangrur Rupnagar
Ropar Fazilka Tarn Taran Gurdaspur Nawanshahr Sikar Alwar Bharatpur Bhilwara Pali Sri Ganganagar
Ganganagar Hanumangarh Churu Jhunjhunu Barmer Jaisalmer Nagaur Tonk Sawai Madhopur Bundi
Chittorgarh Dungarpur Banswara Pratapgarh Rajsamand Dausa Dholpur Karauli Beawar Kishangarh
Makrana Mount Abu Jaipur Jodhpur Udaipur Kota Bikaner Ajmer Lucknow Kanpur Ghaziabad Agra
Varanasi Prayagraj Allahabad Meerut Noida Greater Noida Bareilly Aligarh Moradabad Saharanpur
Gorakhpur Mathura Firozabad Jhansi Muzaffarnagar Rampur Shahjahanpur Hapur Ayodhya Faizabad
Mirzapur Sultanpur Basti Deoria Azamgarh Mau Ballia Jaunpur Ghazipur Sitapur Lakhimpur Kheri
Bahraich Gonda Barabanki Rae Bareli Raebareli Unnao Hardoi Farrukhabad Etawah Mainpuri Budaun
Pilibhit Bulandshahr Bijnor Amroha Sambhal Kasganj Etah Fatehpur Kaushambi Chitrakoot Banda
Mahoba Hamirpur Lalitpur Sonbhadra Robertsganj Shamli Orai Dehradun Haridwar Roorkee Haldwani
Nainital Rudrapur Kashipur Rishikesh Mussoorie Almora Pithoragarh Bageshwar Chamoli Gopeshwar
Pauri Srinagar Tehri Uttarkashi Kotdwar Ramnagar Khatima Kolkata Calcutta Howrah Siliguri Durgapur
Asansol Haldia Kharagpur Malda English Bazar Berhampore Baharampur Jalpaiguri Cooch Behar
Alipurduar Raiganj Balurghat Krishnanagar Bardhaman Burdwan Bankura Purulia Medinipur Tamluk
Diamond Harbour Barrackpore Barasat Habra Chandannagar Serampore Rishra Kalyani Dankuni Bolpur
Santiniketan Hyderabad Warangal Hanamkonda Nizamabad Karimnagar Khammam Ramagundam Mahbubnagar
Mahabubnagar Nalgonda Adilabad Siddipet Suryapet Jagtial Kamareddy Mancherial Nirmal Sangareddy
Vikarabad Wanaparthy Bhupalpally Kothagudem Narayanpet Medak Jangaon Chennai Madras Coimbatore
Madurai Salem Tiruchirappalli Trichy Tiruppur Erode Vellore Thoothukudi Tuticorin Tirunelveli
Dindigul Thanjavur Nagercoil Kanchipuram Chengalpattu Hosur Karur Namakkal Cuddalore Villupuram
Virudhunagar Sivakasi Ramanathapuram Sivaganga Pudukkottai Nagapattinam Mayiladuthurai Tiruvallur
Tiruvannamalai Dharmapuri Krishnagiri Ariyalur Perambalur Ooty Udhagamandalam Pollachi Karaikudi
Kumbakonam Tiruchengode Kallakurichi Arakkonam Sriperumbudur Ambur Ranipet
Delhi New Delhi Srinagar Jammu Leh Kargil Port Blair Sri Vijaya Puram Silvassa Daman Diu Kavaratti
Agatti Karaikal Mahe Yanam
"""
for _city in re.findall(r"[A-Za-z][A-Za-z&.'-]*(?:\s+[A-Za-z][A-Za-z&.'-]*)*", V11_CITY_BLOCK):
    _c = clean_text(_city).lower()
    if _c and _c not in INDIA_CITIES and len(_c) > 2:
        INDIA_CITIES.append(_c)
INDIA_CITIES = sorted(set(INDIA_CITIES))
INDIA_REGEX = re.compile(
    "|".join([r"\b" + re.escape(x) + r"\b" for x in INDIA_STATES + INDIA_CITIES] + [r"\bindia\b", r"\brepublic of india\b"]),
    re.I,
)

# Country-code/structured India evidence should win over the city list, so
# an unlisted small town is not silently rejected.

def v11_classify_india(location="", country_code="", address_country="", description="", workplace=""):
    vals = [clean_text(location), clean_text(country_code), clean_text(address_country)]
    if any(re.search(r"^(?:in|ind|india)$", x, re.I) for x in vals if x):
        return True
    if any(INDIA_REGEX.search(x) for x in vals if x):
        return True
    text = clean_text(description)[:50000]
    if INDIA_COUNTRY_RE.search(text) and workplace in {"Remote", "Hybrid"}:
        return True
    return bool(INDIA_REGEX.search(text)) and workplace in {"Remote", "Hybrid"}


def v11_allowed_india(location="", country_code="", address_country="", description="", workplace=""):
    return workplace in {"Remote", "Hybrid"} and v11_classify_india(location, country_code, address_country, description, workplace)

# Override only the location decision; retain the tested workplace classifier.
classify_india = v11_classify_india
allowed_india = v11_allowed_india

# ============================================================
# V11 STRUCTURED DATA / CAREER PARSING
# ============================================================

def _iter_jsonld_objects(soup):
    for script in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        stack = obj if isinstance(obj, list) else [obj]
        for item in stack:
            if isinstance(item, dict) and "@graph" in item and isinstance(item["@graph"], list):
                stack.extend(x for x in item["@graph"] if isinstance(x, dict))
        for item in stack:
            if isinstance(item, dict):
                yield item


def _job_location_text(item):
    locs = item.get("jobLocation") or item.get("job_location") or []
    if isinstance(locs, dict):
        locs = [locs]
    parts = []
    for loc in locs if isinstance(locs, list) else []:
        if not isinstance(loc, dict):
            continue
        addr = loc.get("address") or {}
        if isinstance(addr, dict):
            parts.extend([
                clean_text(addr.get("streetAddress")), clean_text(addr.get("addressLocality")),
                clean_text(addr.get("addressRegion")), clean_text(addr.get("postalCode")),
                clean_text(addr.get("addressCountry")),
            ])
        else:
            parts.append(clean_text(addr))
    req = item.get("applicantLocationRequirements") or item.get("applicant_location_requirements") or []
    if isinstance(req, dict):
        req = [req]
    for r in req if isinstance(req, list) else []:
        if isinstance(r, dict):
            parts.extend([clean_text(r.get("name")), clean_text((r.get("address") or {}).get("addressCountry") if isinstance(r.get("address"), dict) else "")])
    return ", ".join(x for x in parts if x)


def _structured_workplace(item, body=""):
    jt = clean_text(item.get("jobLocationType") or item.get("job_location_type"))
    if jt and "TELECOMMUTE" in jt.upper():
        return "Remote"
    loc = _job_location_text(item)
    text = " ".join([loc, body[:12000]])
    if HYBRID_RE.search(text):
        return "Hybrid"
    if REMOTE_RE.search(text):
        return "Remote"
    return ""


def parse_structured_jobpostings(html, base_url, company_hint="", source="Company Career"):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for item in _iter_jsonld_objects(soup):
        typ = item.get("@type")
        types = typ if isinstance(typ, list) else [typ]
        if not any(str(x).lower() == "jobposting" for x in types):
            continue
        title = clean_text(item.get("title") or item.get("name"))
        if not title_relevant(title):
            continue
        body = clean_text(item.get("description"))
        location = _job_location_text(item)
        workplace = _structured_workplace(item, body)
        if not workplace:
            # Remote jobPosting can be encoded only with applicantLocationRequirements.
            if item.get("jobLocationType"):
                workplace = "Remote" if "tele" in clean_text(item.get("jobLocationType")).lower() else ""
        country_code = ""
        for loc in (item.get("jobLocation") or [] if isinstance(item.get("jobLocation"), list) else [item.get("jobLocation")]):
            if isinstance(loc, dict):
                addr = loc.get("address")
                if isinstance(addr, dict):
                    country_code = clean_text(addr.get("addressCountry")) or country_code
        if not v11_allowed_india(location, country_code, country_code, body, workplace):
            continue
        posted = parse_date(item.get("datePosted") or item.get("dateCreated") or item.get("validFrom"))
        if not recent_enough(posted):
            continue
        job_url = clean_text(item.get("url") or item.get("sameAs"))
        if not job_url:
            job_url = base_url
        job_url = urljoin(base_url, job_url)
        org = item.get("hiringOrganization") or {}
        company = company_hint or clean_text(org.get("name") if isinstance(org, dict) else "")
        score, matched = score_job(title, body, location)
        score += 20
        if posted:
            score += 5
        out.append({
            "added": datetime.now(timezone.utc).date().isoformat(),
            "title": title, "company": company, "location": location or "India",
            "workplace": workplace, "posted_date": posted, "job_url": normalize_url(job_url) or job_url,
            "source": source, "score": score, "matched": matched,
        })
    return out


def _extract_job_links(soup, base_url):
    candidates = []
    keywords = re.compile(
        r"\b(?:apply|customer|client|support|success|service|helpdesk|help\s*desk|technical|product|onboarding|implementation|account|jobs?|careers?|vacanc|opening|position)\b",
        re.I,
    )
    path_re = re.compile(r"/(?:jobs?|careers?|openings?|positions?|vacancies?)(?:/|[-_])", re.I)
    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a.get("href", ""))
        if not href.startswith(("http://", "https://")):
            continue
        txt = clean_text(a.get_text(" ", strip=True))
        path = urlparse(href).path
        if keywords.search(txt) or path_re.search(path):
            candidates.append((href, txt))
    # Also recognize forms/buttons with action URLs.
    for tag in soup.find_all(["form", "iframe"]):
        href = tag.get("action") or tag.get("src")
        if href:
            href = urljoin(base_url, href)
            if href.startswith(("http://", "https://")):
                candidates.append((href, clean_text(tag.get_text(" ", strip=True))))
    return list(dict.fromkeys(candidates))


def crawl_career_url_v10(company, url, source="Company Career", max_job_pages=8):
    """Fetch a public employer career page and return direct job records plus ATS links."""
    if not url:
        return [], []
    html, final_url = get_html(url, timeout=V11_HTML_TIMEOUT)
    if not html:
        return [], []
    soup = BeautifulSoup(html, "html.parser")
    jobs = parse_structured_jobpostings(html, final_url, company, source)
    career_links = []
    seen = set()

    # Capture exact ATS links, even when embedded in buttons/iframes.
    for a in soup.find_all(["a", "iframe", "script"], href=True) if False else []:
        pass
    for a in soup.find_all("a", href=True):
        href = urljoin(final_url, a.get("href", ""))
        ats = ats_from_url(href)
        if ats:
            career_links.append((ats, href))

    # Embedded script URLs are common on modern ATS widgets.
    for script in soup.find_all("script", src=True):
        href = urljoin(final_url, script.get("src", ""))
        ats = ats_from_url(href)
        if ats:
            career_links.append((ats, href))

    for href, anchor_text in _extract_job_links(soup, final_url):
        key = normalize_url(href)
        if not key or key in seen:
            continue
        seen.add(key)
        if len(seen) > max_job_pages:
            break
        href_ats = ats_from_url(href)
        # If the anchor itself is clearly a role page, fetch it. If it is just
        # a generic /jobs page, it will be handled as a shallow second pass.
        if href_ats or re.search(r"/(?:job|jobs|career|careers|opening|openings|position|vacanc)", urlparse(href).path, re.I) or title_relevant(anchor_text):
            page_html, page_final = get_html(href, timeout=V11_HTML_TIMEOUT)
            if not page_html:
                continue
            structured = parse_structured_jobpostings(page_html, page_final, company, source)
            if structured:
                jobs.extend(structured)
                continue
            ps = BeautifulSoup(page_html, "html.parser")
            title = ""
            for selector in [
                ('meta', {'property': 'og:title'}),
                ('meta', {'name': 'twitter:title'}),
            ]:
                m = ps.find(*selector)
                if m:
                    title = clean_text(m.get("content"))
                    if title:
                        break
            if not title and ps.find("h1"):
                title = clean_text(ps.find("h1").get_text(" ", strip=True))
            if not title and ps.title:
                title = clean_text(ps.title.get_text(" "))
            if not title_relevant(title):
                continue
            body = clean_text(ps.get_text(" ", strip=True))[:50000]
            workplace = "Hybrid" if HYBRID_RE.search(body) else ("Remote" if REMOTE_RE.search(body) else "")
            # Use structured job JSON buried in the page whenever possible.
            if not workplace:
                for item in _iter_jsonld_objects(ps):
                    workplace = _structured_workplace(item, body)
                    if workplace:
                        break
            if not v11_allowed_india(body, "", "", body, workplace):
                continue
            posted = ""
            # Look for common date fields in visible/script text.
            m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", body)
            if m:
                posted = m.group(1)
            if posted and not recent_enough(posted):
                continue
            score, matched = score_job(title, body, "India")
            score += 20
            jobs.append({
                "added": datetime.now(timezone.utc).date().isoformat(),
                "title": title, "company": company, "location": "India",
                "workplace": workplace, "posted_date": posted, "job_url": normalize_url(page_final) or page_final,
                "source": source, "score": score, "matched": matched,
            })

    return jobs, career_links


def crawl_nonstandard_ats_url(company, url, ats):
    return crawl_career_url_v10(company, url, source=f"{ats}->Direct", max_job_pages=10)

# ============================================================
# V11 DISCOVERY GRAPH
# ============================================================

def _raw_json(url, timeout=70):
    try:
        r = SESSION.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        return None
    return None


def load_openjobs_india_seeds():
    data = _raw_json(V11_COMPANY_DATA_URL, timeout=90)
    if not isinstance(data, list):
        print("OPENJOBS DATASET: unavailable")
        return []
    out = []
    for item in data:
        if not isinstance(item, dict):
            continue
        countries = item.get("countries") or []
        if not any(str(x).strip().lower() == "india" for x in countries):
            continue
        name = clean_text(item.get("name"))
        website = clean_text(item.get("website"))
        ats_links = item.get("ats_links") or []
        list_urls = item.get("list_urls") or []
        if not name:
            continue
        out.append({"company": name, "website": website, "ats_links": ats_links, "career_links": list_urls, "source": "OpenJobs"})
    print(f"OPENJOBS: {len(out)} India-hiring companies loaded")
    return out


def load_awesome_career_pages():
    data = _raw_json(V11_CAREER_DIRECTORY_URL, timeout=60)
    if not isinstance(data, list):
        print("CAREER DIRECTORY: unavailable")
        return []
    out = []
    for item in data:
        if not isinstance(item, dict):
            continue
        name = clean_text(item.get("Title") or item.get("title") or item.get("Company"))
        url = clean_text(item.get("Link") or item.get("link") or item.get("URL"))
        if not name or not url:
            continue
        if not re.match(r"^https?://", url, re.I):
            url = "https://" + url.lstrip("/")
        out.append({"company": name, "website": url, "career_links": [url], "ats_links": [], "source": "Career Directory"})
    print(f"CAREER DIRECTORY: {len(out)} career URLs loaded")
    return out


def load_india_ats_directory():
    try:
        r = SESSION.get(V11_INDIA_ATS_URL, timeout=50)
        if r.status_code != 200:
            print(f"INDIA ATS DIRECTORY: HTTP {r.status_code}")
            return []
        urls = re.findall(r"https?://[^\s\"'<>]+", r.text)
        out = []
        seen = set()
        for url in urls:
            url = url.rstrip("),.;'")
            ats = ats_from_url(url)
            if not ats:
                continue
            k = (ats, normalize_url(url))
            if k in seen:
                continue
            seen.add(k)
            out.append((ats, url))
        print(f"INDIA ATS DIRECTORY: {len(out)} exact ATS links found")
        return out
    except Exception as exc:
        print(f"INDIA ATS DIRECTORY ERROR: {exc}")
        return []


def company_rotation(items, budget, salt="career"):
    items = list(items)
    if len(items) <= budget:
        return items
    day = datetime.now(timezone.utc).date().toordinal()
    # Stable daily rotation so a larger universe gets covered across the week.
    scored = []
    for item in items:
        key = clean_text(item.get("company") or item.get("website") or "") + salt
        digest = hashlib.sha256((key + str(day // 1)).encode()).hexdigest()
        scored.append((digest, item))
    scored.sort(key=lambda x: x[0])
    return [x[1] for x in scored[:budget]]


def merge_company_universe(openjobs, career_pages, wellfound, cutshort, existing):
    merged = {}
    for item in openjobs + career_pages + wellfound + cutshort + existing:
        name = clean_text(item.get("company"))
        if not name:
            continue
        key = re.sub(r"[^a-z0-9]+", "", name.lower())
        current = merged.setdefault(key, {"company": name, "website": "", "career_links": [], "ats_links": [], "source": "Company"})
        if item.get("website") and not current.get("website"):
            current["website"] = item.get("website")
        current["career_links"].extend(item.get("career_links") or [])
        current["ats_links"].extend(item.get("ats_links") or [])
        src = item.get("source") or "Company"
        if src not in current["source"]:
            current["source"] += "," + src
    for item in merged.values():
        item["career_links"] = list(dict.fromkeys([x for x in item.get("career_links", []) if x]))
        item["ats_links"] = list(dict.fromkeys([x for x in item.get("ats_links", []) if x]))
    return list(merged.values())


def extract_exact_job_url_from_page(source_url, title_hint=""):
    """Return a likely direct employer/ATS job URL from a public aggregator page."""
    html, final_url = get_html(source_url, timeout=V11_HTML_TIMEOUT)
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    candidate_urls = []

    for item in _iter_jsonld_objects(soup):
        if str(item.get("@type", "")).lower() == "jobposting":
            for key in ("url", "sameAs", "applyUrl", "applicationUrl"):
                v = item.get(key)
                vals = v if isinstance(v, list) else [v]
                for x in vals:
                    if isinstance(x, str):
                        candidate_urls.append(urljoin(final_url, x))
            org = item.get("hiringOrganization")
            if isinstance(org, dict):
                for x in org.get("sameAs", []) if isinstance(org.get("sameAs"), list) else [org.get("sameAs")]:
                    if isinstance(x, str):
                        candidate_urls.append(urljoin(final_url, x))

    for a in soup.find_all("a", href=True):
        href = urljoin(final_url, a.get("href", ""))
        if not href.startswith(("http://", "https://")):
            continue
        host = urlparse(href).netloc.lower()
        txt = clean_text(a.get_text(" ", strip=True)).lower()
        if any(x in host for x in [
            "himalayas.app", "startup.jobs", "jobicy.com", "remoteok.com", "remotejobs.org",
            "wellfound.com", "cutshort.io", "weworkremotely.com",
        ]):
            continue
        if ats_from_url(href) or any(x in txt for x in ["apply", "career", "company site", "employer site", "view job"]):
            candidate_urls.append(href)

    # Prefer URLs whose anchor text matches the title hint and then ATS links.
    uniq = list(dict.fromkeys(candidate_urls))
    if title_hint:
        title_words = set(re.findall(r"[a-z0-9]+", title_hint.lower()))
        scored = []
        for u in uniq:
            score = 0
            if ats_from_url(u): score += 50
            if re.search(r"/(?:job|jobs|careers?|openings?)/", urlparse(u).path, re.I): score += 10
            if title_words:
                score += sum(1 for w in title_words if w in u.lower())
            scored.append((score, u))
        scored.sort(key=lambda x: (-x[0], len(x[1])))
        return scored[0][1] if scored else ""
    for u in uniq:
        if ats_from_url(u):
            return u
    return uniq[0] if uniq else ""


def resolve_public_feed_jobs_to_direct(jobs, budget=140):
    if not jobs:
        return []
    selected = sorted(jobs, key=lambda x: (x.get("score", 0), x.get("posted_date", "")), reverse=True)[:budget]
    out = []
    def one(j):
        direct = extract_exact_job_url_from_page(j.get("job_url", ""), j.get("title", ""))
        if not direct:
            return None
        if ats_from_url(direct) or urlparse(direct).netloc.lower() not in {
            "himalayas.app", "startup.jobs", "jobicy.com", "remoteok.com", "remotejobs.org", "wellfound.com", "cutshort.io", "weworkremotely.com"
        }:
            x = dict(j)
            x["job_url"] = normalize_url(direct) or direct
            x["source"] = f"{j.get('source','Feed')}->Direct"
            return x
        return None
    with ThreadPoolExecutor(max_workers=min(V11_WORKERS, 16)) as ex:
        futures = {ex.submit(one, j): j for j in selected}
        for f in as_completed(futures):
            try:
                item = f.result()
                if item:
                    out.append(item)
            except Exception:
                pass
    return out


def v11_discover_company_routes(companies):
    exact_ats_urls = []
    career_urls = []
    seen_ats = set()
    seen_career = set()
    for item in companies:
        company = item.get("company", "")
        urls = (item.get("ats_links") or []) + (item.get("career_links") or [])
        if item.get("website"):
            urls.append(item.get("website"))
        for url in urls:
            if not url or not re.match(r"^https?://", str(url), re.I):
                continue
            ats = ats_from_url(url)
            if ats:
                k = (ats, normalize_url(url))
                if k not in seen_ats:
                    seen_ats.add(k)
                    exact_ats_urls.append((company, ats, url))
            else:
                u = normalize_url(url)
                if u and u not in seen_career:
                    seen_career.add(u)
                    career_urls.append((company, u))
    return exact_ats_urls, career_urls


def v11_crawl_company_routes(companies):
    exact_ats_urls, career_urls = v11_discover_company_routes(companies)
    print(f"V11 ROUTES: {len(exact_ats_urls)} exact ATS links, {len(career_urls)} career/company URLs")
    return exact_ats_urls, career_urls


def v11_crawl_career_universe(companies):
    # Prioritize explicit career URLs from ATS-rich company records, then
    # rotate the broader career directory so the universe grows over time.
    exact_ats, career_urls = v11_crawl_company_routes(companies)
    career_tasks = list(career_urls)
    if len(career_tasks) > V11_CAREER_BUDGET:
        day = datetime.now(timezone.utc).date().toordinal()
        keyed = []
        for company, url in career_tasks:
            h = hashlib.sha256(f"{company}|{url}|{day % 14}".encode()).hexdigest()
            keyed.append((h, company, url))
        keyed.sort()
        career_tasks = [(c, u) for _, c, u in keyed[:V11_CAREER_BUDGET]]

    all_jobs = []
    all_records = []
    def one(task):
        company, url = task
        # Avoid spending a request on known non-career social URLs.
        host = urlparse(url).netloc.lower()
        if any(x in host for x in ["linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com"]):
            return [], []
        return crawl_career_url_v10(company, url, source="Company Career", max_job_pages=8)

    with ThreadPoolExecutor(max_workers=V11_WORKERS) as ex:
        futures = {ex.submit(one, t): t for t in career_tasks}
        done = 0
        for f in as_completed(futures):
            try:
                j, links = f.result()
            except Exception:
                j, links = [], []
            all_jobs.extend(j)
            company, url = futures[f]
            for ats, ats_url in links:
                all_records.append({
                    "company": company, "ats": ats, "ats_url": ats_url,
                    "website": url, "jobs_found": len(j), "status": "ACTIVE",
                })
            done += 1
            if done % 100 == 0:
                print(f"CAREER PAGE PROGRESS: {done}/{len(career_tasks)}")
    print(f"DIRECT COMPANY JOBS: {len(all_jobs)}")
    print(f"DIRECT COMPANY ATS LINKS: {len(all_records)}")
    return exact_ats, all_records, all_jobs


def v11_board_sets_from_exact_links(exact_ats_urls):
    boards = {k: set() for k in FETCHERS}
    nonstandard = []
    for company, ats, url in exact_ats_urls:
        slug = extract_ats_slug(ats, url)
        if ats in FETCHERS and slug:
            boards[ats].add(slug)
        else:
            nonstandard.append((company, ats, url))
    return boards, nonstandard


def v11_crawl_nonstandard_exact(nonstandard, budget=500):
    tasks = list(dict.fromkeys(nonstandard))[:budget]
    out_jobs = []
    out_records = []
    if not tasks:
        return out_jobs, out_records
    def one(item):
        company, ats, url = item
        return crawl_nonstandard_ats_url(company, url, ats)
    with ThreadPoolExecutor(max_workers=V11_WORKERS) as ex:
        futures = {ex.submit(one, t): t for t in tasks}
        for f in as_completed(futures):
            company, ats, url = futures[f]
            try:
                jobs, links = f.result()
            except Exception:
                jobs, links = [], []
            out_jobs.extend(jobs)
            out_records.append({
                "company": company, "ats": ats, "ats_url": url,
                "website": url, "jobs_found": len(jobs), "status": "ACTIVE",
            })
    print(f"NONSTANDARD ATS DIRECT JOBS: {len(out_jobs)}")
    return out_jobs, out_records


def v11_clean_direct_jobs(jobs):
    """Ensure every V11 direct job points to employer/ATS rather than an aggregator."""
    blocked = {
        "himalayas.app", "startup.jobs", "jobicy.com", "remoteok.com", "remotejobs.org",
        "wellfound.com", "cutshort.io", "weworkremotely.com",
    }
    out = []
    for j in jobs:
        u = clean_text(j.get("job_url"))
        host = urlparse(u).netloc.lower()
        if not u or host in blocked:
            continue
        if not title_relevant(j.get("title", "")):
            continue
        if j.get("workplace") not in {"Remote", "Hybrid"}:
            continue
        # Direct jobs from company/ATS parsers already contain the India evidence.
        out.append(j)
    return out


def should_run_v11_archive():
    if not V11_ARCHIVE_WEEKLY:
        return False
    return datetime.now(timezone.utc).date().toordinal() % 7 == 0

# ============================================================
# V11 MAIN
# ============================================================


# ============================================================
# REMOTE4.ME V11 — SHARDED HIGH-COVERAGE ORCHESTRATOR
# ============================================================

import argparse
from collections import Counter, defaultdict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

V11_VERSION = "11.0"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
BOARDS_FILE = os.path.join(DATA_DIR, "boards.json")
COMPANIES_FILE = os.path.join(DATA_DIR, "companies.json")

# High-coverage defaults. They can be overridden by GitHub Actions env vars.
V11_COMPANY_DISCOVERY_LIMIT = int(os.environ.get("COMPANY_DISCOVERY_LIMIT", "15000"))
V11_CAREER_DAILY_BUDGET = int(os.environ.get("CAREER_DAILY_BUDGET", "1400"))
V11_CAREER_PAGE_LIMIT = int(os.environ.get("CAREER_PAGE_LIMIT", "4"))
V11_FEED_DIRECT_BUDGET = int(os.environ.get("FEED_DIRECT_BUDGET", "350"))
V11_WORKDAY_MAX_PAGES_PER_KEYWORD = int(os.environ.get("WORKDAY_MAX_PAGES_PER_KEYWORD", "3"))
V11_WORKDAY_MAX_RESULTS_PER_BOARD = int(os.environ.get("WORKDAY_MAX_RESULTS_PER_BOARD", "180"))
V11_DISCOVERY_CANDIDATE_LIMIT = int(os.environ.get("DISCOVERY_CANDIDATE_LIMIT", "6000"))
V11_DISCOVERY_VALIDATE_PER_ATS = int(os.environ.get("DISCOVERY_VALIDATE_PER_ATS", "1800"))
V11_DISCOVERY_WORKERS = int(os.environ.get("DISCOVERY_WORKERS", "36"))
V11_CRAWL_WORKERS = int(os.environ.get("CRAWL_WORKERS", "32"))
V11_BOARD_MAX_FAILS = int(os.environ.get("BOARD_MAX_FAILS", "5"))
V11_JOB_MAX_AGE_DAYS = int(os.environ.get("JOB_MAX_AGE_DAYS", "30"))
V11_ALLOW_APAC_WORLDWIDE = os.environ.get("ALLOW_APAC_WORLDWIDE", "1").lower() not in {"0","false","no"}
V11_ROTATION_DAYS = int(os.environ.get("COMPANY_ROTATION_DAYS", "10"))
V11_ATS_DAILY_CAP = int(os.environ.get("ATS_DAILY_CAP", "0"))  # 0 = all live boards in selected ATS shard

# Optional feed credentials. These are NOT required.
ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID", "").strip()
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "").strip()
JOOBLE_API_KEY = os.environ.get("JOOBLE_API_KEY", "").strip()

# Large Indian role vocabulary. Title patterns remain the hard quality gate;
# context terms rescue specialized support titles.
V11_EXTRA_TITLE_PATTERNS = [
    r"\bvoice\s+process(?:\s+(?:executive|associate|agent|specialist|representative))?\b",
    r"\bnon[- ]?voice\s+process(?:\s+(?:executive|associate|agent|specialist|representative))?\b",
    r"\blive\s+chat\s+(?:agent|support|specialist|executive|associate)\b",
    r"\bemail\s+(?:support|specialist|associate|executive|agent)\b",
    r"\bcustomer\s+happiness\b",
    r"\bcustomer\s+champion\b",
    r"\bescalation\s+engineer\b",
    r"\bcontent\s+moderator\b",
    r"\btrust\s*(?:and|&)\s*safety\s+(?:analyst|associate|specialist|agent|operations)\b",
    r"\bsupport\s+ops(?:erations)?\b",
    r"\bpayments?\s+support\b",
    r"\bdispute\s+resolution\s+(?:specialist|analyst|associate|manager|agent)\b",
    r"\bchargeback\s+(?:analyst|specialist|associate|operations|support)\b",
    r"\bsuccess\s+engineer\b",
    r"\bcare\s+associate\b",
    r"\bcare\s+(?:executive|specialist|agent|advisor)\b",
    r"\bcustomer\s+operations?\b",
    r"\bclient\s+operations?\b",
    r"\bcustomer\s+retention\b",
    r"\bcustomer\s+adoption\b",
    r"\bcustomer\s+enablement\b",
    r"\bcustomer\s+education\b",
    r"\bcustomer\s+renewals?\b",
    r"\baccount\s+success\b",
    r"\bmember\s+success\b",
    r"\btechnical\s+account\s+(?:manager|executive|specialist|lead|director)\b",
    r"\bservice\s+delivery\b",
    r"\bcase\s+manager\b.*\bcustomer\b",
    r"\bcase\s+management\b.*\bcustomer\b",
    r"\bcustomer\s+advocat(?:e|es|ion)\b",
    r"\bvoice\s+of\s+customer\b",
    r"\bcontact\s+center\b",
    r"\bcontact\s+centre\b",
    r"\bcall\s+center\b.*\b(?:customer|support|service)\b",
    r"\bcall\s+centre\b.*\b(?:customer|support|service)\b",
    r"\bservice\s+desk\b",
    r"\bhelp\s*desk\b",
    r"\btechnical\s+help\s*desk\b",
    r"\bapplication\s+support\b",
    r"\bplatform\s+support\b",
    r"\bsoftware\s+support\b",
    r"\bcloud\s+support\b",
    r"\bdatabase\s+support\b",
    r"\bapi\s+support\b",
    r"\bintegration\s+support\b",
    r"\bdeveloper\s+support\b",
    r"\btechnical\s+success\b",
]
for _p in V11_EXTRA_TITLE_PATTERNS:
    if _p not in TARGET_TITLE_PATTERNS:
        TARGET_TITLE_PATTERNS.append(_p)
TITLE_REGEX = re.compile("|".join(TARGET_TITLE_PATTERNS), re.I)

# Broader India state/city aliases. The list is intentionally large; structured country
# codes remain the primary signal where the ATS provides them.
V11_EXTRA_INDIA_TERMS = [
    # states / UTs and common abbreviations
    "ap", "ts", "tn", "ka", "kl", "mh", "gj", "rj", "up", "uk", "wb", "od", "or",
    "mp", "cg", "jh", "br", "pb", "hr", "hp", "ga", "as", "sikkim", "manipur", "meghalaya",
    "mizoram", "nagaland", "tripura", "arunachal", "chandigarh", "jammu", "kashmir", "ladakh",
    # Andhra Pradesh
    "visakhapatnam","vizag","vijayawada","guntur","tirupati","nellore","kakinada","rajahmundry","kurnool","kadapa","ongole","eluru","anantapur","anantapuramu","bhimavaram","machilipatnam","chittoor","srikakulam","vizianagaram","tenali","proddatur","adoni","hindupur",
    # Arunachal Pradesh
    "itanagar","naharlagun","pasighat","tawang","bomdila","ziro","tezu","namsai",
    # Assam
    "guwahati","silchar","dibrugarh","jorhat","nagaon","tinsukia","tezpur","sivasagar","barpeta","bongaigaon","dhubri","golaghat","diphu","lakhimpur","north lakhimpur",
    # Bihar
    "patna","gaya","bhagalpur","muzaffarpur","darbhanga","purnia","begusarai","ara","arrah","katihar","chapra","motihari","sasaram","hajipur","samastipur","munger","sitamarhi","siwan","bettiah",
    # Chhattisgarh
    "raipur","bhilai","durg","bilaspur","korba","rajnandgaon","jagdalpur","ambikapur",
    # Goa
    "panaji","panjim","margao","vasco da gama","mapusa","ponda",
    # Gujarat
    "ahmedabad","surat","vadodara","baroda","rajkot","gandhinagar","bhavnagar","jamnagar","junagadh","anand","nadiad","mehsana","morbi","bharuch","vapi","navsari","godhra","palanpur","valsad","surendranagar","deesa","veraval",
    # Haryana
    "gurgaon","gurugram","faridabad","panipat","ambala","hisar","rohtak","sonipat","karnal","yamunanagar","panchkula","bhiwani","sirsa","bahadurgarh","rewari",
    # Himachal
    "shimla","dharamshala","dharamsala","solan","mandi","kullu","manali","baddi","una","hamirpur","sirmaur",
    # Jharkhand
    "ranchi","jamshedpur","dhanbad","bokaro","deoghar","hazaribagh","giridih","ramgarh",
    # Karnataka
    "bengaluru","bangalore","mysuru","mysore","mangaluru","mangalore","hubballi","hubli","belagavi","belgaum","davangere","dharwad","ballari","bellary","shivamogga","shimoga","tumakuru","tumkur","udupi","kalaburagi","gulbarga","bidar","hassan","mandya","raichur","vijayapura","bijapur","chitradurga","kolar","chikmagalur","chikkamagaluru","hosur","electronic city","whitefield","koramangala",
    # Kerala
    "thiruvananthapuram","trivandrum","kochi","cochin","kozhikode","calicut","thrissur","kollam","alappuzha","alleppey","palakkad","kannur","kottayam","malappuram","kasaragod","ernakulam",
    # MP
    "indore","bhopal","jabalpur","gwalior","ujjain","sagar","satna","rewa","dewas","ratlam","vidisha","chhindwara","katni","singrauli","burhanpur","khandwa",
    # Maharashtra
    "mumbai","bombay","pune","nagpur","nashik","nasik","aurangabad","chhatrapati sambhajinagar","thane","navi mumbai","kolhapur","solapur","amravati","nanded","sangli","satara","jalgaon","akola","latur","ahmednagar","chandrapur","dhule","ratnagiri","vasai","virar","panvel",
    # Odisha
    "bhubaneswar","bbsr","cuttack","rourkela","berhampur","brahmapur","sambalpur","balasore","baripada","jeypore","koraput","angul","jharsuguda","puri",
    # Punjab
    "ludhiana","amritsar","jalandhar","patiala","bathinda","mohali","sahibzada ajit singh nagar","pathankot","hoshiarpur","moga","firozpur","zirakpur","rajpura",
    # Rajasthan
    "jaipur","jodhpur","udaipur","kota","ajmer","bikaner","alwar","bharatpur","bhilwara","sikar","pali","sri ganganagar","kota","barmer","bundi","chittorgarh","beawar","kishangarh","hanumangarh",
    # Tamil Nadu
    "chennai","madras","coimbatore","kovai","madurai","trichy","tiruchirappalli","salem","tiruppur","tirunelveli","thoothukudi","tuticorin","erode","vellore","tiruvannamalai","thanjavur","dindigul","nagercoil","karur","hosur","kanchipuram","chengalpattu","cuddalore","pondicherry","puducherry","avadi","tambaram","ambattur","sriperumbudur",
    # Telangana
    "hyderabad","secunderabad","warangal","hanamkonda","nizamabad","karimnagar","khammam","mahbubnagar","adilabad","suryapet","nalgonda","siddipet","ramagundam",
    # Uttar Pradesh
    "lucknow","noida","greater noida","ghaziabad","kanpur","varanasi","prayagraj","allahabad","agra","meerut","bareilly","aligarh","moradabad","saharanpur","gorakhpur","mathura","vrindavan","ayodhya","faizabad","jhansi","muzaffarnagar","rampur","firozabad","bulandshahr","gonda","sitapur","barabanki",
    # Uttarakhand
    "dehradun","haridwar","roorkee","rishikesh","haldwani","nainital","rudrapur","kashipur","almora","pantnagar",
    # West Bengal
    "kolkata","calcutta","howrah","siliguri","durgapur","asansol","kharagpur","bardhaman","burdwan","malda","haldia","berhampore","raiganj","new town","salt lake","newtown",
    # Delhi / NCR
    "new delhi","delhi","ncr","national capital region","dwarka","noida","gurugram","faridabad","ghaziabad",
    # J&K / Ladakh
    "srinagar","jammu","anantnag","leh","kargil",
    # Northeast / misc
    "aizawl","imphal","shillong","kohima","agartala","gangtok","port blair","silvassa","daman","diu","kavaratti","puducherry",
]
for _x in V11_EXTRA_INDIA_TERMS:
    if _x not in INDIA_CITIES:
        INDIA_CITIES.append(_x)
for _x in V11_EXTRA_INDIA_TERMS:
    if _x not in INDIA_STATES:
        # no-op for city aliases; states list stays semantically clean
        pass

# Recompiled regex with the larger city dictionary.
INDIA_CITY_REGEX = re.compile(r"(?:\b(?:" + "|".join(re.escape(x) for x in sorted(set(INDIA_CITIES), key=len, reverse=True)) + r")\b)", re.I)

# Eligibility tiers: strict India, plus remote APAC/worldwide discovery. The latter are
# clearly marked so Remote4.me can choose the publishing policy later.
APAC_RE = re.compile(r"\b(?:apac|asia[- ]pacific|asia pacific|south asia|asia)\b", re.I)
WORLDWIDE_RE = re.compile(r"\b(?:worldwide|world[- ]wide|global|anywhere|work from anywhere|remote anywhere|all countries|international remote)\b", re.I)
INDIA_ELIGIBILITY_COLUMNS = ["India Eligibility"]


def v11_india_eligibility(location="", country_code="", address_country="", description="", workplace=""):
    hay = " ".join([clean_text(location), clean_text(country_code), clean_text(address_country), clean_text(description)[:30000]])
    if re.search(r"\b(?:india|indian|in)\b", clean_text(country_code).lower()) or re.search(r"\bindia\b", hay, re.I) or INDIA_CITY_REGEX.search(clean_text(location)):
        return "India-specific"
    if workplace == "Remote" and APAC_RE.search(hay):
        return "APAC"
    if workplace == "Remote" and WORLDWIDE_RE.search(hay):
        return "Worldwide remote"
    return ""


def v11_accepts_location(location="", country_code="", address_country="", description="", workplace=""):
    if workplace not in {"Remote", "Hybrid"}:
        return False, ""
    eligibility = v11_india_eligibility(location, country_code, address_country, description, workplace)
    return bool(eligibility and (eligibility == "India-specific" or V11_ALLOW_APAC_WORLDWIDE)), eligibility


def v11_allowed_india(location="", country_code="", address_country="", description="", workplace=""):
    ok, _eligibility = v11_accepts_location(location, country_code, address_country, description, workplace)
    return ok


# Hook V11 title/context/location logic over the stable helpers.
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
    for p in TARGET_TITLE_PATTERNS:
        m = re.search(p, t, re.I)
        if m:
            phrase = m.group(0).strip().lower()
            if phrase not in matches:
                matches.append(phrase)
            score += 60
            break
    for term in SUPPORT_CONTEXT_TERMS:
        if term in d:
            score += 2
            if term not in matches and len(matches) < 8:
                matches.append(term)
    if INDIA_REGEX.search(l) or INDIA_CITY_REGEX.search(l):
        score += 15
    if REMOTE_RE.search(l): score += 15
    if HYBRID_RE.search(l): score += 12
    return score, ", ".join(matches[:8])


def _v11_add_meta(job, direct=True, eligibility=None, board_key=""):
    j = dict(job)
    j["direct"] = bool(direct)
    j["eligibility"] = eligibility or j.get("eligibility", "")
    j["board_key"] = board_key or j.get("board_key", "")
    j["first_seen"] = j.get("first_seen") or datetime.now(timezone.utc).date().isoformat()
    j["last_seen"] = datetime.now(timezone.utc).date().isoformat()
    j["status"] = "Open"
    j["missed_runs"] = 0
    return j

# --------------------------
# HTTP HARDENING
# --------------------------
V11_HOST_LIMIT = int(os.environ.get("HOST_CONCURRENCY", "6"))
_HOST_SEMAPHORES = {}
_HOST_SEM_LOCK = None

try:
    import threading
    _HOST_SEM_LOCK = threading.Lock()
except Exception:
    pass

# Increase connection pool for 32-worker jobs.
try:
    _retry = Retry(total=0, connect=0, read=0, redirect=0)
    _adapter = HTTPAdapter(pool_connections=60, pool_maxsize=60, max_retries=_retry)
    SESSION.mount("http://", _adapter)
    SESSION.mount("https://", _adapter)
except Exception:
    pass


def _host_semaphore(url):
    host = urlparse(url).netloc.lower().split(":")[0]
    if _HOST_SEM_LOCK:
        with _HOST_SEM_LOCK:
            if host not in _HOST_SEMAPHORES:
                import threading
                _HOST_SEMAPHORES[host] = threading.BoundedSemaphore(V11_HOST_LIMIT)
            return _HOST_SEMAPHORES[host]
    return None


def get_json_status(url, params=None, method="GET", json_body=None, timeout=REQUEST_TIMEOUT):
    """Status-aware JSON request used by board discovery/liveness and Workday."""
    last_status = "error"
    for attempt in range(4):
        sem = _host_semaphore(url)
        try:
            if sem: sem.acquire()
            if method.upper() == "POST":
                r = SESSION.post(url, params=params, json=json_body, timeout=timeout)
            else:
                r = SESSION.get(url, params=params, timeout=timeout)
            status = r.status_code
            if status == 200:
                try:
                    return r.json(), "ok", status
                except ValueError:
                    return None, "bad_json", status
            if status == 404:
                return None, "not_found", status
            if status == 429:
                wait = min(12, float(r.headers.get("Retry-After", "2") or 2))
                last_status = "rate_limited"
                if attempt < 3:
                    time.sleep(wait)
                    continue
                return None, last_status, status
            if status in RETRY_CODES:
                last_status = "error"
                if attempt < 3:
                    time.sleep((2 ** attempt) + 0.25)
                    continue
                return None, last_status, status
            return None, "error", status
        except requests.RequestException:
            last_status = "error"
            if attempt < 3:
                time.sleep((2 ** attempt) + 0.25)
                continue
            return None, last_status, 0
        finally:
            try:
                if sem: sem.release()
            except Exception:
                pass
    return None, last_status, 0


def get_html(url, timeout=HTML_TIMEOUT):
    for attempt in range(3):
        sem = _host_semaphore(url)
        try:
            if sem: sem.acquire()
            r = SESSION.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r.text, r.url
            if r.status_code == 429 and attempt < 2:
                time.sleep(min(10, float(r.headers.get("Retry-After", "2") or 2)))
                continue
            if r.status_code in RETRY_CODES and attempt < 2:
                time.sleep(1.5 * (attempt + 1))
                continue
            return "", r.url
        except requests.RequestException:
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
            else:
                return "", url
        finally:
            try:
                if sem: sem.release()
            except Exception:
                pass
    return "", url


# --------------------------
# WORKDAY PUBLIC CXS API
# --------------------------
WD_RE = re.compile(r"https?://([^./?#]+)\.(wd\d+)\.myworkdayjobs\.com/([^?#]+)", re.I)
WD_KEYWORDS = [
    "customer support", "customer success", "technical support", "customer service",
    "service desk", "support engineer", "product support", "application support",
    "customer care", "client support", "technical account manager", "help desk",
]


def parse_workday_board_url(url):
    m = WD_RE.match(url or "")
    if not m:
        return None
    tenant, shard, path = m.group(1), m.group(2), m.group(3).strip("/")
    parts = [p for p in path.split("/") if p]
    locale = ""
    if parts and re.match(r"^[a-z]{2}(?:-[A-Z]{2})?$", parts[0]):
        locale = parts.pop(0)
    if not parts:
        return None
    site = parts[0]
    return {"tenant": tenant, "shard": shard, "site": site, "locale": locale,
            "url": url, "key": f"{tenant}|{shard}|{site}"}


def workday_job_url(board, external_path):
    p = parse_workday_board_url(board.get("url", "")) or board
    origin = f"https://{p['tenant']}.{p['shard']}.myworkdayjobs.com"
    prefix = "/".join(x for x in [p.get("locale", ""), p.get("site", "")] if x)
    path = str(external_path or "").lstrip("/")
    return f"{origin}/{prefix}/{path}" if prefix else f"{origin}/{path}"


def fetch_workday_board(board, company_hint=""):
    if isinstance(board, str):
        parts = board.split("|", 2)
        if len(parts) != 3: return []
        tenant, shard, site = parts
        info = {"tenant":tenant,"shard":shard,"site":site,"locale":"","url":f"https://{tenant}.{shard}.myworkdayjobs.com/{site}"}
    else:
        info = board
    api = f"https://{info['tenant']}.{info['shard']}.myworkdayjobs.com/wday/cxs/{info['tenant']}/{info['site']}/jobs"
    out, seen = [], set()
    for kw in WD_KEYWORDS:
        offset = 0
        for _ in range(V11_WORKDAY_MAX_PAGES_PER_KEYWORD):
            data, status, _code = get_json_status(api, method="POST", timeout=25, json_body={
                "appliedFacets": {}, "limit": 20, "offset": offset, "searchText": kw
            })
            if not isinstance(data, dict): break
            posts = data.get("jobPostings") or []
            if not posts: break
            for p in posts:
                path = clean_text(p.get("externalPath"))
                title = clean_text(p.get("title"))
                if not path or path in seen or not title_relevant(title):
                    continue
                seen.add(path)
                loc = clean_text(p.get("locationsText"))
                detail = {}
                if ("location" in loc.lower() and not INDIA_REGEX.search(loc)) or not loc:
                    detail_url = f"{api[:-5]}{path}"
                    d, ds, _ = get_json_status(detail_url, timeout=25)
                    if isinstance(d, dict):
                        detail = d.get("jobPostingInfo") or d
                        loc = clean_text(detail.get("location") or loc)
                desc = clean_text(detail.get("jobDescription") or p.get("jobDescription") or "")
                country = detail.get("country") if isinstance(detail.get("country"), dict) else {}
                country_text = clean_text(country.get("descriptor") if isinstance(country, dict) else country)
                workplace = clean_text(detail.get("remoteType"))
                workplace = "Remote" if workplace.lower() in {"remote","fully remote"} else ("Hybrid" if workplace.lower() == "hybrid" else workplace)
                if workplace not in {"Remote", "Hybrid"}:
                    workplace = classify_workplace(loc, structured_data=detail, description=desc)
                ok, elig = v11_accepts_location(loc, "", country_text, desc, workplace)
                if not ok: continue
                posted = parse_date(detail.get("startDate") or p.get("startDate"))
                job_url = workday_job_url(info, path)
                sc, matched = score_job(title, desc, loc)
                out.append(_v11_add_meta({
                    "title":title, "company":company_hint or humanize_slug(info["tenant"]), "location":loc or "India / Remote",
                    "workplace":workplace, "posted_date":posted, "job_url":job_url,
                    "source":"Workday", "score":sc, "matched":matched,
                    "eligibility":elig, "board_key":info.get("key", "")
                }))
                if len(out) >= V11_WORKDAY_MAX_RESULTS_PER_BOARD: return out
            offset += 20
    return out




def fetch_remotejobs_org():
    jobs=[]
    for offset in range(0,300,50):
        data,status,_=get_json_status("https://remotejobs.org/api/v1/jobs",params={"category":"customer-support","limit":50,"offset":offset},timeout=25)
        if not isinstance(data,dict): break
        items=data.get("data") or data.get("jobs") or []
        if not items: break
        for j in items:
            if not isinstance(j,dict): continue
            title=clean_text(j.get("title")); desc=clean_text(j.get("description"))
            company=clean_text((j.get("company") or {}).get("name") if isinstance(j.get("company"),dict) else j.get("company"))
            loc=clean_text(j.get("location")); posted=parse_date(j.get("posted_at") or j.get("created_at"))
            link=clean_text(j.get("url") or j.get("apply_url"))
            _append_public_job(jobs,title,company,loc,"Remote" if REMOTE_RE.search(loc) else "Hybrid" if HYBRID_RE.search(loc) else "",posted,link,"RemoteJobs.org",desc)
        if len(items)<50: break
    return jobs

# --------------------------
# OPTIONAL AGGREGATOR DISCOVERY SOURCES
# --------------------------
def fetch_adzuna():
    jobs=[]
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY): return jobs
    for page in range(1,5):
        url=f"https://api.adzuna.com/v1/api/jobs/in/search/{page}"
        data, status, _=get_json_status(url, params={"app_id":ADZUNA_APP_ID,"app_key":ADZUNA_APP_KEY,"results_per_page":50,"what":"customer support customer success technical support","where":"India","content-type":"application/json"}, timeout=25)
        if not isinstance(data,dict): break
        items=data.get("results") or []
        if not items: break
        for j in items:
            title=clean_text(j.get("title")); desc=clean_text(j.get("description")); loc=clean_text((j.get("location") or {}).get("display_name") if isinstance(j.get("location"),dict) else j.get("location")); link=clean_text(j.get("redirect_url") or j.get("url")); posted=parse_date(j.get("created"))
            _append_public_job(jobs,title,clean_text((j.get("company") or {}).get("display_name") if isinstance(j.get("company"),dict) else j.get("company")),loc,"Remote" if REMOTE_RE.search(loc) else "Hybrid" if HYBRID_RE.search(loc) else "",posted,link,"Adzuna",desc)
    return jobs


def fetch_jooble():
    jobs=[]
    if not JOOBLE_API_KEY: return jobs
    # Jooble endpoint format is key-based and may change; keep this optional and
    # fail-soft. It is a discovery signal, not a required dependency.
    url=f"https://jooble.org/api/{JOOBLE_API_KEY}"
    payload={"keywords":"customer support customer success technical support","location":"India","page":1}
    data,status,_=get_json_status(url,method="POST",json_body=payload,timeout=25)
    if isinstance(data,dict):
        for j in data.get("jobs") or []:
            title=clean_text(j.get("title")); desc=clean_text(j.get("snippet")); loc=clean_text(j.get("location")); posted=parse_date(j.get("updated")); link=clean_text(j.get("link"))
            _append_public_job(jobs,title,clean_text(j.get("company")),loc,"Remote" if REMOTE_RE.search(loc) else "Hybrid" if HYBRID_RE.search(loc) else "",posted,link,"Jooble",desc)
    return jobs


# --------------------------
# COMPANY / ATS DISCOVERY MANIFEST
# --------------------------
def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path, obj):
    _ensure_data_dir()
    tmp=path+".tmp"
    with open(tmp,"w",encoding="utf-8") as f:
        json.dump(obj,f,ensure_ascii=False,indent=2)
    os.replace(tmp,path)


def board_key_from_url(ats, url):
    ats=clean_text(ats)
    if ats == "Workday":
        x=parse_workday_board_url(url)
        return x["key"] if x else ""
    return extract_ats_slug(ats, url)


def canonical_ats_name(ats):
    s=clean_text(ats)
    # Normalize aliases from the discovery directories.
    aliases={
        "smart recruiter":"SmartRecruiters", "smartrecruiter":"SmartRecruiters",
        "smartrecruiters":"SmartRecruiters", "greenhouse":"Greenhouse", "lever":"Lever",
        "ashby":"Ashby", "workable":"Workable", "recruitee":"Recruitee", "breezy":"Breezy",
        "teamtailor":"Teamtailor", "bamboohr":"BambooHR", "personio":"Personio", "workday":"Workday",
        "zohorecruit":"Zoho Recruit", "zoho recruit":"Zoho Recruit", "darwinbox":"Darwinbox",
        "oracle":"Oracle Recruiting", "oracle recruiting":"Oracle Recruiting", "freshteam":"Freshteam",
        "pinpoint":"Pinpoint", "rippling":"Rippling", "keka":"Keka", "zwayam":"Zwayam",
        "peoplestrong":"PeopleStrong", "hrone":"HROne", "jobvite":"Jobvite", "jazzhr":"JazzHR",
        "comeet":"Comeet", "manatal":"Manatal", "paylocity":"Paylocity", "ukg":"UKG",
        "fountain":"Fountain", "phenom":"Phenom", "jobylon":"Jobylon", "talentlyft":"TalentLyft",
        "crelate":"Crelate", "ismartrecruit":"iSmartRecruit", "recruiterflow":"Recruiterflow",
        "inhire":"InHire", "successfactors":"SuccessFactors", "icims":"iCIMS", "taleo":"Taleo",
        "adp":"ADP", "join.com":"Join.com", "company career":"Company Career",
    }
    return aliases.get(s.lower(), s)


def discover_company_manifest():
    started=datetime.now(timezone.utc)
    print("DISCOVERY: loading large company universe")
    openjobs=load_openjobs_india_seeds()
    careers=load_awesome_career_pages()
    india_ats=load_india_ats_directory()
    # Refresh directory seeds; these pages are cheap compared with ATS crawling.
    try: wf_companies,_=extract_wellfound_seeds()
    except Exception: wf_companies=[]
    try: cs_companies,_=extract_cutshort_seeds()
    except Exception: cs_companies=[]
    existing=_load_json(COMPANIES_FILE,[])
    companies=merge_company_universe(openjobs, careers, wf_companies, cs_companies, existing)
    companies=companies[:V11_COMPANY_DISCOVERY_LIMIT]

    board_map=defaultdict(dict)
    career_map={}
    # The India-specific career-ops directory is an exact ATS map rather than a name guesser.
    for ats_raw, url in india_ats:
        ats=canonical_ats_name(ats_raw)
        key=board_key_from_url(ats,url)
        if not key:
            continue
        board_map[ats].setdefault(key,{"key":key,"url":url,"company":"","active":True,"last_ok":"","fail_count":0,"jobs_found_last":0,"source":"India ATS Directory"})

    for item in companies:
        company=clean_text(item.get("company"))
        for url in (item.get("ats_links") or []) + (item.get("career_links") or []):
            if not isinstance(url,str) or not url.startswith("http"): continue
            ats=canonical_ats_name(ats_from_url(url))
            if ats:
                key=board_key_from_url(ats,url)
                if key:
                    rec=board_map[ats].setdefault(key,{"key":key,"url":url,"company":company,"active":True,"last_ok":"","fail_count":0,"jobs_found_last":0,"source":item.get("source","Directory")})
                    if company and not rec.get("company"): rec["company"]=company
            else:
                u=normalize_url(url)
                if u and u not in career_map:
                    career_map[u]={"company":company,"url":u,"source":item.get("source","Directory"),"ats":"Company Career"}
    # Exact Workday URLs can also be embedded in any discovered ATS links or website fields.
    for item in companies:
        company=clean_text(item.get("company"))
        for url in list(item.get("ats_links") or []) + list(item.get("career_links") or []) + ([item.get("website")] if item.get("website") else []):
            if not isinstance(url,str): continue
            x=parse_workday_board_url(url)
            if x:
                board_map["Workday"].setdefault(x["key"],{"key":x["key"],"url":url,"company":company or humanize_slug(x["tenant"]),"active":True,"last_ok":"","fail_count":0,"jobs_found_last":0,"source":item.get("source","Directory")})

    # Add a large but controlled archive/Common Crawl discovery pass for all known public ATS hosts.
    print("DISCOVERY: archive/common-crawl ATS harvesting")
    try:
        archive_candidates=discover_ats_boards_from_archives()
    except Exception as exc:
        print(f"DISCOVERY archive ERROR: {exc}")
        archive_candidates={}
    for ats,slugs in archive_candidates.items():
        ats=canonical_ats_name(ats)
        if ats not in FETCHERS and ats != "Workday":
            continue
        for slug in sorted(slugs,key=str.lower)[:V11_DISCOVERY_CANDIDATE_LIMIT]:
            url=public_ats_board_url(ats,slug) if ats in FETCHERS else ""
            if ats=="Workday":
                # Archive extractor generally yields a tenant/site slug. If it is not a URL, skip rather than guess.
                continue
            if not url: continue
            board_map[ats].setdefault(slug,{"key":slug,"url":url,"company":humanize_slug(slug),"active":True,"last_ok":"","fail_count":0,"jobs_found_last":0,"source":"Wayback/Common Crawl"})

    # Save company universe separately so daily career crawling can rotate through it.
    _save_json(COMPANIES_FILE, companies)

    # Validate exact public boards. We validate much more than the old 250 cap but
    # never let validation failure kill the manifest.
    boards_out={}
    for ats,records in board_map.items():
        recs=list(records.values())
        # Prioritize exact directory links / known live routes, then archive candidates.
        recs.sort(key=lambda r: (0 if "OpenJobs" in r.get("source","") or "Career Directory" in r.get("source","") else 1, r.get("company","").lower()))
        candidates=recs[:V11_DISCOVERY_VALIDATE_PER_ATS]
        print(f"DISCOVERY VALIDATING {ats}: {len(candidates)} boards")
        valid=[]
        def one(rec):
            return validate_board_record(rec)
        with ThreadPoolExecutor(max_workers=V11_DISCOVERY_WORKERS) as ex:
            futs={ex.submit(one,r):r for r in candidates}
            for f in as_completed(futs):
                rec=futs[f]
                try:
                    ok,jcount,status=f.result()
                except Exception:
                    ok,jcount,status=False,0,"error"
                nr=dict(rec)
                now=datetime.now(timezone.utc).date().isoformat()
                if ok:
                    nr.update({"active":True,"last_ok":now,"fail_count":0,"jobs_found_last":jcount})
                    valid.append(nr)
                else:
                    # preserve existing previously-live boards and only retire after repeated failures
                    prev=rec.get("fail_count",0) or 0
                    nr["fail_count"]=prev+1
                    nr["active"]=prev+1 < V11_BOARD_MAX_FAILS
                    nr["last_ok"]=rec.get("last_ok","")
                    nr["jobs_found_last"]=0
                    if nr["active"]:
                        valid.append(nr)
        # Also preserve old manifest boards that weren't in this discovery slice.
        old=_load_json(BOARDS_FILE,{})
        for oldr in (old.get("boards",{}).get(ats,[]) if isinstance(old,dict) else []):
            if oldr.get("active") and oldr.get("key") not in {x.get("key") for x in valid}:
                valid.append(oldr)
        # dedupe
        d={r["key"]:r for r in valid if r.get("key")}
        boards_out[ats]=sorted(d.values(),key=lambda x:(not x.get("active",True),x.get("company","").lower(),x.get("key","")))

    manifest={
        "version":V11_VERSION,
        "generated_at":datetime.now(timezone.utc).isoformat(),
        "boards":boards_out,
        "careers":sorted(career_map.values(),key=lambda x:(x.get("company","").lower(),x.get("url",""))),
        "company_count":len(companies),
        "board_count":sum(len(v) for v in boards_out.values()),
    }
    _save_json(BOARDS_FILE,manifest)
    elapsed=(datetime.now(timezone.utc)-started).total_seconds()
    print(f"DISCOVERY COMPLETE: companies={len(companies)} boards={manifest['board_count']} careers={len(manifest['careers'])} runtime={int(elapsed)}s")
    return manifest


def validate_board_record(rec):
    ats=canonical_ats_name(rec.get("ats", ""))
    key=rec.get("key","")
    if ats=="Workday":
        b=rec.get("url") or key
        info=parse_workday_board_url(b) if isinstance(b,str) else None
        if not info:
            return False,0,"error"
        api=f"https://{info['tenant']}.{info['shard']}.myworkdayjobs.com/wday/cxs/{info['tenant']}/{info['site']}/jobs"
        data,status,code=get_json_status(api, method="POST", timeout=20, json_body={"appliedFacets":{},"limit":20,"offset":0,"searchText":""})
        return isinstance(data,dict) and isinstance(data.get("jobPostings"),list), len((data or {}).get("jobPostings") or []) if isinstance(data,dict) else 0, status
    url=rec.get("url") or public_ats_board_url(ats,key)
    if not url: return False,0,"error"
    # Probe using the public board/career URL rather than a filtered job fetch. Zero matching jobs
    # must not make an otherwise live board disappear.
    if ats in {"Greenhouse","Ashby","Lever","Workable","SmartRecruiters"}:
        if ats=="Greenhouse": api=f"https://boards-api.greenhouse.io/v1/boards/{quote(key,safe='')}/jobs"
        elif ats=="Ashby": api=f"https://api.ashbyhq.com/posting-api/job-board/{quote(key,safe='')}"
        elif ats=="Lever": api=f"https://api.lever.co/v0/postings/{quote(key,safe='')}"
        elif ats=="Workable": api=f"https://www.workable.com/api/accounts/{quote(key,safe='')}?details=false"
        else: api=f"https://api.smartrecruiters.com/v1/companies/{quote(key,safe='')}/postings"
        data,status,code=get_json_status(api,timeout=22)
        count=0
        if isinstance(data,dict): count=len(data.get("jobs") or data.get("content") or data.get("postings") or [])
        elif isinstance(data,list): count=len(data)
        return status=="ok",count,status
    html,final=get_html(url,timeout=20)
    return bool(html), 0, "ok" if html else "error"


def load_manifest():
    m=_load_json(BOARDS_FILE,{})
    if not isinstance(m,dict) or not m.get("boards"):
        return discover_company_manifest()
    return m


def rotate_records(records,budget,salt="v11"):
    records=list(records)
    if len(records)<=budget:return records
    day=datetime.now(timezone.utc).date().toordinal()
    keyed=[]
    for r in records:
        k=f"{r.get('company','')}|{r.get('url','')}|{salt}|{day//V11_ROTATION_DAYS}"
        h=hashlib.sha256(k.encode()).hexdigest()
        keyed.append((h,r))
    keyed.sort()
    return [r for _,r in keyed[:budget]]


# --------------------------
# DAILY ATS WORKER / CAREER WORKER
# --------------------------
def daily_crawl_nonstandard():
    manifest=load_manifest()
    rows=[]
    for ats,recs in (manifest.get("boards",{}) or {}).items():
        ats=canonical_ats_name(ats)
        if ats in FETCHERS or ats=="Workday":
            continue
        for r in recs or []:
            if r.get("active",True) and r.get("url"):
                rr=dict(r); rr["ats"]=ats; rows.append(rr)
    rows=rotate_records(rows,int(os.environ.get("NONSTANDARD_DAILY_BUDGET","700")),"nonstandard")
    print(f"NONSTANDARD ATS/CAREER ROUTES: {len(rows)}")
    out=[]
    links=[]
    def one(rec):
        company=clean_text(rec.get("company")); url=rec.get("url"); ats=rec.get("ats","Company Career")
        if not url:return [],[]
        return crawl_career_url_v11(company,url,source=f"{ats}->Direct",max_job_pages=4)
    with ThreadPoolExecutor(max_workers=V11_CRAWL_WORKERS) as ex:
        futs={ex.submit(one,r):r for r in rows}
        done=0
        for f in as_completed(futs):
            try:j,found=f.result()
            except Exception:j,found=[],[]
            for x in j:
                x["direct"]=True
                x["eligibility"]=x.get("eligibility") or v11_india_eligibility(x.get("location",""),"",x.get("location",""),"",x.get("workplace",""))
            out.extend(j); links.extend(found); done+=1
            if done%100==0: print(f"NONSTANDARD PROGRESS: {done}/{len(rows)}")
    return out,links


def fetch_board_record(rec):
    ats=canonical_ats_name(rec.get("ats",""))
    key=rec.get("key","")
    company=clean_text(rec.get("company"))
    try:
        if ats=="Workday":
            jobs=fetch_workday_board(rec, company_hint=company)
        elif ats in FETCHERS:
            jobs=FETCHERS[ats](key)
        else:
            return [], {"key":key,"ats":ats,"company":company,"ok":False,"status":"unsupported","jobs_found":0}
        for j in jobs:
            j["board_key"]=key
            if not j.get("company") or j.get("company") == humanize_slug(key): j["company"]=company or j.get("company")
            j["direct"]=True
            if not j.get("eligibility"):
                j["eligibility"]=v11_india_eligibility(j.get("location",""),"",j.get("location",""),j.get("description",""),j.get("workplace","")) or j.get("eligibility","")
        return jobs, {"key":key,"ats":ats,"company":company,"ok":True,"status":"ok","jobs_found":len(jobs)}
    except Exception as exc:
        return [], {"key":key,"ats":ats,"company":company,"ok":False,"status":"error","jobs_found":0,"error":f"{type(exc).__name__}: {exc}"}


def daily_crawl_ats(ats):
    manifest=load_manifest()
    records=[dict(r,ats=ats) for r in (manifest.get("boards",{}).get(ats,[]) or []) if r.get("active",True)]
    if V11_ATS_DAILY_CAP and len(records)>V11_ATS_DAILY_CAP:
        records=rotate_records(records,V11_ATS_DAILY_CAP,f"ats-{ats}")
    print(f"ATS {ats}: {len(records)} boards")
    out=[]; statuses=[]
    with ThreadPoolExecutor(max_workers=V11_CRAWL_WORKERS) as ex:
        futs={ex.submit(fetch_board_record,r):r for r in records}
        done=0
        for f in as_completed(futs):
            jobs,stat=f.result()
            out.extend(jobs); statuses.append(stat)
            done+=1
            if done%100==0: print(f"ATS {ats} PROGRESS: {done}/{len(records)}")
    return out,statuses


def daily_crawl_careers():
    manifest=load_manifest()
    records=rotate_records(manifest.get("careers",[]) or [],V11_CAREER_DAILY_BUDGET,"careers")
    print(f"CAREER PAGES: {len(records)}")
    out=[]; discovered=[]
    def one(rec):
        company=clean_text(rec.get("company")); url=rec.get("url")
        if not url:return [],[]
        return crawl_career_url_v10(company,url,source="Company Career",max_job_pages=V11_CAREER_PAGE_LIMIT)
    with ThreadPoolExecutor(max_workers=V11_CRAWL_WORKERS) as ex:
        futs={ex.submit(one,r):r for r in records}
        done=0
        for f in as_completed(futs):
            try:j,links=f.result()
            except Exception:j,links=[],[]
            for job in j:
                job["direct"]=True; job["eligibility"]=job.get("eligibility") or v11_india_eligibility(job.get("location",""),"",job.get("location",""),"",job.get("workplace",""))
            out.extend(j)
            for ats,url in links:
                discovered.append({"company":futs[f].get("company",""),"ats":canonical_ats_name(ats),"url":url,"source":"Company Career"})
            done+=1
            if done%100==0: print(f"CAREER PROGRESS: {done}/{len(records)}")
    return out,discovered


def feed_direct_only():
    feeds=[
        ("Himalayas",fetch_himalayas),
        ("Remotive",fetch_remotive),
        ("Arbeitnow",fetch_arbeitnow),
        ("Working Nomads",fetch_working_nomads),
        ("We Work Remotely",fetch_wwr_rss),
        ("Jobicy",fetch_jobicy),
        ("Remote OK",fetch_remoteok),
        ("Startup Jobs",fetch_startup_jobs),
        ("RemoteJobs.org",fetch_remotejobs_org),
        ("Adzuna",fetch_adzuna),
        ("Jooble",fetch_jooble),
    ]
    raw=[]; stats=[]
    for name,fn in feeds:
        if name in {"Adzuna","Jooble"} and ((name=="Adzuna" and not (ADZUNA_APP_ID and ADZUNA_APP_KEY)) or (name=="Jooble" and not JOOBLE_API_KEY)):
            continue
        try:
            j=fn(); stats.append({"source":name,"accepted_raw":len(j),"error":""}); raw.extend(j); print(f"FEED {name}: {len(j)} raw")
        except Exception as exc:
            stats.append({"source":name,"accepted_raw":0,"error":f"{type(exc).__name__}: {exc}"}); print(f"FEED {name}: ERROR {type(exc).__name__}: {exc}")
    # Existing feed-to-direct resolver is retained, but the candidate budget is much larger.
    direct=resolve_public_feed_jobs_to_direct(raw,budget=min(len(raw),V11_FEED_DIRECT_BUDGET))
    print(f"FEEDS DIRECT RESOLVED: {len(direct)}")
    return direct,stats


# --------------------------
# DIRECT-ONLY CLEANING / DEDUPE
# --------------------------
def normalize_identity(j):
    company=re.sub(r"[^a-z0-9]+","",clean_text(j.get("company","")).lower())
    title=re.sub(r"[^a-z0-9]+","",clean_text(j.get("title","")).lower())
    loc=re.sub(r"[^a-z0-9]+","",clean_text(j.get("location","")).lower())
    return company,title,loc


def merge_direct_jobs(jobs):
    blocked={"himalayas.app","startup.jobs","jobicy.com","remoteok.com","remotejobs.org","wellfound.com","cutshort.io","weworkremotely.com","remotive.com","arbeitnow.com","workingnomads.com"}
    unique={}
    for j in jobs:
        u=normalize_url(j.get("job_url","")); host=urlparse(u).netloc.lower()
        if not u or host in blocked or not title_relevant(j.get("title","")): continue
        workplace=j.get("workplace","")
        if workplace not in {"Remote","Hybrid"}: continue
        elig=j.get("eligibility") or v11_india_eligibility(j.get("location",""),"",j.get("location",""),j.get("description",""),workplace)
        if not elig or (elig != "India-specific" and not V11_ALLOW_APAC_WORLDWIDE): continue
        j=dict(j); j["job_url"]=u; j["eligibility"]=elig; j["direct"]=True
        ukey=("url",u)
        ident=normalize_identity(j)
        # Prefer ATS/company URLs over generic career URLs, and newer dates.
        rank=(1 if ats_from_url(u) else 0, j.get("score",0), j.get("posted_date","") or "")
        existing=unique.get(ukey)
        if not existing or rank>existing[0]: unique[ukey]=(rank,j)
    # Cross-source identity dedupe, but prefer direct ATS first.
    byid={}
    for _k,(_rank,j) in unique.items():
        ident=normalize_identity(j)
        old=byid.get(ident)
        rank=(1 if ats_from_url(j.get("job_url","")) else 0, j.get("score",0), j.get("posted_date","") or "")
        if not old or rank>old[0]: byid[ident]=(rank,j)
    return sorted([x[1] for x in byid.values()],key=lambda j:(j.get("score",0),j.get("posted_date","") or ""),reverse=True)


# --------------------------
# SHEETS V11
# --------------------------
V11_JOBS_HEADERS = [
    "Added","Job Title","Company","Location","Work Type","Posted","Job URL","Source","Score","Matched Keywords",
    "India Eligibility","Direct","First Seen","Last Seen","Status","Missed Runs",
]
V11_COMPANY_HEADERS = [
    "Company","Domain","Career/ATS URL","ATS","Last Checked","Jobs Found","Status","Last OK","Fail Count","Discovery Source",
]
STATS_HEADERS=["Run Date","Run ID","Mode","Source","Raw/Boards","Accepted","New Rows","Errors","Runtime Seconds","Notes"]


def ensure_headers_v11(ws, headers):
    current=ws.row_values(1)
    if not current:
        ws.update([headers],"A1")
        return
    # Preserve current columns; append missing fields to avoid breaking existing sheet consumers.
    missing=[h for h in headers if h not in current]
    if missing:
        new=current+missing
        end_col=chr(64+len(new)) if len(new)<=26 else None
        # Use cell-range helper for >26 columns.
        from gspread.utils import rowcol_to_a1
        ws.update([new],f"A1:{rowcol_to_a1(1,len(new))}")


def sheet_col_index(headers): return {h:i for i,h in enumerate(headers)}


def append_jobs_v11(ws,jobs):
    headers=ws.row_values(1)
    idx=sheet_col_index(headers)
    rows=ws.get_all_values()
    existing={}
    for rno,row in enumerate(rows[1:],start=2):
        if "Job URL" in idx and len(row)>idx["Job URL"]:
            u=normalize_url(row[idx["Job URL"]])
            if u: existing[u]=rno
    today=datetime.now(timezone.utc).date().isoformat()
    updates=[]; additions=[]; seen_urls=set()
    # First, increment missed runs for currently-open direct jobs. This is batched.
    if "Missed Runs" in idx and "Status" in idx:
        for rno,row in enumerate(rows[1:],start=2):
            status=clean_text(row[idx["Status"]]) if len(row)>idx["Status"] else ""
            if status.lower()=="open" and len(row)>idx.get("Job URL",0):
                try: miss=int(clean_text(row[idx["Missed Runs"]]) or "0")+1
                except: miss=1
                if miss>=3: status="Closed"
                updates.append({"range":f"{__import__('gspread').utils.rowcol_to_a1(rno,idx['Status']+1)}:{__import__('gspread').utils.rowcol_to_a1(rno,idx['Missed Runs']+1)}","values":[[status, str(miss)]]})
    for j in jobs:
        u=normalize_url(j.get("job_url",""));
        if not u or u in seen_urls: continue
        seen_urls.add(u)
        if u in existing:
            rno=existing[u]
            vals={
                "Last Seen":today,"Status":"Open","Missed Runs":"0",
            }
            if "India Eligibility" in idx: vals["India Eligibility"]=j.get("eligibility","")
            for h,v in vals.items():
                col=idx[h]+1
                updates.append({"range":f"{__import__('gspread').utils.rowcol_to_a1(rno,col)}","values":[[v]]})
            continue
        row=[""]*len(headers)
        mapping={"Added":today,"Job Title":j.get("title",""),"Company":j.get("company",""),"Location":j.get("location",""),"Work Type":j.get("workplace",""),"Posted":j.get("posted_date",""),"Job URL":u,"Source":j.get("source",""),"Score":j.get("score",0),"Matched Keywords":j.get("matched",""),"India Eligibility":j.get("eligibility",""),"Direct":"Yes","First Seen":today,"Last Seen":today,"Status":"Open","Missed Runs":"0"}
        for h,v in mapping.items():
            if h in idx: row[idx[h]]=v
        additions.append(row)
    from gspread.utils import rowcol_to_a1
    for i in range(0,len(updates),400):
        ws.batch_update(updates[i:i+400],value_input_option="RAW")
    for i in range(0,len(additions),1000):
        ws.append_rows(additions[i:i+1000],value_input_option="RAW")
    return len(additions)


def upsert_companies_v11(ws,records):
    headers=ws.row_values(1)
    idx=sheet_col_index(headers)
    rows=ws.get_all_values()
    existing={}
    for rno,row in enumerate(rows[1:],start=2):
        company=clean_text(row[idx["Company"]]).lower() if "Company" in idx and len(row)>idx["Company"] else ""
        url=normalize_url(row[idx["Career/ATS URL"]]) if "Career/ATS URL" in idx and len(row)>idx["Career/ATS URL"] else ""
        if company and url: existing[(company,url)]=rno
    today=datetime.now(timezone.utc).date().isoformat()
    updates=[]; additions=[]
    for rec in records:
        url=normalize_url(rec.get("url","") or rec.get("Career/ATS URL","")); company=clean_text(rec.get("company","")).strip()
        if not company or not url: continue
        vals={
            "Company":company,"Domain":rec.get("domain") or urlparse(url).netloc,"Career/ATS URL":url,"ATS":canonical_ats_name(rec.get("ats","Company Career")),
            "Last Checked":today,"Jobs Found":str(rec.get("jobs_found",0) or 0),"Status":rec.get("status","ACTIVE"),"Last OK":rec.get("last_ok",today if rec.get("ok") else ""),"Fail Count":str(rec.get("fail_count",0) or 0),"Discovery Source":rec.get("source",rec.get("discovery_source","")),
        }
        k=(company.lower(),url); rno=existing.get(k)
        if rno:
            row=[""]*len(headers)
            for h in headers:
                if h in vals: row[idx[h]]=vals[h]
            updates.append({"range":f"A{rno}:{__import__('gspread').utils.rowcol_to_a1(rno,len(headers))}","values":[row]})
        else:
            row=[""]*len(headers)
            for h,v in vals.items():
                if h in idx: row[idx[h]]=v
            additions.append(row)
    for i in range(0,len(updates),400): ws.batch_update(updates[i:i+400],value_input_option="RAW")
    for i in range(0,len(additions),1000): ws.append_rows(additions[i:i+1000],value_input_option="RAW")
    return len(additions)+len(updates)


def write_stats(ws,run_id,mode,stats,runtime,notes=""):
    headers=ws.row_values(1)
    if not headers: ws.update([STATS_HEADERS],"A1"); headers=STATS_HEADERS
    existing=len(ws.get_all_values())
    rows=[]
    today=datetime.now(timezone.utc).isoformat()
    for s in stats:
        rows.append([today,run_id,mode,s.get("source",""),s.get("raw_or_boards",s.get("accepted_raw",s.get("boards",0))),s.get("accepted",s.get("jobs_found",s.get("accepted_raw",0))),s.get("new_rows",0),s.get("errors",s.get("error","")),str(int(runtime)),s.get("notes",notes)])
    if rows: ws.append_rows(rows,value_input_option="RAW")


def get_spreadsheet_v11():
    return get_spreadsheet()


def build_registry_records(manifest, statuses, career_discovered):
    bykey={(canonical_ats_name(s.get("ats")),s.get("key")):s for s in statuses}
    records=[]
    for ats,rows in (manifest.get("boards",{}) or {}).items():
        for r in rows:
            if not r.get("active",True): continue
            s=bykey.get((canonical_ats_name(ats),r.get("key")),{})
            rec=dict(r)
            rec["ats"]=canonical_ats_name(ats); rec["company"]=r.get("company") or humanize_slug(r.get("key","")); rec["url"]=r.get("url") or public_ats_board_url(rec["ats"],r.get("key","")); rec["jobs_found"]=s.get("jobs_found",r.get("jobs_found_last",0)); rec["ok"]=s.get("ok",False); rec["last_ok"]=datetime.now(timezone.utc).date().isoformat() if s.get("ok") else r.get("last_ok",""); rec["fail_count"]=0 if s.get("ok") else int(r.get("fail_count",0))+1; rec["status"]="ACTIVE" if rec.get("active",True) and rec.get("fail_count",0)<V11_BOARD_MAX_FAILS else "INACTIVE"; rec["source"]=r.get("source","")
            records.append(rec)
    for r in career_discovered:
        if isinstance(r, dict):
            records.append({"company":r.get("company",""),"url":r.get("url","") or r.get("ats_url",""),"ats":r.get("ats","Company Career"),"jobs_found":0,"status":"ACTIVE","source":r.get("source","Company Career")})
        elif isinstance(r, (list, tuple)) and len(r) >= 2:
            records.append({"company":"","url":r[1],"ats":r[0],"jobs_found":0,"status":"ACTIVE","source":"Company Career"})
    return records


def write_board_status_artifact(ats,statuses,path):
    _ensure_data_dir();
    with open(path,"w",encoding="utf-8") as f: json.dump({"ats":ats,"statuses":statuses,"generated_at":datetime.now(timezone.utc).isoformat()},f,ensure_ascii=False,indent=2)


# --------------------------
# CLI MODES
# --------------------------
def run_discover():
    return discover_company_manifest()


def run_ats_cli(ats,out_path):
    if ats == "Nonstandard":
        jobs,links=daily_crawl_nonstandard()
        statuses=[{"ats":"Nonstandard","ok":True,"jobs_found":len(jobs),"key":"nonstandard","company":"routes"}]
    else:
        jobs,statuses=daily_crawl_ats(ats)
        links=[]
    jobs=merge_direct_jobs(jobs)
    os.makedirs(os.path.dirname(out_path),exist_ok=True)
    with open(out_path,"w",encoding="utf-8") as f:
        for j in jobs: f.write(json.dumps(j,ensure_ascii=False)+"\n")
    status_path=os.path.splitext(out_path)[0]+".status.json"
    write_board_status_artifact(ats,statuses,status_path)
    if links:
        with open(os.path.splitext(out_path)[0]+".links.json","w",encoding="utf-8") as f: json.dump(links,f,ensure_ascii=False,indent=2)
    print(f"ATS {ats}: DIRECT JOBS {len(jobs)}")


def run_careers_cli(out_path):
    jobs,disc=daily_crawl_careers(); jobs=merge_direct_jobs(jobs)
    os.makedirs(os.path.dirname(out_path),exist_ok=True)
    with open(out_path,"w",encoding="utf-8") as f:
        for j in jobs:f.write(json.dumps(j,ensure_ascii=False)+"\n")
    with open(os.path.splitext(out_path)[0]+".links.json","w",encoding="utf-8") as f:json.dump(disc,f,ensure_ascii=False,indent=2)
    print(f"CAREERS: DIRECT JOBS {len(jobs)} exact routes discovered {len(disc)}")


def run_feeds_cli(out_path):
    jobs,stats=feed_direct_only(); jobs=merge_direct_jobs(jobs)
    os.makedirs(os.path.dirname(out_path),exist_ok=True)
    with open(out_path,"w",encoding="utf-8") as f:
        for j in jobs:f.write(json.dumps(j,ensure_ascii=False)+"\n")
    with open(os.path.splitext(out_path)[0]+".stats.json","w",encoding="utf-8") as f:json.dump(stats,f,ensure_ascii=False,indent=2)
    print(f"FEEDS: DIRECT JOBS {len(jobs)}")


def run_merge(out_dir,write_sheet=False):
    started=datetime.now(timezone.utc); run_id=started.strftime("%Y%m%dT%H%M%SZ")
    jobs=[]
    for p in sorted(Path(out_dir).glob("*.jsonl")):
        with open(p,"r",encoding="utf-8") as f:
            for line in f:
                try: jobs.append(json.loads(line))
                except Exception: pass
    final=merge_direct_jobs(jobs)
    print(f"MERGE: raw direct rows={len(jobs)} final unique={len(final)}")
    if not write_sheet:
        path=Path(out_dir)/"merged.jsonl"
        with path.open("w",encoding="utf-8") as f:
            for j in final:f.write(json.dumps(j,ensure_ascii=False)+"\n")
        return
    spreadsheet=get_spreadsheet_v11()
    jobs_ws=get_ws(spreadsheet,"JOBS",rows=10000,cols=20)
    companies_ws=get_ws(spreadsheet,"COMPANIES",rows=30000,cols=12)
    stats_ws=get_ws(spreadsheet,"STATS",rows=5000,cols=len(STATS_HEADERS))
    ensure_headers_v11(jobs_ws,V11_JOBS_HEADERS)
    ensure_headers_v11(companies_ws,V11_COMPANY_HEADERS)
    ensure_headers_v11(stats_ws,STATS_HEADERS)
    new_rows=append_jobs_v11(jobs_ws,final)
    # Merge board statuses from all artifacts.
    statuses=[]; career_links=[]
    for p in sorted(Path(out_dir).glob("*.status.json")):
        try: statuses.extend(_load_json(str(p),{}).get("statuses",[]))
        except Exception: pass
    for p in sorted(Path(out_dir).glob("*.links.json")):
        try: career_links.extend(_load_json(str(p),[]))
        except Exception: pass
    manifest=load_manifest()
    registry=build_registry_records(manifest,statuses,career_links)
    registry_count=upsert_companies_v11(companies_ws,registry)
    src_counts=Counter(j.get("source","") for j in final)
    stat_rows=[]
    for src,cnt in sorted(src_counts.items()): stat_rows.append({"source":src,"raw_or_boards":cnt,"accepted":cnt,"new_rows":0})
    by_ats=defaultdict(lambda:{"boards":0,"errors":0,"jobs":0})
    for st in statuses:
        a=canonical_ats_name(st.get("ats","")); by_ats[a]["boards"]+=1; by_ats[a]["jobs"]+=int(st.get("jobs_found",0) or 0)
        if not st.get("ok",False): by_ats[a]["errors"]+=1
    for a,m in sorted(by_ats.items()): stat_rows.append({"source":f"ATS::{a}","raw_or_boards":m["boards"],"accepted":m["jobs"],"errors":str(m["errors"])})
    stat_rows.append({"source":"TOTAL","raw_or_boards":len(jobs),"accepted":len(final),"new_rows":new_rows,"notes":f"registry updates={registry_count}"})
    runtime=int((datetime.now(timezone.utc)-started).total_seconds())
    write_stats(stats_ws,run_id,"daily",stat_rows,runtime)
    print(f"FINAL UNIQUE DIRECT JOBS: {len(final)}")
    print(f"NEW ROWS WRITTEN: {new_rows}")
    print(f"COMPANY REGISTRY UPSERTS: {registry_count}")
    print(f"RUNTIME SECONDS: {runtime}")


def main_v11():
    parser=argparse.ArgumentParser(description="Remote4.me V11 India-first high coverage crawler")
    parser.add_argument("--discover",action="store_true")
    parser.add_argument("--ats",choices=["Greenhouse","Lever","Ashby","Workable","SmartRecruiters","Recruitee","Breezy","Teamtailor","BambooHR","Personio","Workday","Nonstandard"],default="")
    parser.add_argument("--careers",action="store_true")
    parser.add_argument("--feeds",action="store_true")
    parser.add_argument("--out",default="out/jobs.jsonl")
    parser.add_argument("--merge",default="")
    parser.add_argument("--write-sheet",action="store_true")
    args=parser.parse_args()
    print("================================================")
    print("REMOTE4.ME JOB CRAWLER V11 — INDIA FIRST / HIGH COVERAGE")
    print("================================================")
    print(f"Started UTC: {datetime.now(timezone.utc).isoformat()}")
    print("Public feeds + exact ATS + company career pages + Common Crawl/Wayback")
    print(f"Startup Jobs API: {'configured' if STARTUP_JOBS_API_KEY else 'NOT configured'}")
    print(f"Job window: {V11_JOB_MAX_AGE_DAYS} days / open-posting lifecycle")
    print(f"APAC/Worldwide remote tier: {'enabled' if V11_ALLOW_APAC_WORLDWIDE else 'disabled'}")
    if args.discover: return run_discover()
    if args.ats: return run_ats_cli(args.ats,args.out)
    if args.careers: return run_careers_cli(args.out)
    if args.feeds: return run_feeds_cli(args.out)
    if args.merge: return run_merge(args.merge,args.write_sheet)
    # Backward-compatible default: feeds only direct-resolution + all ATS/careers sequentially.
    # GitHub Actions V11 should use the sharded workflows instead.
    tmp="out"
    os.makedirs(tmp,exist_ok=True)
    run_feeds_cli(os.path.join(tmp,"feeds.jsonl"))
    for ats in ["Greenhouse","Lever","Ashby","Workable","SmartRecruiters","Recruitee","Breezy","Teamtailor","BambooHR","Personio","Workday","Nonstandard"]:
        run_ats_cli(ats,os.path.join(tmp,f"{ats.lower()}.jsonl"))
    run_careers_cli(os.path.join(tmp,"careers.jsonl"))
    run_merge(tmp,True)


# Compatibility aliases for the renamed expansion helpers.
try:
    crawl_career_url_v10 = crawl_career_url_v11
except NameError:
    pass
try:
    v10_allowed_india = v11_allowed_india
except NameError:
    pass

if __name__ == "__main__":
    main_v11()
