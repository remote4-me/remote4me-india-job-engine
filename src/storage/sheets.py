import os
import json
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = {
    "VERIFIED_JOBS": [
        "Added", "Job Title", "Company", "Location", "Work Style",
        "Posted", "Official Job URL", "Found Via", "Relevance Hits"
    ],
    "DISCOVERED_JOBS": [
        "Added", "Job Title", "Company", "Location", "Posted",
        "Discovery URL", "Official URL", "Found Via", "Status"
    ],
    "NEEDS_REVIEW": [
        "Added", "Job Title", "Company", "Location", "Discovery URL",
        "Reason", "Found Via"
    ],
    "COMPANIES": [
        "Company", "Domain", "Career URL", "ATS", "ATS Board URL", "India Signal", "Source", "Last Seen"
    ],
    "RUN_LOG": [
        "Run Time", "Discovered", "India Matches", "Support Matches",
        "Verified", "Review", "Errors"
    ],
}


def client():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    info = json.loads(raw)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure(ws, name, headers):
    try:
        sh = ws.worksheet(name)
    except gspread.WorksheetNotFound:
        sh = ws.add_worksheet(title=name, rows=5000, cols=max(14, len(headers)))
    if sh.row_values(1) != headers:
        sh.update("A1", [headers])
    return sh


def append_unique(sh, rows, key_fn):
    if not rows:
        return
    vals = sh.get_all_values()
    existing = set()
    for row in vals[1:]:
        if row:
            existing.add(key_fn(row))
    fresh = []
    seen = set(existing)
    for row in rows:
        key = key_fn(row)
        if key and key not in seen:
            fresh.append(row)
            seen.add(key)
    if fresh:
        sh.append_rows(fresh, value_input_option="USER_ENTERED")


def _company_key(row):
    # ATS board is the strongest identity. Fall back to domain + company.
    board = str(row[4] if len(row) > 4 else "").strip().lower().rstrip("/")
    if board:
        return "board:" + board
    domain = str(row[1] if len(row) > 1 else "").strip().lower()
    company = str(row[0] if row else "").strip().lower()
    return "company:" + domain + "|" + company


def read_companies(limit=250):
    """Read the persistent company queue, prioritising never/least-recently checked sources."""
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    if not sheet_id or not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip():
        return []
    try:
        book = client().open_by_key(sheet_id)
        sh = book.worksheet("COMPANIES")
        rows = sh.get_all_records()
    except Exception as e:
        print("Company sheet read error:", e)
        return []
    out = []
    seen = set()
    for r in rows:
        company = str(r.get("Company", "")).strip()
        domain_url = str(r.get("Domain", "")).strip()
        career = str(r.get("Career URL", "")).strip()
        ats = str(r.get("ATS", "")).strip().lower()
        board = str(r.get("ATS Board URL", "")).strip()
        last_seen = str(r.get("Last Seen", "")).strip()
        key = _company_key([company, domain_url, career, ats, board])
        if key in seen or not (company or domain_url or career or board):
            continue
        board_low = board.lower()
        career_low = career.lower()
        bad_path = any(x in career_low for x in ("/privacy", "/policy", "/policies", "/terms", "/about", "/addons", "/blog", "/help", "/contact", "/legal"))
        ats_host = any(x in board_low for x in ("greenhouse", "lever.co", "ashbyhq", "workable", "workday", "smartrecruiters", "icims", "successfactors", "oraclecloud", "jobvite", "teamtailor", "bamboohr", "recruitee", "jazz.co", "breezy", "pinpoint", "personio"))
        career_hint = any(x in career_low for x in ("/career", "/careers", "/jobs", "/join-us", "/joinus", "/work-with-us", "/opportunities", "/job-openings", "/open-positions", "/vacancies", "/apply"))
        if bad_path or not (ats_host or career_hint or domain_url):
            continue
        seen.add(key)
        out.append({"company": company, "domain": domain_url, "career_url": career, "ats": ats, "board": board, "last_seen": last_seen})
    # Empty Last Seen first; then oldest timestamp. This prevents the first N
    # companies from being crawled forever while newer companies starve.
    out.sort(key=lambda x: (bool(x.get("last_seen")), x.get("last_seen") or ""))
    return out[:limit]


def _upsert_companies(sh, rows):
    if not rows:
        return
    values = sh.get_all_values()
    existing = {}
    for idx, row in enumerate(values[1:], start=2):
        if row:
            existing[_company_key(row)] = (idx, row)
    to_append = []
    for row in rows:
        key = _company_key(row)
        if not key or key == "company:|":
            continue
        if key in existing:
            idx, old = existing[key]
            merged = list(old) + [""] * max(0, 8 - len(old))
            # New non-empty values replace stale values; Last Seen is refreshed.
            for i in range(min(8, len(row))):
                if str(row[i]).strip():
                    merged[i] = row[i]
            sh.update(f"A{idx}:H{idx}", [merged[:8]], value_input_option="USER_ENTERED")
        else:
            to_append.append(row[:8])
    if to_append:
        sh.append_rows(to_append, value_input_option="USER_ENTERED")


def write(result):
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("Missing GOOGLE_SHEET_ID")
    gc = client()
    book = gc.open_by_key(sheet_id)
    for name, headers in HEADERS.items():
        ensure(book, name, headers)

    append_unique(book.worksheet("VERIFIED_JOBS"), result["verified"], lambda r: (r[6] if len(r) > 6 else "").strip().lower())
    append_unique(book.worksheet("DISCOVERED_JOBS"), result["discovered"], lambda r: (r[5] if len(r) > 5 else "").strip().lower())
    append_unique(book.worksheet("NEEDS_REVIEW"), result["review"], lambda r: (r[4] if len(r) > 4 else "").strip().lower())
    _upsert_companies(book.worksheet("COMPANIES"), result["companies"])
    book.worksheet("RUN_LOG").append_row(result["log"], value_input_option="USER_ENTERED")
