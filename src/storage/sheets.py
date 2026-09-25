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
        "Company", "Domain", "Career URL", "ATS", "India Signal", "Source", "Last Seen"
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
        sh = ws.add_worksheet(title=name, rows=2000, cols=max(12, len(headers)))
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


def write(result):
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("Missing GOOGLE_SHEET_ID")
    gc = client()
    book = gc.open_by_key(sheet_id)
    for name, headers in HEADERS.items():
        ensure(book, name, headers)

    append_unique(
        book.worksheet("VERIFIED_JOBS"),
        result["verified"],
        lambda r: (r[6] if len(r) > 6 else "").strip().lower(),
    )
    append_unique(
        book.worksheet("DISCOVERED_JOBS"),
        result["discovered"],
        lambda r: (r[5] if len(r) > 5 else "").strip().lower(),
    )
    append_unique(
        book.worksheet("NEEDS_REVIEW"),
        result["review"],
        lambda r: (r[4] if len(r) > 4 else "").strip().lower(),
    )
    append_unique(
        book.worksheet("COMPANIES"),
        result["companies"],
        lambda r: "|".join(str(x).strip().lower() for x in r[:3]),
    )
    book.worksheet("RUN_LOG").append_row(result["log"], value_input_option="USER_ENTERED")
