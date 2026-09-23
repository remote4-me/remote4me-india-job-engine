import os, json, gspread
from google.oauth2.service_account import Credentials

SCOPES=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

HEADERS={
"VERIFIED_JOBS":["Added","Job Title","Company","Location","Work Style","Posted","Official Job URL","Found Via","Relevance Hits"],
"DISCOVERED_JOBS":["Added","Job Title","Company","Location","Posted","Discovery URL","Official URL","Found Via","Status"],
"NEEDS_REVIEW":["Added","Job Title","Company","Location","Discovery URL","Reason","Found Via"],
"COMPANIES":["Company","Domain","Career URL","ATS","India Signal","Source","Last Seen"],
"RUN_LOG":["Run Time","Discovered","India Matches","Support Matches","Verified","Review","Errors"]
}

def client():
    raw=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    if not raw: raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    info=json.loads(raw)
    creds=Credentials.from_service_account_info(info,scopes=SCOPES)
    return gspread.authorize(creds)

def ensure(ws,name,headers):
    try: sh=ws.worksheet(name)
    except gspread.WorksheetNotFound:
        sh=ws.add_worksheet(title=name,rows=1000,cols=max(12,len(headers)))
    if sh.row_values(1) != headers:
        sh.update("A1",[headers])
    return sh

def append_unique(sh, rows, key_index=0):
    if not rows: return
    existing=set()
    vals=sh.get_all_values()
    for row in vals[1:]:
        if len(row)>key_index and row[key_index]: existing.add(row[key_index])
    fresh=[r for r in rows if len(r)>key_index and r[key_index] not in existing]
    if fresh: sh.append_rows(fresh,value_input_option="USER_ENTERED")

def write(result):
    sheet_id=os.getenv("GOOGLE_SHEET_ID","").strip()
    if not sheet_id: raise RuntimeError("Missing GOOGLE_SHEET_ID")
    gc=client()
    book=gc.open_by_key(sheet_id)
    for name,h in HEADERS.items(): ensure(book,name,h)
    append_unique(book.worksheet("VERIFIED_JOBS"),result["verified"],0)
    append_unique(book.worksheet("DISCOVERED_JOBS"),result["discovered"],0)
    append_unique(book.worksheet("NEEDS_REVIEW"),result["review"],0)
    append_unique(book.worksheet("COMPANIES"),result["companies"],0)
    book.worksheet("RUN_LOG").append_row(result["log"],value_input_option="USER_ENTERED")
