#!/usr/bin/env python3
"""
daily_stats_full.py

Automates Daily Stats updates for all company sheets in the provided Google Spreadsheet.

- Reads companies list from local mongo_creds.companies
- For each sheet (company tab) it:
    * finds yesterday's row (e.g. "December 2") or appends it
    * selects the correct target DB (directclients_prod or prod_jobiak_ai) by reading
      mongo_uri from local mongo_creds.creds collection
    * runs queries based on domainType
    * computes Active Jobs, Postings/Updates/Repost, Posted Bot %, Feed Expires/Manual Expires,
      Expires Bot %, Views
    * updates columns C-H for the yesterday row

Debug prints are included inside functions to show executed query shapes and collection names.
"""

import re
import time
import itertools
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Any, Dict

import gspread
from pymongo import MongoClient
from pymongo.collection import Collection

# ------------- CONFIG -------------
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1cTpy8i9FN9cs62UOCTrMUOgSplYZKQ0J5ijuffwiPcI"
LOCAL_MONGO_URI = "mongodb://localhost:27017/"
LOCAL_DB_NAME = "mongo_creds"
COMPANIES_COLLECTION = "companies"
CREDS_COLLECTION = "creds"   # contains documents: { "domain": "...", "mongo_uri": "mongodb://..." }

# gspread credentials file (service account)
GSPREAD_CREDENTIALS_FILE = "credentials.json"

# When building $in lists, chunk size (avoid huge in-lists)
IN_CHUNK_SIZE = 800

# Regex constants (raw strings)
IP_66_RE = re.compile(r".*66\.249.*", re.IGNORECASE)
UA_GOOGLE_RE = re.compile(r".*google.*", re.IGNORECASE)

# ------------- HELPERS -------------
def format_sheet_date_no_year(dt: datetime) -> str:
    """Produce 'December 2' (no leading zero). dt should be UTC date."""
    return f"{dt.strftime('%B')} {dt.day}"

def utc_day_bounds_for_yesterday() -> Tuple[datetime, datetime]:
    """Return (start_utc, end_utc):
       start_utc = yesterday at 00:00:00 UTC
       end_utc = today at 00:00:00 UTC
    """
    now = datetime.now(timezone.utc)
    today_utc = datetime(year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc)
    yesterday_utc = today_utc - timedelta(days=1)
    return yesterday_utc, today_utc

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk

# ------------- DB selection helper -------------
def get_target_db_from_local_creds(local_client: MongoClient, domain_type: str):
    """
    Return a (MongoClient, Database) pair for the given domain_type.
    domain_type: 'proxy' or 'subdomain'
    It reads mongo_uri from local_db.creds collection where 'domain' matches.
    """
    creds_db = local_client[LOCAL_DB_NAME]
    creds_coll = creds_db[CREDS_COLLECTION]

    if domain_type == "proxy":
        domain_key = "prod_jobiak_ai"
    else:
        domain_key = "directclients_prod"

    doc = creds_coll.find_one({"domain": domain_key}, {"mongo_uri": 1})
    if not doc or "mongo_uri" not in doc:
        raise RuntimeError(f"No mongo_uri found in {LOCAL_DB_NAME}.{CREDS_COLLECTION} for domain '{domain_key}'")

    mongo_uri = doc["mongo_uri"]
    tgt_client = MongoClient(mongo_uri)

    # Use DB named exactly domain_key (per your instruction)
    target_db = tgt_client[domain_key]
    return tgt_client, target_db

# ------------- QUERY WORKFLOWS (with debug prints) -------------
def compute_active_jobs(target_db, domain_type: str, employer_id: str, start: datetime, end: datetime) -> int:
    """
    Returns integer active_count.
    Proxy: count in collection 'job' documents matching:
        { employerId: employer_id, isActive: True, status: 'posted' }
    Subdomain: count in 'Target_P4_Opt' matching:
        { employerId: employer_id, gpost: 5, job_status: { $ne: 3 } }
    """
    if domain_type == "proxy":
        coll: Collection = target_db["job"]
        q = {"employerId": employer_id, "isActive": True, "status": "posted"}
    else:
        coll: Collection = target_db["Target_P4_Opt"]
        q = {"employerId": employer_id, "gpost": 5, "job_status": {"$ne": 3}}

    # Debug print
    print(f"    [ActiveJobs] DB: {target_db.name}  Coll: {coll.name}  Query: {q}")
    cnt = coll.count_documents(q)
    return cnt

def compute_postings_updates(target_db, domain_type: str, employer_id: str, start: datetime, end: datetime) -> Tuple[int, List[Any]]:
    """
    Returns tuple (total_postings_count, id_list)
    Proxy: collection 'job', filter by employerId & isActive & status 'posted' & datePosted between start/end.
           return jobId values for later bot% query.
    Subdomain: collection 'Target_P4_Opt', filter gpost==5, job_status !=3, gpost_date between start/end.
           return jobPubJobId values for later bot% query.
    """
    if domain_type == "proxy":
        coll: Collection = target_db["job"]
        q = {
            "employerId": employer_id,
            "isActive": True,
            "status": "posted",
            "datePosted": {"$gte": start, "$lt": end}
        }
        projection = {"jobId": 1}
    else:
        coll: Collection = target_db["Target_P4_Opt"]
        q = {
            "employerId": employer_id,
            "gpost": 5,
            "job_status": {"$ne": 3},
            "gpost_date": {"$gte": start, "$lt": end}
        }
        projection = {"jobPubJobId": 1}

    print(f"    [Postings] DB: {target_db.name}  Coll: {coll.name}  Query: {q}")
    cursor = coll.find(q, projection)
    ids = []
    if domain_type == "proxy":
        ids = [doc.get("jobId") for doc in cursor if doc.get("jobId") is not None]
    else:
        ids = [doc.get("jobPubJobId") for doc in cursor if doc.get("jobPubJobId") is not None]

    return len(ids), ids

def compute_posted_bot_percent(target_db, domain_type: str, employer_id: str, start: datetime, end: datetime, posting_ids: List[Any]) -> float:
    """
    Compute posted bot %:
    Proxy query on userAnalytics:
      { employerId: employer_id, childJobId: { $in: posting_ids }, isBot: true, country: "United States",
        createdDt: {$gte: start, $lt: end}, ipAddress: /.*66\.249.*/i }
      compute unique childJobId found -> unique_bot_count
      percent = unique_bot_count / total_postings_count * 100

    Subdomain query on userAnalytics:
      { jobPubJobId: {$in: posting_ids}, isBot: true, createdDt range, userAgent: /.*google.*/i }
      compute unique jobPubJobId
    """
    if not posting_ids:
        print("    [PostedBot%] No posting_ids -> 0%")
        return 0.0

    ua_coll: Collection = target_db["userAnalytics"]
    total_postings = len(posting_ids)
    unique_bot_ids = set()

    if domain_type == "proxy":
        base_filter = {
            "employerId": employer_id,
            "isBot": True,
            "country": "United States",
            "createdDt": {"$gte": start, "$lt": end},
        }
        # process in chunks
        for chunk in chunked(posting_ids, IN_CHUNK_SIZE):
            q = dict(base_filter)
            q["childJobId"] = {"$in": chunk}
            q["ipAddress"] = IP_66_RE
            print(f"    [PostedBot%][proxy] Query chunk -> Coll: {ua_coll.name} Query: truncated childJobId in {len(chunk)} items, other filters present")
            ids = ua_coll.distinct("childJobId", q)
            unique_bot_ids.update([i for i in ids if i is not None])
    else:
        base_filter = {
            "isBot": True,
            "createdDt": {"$gte": start, "$lt": end},
        }
        for chunk in chunked(posting_ids, IN_CHUNK_SIZE):
            q = dict(base_filter)
            q["jobPubJobId"] = {"$in": chunk}
            q["userAgent"] = UA_GOOGLE_RE
            print(f"    [PostedBot%][subdomain] Query chunk -> Coll: {ua_coll.name} Query: truncated jobPubJobId in {len(chunk)} items")
            ids = ua_coll.distinct("jobPubJobId", q)
            unique_bot_ids.update([i for i in ids if i is not None])

    unique_bot_count = len(unique_bot_ids)
    percent = (unique_bot_count / total_postings * 100) if total_postings else 0.0
    print(f"    [PostedBot%] unique_bot_count={unique_bot_count} total_postings={total_postings} percent={round(percent,0)}")
    return round(percent, 0)

def compute_feed_expires(target_db, domain_type: str, employer_id: str, start: datetime, end: datetime) -> Tuple[int, List[Any]]:
    """
    Proxy: job collection where isActive==False, status 'removed', updatedAt between start and end -> jobId
    Subdomain: Target_P4_Opt where gpost==6 and gpost_expire_date between start/end -> jobPubJobId
    """
    if domain_type == "proxy":
        coll: Collection = target_db["job"]
        q = {
            "employerId": employer_id,
            "isActive": False,
            "status": "removed",
            "updatedAt": {"$gte": start, "$lt": end}
        }
        projection = {"jobId": 1}
    else:
        coll: Collection = target_db["Target_P4_Opt"]
        q = {
            "employerId": employer_id,
            "gpost": 6,
            "gpost_expire_date": {"$gte": start, "$lt": end}
        }
        projection = {"jobPubJobId": 1}

    print(f"    [FeedExpires] DB: {target_db.name}  Coll: {coll.name}  Query: {q}")
    cursor = coll.find(q, projection)
    if domain_type == "proxy":
        ids = [doc.get("jobId") for doc in cursor if doc.get("jobId") is not None]
    else:
        ids = [doc.get("jobPubJobId") for doc in cursor if doc.get("jobPubJobId") is not None]
    return len(ids), ids

def compute_expires_bot_percent(target_db, domain_type: str, employer_id: str, start: datetime, end: datetime, expire_ids: List[Any]) -> float:
    """
    Similar to posted bot percent, but based on expire_ids and counts from userAnalytics.
    """
    if not expire_ids:
        print("    [ExpiresBot%] No expire_ids -> 0%")
        return 0.0

    ua_coll: Collection = target_db["userAnalytics"]
    unique_bot_ids = set()

    if domain_type == "proxy":
        base_filter = {
            "employerId": employer_id,
            "isBot": True,
            "country": "United States",
            "createdDt": {"$gte": start, "$lt": end},
        }
        for chunk in chunked(expire_ids, IN_CHUNK_SIZE):
            q = dict(base_filter)
            q["childJobId"] = {"$in": chunk}
            q["ipAddress"] = IP_66_RE
            print(f"    [ExpiresBot%][proxy] Query chunk -> Coll: {ua_coll.name} childJobId in {len(chunk)}")
            ids = ua_coll.distinct("childJobId", q)
            unique_bot_ids.update([i for i in ids if i is not None])
    else:
        base_filter = {"isBot": True, "createdDt": {"$gte": start, "$lt": end}}
        for chunk in chunked(expire_ids, IN_CHUNK_SIZE):
            q = dict(base_filter)
            q["jobPubJobId"] = {"$in": chunk}
            q["userAgent"] = UA_GOOGLE_RE
            print(f"    [ExpiresBot%][subdomain] Query chunk -> Coll: {ua_coll.name} jobPubJobId in {len(chunk)}")
            ids = ua_coll.distinct("jobPubJobId", q)
            unique_bot_ids.update([i for i in ids if i is not None])

    unique_bot_count = len(unique_bot_ids)
    total = len(expire_ids)
    percent = (unique_bot_count / total * 100) if total else 0.0
    print(f"    [ExpiresBot%] unique_bot_count={unique_bot_count} total_expire_ids={total} percent={round(percent,0)}")
    return round(percent, 0)

def compute_views(target_db, domain_type: str, employer_id: str, start: datetime, end: datetime) -> int:
    """
    Query userAnalytics for unique views:
    Proxy -> distinct 'jobId' with filters
    Subdomain -> distinct 'jobPubJobId' with filters
    """
    ua_coll: Collection = target_db["userAnalytics"]
    base_filter = {
        "browserType": {"$ne": "Unknown"},
        "deviceType": {"$ne": "Unknown"},
        "isFromGoogle": True,
        "isBot": False,
        "createdDt": {"$gte": start, "$lt": end},
        "country": {"$ne": "india"}
    }

    base_filter["employerId"] = employer_id

    if domain_type == "proxy":
        try:
            lst = ua_coll.distinct("jobId", base_filter)
            count = len([x for x in lst if x is not None])
            print(f"    [Views][proxy] DB: {target_db.name} Coll: {ua_coll.name} distinct jobId count = {count}")
            return count
        except Exception:
            pipeline = [{"$match": base_filter}, {"$group": {"_id": "$jobId"}}, {"$count": "c"}]
            res = list(ua_coll.aggregate(pipeline))
            return res[0]["c"] if res else 0
    else:
        try:
            lst = ua_coll.distinct("jobPubJobId", base_filter)
            count = len([x for x in lst if x is not None])
            print(f"    [Views][subdomain] DB: {target_db.name} Coll: {ua_coll.name} distinct jobPubJobId count = {count}")
            return count
        except Exception:
            pipeline = [{"$match": base_filter}, {"$group": {"_id": "$jobPubJobId"}}, {"$count": "c"}]
            res = list(ua_coll.aggregate(pipeline))
            return res[0]["c"] if res else 0

# ------------- MAIN ORCHESTRATION -------------
def main():
    # connect to local companies DB
    local_client = MongoClient(LOCAL_MONGO_URI)
    local_db = local_client[LOCAL_DB_NAME]
    companies_coll = local_db[COMPANIES_COLLECTION]

    # connect to Google Sheets (gspread)
    try:
        gc = gspread.service_account(filename=GSPREAD_CREDENTIALS_FILE)
    except Exception as e:
        print(f"ERROR: cannot authenticate gspread service account: {e}")
        return

    try:
        sh = gc.open_by_url(SPREADSHEET_URL)
    except Exception as e:
        print(f"ERROR: cannot open spreadsheet by URL: {e}")
        return

    worksheets = sh.worksheets()

    # compute date bounds
    start_utc, end_utc = utc_day_bounds_for_yesterday()
    sheet_date_label = format_sheet_date_no_year(start_utc)  # e.g. "December 2"

    print(f"Running daily stats update for date label: {sheet_date_label} (UTC range {start_utc} -> {end_utc})")
    # loop through each sheet
    for ws in worksheets:
        company_name = ws.title.strip()
        print(f"\nProcessing sheet/tab: {company_name}")

        # find company doc in local companies collection
        comp_doc = companies_coll.find_one({"companyName": company_name})
        if not comp_doc:
            print(f"  SKIP: no company config for '{company_name}' in {LOCAL_DB_NAME}.{COMPANIES_COLLECTION}")
            continue

        domain_type = comp_doc.get("domainType", "").lower()
        employer_id = comp_doc.get("employerId")
        # target_mongo_uri is no longer used directly; we select from local creds collection
        if not domain_type or not employer_id:
            print(f"  SKIP: missing domainType/employerId for {company_name} (got domainType={domain_type}, employerId={employer_id})")
            continue

        # choose the correct target DB by reading local creds
        try:
            tgt_client, target_db = get_target_db_from_local_creds(local_client, domain_type)
        except Exception as e:
            print(f"  ERROR: cannot get target DB for domain_type='{domain_type}' - {e}")
            continue

        # Find or append yesterday's row in column A
        try:
            dates = ws.col_values(1)
            if sheet_date_label in dates:
                row_index = dates.index(sheet_date_label) + 1
                print(f"  Found row {row_index} for date {sheet_date_label}")
            else:
                row_index = len(dates) + 1
                ws.update_cell(row_index, 1, sheet_date_label)
                print(f"  Appended new row {row_index} with date {sheet_date_label}")
        except Exception as e:
            print(f"  ERROR reading/writing date column in sheet {company_name}: {e}")
            continue

        # compute metrics
        try:
            # 1) Active Jobs
            active_jobs = compute_active_jobs(target_db, domain_type, employer_id, start_utc, end_utc)
            print(f"  Active Jobs: {active_jobs}")

            # 2) Postings/Updates/Repost (daily) - also returns posting IDs
            postings_count, posting_ids = compute_postings_updates(target_db, domain_type, employer_id, start_utc, end_utc)
            print(f"  Postings/Updates/Repost count: {postings_count} (ids: {len(posting_ids)})")

            # 3) Posted Bot %
            posted_bot_percent = compute_posted_bot_percent(target_db, domain_type, employer_id, start_utc, end_utc, posting_ids)
            print(f"  Posted Bot %: {posted_bot_percent}%")

            # 4) Feed Expires/Manual Expires + ids
            expires_count, expires_ids = compute_feed_expires(target_db, domain_type, employer_id, start_utc, end_utc)
            print(f"  Feed Expires/Manual Expires count: {expires_count}")

            # 5) Expires Bot %
            expires_bot_percent = compute_expires_bot_percent(target_db, domain_type, employer_id, start_utc, end_utc, expires_ids)
            print(f"  Expires Bot %: {expires_bot_percent}%")

            # 6) Views
            views_count = compute_views(target_db, domain_type, employer_id, start_utc, end_utc)
            print(f"  Views (unique): {views_count}")
        except Exception as e:
            print(f"  ERROR computing metrics for {company_name}: {e}")
            continue

        # update sheet columns C-H (3-8)
        try:
            ws.update_cell(row_index, 3, int(active_jobs))  # C
            ws.update_cell(row_index, 4, int(postings_count))  # D
            # format percent with percent sign
            ws.update_cell(row_index, 5, f"{int(posted_bot_percent)}%")  # E
            ws.update_cell(row_index, 6, int(expires_count))  # F
            ws.update_cell(row_index, 7, f"{int(expires_bot_percent)}%")  # G
            ws.update_cell(row_index, 8, int(views_count))  # H
            print(f"  Updated sheet {company_name} row {row_index} columns C-H")
        except Exception as e:
            print(f"  ERROR updating sheet {company_name}: {e}")
            continue

        # polite sleep to avoid hitting Google API rate-limits when multiple sheets
        time.sleep(0.5)

    print("\nAll sheets processed. Done.")

if __name__ == "__main__":
    main()
