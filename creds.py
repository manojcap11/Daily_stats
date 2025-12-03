import time
import logging
from datetime import datetime, timezone
from pymongo import MongoClient, errors
from urllib.parse import urlparse, urlunparse, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

# === CONFIG ===
LOCAL_MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "mongo_creds"
URI_COLLECTION = "uri"
CREDS_COLLECTION = "creds"
MAX_THREADS = 10  # Adjust based on number of URIs

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# === Helper Functions ===
def parse_ip(uri: str):
    """Extract host/IP from Mongo URI."""
    try:
        return urlparse(uri).hostname
    except Exception:
        return None


def safe_encode_mongo_uri(uri: str) -> str:
    """Ensure username/password in Mongo URI are safely URL-encoded."""
    try:
        if not uri.startswith("mongodb://"):
            return uri
        parsed = urlparse(uri)
        if parsed.username or parsed.password:
            # Encode credentials safely
            netloc = ""
            if parsed.username:
                netloc += quote_plus(parsed.username)
            if parsed.password:
                netloc += f":{quote_plus(parsed.password)}"
            if netloc:
                netloc += f"@{parsed.hostname}"
            else:
                netloc = parsed.hostname
            if parsed.port:
                netloc += f":{parsed.port}"
            # Rebuild the URI
            rebuilt = urlunparse(("mongodb", netloc, parsed.path, "", "", ""))
            return rebuilt
        return uri
    except Exception:
        return uri


def check_mongo(uri: str):
    """
    Try connecting to Mongo and list databases.
    Returns connection status, latency, and DB list.
    """
    result = {
        "mongo_uri": uri,
        "connected": False,
        "error": None,
        "databases": [],
        "ping_ms": None,
    }

    if not str(uri).strip().startswith("mongodb://"):
        result["error"] = "Invalid Mongo URI format"
        return result

    # Safely encode credentials to avoid ValueError: Port contains non-digit characters
    safe_uri = safe_encode_mongo_uri(uri)

    try:
        start_time = time.time()
        client = MongoClient(safe_uri, serverSelectionTimeoutMS=4000)
        client.admin.command("ping")
        latency = round((time.time() - start_time) * 1000, 2)
        dbs = [d for d in client.list_database_names() if d not in ["admin", "local", "config"]]
        result.update({"connected": True, "databases": dbs, "ping_ms": latency})
    except Exception as e:
        result["error"] = str(e)

    return result


def update_uri_status(uri_col, result):
    """Update connection status, latency, and error info in the URI collection."""
    uri_col.update_one(
        {"mongo_uri": result["mongo_uri"]},
        {
            "$set": {
                "connected": result["connected"],
                "error": result["error"],
                "ping_ms": result["ping_ms"],
                "last_checked": datetime.now(timezone.utc),
            }
        },
        upsert=False,
    )


def update_creds(creds_col, result):
    """Update or insert database info into creds collection."""
    if not result["connected"] or not result["databases"]:
        return

    ip = parse_ip(result["mongo_uri"])

    for db_name in result["databases"]:
        doc = {
            "domain": db_name,
            "ip": ip,
            "mongo_uri": result["mongo_uri"],
            "active": "prod" in db_name.lower(),
            "ping_ms": result["ping_ms"],
            "last_checked": datetime.now(timezone.utc),
        }

        creds_col.update_one(
            {"domain": db_name},
            {"$set": doc},
            upsert=True,
        )


def main():
    local_client = MongoClient(LOCAL_MONGO_URI)
    db = local_client[DB_NAME]
    uri_col = db[URI_COLLECTION]
    creds_col = db[CREDS_COLLECTION]

    # Get URIs from collection
    uris = [
        doc["mongo_uri"]
        for doc in uri_col.find({}, {"_id": 0, "mongo_uri": 1})
        if doc.get("mongo_uri")
    ]
    if not uris:
        logging.error(":x: No URIs found in the 'uri' collection.")
        return

    logging.info(f":mag: Checking {len(uris)} Mongo URIs...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(check_mongo, uri) for uri in uris]
        for future in as_completed(futures):
            results.append(future.result())

    for res in results:
        update_uri_status(uri_col, res)
        if res["connected"]:
            update_creds(creds_col, res)
            logging.info(
                f":white_check_mark: {res['mongo_uri']} | {len(res['databases'])} DBs | {res['ping_ms']} ms"
            )
        else:
            logging.warning(
                f":warning: {res['mongo_uri']} | Connection failed | Error: {res['error']}"
            )

    logging.info(":dart: Mongo URI scan and update complete.")


if __name__ == "__main__":
    main()