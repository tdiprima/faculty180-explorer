"""
Fetches user lists from Interfolio APIs (RPT, FS, FAR) and saves results to JSON.
Uses proper HMAC authentication instead of Bearer tokens.
Includes pagination support to fetch all pages automatically.
You CAN rage-quit (ctrl-c)

Usage:
    python get_all_users.py RPT
    python get_all_users.py FS
    python get_all_users.py FAR
"""

import base64
import hashlib
import hmac
import json
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ===========================
# GLOBAL VARIABLES FOR SIGNAL HANDLING
# ===========================
interrupted = False
collected_users = []
output_filename = ""


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully by saving collected data."""
    global interrupted, collected_users, output_filename

    print(f"\n[WARNING] Interrupted by user (Ctrl+C)!")

    if collected_users and output_filename:
        print(f"[INFO] Saving {len(collected_users)} users collected so far...")
        try:
            save_json(collected_users, output_filename)
            print(f"[INFO] ✅ Successfully saved partial results to {output_filename}")
        except Exception as e:
            print(f"[ERROR] ❌ Failed to save partial results: {e}")
    else:
        print("[INFO] No data collected yet, nothing to save.")

    print("[INFO] Exiting...")
    sys.exit(130)  # Standard exit code for Ctrl+C


# ===========================
# CONFIG FROM ENVIRONMENT
# ===========================
PUBLIC_KEY = os.getenv("API_PUBLIC_KEY")
PRIVATE_KEY = os.getenv("API_PRIVATE_KEY")
TENANT_ID = os.getenv("TENANT_1_ID")
DATABASE_ID = os.getenv("TENANT_1_DATABASE_ID")

# ===========================
# API ENDPOINT MAP
# ===========================
API_MAP = {
    "RPT": {
        "host": "https://logic.interfolio.com",
        "base_request_string": f"/byc/core/tenure/{TENANT_ID}/institutions/{TENANT_ID}/users/search",
    },
    "FS": {
        "host": "https://logic.interfolio.com",
        "base_request_string": f"/byc/core/search/{TENANT_ID}/institutions/{TENANT_ID}/users/search",
    },
    "FAR": {
        "host": "https://faculty180.interfolio.com/api.php",
        "base_request_string": "/users",
    },
}


def generate_auth_header(method, request_string):
    """Generate HMAC Authorization header matching working code format."""
    # Generate timestamp in the correct format
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    # Format for HMAC (matching working code exactly)
    verb_request_string = method + "\n\n\n" + timestamp + "\n" + request_string

    # Compute HMAC-SHA1
    encrypted_string = hmac.new(
        bytes(PRIVATE_KEY, "UTF-8"), bytes(verb_request_string, "UTF-8"), hashlib.sha1
    ).digest()

    # Base64 encode the signature
    signature = str(base64.b64encode(encrypted_string), "UTF-8")

    # Construct Authorization header with INTF prefix
    auth_header = "INTF " + PUBLIC_KEY + ":" + signature

    return auth_header, timestamp


def fetch_page(system: str, page: int = 1, limit: int = 100):
    """Fetch a single page of user data from the chosen Interfolio system."""
    if system not in API_MAP:
        raise ValueError(f"Unknown system '{system}'. Use RPT, FS, or FAR.")

    cfg = API_MAP[system]
    method = "GET"

    # Build request string with pagination parameters
    # This MUST include the query params for HMAC to work
    base_request = cfg["base_request_string"]
    request_string = f"{base_request}?limit={limit}&page={page}"

    # Generate proper HMAC authentication using the full request string
    auth_header, timestamp = generate_auth_header(method, request_string)

    # Build headers based on system type
    headers = {
        "TimeStamp": timestamp,
        "Authorization": auth_header,
        "Accept": "application/json",
    }

    # FAR needs the database ID header
    if system == "FAR":
        headers["INTF-DatabaseID"] = DATABASE_ID

    # Build the actual URL (host + request_string)
    url = cfg["host"] + request_string

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.HTTPError as e:
        raise requests.exceptions.HTTPError(f"HTTP {response.status_code} - {e}")
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(f"Request failed - {e}")
    except ValueError:
        raise ValueError("Response was not valid JSON")


def fetch_users(system: str, limit: int = 100):
    """Fetch all user data from the chosen Interfolio system using pagination."""
    global collected_users  # Use global variable for signal handler access

    print(f"[INFO] Fetching users from {system} with pagination...")
    print(f"[INFO] 💡 Press Ctrl+C at any time to save partial results and exit")

    collected_users = []  # Reset the global list
    page = 1

    while True:
        print(f"[INFO] Fetching page {page}...")

        try:
            page_data = fetch_page(system, page, limit)
        except Exception as e:
            print(f"[ERROR] Failed to fetch page {page}: {e}")
            if collected_users:
                print(
                    f"[INFO] Returning {len(collected_users)} users collected before error"
                )
                return collected_users
            else:
                sys.exit(f"[ERROR] No data collected before error occurred")

        # Handle different response structures
        if isinstance(page_data, list):
            # Direct list response
            users = page_data
        elif isinstance(page_data, dict):
            # Check common pagination response formats
            if "data" in page_data:
                users = page_data["data"]
            elif "users" in page_data:
                users = page_data["users"]
            elif "results" in page_data:
                users = page_data["results"]
            else:
                # Assume the dict itself contains user data if no standard pagination keys
                users = [page_data] if page_data else []
        else:
            users = []

        if not users:
            print(f"[INFO] No more users found on page {page}. Stopping pagination.")
            break

        collected_users.extend(users)
        print(
            f"[INFO] Page {page}: Found {len(users)} users (total so far: {len(collected_users)})"
        )

        # Check if we got fewer results than requested, indicating last page
        if len(users) < limit:
            print(
                f"[INFO] Received {len(users)} users (less than limit of {limit}). This was the last page."
            )
            break

        page += 1

    print(
        f"[INFO] Finished! Collected {len(collected_users)} total users across {page} page(s)."
    )
    return collected_users


def save_json(data, filename: str):
    """Save API results to a JSON file."""
    path = Path(filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(
        f"[INFO] Saved {len(data) if isinstance(data, list) else 'results'} items to {path}"
    )


if __name__ == "__main__":
    # Set up signal handler for graceful Ctrl+C handling
    signal.signal(signal.SIGINT, signal_handler)

    if len(sys.argv) != 2:
        sys.exit("Usage: python get_all_users.py <RPT|FS|FAR>")

    system = sys.argv[1].upper()

    # Set global output filename for signal handler
    output_filename = f"{system.lower()}_users.json"

    # You can adjust the page size here if needed
    page_size = 100

    results = fetch_users(system, limit=page_size)

    # Save results (this will be the normal completion path)
    save_json(results, output_filename)
