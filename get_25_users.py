"""
Extract and display Interfolio user data.
Hand-roll HMAC authentication headers.
Hit the endpoint.

Usage:
    python get_25_users.py RPT
    python get_25_users.py FS
    python get_25_users.py FAR
"""

import base64
import hashlib
import hmac
import os
import sys
from datetime import UTC, datetime

import requests
from dotenv import load_dotenv

load_dotenv()

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
        "request_string": f"/byc/core/tenure/{TENANT_ID}/institutions/{TENANT_ID}/users/search",
    },
    "FS": {
        "host": "https://logic.interfolio.com",
        "request_string": f"/byc/core/search/{TENANT_ID}/institutions/{TENANT_ID}/users/search",
    },
    "FAR": {
        "host": "https://faculty180.interfolio.com/api.php",
        "request_string": "/users",
    },
}


def generate_auth_header(method, request_string):
    """Generate HMAC Authorization header."""
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    verb_request_string = method + "\n\n\n" + timestamp + "\n" + request_string
    encrypted_string = hmac.new(
        bytes(PRIVATE_KEY, "UTF-8"), bytes(verb_request_string, "UTF-8"), hashlib.sha1
    ).digest()
    signature = str(base64.b64encode(encrypted_string), "UTF-8")
    auth_header = "INTF " + PUBLIC_KEY + ":" + signature
    return auth_header, timestamp


def fetch_users(system: str):
    """Fetch user data from the chosen Interfolio system."""
    if system not in API_MAP:
        sys.exit(f"[ERROR] Unknown system '{system}'. Use RPT, FS, or FAR.")

    cfg = API_MAP[system]
    method = "GET"
    auth_header, timestamp = generate_auth_header(method, cfg["request_string"])

    headers = {
        "TimeStamp": timestamp,
        "Authorization": auth_header,
        "Accept": "application/json",
    }

    if system == "FAR":
        headers["INTF-DatabaseID"] = DATABASE_ID

    url = cfg["host"] + cfg["request_string"]
    print(f"[INFO] Fetching users from {system} ({url})...")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.HTTPError as e:
        sys.exit(f"[ERROR] HTTP {response.status_code} - {e}")
    except requests.exceptions.RequestException as e:
        sys.exit(f"[ERROR] Request failed - {e}")
    except ValueError:
        sys.exit("[ERROR] Response was not valid JSON")


def display_users(data, system):
    """Display user information in clean format."""
    # Extract users from results array
    users = data.get("results", [])

    if not users:
        print("[INFO] No users found in the response")
        return

    print(f"\n{'='*60}")
    print(f"Showing: {len(users)} users")

    for i, user in enumerate(users, 1):
        print(f"\n--- USER {i} ---")

        # Basic user information
        first_name = user.get("first_name", "N/A")
        last_name = user.get("last_name", "N/A")
        email = user.get("email", "N/A")
        role = user.get("role", "N/A")

        print(f"Name: {first_name} {last_name}")
        print(f"Email: {email}")
        print(f"Role: {role}")

        # Administrator units
        admin_units = user.get("administrator_unit_names", [])
        if admin_units:
            print(f"Administrator Units: {', '.join(admin_units)}")

        # Titles
        titles = user.get("titles", [])
        if titles:
            print("Titles:")
            for title in titles:
                title_name = title.get("name", "N/A")
                unit_name = title.get("unit_name", "N/A")
                print(f"  - {title_name} ({unit_name})")
        else:
            print("Titles: None")

        print("-" * 40)

    print(f"\n[INFO] Displayed {len(users)} users")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python get_25_users.py <RPT|FS|FAR>")

    system = sys.argv[1].upper()
    results = fetch_users(system)
    display_users(results, system)
