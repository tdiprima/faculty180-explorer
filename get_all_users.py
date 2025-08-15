"""
Fetches user lists from Interfolio APIs (RPT, FS, FAR) and saves results to JSON.
Uses proper HMAC authentication with concurrent page fetching for improved performance.
Includes pagination support to fetch all pages automatically.
You CAN rage-quit (ctrl-c)

Usage:
    python get_all_users.py RPT
    python get_all_users.py FS
    python get_all_users.py FAR
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import httpx
from dotenv import load_dotenv

load_dotenv()

# ===========================
# LOGGING CONFIGURATION
# ===========================
def setup_logging():
    """Configure logging with both console and file output."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('user_fetch.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ===========================
# CONFIGURATION
# ===========================
CONCURRENT_PAGES = 5  # Number of pages to fetch concurrently
TIMEOUT = 30  # Request timeout in seconds
DEFAULT_PAGE_SIZE = 100

# Global variables for signal handling
interrupted = False
collected_users = []
output_filename = ""


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully by saving collected data."""
    global interrupted, collected_users, output_filename

    logger.warning("Interrupted by user (Ctrl+C)!")

    if collected_users and output_filename:
        logger.info(f"Saving {len(collected_users)} users collected so far...")
        try:
            save_json(collected_users, output_filename)
            logger.info(f"✅ Successfully saved partial results to {output_filename}")
        except Exception as e:
            logger.error(f"❌ Failed to save partial results: {e}")
    else:
        logger.info("No data collected yet, nothing to save.")

    logger.info("Exiting...")
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


def generate_auth_header(method: str, request_string: str, timestamp: Optional[str] = None) -> tuple[str, str]:
    """Generate HMAC Authorization header matching working code format."""
    if timestamp is None:
        timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    # Format for HMAC (matching working code exactly)
    verb_request_string = f"{method}\n\n\n{timestamp}\n{request_string}"

    # Compute HMAC-SHA1
    encrypted_string = hmac.new(
        PRIVATE_KEY.encode('utf-8'), 
        verb_request_string.encode('utf-8'), 
        hashlib.sha1
    ).digest()

    # Base64 encode the signature
    signature = base64.b64encode(encrypted_string).decode('utf-8')

    # Construct Authorization header with INTF prefix
    auth_header = f"INTF {PUBLIC_KEY}:{signature}"

    return auth_header, timestamp


async def fetch_page(client: httpx.AsyncClient, system: str, page: int = 1, limit: int = DEFAULT_PAGE_SIZE) -> dict:
    """Fetch a single page of user data from the chosen Interfolio system."""
    if system not in API_MAP:
        raise ValueError(f"Unknown system '{system}'. Use RPT, FS, or FAR.")

    cfg = API_MAP[system]
    method = "GET"

    # Build request string with pagination parameters
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
        logger.debug(f"Requesting {url}")
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP {e.response.status_code} error for {url}: {e}")
        raise
    except httpx.RequestError as e:
        logger.error(f"Request failed for {url}: {e}")
        raise
    except ValueError as e:
        logger.error(f"Invalid JSON response from {url}: {e}")
        raise


def extract_users_from_response(page_data: Union[dict, list]) -> List[dict]:
    """Extract users from API response, handling different response formats."""
    if isinstance(page_data, list):
        return page_data
    
    if isinstance(page_data, dict):
        # Check common pagination response formats
        for key in ["data", "users", "results"]:
            if key in page_data:
                return page_data[key]
        
        # If no standard pagination keys, treat dict as single user
        return [page_data] if page_data else []
    
    return []


async def fetch_users_concurrent(system: str, limit: int = DEFAULT_PAGE_SIZE) -> List[dict]:
    """Fetch all users with concurrent page requests for improved performance."""
    global collected_users
    collected_users = []
    
    timeout = httpx.Timeout(TIMEOUT)
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        # First, fetch page 1 to understand the data structure
        logger.info(f"Fetching first page from {system}...")
        try:
            first_page_data = await fetch_page(client, system, 1, limit)
            first_page_users = extract_users_from_response(first_page_data)
            
            if not first_page_users:
                logger.info("No users found.")
                return []
            
            collected_users.extend(first_page_users)
            logger.info(f"Page 1: Found {len(first_page_users)} users")
            
            # If we got fewer results than the limit, we're done
            if len(first_page_users) < limit:
                logger.info(f"Only one page of data. Total: {len(collected_users)} users")
                return collected_users
            
        except Exception as e:
            logger.error(f"Failed to fetch first page: {e}")
            return []
        
        # Fetch remaining pages concurrently
        page = 2
        while True:
            # Create tasks for concurrent page fetching
            tasks = []
            page_numbers = []
            
            for i in range(CONCURRENT_PAGES):
                current_page = page + i
                tasks.append(fetch_page(client, system, current_page, limit))
                page_numbers.append(current_page)
            
            logger.info(f"Fetching pages {page_numbers[0]}-{page_numbers[-1]} concurrently...")
            
            try:
                # Execute concurrent requests
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                pages_with_data = 0
                for i, result in enumerate(results):
                    current_page = page_numbers[i]
                    
                    if isinstance(result, Exception):
                        logger.warning(f"Failed to fetch page {current_page}: {result}")
                        continue
                    
                    users = extract_users_from_response(result)
                    if users:
                        collected_users.extend(users)
                        pages_with_data += 1
                        logger.info(f"Page {current_page}: Found {len(users)} users (total: {len(collected_users)})")
                    else:
                        logger.debug(f"Page {current_page}: No users found")
                    
                    # If this page had fewer results than the limit, we've reached the end
                    if len(users) < limit:
                        logger.info(f"Page {current_page} had {len(users)} users (less than limit). Stopping.")
                        return collected_users
                
                # If no pages had data, we're done
                if pages_with_data == 0:
                    logger.info("No more data found. Stopping pagination.")
                    break
                    
                page += CONCURRENT_PAGES
                
            except Exception as e:
                logger.error(f"Error during concurrent fetching: {e}")
                break
    
    logger.info(f"Finished! Collected {len(collected_users)} total users.")
    return collected_users


def fetch_users(system: str, limit: int = DEFAULT_PAGE_SIZE):
    """Fetch all user data from the chosen Interfolio system using pagination."""
    logger.info(f"Fetching users from {system} with concurrent pagination...")
    logger.info("💡 Press Ctrl+C at any time to save partial results and exit")
    
    try:
        return asyncio.run(fetch_users_concurrent(system, limit))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return collected_users


def save_json(data: List[dict], filename: str) -> None:
    """Save API results to a JSON file."""
    path = Path(filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    count = len(data) if isinstance(data, list) else "unknown"
    logger.info(f"Saved {count} items to {path}")


if __name__ == "__main__":
    # Set up signal handler for graceful Ctrl+C handling
    signal.signal(signal.SIGINT, signal_handler)

    if len(sys.argv) != 2:
        sys.exit("Usage: python get_all_users.py <RPT|FS|FAR>")

    system = sys.argv[1].upper()

    # Set global output filename for signal handler
    output_filename = f"{system.lower()}_users.json"

    # You can adjust the page size here if needed
    page_size = DEFAULT_PAGE_SIZE

    results = fetch_users(system, limit=page_size)

    # Save results (this will be the normal completion path)
    save_json(results, output_filename)