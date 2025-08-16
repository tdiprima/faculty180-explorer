"""
Using interfolio_api - still can't find my user.
Now with parallel processing to utilize multiple CPU cores!

# Fast search (default: max 3 users, early exit)
FIRSTNAME="John" LASTNAME="Doe" python get_user.py

# Exhaustive search (find up to 10 users)
MAX_USERS=10 EARLY_EXIT=false FIRSTNAME="John" LASTNAME="Doe" python get_user.py

# Custom configuration with parallel processing
MAX_USERS=5 EARLY_EXIT=true WORKERS=16 FIRSTNAME="John" LASTNAME="Doe" python get_user.py
"""

import os
import logging
import sys
import signal
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
from functools import partial

from dotenv import load_dotenv
from interfolio_api import InterfolioFAR

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('user_search.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ===========================
# GLOBAL VARIABLES FOR SIGNAL HANDLING
# ===========================
interrupted = False
found_users_global = {}
search_firstname = ""
search_lastname = ""

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully by reporting partial search results."""
    global interrupted, found_users_global, search_firstname, search_lastname
    
    print(f"\n[WARNING] Search interrupted by user (Ctrl+C)!")
    
    if found_users_global:
        logger.info(f"🔍 Partial results for {search_firstname} {search_lastname}:")
        logger.info(f"✅ Found {len(found_users_global)} user(s) before interruption")
        
        # Display the partial results
        display_found_users_simple(found_users_global)
        
        logger.info(f"🛑 Search was interrupted. Results above are partial.")
    else:
        logger.info("ℹ️ No users found before interruption.")
    
    logger.info("Exiting...")
    sys.exit(130)  # Standard exit code for Ctrl+C

def connect_far():
    """Connect to Faculty180 API."""
    return InterfolioFAR(
        public_key=os.getenv("INTERFOLIO_PUBLIC_KEY"),
        private_key=os.getenv("INTERFOLIO_PRIVATE_KEY"),
        database_id=os.getenv("INTERFOLIO_DB_ID"),
    )

def display_found_users_simple(found_users):
    """Display found users without fetching profiles (for signal handler)."""
    for user_id, user_info in found_users.items():
        logger.info(f"\n🎯 FOUND USER:")
        logger.info(f"   User ID: {user_id}")
        logger.info(f"   Found in sections: {', '.join(set(user_info['sections_found']))}")
        
        if user_info['matching_fields']:
            logger.info(f"   Matching fields:")
            seen_values = set()
            for match in user_info['matching_fields']:
                if match['value'] not in seen_values:
                    logger.info(f"      {match['field']}: {match['value']}")
                    seen_values.add(match['value'])

def display_found_users(found_users):
    """Display found users with their profile information."""
    if not found_users:
        return
        
    far = connect_far()
    
    # Batch fetch profiles
    user_profiles = {}
    for user_id in found_users.keys():
        try:
            profile = far.get_user(user_id=str(user_id))
            user_profiles[user_id] = profile
        except Exception as e:
            logger.warning(f"Could not fetch profile for user {user_id}: {e}")
            user_profiles[user_id] = None
    
    # Display results
    for user_id, user_info in found_users.items():
        logger.info(f"\n🎯 FOUND USER:")
        logger.info(f"   User ID: {user_id}")
        logger.info(f"   Found in sections: {', '.join(set(user_info['sections_found']))}")
        
        # Display matching fields to verify identity
        if user_info['matching_fields']:
            logger.info(f"   Matching fields:")
            seen_values = set()
            for match in user_info['matching_fields']:
                if match['value'] not in seen_values:
                    logger.info(f"      {match['field']}: {match['value']}")
                    seen_values.add(match['value'])
        
        # Display profile verification
        profile = user_profiles.get(user_id)
        if profile and isinstance(profile, dict):
            # Extract name information if available
            first_name = profile.get('first_name') or profile.get('firstName') or 'N/A'
            last_name = profile.get('last_name') or profile.get('lastName') or 'N/A'
            email = profile.get('email') or 'N/A'
            
            logger.info(f"   Profile verification:")
            logger.info(f"      First Name: {first_name}")
            logger.info(f"      Last Name: {last_name}")
            logger.info(f"      Email: {email}")
        elif profile:
            logger.info(f"   Profile: {profile}")
        else:
            logger.info(f"   Profile: Could not fetch")

def search_page_worker(page_info):
    """
    Worker function to search a single page for users.
    This runs in a separate process.
    """
    page, page_size, firstname_lower, lastname_lower, name_variations = page_info
    
    try:
        # Each worker needs its own connection
        far = InterfolioFAR(
            public_key=os.getenv("INTERFOLIO_PUBLIC_KEY"),
            private_key=os.getenv("INTERFOLIO_PRIVATE_KEY"),
            database_id=os.getenv("INTERFOLIO_DB_ID"),
        )
        
        offset = (page - 1) * page_size
        page_data = far.get_user_data(limit=page_size, offset=offset)
        
        if not page_data:
            return page, []  # No data found
        
        found_users = {}
        sections_processed = 0
        
        for section_num, record in enumerate(page_data):
            if isinstance(record, dict) and "activities" in record:
                section_name = record.get("section", {}).get("name", "Unknown Section")
                activities = record["activities"]
                sections_processed += 1

                for activity in activities:
                    if isinstance(activity, dict):
                        # Get user ID for this activity
                        user_id = activity.get("userid") or activity.get("facultyid")
                        if not user_id:
                            continue

                        # Skip if we already found this user in this page
                        if str(user_id) in found_users:
                            continue

                        # Quick check: only process activities with name-like fields
                        fields = activity.get("fields", {})
                        has_name_fields = any(
                            key for key in fields.keys() 
                            if any(term in key.lower() for term in ['name', 'author', 'faculty', 'person', 'user'])
                        )
                        
                        if not has_name_fields:
                            continue

                        # Optimized search: only check relevant fields first
                        found_match = False
                        matching_field = None
                        matching_value = None

                        # First pass: check only name-related fields
                        for key, value in fields.items():
                            if isinstance(value, str) and len(value) > 2:  # Skip very short values
                                key_lower = key.lower()
                                if any(term in key_lower for term in ['name', 'author', 'faculty', 'person']):
                                    value_lower = value.lower()
                                    
                                    # Check exact name variations first (fastest)
                                    for name_var in name_variations:
                                        if name_var in value_lower:
                                            found_match = True
                                            matching_field = key
                                            matching_value = value
                                            break
                                    
                                    if found_match:
                                        break
                                    
                                    # Check if both names appear separately
                                    if firstname_lower in value_lower and lastname_lower in value_lower:
                                        found_match = True
                                        matching_field = key
                                        matching_value = value
                                        break
                        
                        # Second pass: if not found, check all fields (slower)
                        if not found_match:
                            activity_str = str(activity).lower()
                            for name_var in name_variations:
                                if name_var in activity_str:
                                    found_match = True
                                    # Find which field actually matched
                                    for key, value in fields.items():
                                        if isinstance(value, str) and name_var in value.lower():
                                            matching_field = key
                                            matching_value = value
                                            break
                                    break

                        if found_match:
                            # Store user info
                            found_users[str(user_id)] = {
                                'user_id': user_id,
                                'sections_found': [section_name],
                                'matching_fields': [{
                                    'field': matching_field,
                                    'value': matching_value
                                }] if matching_field else []
                            }

        return page, found_users, len(page_data)
        
    except Exception as e:
        return page, f"ERROR: {str(e)}", 0

def find_user_parallel(max_users=3, early_exit=True, page_size=25, max_workers=None):
    """
    Search through activity data using parallel processing.
    
    Args:
        max_users: Maximum number of users to find before stopping (default: 3)
        early_exit: Stop searching after finding max_users (default: True)
        page_size: Number of records to fetch per page (default: 25)
        max_workers: Number of parallel workers (default: CPU count)
    """
    global found_users_global, search_firstname, search_lastname
    
    firstname = os.getenv("FIRSTNAME")
    lastname = os.getenv("LASTNAME")
    
    # Set global variables for signal handler
    search_firstname = firstname
    search_lastname = lastname

    if not firstname or not lastname:
        logger.error("FIRSTNAME and LASTNAME environment variables must be set")
        return None

    if max_workers is None:
        max_workers = min(mp.cpu_count(), 16)  # Cap at 16 to avoid overwhelming the API
    
    logger.info(f"Searching for {firstname} {lastname} in Faculty180 activity data...")
    logger.info(f"💡 Press Ctrl+C at any time to see partial results and exit")
    logger.info(f"🚀 Using {max_workers} parallel workers (page size: {page_size})")

    # Pre-compile search terms for efficiency
    firstname_lower = firstname.lower()
    lastname_lower = lastname.lower()
    name_variations = [
        f"{firstname_lower} {lastname_lower}",
        f"{lastname_lower}, {firstname_lower}",
        f"{firstname_lower[0]}. {lastname_lower}",
        f"{lastname_lower} {firstname_lower}",
        f"{lastname_lower},{firstname_lower}",
    ]

    try:
        found_users = {}
        found_users_global = found_users  # For signal handler access
        total_sections_processed = 0
        
        # Quick estimation - just start processing and discover size dynamically
        far = connect_far()
        logger.info("📊 Starting dynamic search (will discover data size as we go)...")
        
        # Start with a reasonable estimate and adjust dynamically
        estimated_pages = 200  # Start with this assumption, adjust as needed
        
        logger.info(f"📈 Starting with estimated ~{estimated_pages} pages (will adjust dynamically)")
        
        # Process pages in batches to avoid overwhelming the API
        batch_size = max_workers * 2  # Process 2x workers worth of pages at a time
        page = 1
        consecutive_empty_batches = 0
        max_empty_batches = 3  # Stop after 3 consecutive empty batches
        
        while page <= estimated_pages and consecutive_empty_batches < max_empty_batches:
            # Create batch of page tasks
            batch_end = min(page + batch_size - 1, estimated_pages)
            page_tasks = []
            
            for p in range(page, batch_end + 1):
                page_info = (p, page_size, firstname_lower, lastname_lower, name_variations)
                page_tasks.append(page_info)
            
            logger.info(f"🔄 Processing pages {page}-{batch_end} with {len(page_tasks)} workers...")
            
            batch_found_any_data = False
            
            # Process this batch in parallel
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                future_to_page = {executor.submit(search_page_worker, task): task[0] for task in page_tasks}
                
                for future in as_completed(future_to_page):
                    page_num = future_to_page[future]
                    try:
                        result = future.result(timeout=5)  # If it's taking longer than 5 seconds, something's probably wrong anyway.
                        
                        if len(result) == 3:  # Normal result
                            page_num, page_found_users, sections_count = result
                            total_sections_processed += sections_count
                            
                            if sections_count > 0:
                                batch_found_any_data = True
                            
                            if isinstance(page_found_users, dict) and page_found_users:
                                # Merge found users
                                for user_id, user_info in page_found_users.items():
                                    if user_id not in found_users:
                                        found_users[user_id] = user_info
                                        logger.info(f"✅ Page {page_num}: Found user {user_id} ({len(found_users)} total)")
                                    else:
                                        # Merge section info for existing user
                                        found_users[user_id]['sections_found'].extend(user_info['sections_found'])
                                        found_users[user_id]['matching_fields'].extend(user_info['matching_fields'])
                                
                                # Check if we should stop early
                                if early_exit and len(found_users) >= max_users:
                                    logger.info(f"🎯 Found {max_users} users, stopping search early")
                                    executor.shutdown(wait=False)
                                    break
                            
                            if sections_count == 0:
                                logger.info(f"📄 Page {page_num}: No data (reached end)")
                        else:  # Error result
                            page_num, error_msg, _ = result
                            logger.warning(f"⚠️ Page {page_num}: {error_msg}")
                            
                    except Exception as e:
                        logger.error(f"❌ Page {page_num}: Exception {e}")
            
            # Track consecutive empty batches
            if not batch_found_any_data:
                consecutive_empty_batches += 1
                logger.info(f"📭 Batch {page}-{batch_end}: No data found ({consecutive_empty_batches}/{max_empty_batches} empty batches)")
            else:
                consecutive_empty_batches = 0
            
            # Check if we should stop
            if early_exit and len(found_users) >= max_users:
                break
                
            # Progress update
            if total_sections_processed > 0:
                logger.info(f"📊 Processed {total_sections_processed} sections so far, found {len(found_users)} users")
            
            # Dynamically extend search if we're still finding data
            if page >= estimated_pages - batch_size and batch_found_any_data:
                estimated_pages += 100  # Extend search range
                logger.info(f"📈 Still finding data, extending search to ~{estimated_pages} pages")
            
            page = batch_end + 1

        logger.info(f"🏁 Search completed! Processed {total_sections_processed} sections.")

        if found_users:
            logger.info(f"✅ Found {len(found_users)} potential user(s) for {firstname} {lastname}")
            
            # Display found users
            display_found_users(found_users)
            return list(found_users.keys())

        else:
            logger.warning(f"❌ No activities found containing '{firstname} {lastname}'")
            return None

    except Exception as e:
        logger.error(f"Error searching for {firstname} {lastname}: {e}")
        return None


if __name__ == "__main__":
    # Set up signal handler for graceful Ctrl+C handling
    signal.signal(signal.SIGINT, signal_handler)
    
    firstname = os.getenv("FIRSTNAME")
    lastname = os.getenv("LASTNAME")
    
    # Allow command line override for max users, early exit, page size, and workers
    max_users = int(os.getenv("MAX_USERS", "3"))
    early_exit = os.getenv("EARLY_EXIT", "true").lower() == "true"
    page_size = int(os.getenv("PAGE_SIZE", "25"))
    max_workers = int(os.getenv("WORKERS", "0")) or min(mp.cpu_count(), 16)

    if not firstname or not lastname:
        logger.info("Error: Please set FIRSTNAME and LASTNAME environment variables")
        logger.info("Optional: MAX_USERS=5 EARLY_EXIT=false PAGE_SIZE=50 WORKERS=16 for custom search")
        exit(1)

    logger.info(f"🔍 Searching for {firstname} {lastname} in Faculty180...")
    if early_exit:
        logger.info(f"⚡ Using optimized search (max {max_users} users, early exit enabled)")
    else:
        logger.info(f"🔄 Using exhaustive search (max {max_users} users)")
    
    logger.info(f"🚀 Parallel processing: {max_workers} workers, page size: {page_size}")
    
    start_time = time.time()
    user_ids = find_user_parallel(max_users=max_users, early_exit=early_exit, page_size=page_size, max_workers=max_workers)
    search_time = time.time() - start_time

    if user_ids:
        logger.info(f"\n🎉 Search completed in {search_time:.2f} seconds! Found {len(user_ids)} user(s)")
        if len(user_ids) == 1:
            logger.info(f"✅ Verified user: {firstname} {lastname} (ID: {user_ids[0]})")
        else:
            logger.info(f"⚠️ Multiple users found. Please review the results above to identify the correct user.")
    else:
        logger.info(f"\n❌ Could not find {firstname} {lastname} in Faculty180 ({search_time:.2f}s)")
        logger.info("\nPossible reasons:")
        logger.info("- Name might be spelled differently")
        logger.info("- User might not have any activities recorded")
        logger.info("- User might be in a different database/tenant")
        logger.info("\nTry: MAX_USERS=10 EARLY_EXIT=false WORKERS=32 python get_user.py (for exhaustive search)")
        logger.info("Or contact your Faculty180 administrator for the correct user ID.")
