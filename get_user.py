"""
Using interfolio_api - can't find my user.

# Fast search (default: max 3 users, early exit)
FIRSTNAME="John" LASTNAME="Doe" python get_user.py

# Exhaustive search (find up to 10 users)
MAX_USERS=10 EARLY_EXIT=false FIRSTNAME="John" LASTNAME="Doe" python get_user.py

# Custom configuration
MAX_USERS=5 EARLY_EXIT=true FIRSTNAME="John" LASTNAME="Doe" python get_user.py
"""

import os
import logging
import sys

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

# Performance monitoring
import time
from functools import wraps

def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.info(f"{func.__name__} took {end - start:.2f} seconds")
        return result
    return wrapper


def connect_far():
    """Connect to Faculty180 API."""
    return InterfolioFAR(
        public_key=os.getenv("INTERFOLIO_PUBLIC_KEY"),
        private_key=os.getenv("INTERFOLIO_PRIVATE_KEY"),
        database_id=os.getenv("INTERFOLIO_DB_ID"),
    )


def find_user(max_users=3, early_exit=True):
    """
    Search through activity data to find the specified user and display their info.
    
    Args:
        max_users: Maximum number of users to find before stopping (default: 3)
        early_exit: Stop searching after finding max_users (default: True)
    """
    far = connect_far()

    firstname = os.getenv("FIRSTNAME")
    lastname = os.getenv("LASTNAME")

    if not firstname or not lastname:
        logger.error("FIRSTNAME and LASTNAME environment variables must be set")
        return None

    logger.info(f"Searching for {firstname} {lastname} in Faculty180 activity data...")

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
        # Get all activity data
        all_data = far.get_user_data(limit=2000)
        logger.info(f"Searching through {len(all_data)} activity sections...")

        found_users = {}  # user_id -> user_info
        sections_processed = 0

        for section_num, record in enumerate(all_data):
            if isinstance(record, dict) and "activities" in record:
                section_name = record.get("section", {}).get("name", "Unknown Section")
                activities = record["activities"]
                sections_processed += 1

                # Log progress every 10 sections
                if sections_processed % 10 == 0:
                    logger.info(f"Processed {sections_processed} sections, found {len(found_users)} users so far...")

                for activity in activities:
                    if isinstance(activity, dict):
                        # Get user ID for this activity
                        user_id = activity.get("userid") or activity.get("facultyid")
                        if not user_id:
                            continue

                        # Skip if we already found this user
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
                            
                            logger.info(f"Found user {user_id} in {section_name} ({len(found_users)} total)")
                            
                            # Early exit if we found enough users
                            if early_exit and len(found_users) >= max_users:
                                logger.info(f"Found {max_users} users, stopping search early")
                                break
                
                # Break outer loop too if early exit triggered
                if early_exit and len(found_users) >= max_users:
                    break

        if found_users:
            logger.info(f"✅ Found {len(found_users)} potential user(s) for {firstname} {lastname}")
            
            # Display found users with batch profile fetching
            user_profiles = {}
            
            # Batch fetch profiles (if API supports it) or fetch sequentially with timeout
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

            return list(found_users.keys())

        else:
            logger.warning(f"❌ No activities found containing '{firstname} {lastname}'")
            
            # Quick alternative search with just last name (limited scope)
            logger.info(f"Trying quick search with just '{lastname}'...")
            lastname_matches = []
            sections_checked = 0
            max_sections_for_fallback = min(50, len(all_data))  # Limit fallback search
            
            for record in all_data[:max_sections_for_fallback]:
                if isinstance(record, dict) and "activities" in record:
                    sections_checked += 1
                    activities = record["activities"]
                    for activity in activities:
                        if isinstance(activity, dict):
                            user_id = activity.get("userid") or activity.get("facultyid")
                            if not user_id:
                                continue
                            
                            # Quick field check only
                            fields = activity.get("fields", {})
                            for key, value in fields.items():
                                if (isinstance(value, str) and 
                                    len(value) > 3 and 
                                    lastname_lower in value.lower() and
                                    any(term in key.lower() for term in ['name', 'author', 'faculty'])):
                                    
                                    lastname_matches.append({
                                        'user_id': user_id,
                                        'field': key,
                                        'value': value
                                    })
                                    break
                        
                        # Limit total matches for performance
                        if len(lastname_matches) >= 10:
                            break
                    
                    if len(lastname_matches) >= 10:
                        break

            if lastname_matches:
                logger.info(f"\n🔍 Found partial matches for '{lastname}' (checked {sections_checked} sections):")
                seen_users = set()
                for match in lastname_matches:
                    if match['user_id'] not in seen_users:
                        logger.info(f"   User ID {match['user_id']}: {match['field']} = {match['value']}")
                        seen_users.add(match['user_id'])

            return None

    except Exception as e:
        logger.error(f"Error searching for {firstname} {lastname}: {e}")
        return None


if __name__ == "__main__":
    import time
    
    firstname = os.getenv("FIRSTNAME")
    lastname = os.getenv("LASTNAME")
    
    # Allow command line override for max users and early exit
    max_users = int(os.getenv("MAX_USERS", "3"))
    early_exit = os.getenv("EARLY_EXIT", "true").lower() == "true"

    if not firstname or not lastname:
        logger.info("Error: Please set FIRSTNAME and LASTNAME environment variables")
        logger.info("Optional: MAX_USERS=5 EARLY_EXIT=false for exhaustive search")
        exit(1)

    logger.info(f"🔍 Searching for {firstname} {lastname} in Faculty180...")
    if early_exit:
        logger.info(f"⚡ Using optimized search (max {max_users} users, early exit enabled)")
    else:
        logger.info(f"🔍 Using exhaustive search (max {max_users} users)")
    
    start_time = time.time()
    user_ids = find_user(max_users=max_users, early_exit=early_exit)
    search_time = time.time() - start_time

    if user_ids:
        logger.info(f"\n🎉 Search completed in {search_time:.2f} seconds! Found {len(user_ids)} user(s)")
        if len(user_ids) == 1:
            logger.info(f"✅ Verified user: {firstname} {lastname} (ID: {user_ids[0]})")
        else:
            logger.info(f"⚠️  Multiple users found. Please review the results above to identify the correct user.")
    else:
        logger.info(f"\n❌ Could not find {firstname} {lastname} in Faculty180 ({search_time:.2f}s)")
        logger.info("\nPossible reasons:")
        logger.info("- Name might be spelled differently")
        logger.info("- User might not have any activities recorded")
        logger.info("- User might be in a different database/tenant")
        logger.info("\nTry: MAX_USERS=10 EARLY_EXIT=false python get_user.py (for exhaustive search)")
        logger.info("Or contact your Faculty180 administrator for the correct user ID.")
