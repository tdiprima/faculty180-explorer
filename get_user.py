import os

from dotenv import load_dotenv
from interfolio_api import InterfolioFAR

load_dotenv()


def connect_far():
    return InterfolioFAR(
        public_key=os.getenv("INTERFOLIO_PUBLIC_KEY"),
        private_key=os.getenv("INTERFOLIO_PRIVATE_KEY"),
        database_id=os.getenv("INTERFOLIO_DB_ID"),
    )


def find_user_id():
    """
    Search through all activity data to find the specified user's ID
    """
    far = connect_far()

    firstname = os.getenv("FIRSTNAME")
    lastname = os.getenv("LASTNAME")

    if not firstname or not lastname:
        print("Error: FIRSTNAME and LASTNAME environment variables must be set")
        return None

    print(f"Searching for {firstname} {lastname} in Faculty180 activity data...")

    try:
        # Get all activity data - increase limit to search more thoroughly
        all_data = far.get_user_data(limit=2000)
        print(f"Searching through {len(all_data)} activity sections...")

        user_ids = set()

        for section_num, record in enumerate(all_data):
            if isinstance(record, dict) and "activities" in record:
                section_name = record.get("section", {}).get("name", "Unknown Section")
                activities = record["activities"]

                for activity in activities:
                    if isinstance(activity, dict):
                        # Get user ID for this activity
                        user_id = activity.get("userid") or activity.get("facultyid")

                        # Convert entire activity to string and search for the user
                        activity_str = str(activity).lower()

                        # Also specifically check the fields dict
                        fields = activity.get("fields", {})
                        fields_str = str(fields).lower()

                        # Look for the user in various combinations
                        name_variations = [
                            f"{firstname.lower()} {lastname.lower()}",
                            f"{lastname.lower()}, {firstname.lower()}",
                            f"{firstname[0].lower()}. {lastname.lower()}",
                            f"{lastname.lower()} {firstname.lower()}",
                            f"{lastname.lower()},{firstname.lower()}",
                        ]

                        found_match = False
                        for name_var in name_variations:
                            if name_var in activity_str or name_var in fields_str:
                                found_match = True
                                break

                        # Also check if both first and last names appear separately
                        if not found_match:
                            if (
                                firstname.lower() in activity_str
                                and lastname.lower() in activity_str
                            ) or (
                                firstname.lower() in fields_str
                                and lastname.lower() in fields_str
                            ):
                                found_match = True

                        if found_match and user_id:
                            user_ids.add(str(user_id))
                            print(
                                f"🎯 Found {firstname} {lastname} reference for user ID {user_id} in {section_name}"
                            )

                            # Show what matched
                            for key, value in fields.items():
                                if isinstance(value, str):
                                    value_lower = value.lower()
                                    if any(
                                        name in value_lower
                                        for name in [
                                            firstname.lower(),
                                            lastname.lower(),
                                        ]
                                    ):
                                        print(f"   {key}: {value}")

        if user_ids:
            print(
                f"\n✅ Found {len(user_ids)} potential user ID(s) for {firstname} {lastname}: {list(user_ids)}"
            )

            # Test each ID to see which one has publications
            best_id = None
            max_pubs = 0

            for user_id in user_ids:
                try:
                    result = get_user_publications(int(user_id))
                    pub_count = len(result.get("publications", []))
                    print(f"User {user_id}: {pub_count} publications")

                    if pub_count > max_pubs:
                        max_pubs = pub_count
                        best_id = int(user_id)

                    # Show sample publications to verify
                    if pub_count > 0:
                        print("  Sample publications:")
                        for pub in result["publications"][:2]:
                            title = pub.get("title", "No title")
                            authors = pub.get("authors", "No authors")
                            print(f"    - {title}")
                            if authors and authors != "No authors":
                                print(f"      Authors: {authors}")

                except Exception as e:
                    print(f"Error testing publications for user {user_id}: {e}")

            return best_id

        else:
            print(f"❌ No activities found containing '{firstname} {lastname}'")
            print("\nTrying alternative search strategies...")

            # Alternative: search for just the last name
            print(f"Searching for just '{lastname}'...")
            lastname_ids = set()

            for record in all_data:
                if isinstance(record, dict) and "activities" in record:
                    activities = record["activities"]
                    for activity in activities:
                        if isinstance(activity, dict):
                            user_id = activity.get("userid") or activity.get(
                                "facultyid"
                            )
                            activity_str = str(activity).lower()

                            if lastname.lower() in activity_str and user_id:
                                lastname_ids.add(str(user_id))
                                fields = activity.get("fields", {})
                                print(f"Found '{lastname}' for user ID {user_id}")
                                for key, value in fields.items():
                                    if (
                                        isinstance(value, str)
                                        and lastname.lower() in value.lower()
                                    ):
                                        print(f"   {key}: {value}")

            if lastname_ids:
                print(f"Found {lastname} references for user IDs: {list(lastname_ids)}")

            return None

    except Exception as e:
        print(f"Error searching for {firstname} {lastname}: {e}")
        return None


if __name__ == "__main__":
    firstname = os.getenv("FIRSTNAME")
    lastname = os.getenv("LASTNAME")

    if not firstname or not lastname:
        print("Error: Please set FIRSTNAME and LASTNAME environment variables")
        exit(1)

    print(f"🔍 Searching for {firstname} {lastname} in Faculty180...")
    user_id = find_user_id()

    if user_id:
        print(f"\n🎉 Found {firstname} {lastname}! User ID: {user_id}")
        print(f"\nGetting publications for {firstname} {lastname} (ID: {user_id})...")
        result = get_user_publications(user_id)
        publications = result.get("publications", [])

        if publications:
            print(
                f"\n📚 Found {len(publications)} publications for {firstname} {lastname}:"
            )
            for i, pub in enumerate(publications, 1):
                title = pub.get("title") or "(untitled)"
                year = pub.get("year") or "n.d."
                venue = pub.get("venue") or ""
                print(f"{i}. {title} ({year})")
                if venue:
                    print(f"   📍 {venue}")
        else:
            print("No publications found for this user.")
    else:
        print(f"\n❌ Could not find {firstname} {lastname} in Faculty180")
        print("\nPossible reasons:")
        print("- Name might be spelled differently")
        print("- User might not have any activities recorded")
        print("- User might be in a different database/tenant")
        print("\nTry contacting your Faculty180 administrator for the correct user ID.")
