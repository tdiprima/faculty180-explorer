# https://sas-irad.github.io/interfolio-api/
# https://pypi.org/project/interfolio-api/0.8/
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


def is_publication(activity: dict) -> bool:
    # Catch common publication-like records
    pub_types = {"publication", "scholarly_work"}
    text = (activity.get("type") or "").lower()
    return activity.get("activity_type") in pub_types or any(
        word in text
        for word in [
            "journal",
            "paper",
            "book",
            "chapter",
            "conference",
            "proceeding",
            "report",
            "publication",
        ]
    )


def simplify(activity: dict) -> dict:
    # Normalize common bibliographic fields if present
    return {
        "title": activity.get("title"),
        "authors": activity.get("authors"),
        "venue": activity.get("journal") or activity.get("location"),
        "year": activity.get("yearinfo") or activity.get("dateinfo"),
        "type": activity.get("type"),
        "doi": activity.get("DOI") or activity.get("doi"),
        "url": activity.get("URL") or activity.get("url"),
        "pages": activity.get("pages"),
        "volume": activity.get("volume"),
        "issue": activity.get("issue"),
    }


def get_user_publications(user_id: int):
    far = connect_far()
    # A. Try to get the user profile
    profile = far.get_user(user_id=str(user_id))

    publications = []

    # If the profile already contains activities/publications, use them
    profile_activities = profile.get("activities") or profile.get("publications") or []
    publications.extend([simplify(a) for a in profile_activities if is_publication(a)])

    # B. Also pull comprehensive activities and filter by this user
    # This may return many records.
    all_activities = far.get_user_data(limit=2000)
    user_acts = [a for a in all_activities if str(a.get("user_id")) == str(user_id)]
    publications.extend([simplify(a) for a in user_acts if is_publication(a)])

    # De-duplicate by title + year
    seen = set()
    deduped = []
    for p in publications:
        key = (p.get("title"), p.get("year"))
        if key not in seen:
            seen.add(key)
            deduped.append(p)

    return {"user_id": user_id, "publications": deduped}


if __name__ == "__main__":
    result = get_user_publications(os.getenv("INTERFOLIO_TEST_USER_ID"))
    print(f"\nFinal results for user {result['user_id']}:")
    publications = result["publications"]

    if publications:
        print(f"Found {len(publications)} publications:")
        for i, pub in enumerate(publications, 1):
            title = pub.get("title") or "(untitled)"
            year = pub.get("year") or "n.d."
            venue = pub.get("venue") or ""
            print(f"{i}. {title} ({year})")
            if venue:
                print(f"   📍 {venue}")
    else:
        print("No publications found for this user.")
