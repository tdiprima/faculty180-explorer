# https://sas-irad.github.io/interfolio-api/
# https://pypi.org/project/interfolio-api/0.8/
import os

from dotenv import load_dotenv
from interfolio_api import InterfolioFAR

load_dotenv()


class InterfolioPublicationRetriever:
    def __init__(self):
        self.far = InterfolioFAR(
            public_key=os.getenv("INTERFOLIO_PUBLIC_KEY"),
            private_key=os.getenv("INTERFOLIO_PRIVATE_KEY"),
            database_id=os.getenv("INTERFOLIO_DB_ID"),
        )

    def get_user_publications(self, user_id):
        try:
            print(f"Fetching data for user {user_id}...")
            user_data = self.far.get_user(user_id=str(user_id))
            print(f"User data retrieved successfully: {user_data}")  # It's a count! :(
            
            try:
                activity_data = self.far.get_user_data()
                print(f"Activity data retrieved: {len(activity_data) if activity_data else 0} activities")
            except Exception as activity_error:
                print(f"Failed to retrieve activity data: {activity_error}")
                print("This might be due to empty API response or API access issues")
                activity_data = []
            
            user_publications = [
                activity for activity in activity_data 
                if activity.get('user_id') == str(user_id) and
                activity.get('activity_type') in ['publication', 'scholarly_work']
            ]

            print(f"Found {len(user_publications)} publications for user {user_id}")
            return {"user_profile": user_data, "publications": user_publications}
        except Exception as e:
            print(f"Error for user {user_id}: {e}")
            print(f"Error type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            return None


# Usage
retriever = InterfolioPublicationRetriever()
publications = retriever.get_user_publications(os.getenv("INTERFOLIO_TEST_USER_ID"))
