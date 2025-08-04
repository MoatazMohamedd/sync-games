import os
import requests
import json
from google.cloud import firestore
from google.oauth2 import service_account

# -------------------------
# ✅ CONFIG SECTION
# -------------------------


IGDB_ACCESS_TOKEN = os.getenv("IGDB_ACCESS_TOKEN")  # or get with client creds flow

# Firestore
FIRESTORE_PROJECT_ID = os.getenv("FIRESTORE_PROJECT_ID")

# Firestore collection name
COLLECTION_NAME = "games"

# Get your JSON key string
service_account_info = json.loads(os.environ["FIREBASE_CREDENTIALS_JSON"])

# Create credentials object from the dict
credentials = service_account.Credentials.from_service_account_info(service_account_info)

# Create Firestore client with those creds
db = firestore.Client(project=os.getenv("FIRESTORE_PROJECT_ID"), credentials=credentials)

# -------------------------
# ✅ Function: Query IGDB
# -------------------------
def fetch_games_from_igdb(offset=0, limit=500):
    url = "https://api.igdb.com/v4/games"
    headers = {
        "Client-ID": IGDB_CLIENT_ID,
        "Authorization": f"Bearer {IGDB_ACCESS_TOKEN}",
    }

    # Your filter
    body = f"""
        fields id, name, cover.*, total_rating, themes, first_release_date, summary, genres, platforms, popularity, hypes;
        where cover.height >= 0 & total_rating >= 50 & themes != 42;
        limit {limit};
        offset {offset};
    """

    resp = requests.post(url, headers=headers, data=body)
    resp.raise_for_status()
    return resp.json()

# -------------------------
# ✅ Function: Batch upload to Firestore
# -------------------------
def transform_game(raw_game):
    def format_cover_url(url):
        return "https:" + url.replace("t_thumb", "t_cover_big")

    def format_screenshot_url(url):
        return "https:" + url.replace("t_thumb", "t_screenshot_med")

    transformed = {
        "id": raw_game.get("id"),
        "name": raw_game.get("name"),
        "summary": raw_game.get("summary"),
        "storyline": raw_game.get("storyline"),
        "total_rating": raw_game.get("total_rating"),
        "first_release_date": raw_game.get("first_release_date"),
    }

    # Cover: only URL, formatted
    if "cover" in raw_game and raw_game["cover"].get("url"):
        transformed["cover_url"] = format_cover_url(raw_game["cover"]["url"])

    # Screenshots: list of formatted URLs only
    if "screenshots" in raw_game:
        transformed["screenshots"] = [
            format_screenshot_url(ss["url"]) for ss in raw_game["screenshots"] if ss.get("url")
        ]

    # Flatten nested arrays to name strings
    for field in ["player_perspectives", "game_engines", "game_modes", "genres"]:
        if field in raw_game:
            transformed[field] = [
                item["name"] for item in raw_game[field] if item.get("name")
            ]

    return transformed

def upload_games_to_firestore(games):
    batch = db.batch()
    count = 0

    for game in games:
        doc_ref = db.collection(COLLECTION_NAME).document(str(game["id"]))
        batch.set(doc_ref, game)
        count += 1

        # Firestore batch limit is 500 ops
        if count % 500 == 0:
            batch.commit()
            print(f"Committed batch of 500 docs")
            batch = db.batch()

    # Commit any remaining docs
    if count % 500 != 0:
        batch.commit()
        print(f"Committed final batch of {count % 500} docs")

    print(f"✅ Uploaded {count} games")

# -------------------------
# ✅ Main
# -------------------------

def main():
    # Optional: get new token
    # global IGDB_ACCESS_TOKEN
    # IGDB_ACCESS_TOKEN = get_igdb_token()

    # Total games you want to pull
    TOTAL_GAMES = 20000  # Adjust for first run, then do the rest later
    LIMIT_PER_REQUEST = 500

    for offset in range(0, TOTAL_GAMES, LIMIT_PER_REQUEST):
        print(f"Fetching games {offset} - {offset + LIMIT_PER_REQUEST} ...")
        games = fetch_games_from_igdb(offset=offset, limit=LIMIT_PER_REQUEST)
        transformed_games = [transform_game(game) for game in games]
        upload_games_to_firestore(games)

    print("🎉 Done!")

if __name__ == "__main__":
    main()
