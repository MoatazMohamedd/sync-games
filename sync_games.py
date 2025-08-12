import os
import requests
import random
from datetime import datetime
from math import ceil
from google.cloud import firestore
from google.oauth2 import service_account
import time

# -------------------------
# CONFIG
# -------------------------
IGDB_CLIENT_ID = os.getenv("IGDB_CLIENT_ID")
IGDB_ACCESS_TOKEN = os.getenv("IGDB_ACCESS_TOKEN")
FIRESTORE_PROJECT_ID = os.getenv("FIRESTORE_PROJECT_ID")
SERVICE_ACCOUNT_JSON = os.environ.get("FIREBASE_CREDENTIALS_JSON")

if not (IGDB_CLIENT_ID and IGDB_ACCESS_TOKEN and FIRESTORE_PROJECT_ID and SERVICE_ACCOUNT_JSON):
    raise SystemExit("Missing one of required env vars: IGDB_CLIENT_ID, IGDB_ACCESS_TOKEN, FIRESTORE_PROJECT_ID, FIREBASE_CREDENTIALS_JSON")

# Firestore collection for daily homepage docs
COLLECTION_NAME = "homepageData"

# Homepage sizes
FEATURED_COUNT = 8
POPULAR_COUNT = 10
GENRE_COUNT = 12

# your exact genre names (as requested)
GENRE_NAMES = [
    "Point-and-click", "Fighting", "Shooter", "Music", "Platform", "Puzzle", "Racing",
    "Real Time Strategy (RTS)", "Role-playing (RPG)", "Simulator", "Sport", "Strategy",
    "Turn-based strategy (TBS)", "Tactical", "Hack and slash/Beat 'em up", "Quiz/Trivia",
    "Adventure", "Indie", "Arcade", "Visual Novel", "Card & Board Game", "MOBA"
]

# Popularity primitive type ids we will use (per IGDB doc)
# 5: 24hr Peak Players, 2: Want to Play, 6: Positive Reviews
POP_PRIMITIVES = {
    5: {"name": "peak_players_24h", "weight": 0.5},
    2: {"name": "want_to_play", "weight": 0.3},
    6: {"name": "positive_reviews", "weight": 0.2}
}

# How many primitives to fetch per type (top N). Adjust if you need wider coverage.
POP_PRIMITIVE_LIMIT = 500

# IGDB client settings
IGDB_HEADERS = {
    "Client-ID": IGDB_CLIENT_ID,
    "Authorization": f"Bearer {IGDB_ACCESS_TOKEN}",
    "Accept": "application/json"
}
IGDB_API_BASE = "https://api.igdb.com/v4"

# rate limiting safety (sleep between big requests)
REQUEST_SLEEP = 0.15


# -------------------------
# FIRESTORE SETUP
# -------------------------
credentials = service_account.Credentials.from_service_account_info(
    __import__("json").loads(SERVICE_ACCOUNT_JSON)
)
db = firestore.Client(project=FIRESTORE_PROJECT_ID, credentials=credentials)


# -------------------------
# IGDB HELPERS
# -------------------------
def igdb_post(path: str, body: str):
    url = f"{IGDB_API_BASE}/{path}"
    resp = requests.post(url, headers=IGDB_HEADERS, data=body)
    try:
        resp.raise_for_status()
    except Exception as e:
        print("IGDB error:", resp.status_code, resp.text)
        raise
    # rate safety
    time.sleep(REQUEST_SLEEP)
    return resp.json()


def igdb_count(where_clause: str) -> int:
    # POST to /games/count with `where ...;`
    resp = requests.post(f"{IGDB_API_BASE}/games/count", headers=IGDB_HEADERS, data=f"where {where_clause};")
    resp.raise_for_status()
    time.sleep(REQUEST_SLEEP)
    return resp.json().get("count", 0)


def igdb_fetch_games_by_where(where_clause: str, limit: int, offset: int = 0):
    body = f"""
        fields id, name, cover.url, total_rating, storyline, first_release_date, summary, genres.name,
               player_perspectives.name, game_engines.name, game_modes.name, screenshots.url, url;
        where {where_clause};
        limit {limit};
        offset {offset};
    """
    return igdb_post("games", body)


def igdb_fetch_games_by_ids(game_ids: list):
    # IGDB supports where id = (1,2,3)
    if not game_ids:
        return []
    ids_clause = ", ".join(str(i) for i in game_ids)
    body = f"""
        fields id, name, cover.url, total_rating, storyline, first_release_date, summary, genres.name,
               player_perspectives.name, game_engines.name, game_modes.name, screenshots.url, url;
        where id = ({ids_clause});
        limit {len(game_ids)};
    """
    return igdb_post("games", body)


def igdb_fetch_popularity_primitives(pop_type_id: int, limit: int = POP_PRIMITIVE_LIMIT):
    # returns list of {game_id, value}
    body = f"fields game_id,value,popularity_type; sort value desc; limit {limit}; where popularity_type = {pop_type_id};"
    return igdb_post("popularity_primitives", body)


def igdb_get_genre_id_by_name(name: str):
    # exact name match
    body = f'fields id,name; where name = "{name}"; limit 1;'
    results = igdb_post("genres", body)
    if results:
        return results[0].get("id")
    return None


# -------------------------
# TRANSFORM GAME
# -------------------------
def transform_game(raw_game: dict) -> dict:
    def format_cover_url(url):
        if not url:
            return None
        return "https:" + url.replace("t_thumb", "t_cover_big")

    def format_screenshot_url(url):
        if not url:
            return None
        return "https:" + url.replace("t_thumb", "t_screenshot_med")

    transformed = {
        "id": raw_game.get("id"),
        "name": raw_game.get("name"),
        "summary": raw_game.get("summary"),
        "storyline": raw_game.get("storyline"),
        "total_rating": raw_game.get("total_rating"),
        "first_release_date": raw_game.get("first_release_date"),
        "igdb_url": raw_game.get("url")
    }

    cover = raw_game.get("cover")
    if cover and cover.get("url"):
        transformed["cover_url"] = format_cover_url(cover.get("url"))

    screenshots = raw_game.get("screenshots")
    if screenshots:
        transformed["screenshots"] = [format_screenshot_url(s.get("url")) for s in screenshots if s.get("url")]

    for field in ["player_perspectives", "game_engines", "game_modes", "genres"]:
        if field in raw_game:
            try:
                transformed[field] = [item.get("name") for item in raw_game[field] if item.get("name")]
            except Exception:
                pass

    return transformed


# -------------------------
# BUILD SECTIONS
# -------------------------
def build_featured_section():
    where = "cover.height>=0 & (hypes > 25 | total_rating > 50) & themes != 42 & game_type = 0"
    total = igdb_count(where)
    limit = FEATURED_COUNT
    if total <= 0:
        return []
    offset = random.randint(0, max(0, total - limit))
    print(f"[featured] total={total}, offset={offset}")
    raw = igdb_fetch_games_by_where(where, limit, offset)
    return [transform_game(g) for g in raw]


def build_genres_section(genre_name_to_id: dict):
    genres_out = {}
    for name in GENRE_NAMES:
        genre_id = genre_name_to_id.get(name)
        if not genre_id:
            print(f"[genre] WARN: No IGDB id found for genre '{name}', skipping.")
            genres_out[name] = []
            continue
        where = f"genres = {genre_id} & cover.height>=0 & themes != 42 & game_type = 0"
        total = igdb_count(where)
        limit = GENRE_COUNT
        if total <= 0:
            genres_out[name] = []
            continue
        offset = random.randint(0, max(0, total - limit))
        print(f"[genre:{name}] total={total}, offset={offset}")
        raw = igdb_fetch_games_by_where(where, limit, offset)
        genres_out[name] = [transform_game(g) for g in raw]
    return genres_out


def build_popular_section():
    """
    Build 'Popular' section with a custom trending score based on
    hype, follows, rating, and recency — no IGDB 'popularity' field.
    """

    RECENT_RELEASE_DAYS = 180   # 6 months for strong boost
    MAX_GAME_AGE_DAYS = 1095    # 3 years, after which big penalty applies
    RECENT_UPDATE_DAYS = 45     # boosts actively updated games

    def calculate_trend_score(game):
        score = 0
        hype = game.get("hypes", 0) or 0
        follows = game.get("follows", 0) or 0
        rating = game.get("total_rating", 0) or 0
        release_date = game.get("first_release_date")
        updated_at = game.get("updated_at")

        # Base hype, follows, and rating
        score += hype * 1.0
        score += follows * 0.5
        score += rating * 0.3

        # Recency boost / penalty
        if release_date:
            days_since_release = (datetime.utcnow() - datetime.utcfromtimestamp(release_date)).days
            if days_since_release <= RECENT_RELEASE_DAYS:
                score += 25
            elif days_since_release > MAX_GAME_AGE_DAYS:
                score -= 40

        # Recent updates boost
        if updated_at:
            days_since_update = (datetime.utcnow() - datetime.utcfromtimestamp(updated_at)).days
            if days_since_update <= RECENT_UPDATE_DAYS:
                score += 15

        return score

    # Pull a wide pool from IGDB: avoid theme 42 (adult) & require covers
    where = "cover.height>=0 & themes != 42 & game_type = 0"
    raw_games = igdb_post("games", f"""
        fields id, name, cover.url, total_rating, storyline, first_release_date, summary, 
               genres.name, player_perspectives.name, game_engines.name, game_modes.name, 
               screenshots.url, url, follows, hypes, updated_at;
        where {where};
        sort hypes desc;
        limit 300;
    """)

    # Score and sort
    scored = sorted(raw_games, key=calculate_trend_score, reverse=True)

    # Take top N and transform
    top_games = scored[:POPULAR_COUNT]
    return [transform_game(g) for g in top_games]


# -------------------------
# UPLOAD
# -------------------------
def upload_homepage_doc(payload: dict):
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    doc_ref = db.collection(COLLECTION_NAME).document(today_str)
    doc_ref.set(payload)
    print(f"[upload] uploaded homepageData/{today_str}")


# -------------------------
# MAIN
# -------------------------
def main():
    print("[start] building homepage data...")

    # Resolve genre names -> ids from IGDB
    print("[init] resolving genre ids from IGDB...")
    genre_name_to_id = {}
    for name in GENRE_NAMES:
        try:
            gid = igdb_get_genre_id_by_name(name)
            if gid:
                genre_name_to_id[name] = gid
                print(f"  {name} -> {gid}")
            else:
                print(f"  {name} -> (not found)")
        except Exception as e:
            print(f"  error fetching genre {name}: {e}")

    # Build each section
    featured = build_featured_section()
    popular = build_popular_section()
    genres = build_genres_section(genre_name_to_id)

    payload = {
        "createdAt": datetime.utcnow().isoformat() + "Z",
        "featured": featured,
        "popular": popular,
        "genres": genres
    }

    upload_homepage_doc(payload)
    print("[done]")


if __name__ == "__main__":
    main()
