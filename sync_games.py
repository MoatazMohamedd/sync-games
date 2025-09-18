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
DOCUMENT_NAME = "homepage"

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
        where = f"genres = {genre_id} & cover.height>=0 &  total_rating > 50 & themes != 42 & game_type = 0"
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


# --------- New helpers for PopScore-backed popular section ---------
def igdb_fetch_popularity_types():
    """Return list of popularity types from IGDB (id/name)."""
    return igdb_post("popularity_types", "fields id,name,description; limit 500;")

def chunked(iterable, size):
    it = list(iterable)
    for i in range(0, len(it), size):
        yield it[i:i+size]

def build_popular_section():
    """
    Replaces previous function. Uses IGDB popularity_primitives to build a candidate pool,
    normalizes each primitive, merges with hypes/follows/ratings, and ranks.
    """
    print("[popular] fetching popularity types...")
    try:
        pop_types = igdb_fetch_popularity_types()
    except Exception as e:
        print("[popular] failed to fetch popularity_types, falling back to simple pool:", e)
        return _fallback_popular_section()

    # Choose types by keywords (tweak keywords if you want others)
    desired_keywords = ("peak", "player", "player count", "page", "view", "want", "positive", "reviews")
    selected_types = [t for t in pop_types if any(k in (t.get("name") or "").lower() for k in desired_keywords)]
    if not selected_types:
        print("[popular] no popularity types matched keywords, falling back")
        return _fallback_popular_section()

    selected_ids = [t["id"] for t in selected_types]
    print(f"[popular] selected popularity types: {[t['name'] for t in selected_types]}")

    # Fetch top primitives per type
    primitive_by_type = {}
    max_value_by_type = {}
    for tid in selected_ids:
        try:
            rows = igdb_fetch_popularity_primitives(tid, limit=POP_PRIMITIVE_LIMIT)
            primitive_by_type[tid] = {r["game_id"]: r.get("value", 0) for r in rows if r.get("game_id") is not None}
            max_value_by_type[tid] = max((r.get("value", 0) for r in rows), default=1)
            print(f"[popular] type={tid} entries={len(primitive_by_type[tid])} max={max_value_by_type[tid]}")
        except Exception as e:
            print(f"[popular] failed to fetch primitives for type {tid}: {e}")
            primitive_by_type[tid] = {}
            max_value_by_type[tid] = 1

    # Candidate pool = union of all top primitive game_ids
    candidate_ids = set()
    for d in primitive_by_type.values():
        candidate_ids.update(d.keys())

    # Also include a fallback pool pulled by hypes/follows to catch other signals
    try:
        raw_fallback = igdb_post("games", """
            fields id,hypes,follows,total_rating,first_release_date,updated_at,cover.url,url;
            where cover.height>=0 & themes != 42 & game_type = 0;
            sort hypes desc;
            limit 500;
        """)
        candidate_ids.update([g["id"] for g in raw_fallback])
        # keep max hypes/follows for normalization
        max_hypes = max((g.get("hypes") or 0) for g in raw_fallback) or 1
        max_follows = max((g.get("follows") or 0) for g in raw_fallback) or 1
    except Exception:
        max_hypes = max_follows = 1

    if not candidate_ids:
        print("[popular] no candidates found, falling back")
        return _fallback_popular_section()

    # Fetch full game details in batches
    candidate_list = list(candidate_ids)
    game_details = []
    BATCH = 200
    for chunk in chunked(candidate_list, BATCH):
        try:
            game_details.extend(igdb_fetch_games_by_ids(chunk))
        except Exception as e:
            print("[popular] error fetching game batch:", e)

    # Prepare normalization values for hypes/follows/ratings present in our candidate pool
    max_hypes_in_pool = max((g.get("hypes") or 0) for g in game_details) or 1
    max_follows_in_pool = max((g.get("follows") or 0) for g in game_details) or 1
    max_rating = max((g.get("total_rating") or 0) for g in game_details) or 100.0

    # Weights: tweak to taste. Sum approx 1.0 (primitives share a chunk)
    weight_primitives = 0.6
    weight_hypes = 0.15
    weight_follows = 0.1
    weight_rating = 0.15

    # For primitives, distribute weight_primitives equally among chosen types
    if selected_ids:
        per_primitive_weight = weight_primitives / len(selected_ids)
    else:
        per_primitive_weight = 0

    # helper for recency
    RECENT_RELEASE_DAYS = 180
    MAX_GAME_AGE_DAYS = 1095
    RECENT_UPDATE_DAYS = 45

    def compute_score(g):
        gid = g["id"]
        score = 0.0

        # primitives normalized
        for tid in selected_ids:
            raw_val = primitive_by_type.get(tid, {}).get(gid, 0)
            max_val = max_value_by_type.get(tid, 1) or 1
            score += per_primitive_weight * (raw_val / max_val)

        # hypes/follows/rating normalized
        score += weight_hypes * ((g.get("hypes") or 0) / max_hypes_in_pool)
        score += weight_follows * ((g.get("follows") or 0) / max_follows_in_pool)
        score += weight_rating * ((g.get("total_rating") or 0) / max_rating)

        # recency boosts / penalties
        release_date = g.get("first_release_date")
        updated_at = g.get("updated_at")
        if release_date:
            try:
                days_since_release = (datetime.utcnow() - datetime.utcfromtimestamp(release_date)).days
                if days_since_release <= RECENT_RELEASE_DAYS:
                    score += 0.25  # small fixed boost for recent releases
                elif days_since_release > MAX_GAME_AGE_DAYS:
                    score -= 0.25
            except Exception:
                pass

        if updated_at:
            try:
                days_since_update = (datetime.utcnow() - datetime.utcfromtimestamp(updated_at)).days
                if days_since_update <= RECENT_UPDATE_DAYS:
                    score += 0.15
            except Exception:
                pass

        return score

    # Score and pick the top N
    scored = sorted(game_details, key=compute_score, reverse=True)[:POPULAR_COUNT]
    return [transform_game(g) for g in scored]


def _fallback_popular_section():
    # your original quick fallback (keeps previous behaviour)
    where = "cover.height>=0 & themes != 42 & game_type = 0"
    raw_games = igdb_post("games", f"""
        fields id, name, cover.url, total_rating, storyline, first_release_date, summary, 
               genres.name, player_perspectives.name, game_engines.name, game_modes.name, 
               screenshots.url, url, follows, hypes, updated_at;
        where {where};
        sort hypes desc;
        limit 300;
    """)
    scored = sorted(raw_games, key=lambda g: (g.get("hypes") or 0) + (g.get("follows") or 0), reverse=True)
    top_games = scored[:POPULAR_COUNT]
    return [transform_game(g) for g in top_games]


# -------------------------
# UPLOAD
# -------------------------
def upload_homepage_doc(payload: dict):
    doc_ref = db.collection(COLLECTION_NAME).document(DOCUMENT_NAME)
    doc_ref.set(payload)

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
