from flask import Flask, render_template, request, jsonify, render_template_string, url_for
import pandas as pd
import requests, json, time, re, random
from pathlib import Path

app = Flask(__name__)

# ----- Static cache headers -----
@app.after_request
def add_header(response):
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=31536000"
    return response

# ----- Settings -----
TMDB_API_KEY   = "96872e54c86124eeb781f961d12e1aaf"
OMDB_API_KEY   = "bebf6ae6"
INPUT_CSV      = Path("rotten_tomatoes_min85.csv")
CACHE_PATH     = Path("movie_cache.json")
POSTER_DIR     = Path("static/posters")
BAG_PATH       = Path("shuffle_bag.json")
STREAMING_CSV  = Path("streaming_coverage.csv")

# =========================
# Provider priority (Top 10 from provider_summary.csv)
# Used to rank & trim to max 5 per movie
# =========================
PROVIDER_PRIORITY = {
    "Apple TV": 1,
    "Amazon": 2,               # (family bucket for all Amazon variants)
    "Fandango At Home": 3,
    "Google Play Movies": 4,
    "YouTube": 5,
    "Spectrum On Demand": 6,
    "Criterion Channel": 7,
    "HBO Max": 8,              # (we’ll also map “Max” into this family)
    "Amazon Prime Video": 9,   # (kept for back-compat; family map collapses into "Amazon")
    "Philo": 10,
}
DEFAULT_PRIORITY = 999

# =========================
# Provider family mapping
# Collapses variants into one family (e.g., Amazon*, Apple TV+, Max/HBO Max, etc.)
# =========================
def provider_family(name: str) -> str:
    if not name:
        return ""
    n = name.strip().lower()

    # Amazon family
    if any(s in n for s in [
        "amazon video", "amazon prime video", "amazon prime video with ads",
        "mgm plus", "mgm+ amazon", "mgm plus roku premium", "amc+ amazon",
        "hbo max amazon", "starz amazon", "paramount+ amazon", "apple tv plus amazon",
        "fandor amazon", "criterion channel amazon", "starz roku premium channel"  # mixed marketplaces often via Amazon/Roku
    ]):
        return "Amazon"

    # Apple TV / Apple TV+ family
    if "apple tv" in n or "apple tv+" in n or "apple tv plus" in n:
        return "Apple TV"

    # Google family
    if "google play" in n:
        return "Google Play Movies"

    # YouTube family
    if "youtube" in n:
        return "YouTube"

    # HBO Max / Max family (normalize to HBO Max label)
    if "hbo max" in n or n == "max":
        return "HBO Max"

    # Spectrum
    if "spectrum on demand" in n:
        return "Spectrum On Demand"

    # Criterion
    if "criterion channel" in n:
        return "Criterion Channel"

    # Fandango
    if "fandango" in n:
        return "Fandango At Home"

    # Philo
    if "philo" in n:
        return "Philo"

    # Otherwise keep the canonical display name as-is
    # (helps smaller services remain distinct)
    return name.strip()

def provider_priority_for(name: str) -> int:
    fam = provider_family(name)
    return PROVIDER_PRIORITY.get(fam, PROVIDER_PRIORITY.get(name, DEFAULT_PRIORITY))

def dedupe_and_prioritize_by_family(providers: list, limit: int = 5) -> list:
    """
    - Collapse providers into families so that variants do not crowd the list.
    - Prefer entries that have a logo; then prefer ones with a URL.
    - Rank by global priority, then by name for stability.
    - Return at most `limit` providers.
    """
    by_family = {}
    for p in providers:
        fam = provider_family(p.get("name"))
        if not fam:
            continue
        # Choose the "best" representative inside each family:
        # 1) has logo preferred
        # 2) has url preferred
        # 3) otherwise keep the first but allow replacement if better
        curr = by_family.get(fam)
        better = False
        if curr is None:
            better = True
        else:
            p_has_logo = bool(p.get("logo"))
            c_has_logo = bool(curr.get("logo"))
            p_has_url  = bool(p.get("url"))
            c_has_url  = bool(curr.get("url"))
            if p_has_logo and not c_has_logo:
                better = True
            elif p_has_logo == c_has_logo and p_has_url and not c_has_url:
                better = True
        if better:
            # Store with family display name to keep front-end labels clean
            chosen = dict(p)
            chosen["name"] = fam
            by_family[fam] = chosen

    # Sort by global priority (lower is better), then name
    ranked = sorted(
        by_family.values(),
        key=lambda x: (provider_priority_for(x.get("name")), x.get("name") or "")
    )
    return ranked[:limit]

# =========================
# Utilities / Normalizers
# =========================
def norm_year(y):
    """Return normalized year as string ('1999'), or '' if unknown/not numeric."""
    if y is None or (isinstance(y, float) and pd.isna(y)):
        return ""
    try:
        return str(int(float(y)))
    except Exception:
        s = str(y).strip()
        return s if s.isdigit() and len(s) == 4 else ""

_ws_re = re.compile(r"\s+")

def norm_title(t):
    """Lowercased, single-spaced title for key matching."""
    if t is None:
        return ""
    s = str(t).strip().lower()
    s = _ws_re.sub(" ", s)
    return s

def key_for(title, year):
    """Key used for cache & streaming dict."""
    return f"{norm_title(title)}|||{norm_year(year)}"

def safe_filename(title, year):
    clean_title = re.sub(r'[^a-zA-Z0-9]+', '_', str(title)).strip("_").lower()
    yr = norm_year(year) or "na"
    return f"{clean_title}_{yr}.jpg"

# =========================
# JSON cache helpers
# =========================
def load_cache():
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache_obj: dict):
    CACHE_PATH.write_text(json.dumps(cache_obj, indent=2, ensure_ascii=False), encoding="utf-8")

cache = load_cache()

# =========================
# Shuffle bag helpers
# =========================
def load_bag(movie_ids):
    if BAG_PATH.exists():
        try:
            bag = json.loads(BAG_PATH.read_text(encoding="utf-8"))
        except Exception:
            bag = []
    else:
        bag = []
    if not bag:
        bag = list(movie_ids)
        random.shuffle(bag)
        BAG_PATH.write_text(json.dumps(bag), encoding="utf-8")
    return bag

def save_bag(bag):
    BAG_PATH.write_text(json.dumps(bag), encoding="utf-8")

def draw_from_bag(filtered_df):
    ids = list(filtered_df.index)
    bag = load_bag(ids)
    bag = [i for i in bag if i in ids]
    if not bag:
        bag = list(ids)
        random.shuffle(bag)
    idx = bag.pop(0)
    save_bag(bag)
    return filtered_df.loc[idx]

# =========================
# Load streaming CSV
# =========================
STREAMING_DATA = {}
if STREAMING_CSV.exists():
    try:
        df_stream = pd.read_csv(STREAMING_CSV)
        for _, row in df_stream.iterrows():
            t = norm_title(row.get("title", ""))
            y = norm_year(row.get("year", ""))
            prov_raw = row.get("providers", "")
            providers = []
            if pd.notna(prov_raw) and str(prov_raw).strip():
                for p in str(prov_raw).split(","):
                    name = p.strip()
                    if name:
                        # CSV has no logos/URLs, we’ll add those only if TMDb provides
                        providers.append({"name": name, "logo": None, "url": None})
            if t:  # only store if we have a title
                STREAMING_DATA[f"{t}|||{y}"] = providers
        print(f"Loaded {len(STREAMING_DATA)} streaming entries from CSV")
    except Exception as e:
        print("Failed to load streaming CSV:", e)
else:
    print("streaming_coverage.csv not found; continuing without CSV providers.")

def get_csv_providers(title, year):
    """
    Try to find providers from CSV using robust matching:
    1) exact title+year
    2) title with ±1 year (if year provided)
    3) title with empty year (any year)
    Returns a list of dicts {name, logo=None, url=None}
    """
    t = norm_title(title)
    y = norm_year(year)
    # exact
    k_exact = f"{t}|||{y}"
    if k_exact in STREAMING_DATA:
        return STREAMING_DATA[k_exact]
    # ±1 year
    if y.isdigit():
        for off in (-1, 1):
            y_alt = str(int(y) + off)
            k_alt = f"{t}|||{y_alt}"
            if k_alt in STREAMING_DATA:
                return STREAMING_DATA[k_alt]
    # any year
    k_any = f"{t}|||"
    return STREAMING_DATA.get(k_any, [])

def merge_providers(primary, fallback):
    """
    Merge two provider lists (dicts with name/logo/url) and dedupe by exact name.
    Preference to 'primary' entries (likely with logos/urls).
    """
    out, seen = [], set()
    for p in primary:
        nm = (p.get("name") or "").strip()
        if nm and nm not in seen:
            out.append(p); seen.add(nm)
    for p in fallback:
        nm = (p.get("name") or "").strip()
        if nm and nm not in seen:
            out.append(p); seen.add(nm)
    return out

# =========================
# Poster/plot + streaming
# =========================
def get_movie_details(title, year=None):
    ykey = norm_year(year)
    ckey = key_for(title, ykey)

    # Cached?
    if ckey in cache:
        data = cache[ckey]
        # If cached streaming is empty, attempt CSV enrichment
        if not data.get("Streaming"):
            csv_prov = get_csv_providers(title, ykey)
            if csv_prov:
                merged = merge_providers(data.get("Streaming", []), csv_prov)
                # Family dedupe + priority + limit to 5
                merged = dedupe_and_prioritize_by_family(merged, limit=5)
                data["Streaming"] = merged
                cache[ckey] = data
                save_cache(cache)
        return data

    data = {"Poster": None, "Plot": "", "Streaming": []}

    # Local poster lookup
    fname = safe_filename(title, ykey)
    local_path = POSTER_DIR / fname
    if local_path.exists():
        data["Poster"] = f"/static/posters/{fname}"

    # TMDb search
    try:
        params = {"api_key": TMDB_API_KEY, "query": title}
        if ykey:
            params["year"] = int(ykey)
        r = requests.get("https://api.themoviedb.org/3/search/movie", params=params, timeout=10)
        res = r.json()
        if res.get("results"):
            movie = res["results"][0]
            mid = movie["id"]
            if movie.get("poster_path") and not data["Poster"]:
                data["Poster"] = f"https://image.tmdb.org/t/p/w500{movie['poster_path']}"

            # details for plot
            det = requests.get(
                f"https://api.themoviedb.org/3/movie/{mid}",
                params={"api_key": TMDB_API_KEY}, timeout=10
            ).json()
            if det.get("overview"):
                data["Plot"] = det["overview"]

            # Streaming availability (TMDb watch/providers)
            prov = requests.get(
                f"https://api.themoviedb.org/3/movie/{mid}/watch/providers",
                params={"api_key": TMDB_API_KEY}, timeout=10
            ).json()

            if "results" in prov and "US" in prov["results"]:
                us = prov["results"]["US"]
                link = us.get("link")
                tmdb_provs = []
                for ptype in ["flatrate", "rent", "buy"]:
                    if ptype in us:
                        for p in us[ptype]:
                            tmdb_provs.append({
                                "name": p.get("provider_name"),
                                "logo": f"https://image.tmdb.org/t/p/original{p.get('logo_path')}" if p.get("logo_path") else None,
                                "url": link
                            })
                # collapse family + priority + limit
                tmdb_provs = dedupe_and_prioritize_by_family(tmdb_provs, limit=5)
                data["Streaming"] = tmdb_provs

    except Exception as e:
        print("TMDb error:", e)

    # OMDb fallback (poster/plot)
    if not data["Poster"] or not data["Plot"]:
        try:
            omdb_url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={title}"
            if ykey:
                omdb_url += f"&y={ykey}"
            r = requests.get(omdb_url, timeout=10).json()
            if not data["Poster"] and r.get("Poster") and r["Poster"] != "N/A":
                data["Poster"] = r["Poster"]
            if not data["Plot"] and r.get("Plot") and r["Plot"] != "N/A":
                data["Plot"] = r["Plot"]
        except Exception as e:
            print("OMDb error:", e)

    if not data["Poster"]:
        data["Poster"] = "/static/placeholder.png"

    # Merge CSV providers and re-dedupe/limit
    if True:
        csv_prov = get_csv_providers(title, ykey)
        if csv_prov:
            merged = merge_providers(data.get("Streaming", []), csv_prov)
            merged = dedupe_and_prioritize_by_family(merged, limit=5)
            data["Streaming"] = merged

    cache[ckey] = data
    save_cache(cache)
    time.sleep(0.18)  # be nice to external APIs
    return data

# =========================
# CSV normalization
# =========================
def parse_runtime_to_minutes(runtime_val):
    if pd.isna(runtime_val):
        return pd.NA
    s = str(runtime_val).strip().lower()
    m = re.search(r"(\d+)\s*min", s)
    if m:
        return int(m.group(1))
    if s.isdigit():
        return int(s)
    h = re.search(r"(\d+)\s*h", s)
    mins = re.search(r"(\d+)\s*m", s)
    if h and mins:
        return int(h.group(1)) * 60 + int(mins.group(1))
    if h and not mins:
        return int(h.group(1)) * 60
    m2 = re.search(r"(\d+)", s)
    if m2:
        return int(m2.group(1))
    return pd.NA

def extract_year(date_val):
    if pd.isna(date_val):
        return pd.NA
    m = re.search(r"(\d{4})", str(date_val))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return pd.NA
    return pd.NA

_MOVIES_DF = None
_TOTAL_RAW = 0

def load_movies():
    global _MOVIES_DF, _TOTAL_RAW
    if _MOVIES_DF is not None:
        return _MOVIES_DF, _TOTAL_RAW

    if not INPUT_CSV.exists():
        _MOVIES_DF, _TOTAL_RAW = pd.DataFrame(), 0
        return _MOVIES_DF, _TOTAL_RAW

    df = pd.read_csv(INPUT_CSV, on_bad_lines="skip", low_memory=False)
    df["Title"]         = df["movie_title"].astype(str).str.strip()
    df["ContentRating"] = df.get("content_rating", "").astype(str).str.strip()
    df["Genres"]        = df.get("genres", "").astype(str)
    df["Directors"]     = df.get("directors", "").astype(str)
    df["RuntimeMin"]    = df.get("runtime", pd.NA).apply(parse_runtime_to_minutes)
    df["Year"]          = df.get("original_release_date", pd.NA).apply(extract_year)
    df["CriticScore"]   = pd.to_numeric(df.get("tomatometer_rating", pd.NA), errors="coerce")
    df["AudienceScore"] = pd.to_numeric(df.get("audience_rating", pd.NA), errors="coerce")

    cols = [
        "Title", "Year", "ContentRating", "Genres", "Directors",
        "RuntimeMin", "CriticScore", "AudienceScore"
    ]
    df = df[cols].copy()
    _MOVIES_DF = df
    _TOTAL_RAW = len(df)
    return _MOVIES_DF, _TOTAL_RAW

# =========================
# Routes
# =========================
@app.route("/", methods=["GET", "POST"])
def index():
    df, total_raw = load_movies()

    all_rated = ["Any"] + sorted({r.strip() for r in df["ContentRating"].fillna("") if r.strip()})
    all_genres = ["Any"] + sorted({
        g.strip()
        for row in df["Genres"].fillna("")
        for g in str(row).split(",")
        if g.strip()
    })

    selected_rated   = request.form.get("rated", "Any")
    selected_genre   = request.form.get("genre", "Any")
    selected_runtime = request.form.get("runtime", "Any")

    filtered = df.copy()
    if selected_rated != "Any":
        filtered = filtered[filtered["ContentRating"].fillna("").str.lower() == selected_rated.lower()]
    if selected_genre != "Any":
        filtered = filtered[filtered["Genres"].fillna("").str.contains(fr"\b{re.escape(selected_genre)}\b", case=False, regex=True)]
    if selected_runtime != "Any":
        rt = pd.to_numeric(filtered["RuntimeMin"], errors="coerce")
        if selected_runtime == "<90":
            filtered = filtered[rt.fillna(99999) < 90]
        elif selected_runtime == "90-120":
            filtered = filtered[(rt.fillna(-1) >= 90) & (rt <= 120)]
        elif selected_runtime == "120-150":
            filtered = filtered[(rt.fillna(-1) >= 120) & (rt <= 150)]
        elif selected_runtime == "150+":
            filtered = filtered[rt.fillna(-1) >= 150]

    count_after_ui = len(filtered)

    movie = None
    if request.method == "POST" and request.form.get("random") == "1" and count_after_ui > 0:
        row = draw_from_bag(filtered)
        enrich = get_movie_details(row["Title"], row["Year"])
        movie = {
            "Title": row["Title"],
            "Year": int(row["Year"]) if pd.notna(row["Year"]) else "",
            "ContentRating": row["ContentRating"],
            "Genres": row["Genres"],
            "Directors": row["Directors"],
            "RuntimeMin": int(row["RuntimeMin"]) if pd.notna(row["RuntimeMin"]) else None,
            "CriticScore": float(row["CriticScore"]) if pd.notna(row["CriticScore"]) else None,
            "AudienceScore": float(row["AudienceScore"]) if pd.notna(row["AudienceScore"]) else None,
            "Poster": enrich.get("Poster"),
            "Plot": enrich.get("Plot", ""),
            "Streaming": enrich.get("Streaming", [])
        }

    return render_template(
        "index.html",
        all_genres=all_genres,
        all_rated=all_rated,
        selected_rated=selected_rated,
        selected_genre=selected_genre,
        selected_runtime=selected_runtime,
        total_raw=total_raw,
        count=count_after_ui,
        movie=movie
    )

@app.route("/random_movie", methods=["POST"])
def random_movie():
    df, total_raw = load_movies()

    selected_rated   = request.form.get("rated", "Any")
    selected_genre   = request.form.get("genre", "Any")
    selected_runtime = request.form.get("runtime", "Any")

    filtered = df.copy()
    if selected_rated != "Any":
        filtered = filtered[filtered["ContentRating"].fillna("").str.lower() == selected_rated.lower()]
    if selected_genre != "Any":
        filtered = filtered[filtered["Genres"].fillna("").str.contains(fr"\b{re.escape(selected_genre)}\b", case=False, regex=True)]
    if selected_runtime != "Any":
        rt = pd.to_numeric(filtered["RuntimeMin"], errors="coerce")
        if selected_runtime == "<90":
            filtered = filtered[rt.fillna(99999) < 90]
        elif selected_runtime == "90-120":
            filtered = filtered[(rt.fillna(-1) >= 90) & (rt <= 120)]
        elif selected_runtime == "120-150":
            filtered = filtered[(rt.fillna(-1) >= 120) & (rt <= 150)]
        elif selected_runtime == "150+":
            filtered = filtered[rt.fillna(-1) >= 150]

    count_after_ui = len(filtered)
    if count_after_ui == 0:
        return jsonify({"html": "<p>No movies match your filters.</p>", "count": 0, "total": total_raw})

    row = draw_from_bag(filtered)
    enrich = get_movie_details(row["Title"], row["Year"])
    movie = {
        "Title": row["Title"],
        "Year": int(row["Year"]) if pd.notna(row["Year"]) else "",
        "ContentRating": row["ContentRating"],
        "Genres": row["Genres"],
        "Directors": row["Directors"],
        "RuntimeMin": int(row["RuntimeMin"]) if pd.notna(row["RuntimeMin"]) else None,
        "CriticScore": float(row["CriticScore"]) if pd.notna(row["CriticScore"]) else None,
        "AudienceScore": float(row["AudienceScore"]) if pd.notna(row["AudienceScore"]) else None,
        "Poster": enrich.get("Poster"),
        "Plot": enrich.get("Plot", ""),
        "Streaming": enrich.get("Streaming", [])
    }

    html = render_template_string("""{% include '_movie_card.html' %}""", movie=movie)
    return jsonify({"html": html, "count": count_after_ui, "total": total_raw})

# =========================
# Entry
# =========================
if __name__ == "__main__":
    app.run(debug=True)
