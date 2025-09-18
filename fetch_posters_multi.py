import requests
import pandas as pd
from pathlib import Path
import re
import os

# -------------------------------
# CONFIG
# -------------------------------
TMDB_KEY = "96872e54c86124eeb781f961d12e1aaf"
GOOGLE_KEY = "AIzaSyBwUHyEMJA82dXO3mVRXZXpz4-Hkv49dm4"
GOOGLE_CX = "404805dc05d34493a"

INPUT_CSV = Path("missing_posters.csv")
OUTPUT_DIR = Path("static/posters")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def safe_filename(title, year):
    clean_title = re.sub(r'[^a-zA-Z0-9]+', '_', str(title)).strip("_")
    yr = str(year) if pd.notna(year) else "NA"
    return f"{clean_title}_{yr}.jpg"

# -------------------------------
# TMDb fetch
# -------------------------------
def fetch_tmdb(title, year):
    try:
        query = f"{title} {year}"
        url = f"https://api.themoviedb.org/3/search/movie"
        params = {"api_key": TMDB_KEY, "query": title, "year": year}
        r = requests.get(url, params=params, timeout=10).json()
        results = r.get("results", [])
        if not results:
            return None
        poster_path = results[0].get("poster_path")
        if not poster_path:
            return None
        img_url = f"https://image.tmdb.org/t/p/w500{poster_path}"
        return img_url
    except Exception:
        return None

# -------------------------------
# Google CSE fetch
# -------------------------------
def fetch_google(title, year):
    try:
        query = f"{title} {year} movie poster"
        params = {
            "q": query,
            "cx": GOOGLE_CX,
            "key": GOOGLE_KEY,
            "searchType": "image",
            "num": 1
        }
        r = requests.get("https://www.googleapis.com/customsearch/v1", params=params, timeout=10).json()
        items = r.get("items", [])
        if not items:
            return None
        return items[0].get("link")
    except Exception:
        return None

# -------------------------------
# Download helper
# -------------------------------
def download_image(img_url, filename):
    try:
        img_data = requests.get(img_url, timeout=10).content
        filepath = OUTPUT_DIR / filename
        with open(filepath, "wb") as f:
            f.write(img_data)
        return str(filepath)
    except Exception:
        return None

# -------------------------------
# MAIN
# -------------------------------
def main():
    if not INPUT_CSV.exists():
        print("Missing input CSV!")
        return

    df = pd.read_csv(INPUT_CSV)
    successes, failures = [], []

    for idx, row in df.iterrows():
        title, year = row["Title"], row["Year"]
        fname = safe_filename(title, year)
        filepath = OUTPUT_DIR / fname
        if filepath.exists():
            print(f"✅ Already have: {fname}")
            continue

        print(f"\n🔎 {idx+1}/{len(df)} Searching: {title} ({year})")

        # Try TMDb first
        img_url = fetch_tmdb(title, year)
        if not img_url:
            # Try Google
            img_url = fetch_google(title, year)

        if img_url:
            saved = download_image(img_url, fname)
            if saved:
                print(f"✅ Saved: {saved}")
                successes.append(title)
            else:
                print(f"❌ Failed to save image for {title}")
                failures.append(title)
        else:
            print(f"❌ No poster found for {title}")
            failures.append(title)

    print(f"\n🎬 Finished: {len(successes)} posters saved, {len(failures)} still missing.")
    if failures:
        with open("still_missing_multi.txt", "w") as f:
            f.write("\n".join(failures))
        print("List saved to still_missing_multi.txt")

if __name__ == "__main__":
    main()
