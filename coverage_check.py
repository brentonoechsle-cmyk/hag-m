import pandas as pd
import requests
import time
from collections import Counter
import matplotlib.pyplot as plt

# 🔑 Your TMDb API key here
TMDB_API_KEY = "96872e54c86124eeb781f961d12e1aaf"

# JustWatch API endpoint (unofficial)
JUSTWATCH_URL = "https://apis.justwatch.com/content/titles/en_US/popular"

# Load your CSV
df = pd.read_csv("rotten_tomatoes_min85.csv")
df["year"] = pd.to_datetime(df["original_release_date"], errors="coerce").dt.year

results = []
error_messages = []

# 🎬 Expanded mapping of JustWatch provider IDs → names
JUSTWATCH_PROVIDERS = {
    2: "Amazon",
    3: "Google Play Movies",
    7: "iTunes",
    8: "Netflix",
    9: "Amazon Prime Video",
    11: "Vudu",
    15: "Hulu",
    192: "YouTube",
    96: "Criterion Channel",
    99: "Kanopy",
    100: "Paramount+",
    119: "Amazon Video",
    122: "Showtime",
    177: "Starz",
    179: "AMC+",
    184: "Shudder",
    188: "Peacock Premium",
    191: "Peacock TV",
    257: "Discovery+",
    283: "Peacock",
    289: "Sling TV",
    296: "Apple TV",
    350: "Apple TV+",
    337: "Disney+",
    384: "HBO Max",
    486: "Crunchyroll",
    531: "Max",
    619: "Pluto TV",
    746: "Netflix Kids",
}

def search_justwatch(title, year):
    """Search JustWatch for a movie by title/year"""
    headers = {"User-Agent": "Mozilla/5.0"}
    payload = {"query": title, "page_size": 1, "page": 1}
    if not pd.isna(year):
        payload["release_year_from"] = int(year)
        payload["release_year_until"] = int(year)

    r = requests.post(JUSTWATCH_URL, json=payload, headers=headers)
    try:
        data = r.json()
    except ValueError:
        msg = f"⚠️ JustWatch returned non-JSON for {title} ({year})"
        error_messages.append(msg)
        return []

    if "items" in data and data["items"]:
        item = data["items"][0]
        providers = []
        if "offers" in item:
            ids = {offer["provider_id"] for offer in item["offers"]}
            providers = [JUSTWATCH_PROVIDERS.get(i, f"Provider_{i}") for i in ids]
        return providers
    return []

def get_tmdb_id(title, year):
    url = "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": TMDB_API_KEY, "query": title, "year": int(year) if not pd.isna(year) else ""}
    r = requests.get(url, params=params).json()
    if r.get("results"):
        return r["results"][0]["id"]
    return None

def get_tmdb_providers(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/watch/providers"
    params = {"api_key": TMDB_API_KEY}
    r = requests.get(url, params=params).json()
    providers = []
    if "results" in r and "US" in r["results"]:
        for ptype in ["flatrate", "rent", "buy"]:
            if ptype in r["results"]["US"]:
                providers.extend([p["provider_name"] for p in r["results"]["US"][ptype]])
    return list(set(providers))

# Process all movies
for _, row in df.iterrows():
    title, year = row["movie_title"], row["year"]

    providers = search_justwatch(title, year)
    source = "JustWatch"

    if not providers:
        tmdb_id = get_tmdb_id(title, year)
        if tmdb_id:
            providers = get_tmdb_providers(tmdb_id)
            source = "TMDb"

    if not providers:
        source = "None"

    results.append({
        "title": title,
        "year": year,
        "providers": ", ".join(providers),
        "source": source
    })

    time.sleep(0.25)  # avoid hammering APIs

# Save results
out_df = pd.DataFrame(results)
out_df.to_csv("streaming_coverage.csv", index=False)

# Save errors
with open("justwatch_errors.txt", "w", encoding="utf-8") as f:
    for msg in error_messages:
        f.write(msg + "\n")

# Summary
coverage_count = sum(1 for r in results if r["providers"])
unique_errors = len(set(error_messages))
total_movies = len(results)
print(f"✅ Found streaming availability for {coverage_count}/{total_movies} movies")
print(f"⚠️ JustWatch failed {len(error_messages)} times, affecting {unique_errors} unique titles")

# 📊 Provider breakdown
all_providers = []
for r in results:
    if r["providers"]:
        all_providers.extend(r["providers"].split(", "))

provider_counts = Counter(all_providers)

print("\n📊 Coverage by Provider:")
for provider, count in provider_counts.most_common():
    percent = (count / total_movies) * 100
    print(f"  {provider}: {count} movies ({percent:.1f}%)")

pd.DataFrame(
    [(provider, count, (count / total_movies) * 100) for provider, count in provider_counts.items()],
    columns=["provider", "count", "percent"]
).sort_values("count", ascending=False).to_csv("provider_summary.csv", index=False)

# 📊 Multi-provider availability breakdown
provider_counts_per_movie = []
for r in results:
    if r["providers"]:
        num_providers = len(r["providers"].split(", "))
        provider_counts_per_movie.append(num_providers)

multi_counts = Counter(provider_counts_per_movie)

print("\n📊 Multi-Provider Availability:")
for n, count in sorted(multi_counts.items()):
    percent = (count / total_movies) * 100
    print(f"  {count} movies available on {n} provider(s) ({percent:.1f}%)")

pd.DataFrame(
    [(n, count, (count / total_movies) * 100) for n, count in sorted(multi_counts.items())],
    columns=["num_providers", "count", "percent"]
).to_csv("multi_provider_summary.csv", index=False)

# 📈 Visualization: Provider Coverage
if provider_counts:
    providers, counts = zip(*provider_counts.most_common(15))
    percents = [(c / total_movies) * 100 for c in counts]
    plt.figure(figsize=(10, 6))
    bars = plt.barh(providers, counts, color="skyblue")
    plt.xlabel("Number of Movies")
    plt.title("Streaming Coverage by Provider (Top 15)")
    plt.gca().invert_yaxis()

    # add labels
    for bar, c, p in zip(bars, counts, percents):
        plt.text(bar.get_width() + 2, bar.get_y() + bar.get_height()/2,
                 f"{c} ({p:.1f}%)", va="center")

    plt.tight_layout()
    plt.savefig("provider_coverage.png")
    plt.close()
    print("📈 Saved provider coverage chart → provider_coverage.png")

# 📈 Visualization: Multi-provider Availability
if multi_counts:
    nums, counts = zip(*sorted(multi_counts.items()))
    percents = [(c / total_movies) * 100 for c in counts]
    plt.figure(figsize=(8, 5))
    bars = plt.bar(nums, counts, color="lightgreen")
    plt.xlabel("Number of Providers per Movie")
    plt.ylabel("Number of Movies")
    plt.title("Multi-Provider Availability")
    plt.xticks(nums)

    for bar, c, p in zip(bars, counts, percents):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                 f"{c} ({p:.1f}%)", ha="center")

    plt.tight_layout()
    plt.savefig("multi_provider_coverage.png")
    plt.close()
    print("📈 Saved multi-provider coverage chart → multi_provider_coverage.png")
