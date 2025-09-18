import pandas as pd
from pathlib import Path

# Import your existing helpers from app.py
from app import load_movies, get_poster_and_plot

def scan_missing_posters():
    df, total_raw = load_movies()
    missing = []

    for idx, row in df.iterrows():
        enrich = get_poster_and_plot(row["Title"], row["Year"])
        poster = enrich.get("Poster")
        if not poster or "placeholder.png" in poster:
            missing.append({
                "Title": row["Title"],
                "Year": row["Year"]
            })

        # Progress feedback
        if idx % 50 == 0:
            print(f"Checked {idx}/{total_raw}...")

    print(f"\nTotal movies checked: {total_raw}")
    print(f"Movies still missing posters: {len(missing)}")

    # Save results to CSV
    output_path = Path("missing_posters.csv")
    pd.DataFrame(missing).to_csv(output_path, index=False)
    print(f"Saved list to {output_path.resolve()}")

if __name__ == "__main__":
    scan_missing_posters()
