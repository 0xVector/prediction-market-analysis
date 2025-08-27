#!/usr/bin/env python3
import io, requests, pandas as pd, os

BASE = "https://data.brier.fyi"
PAGE = 5000
H = {"Accept": "text/csv", "Prefer": "count=exact"}
SLEEP = 0.2
OUT_DIR = "data"
OUT_FILE = "markets_brier7d.csv"


def fetch_all():
    params = {
        "resolution": "in.(0,1)",
        "score_type": "eq.brier-before-close-days-7",
        "select": ",".join([
            "market_id", "market_title", "market_url", "platform:platform_slug",
            "category:category_slug", "question_id",
            "open_datetime", "close_datetime", "duration_days",
            "traders_count", "volume_usd", "question_invert",
            "resolution", "score_brier7d:score", "grade"
        ])
    }
    print(f"Fetching market scores...")
    r = requests.get(f"{BASE}/market_scores_details", params=params, headers=H, timeout=60)
    r.raise_for_status()
    print("Done.")
    df = pd.read_csv(io.StringIO(r.text.strip()))
    print(f"Fetched {len(df)} rows.")

    print("Fetching probabilities at criterion...")
    r = requests.get(f"{BASE}/criterion_probabilities",
                     params={"criterion_type": "eq.before-close-days-7", "select": "market_id,probability_7d:prob"},
                     headers=H, timeout=60)
    r.raise_for_status()
    print("Done.")
    df_prob = pd.read_csv(io.StringIO(r.text.strip()))
    print(f"Fetched {len(df_prob)} rows.")

    df = df.merge(df_prob, on="market_id", how="left")
    print(f"Merged probabilities, resulting in {len(df)} rows.")

    print(f"Saving to {OUT_DIR}/{OUT_FILE} ...")
    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(f"{OUT_DIR}/{OUT_FILE}", index=False)
    print("Done.")


if __name__ == "__main__":
    fetch_all()
