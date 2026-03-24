import json
import os
from datetime import datetime, timezone
import math

INPUT_FILE = "market-history.json"
OUTPUT_FILE = "market-correlation.json"

WINDOW = 7

CHOKEPOINT_KEYS = [
    "hormuz_impact",
    "suez_impact",
    "bab_el_mandeb_impact",
    "bosporus_impact",
]


def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except:
        return default


def save_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def get_brent(row):
    return row.get("market_brent") or row.get("brent")


def is_valid_row(row):
    brent = get_brent(row)
    if brent is None:
        return False

    for key in CHOKEPOINT_KEYS:
        if row.get(key) is None:
            return False

    return True


# --- Pearson correlation ---
def correlation(x, y):
    n = len(x)
    if n < 2:
        return None

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    var_x = sum((xi - mean_x) ** 2 for xi in x)
    var_y = sum((yi - mean_y) ** 2 for yi in y)

    if var_x == 0 or var_y == 0:
        return None

    return cov / math.sqrt(var_x * var_y)


def compute_rolling(rows):
    result = []

    for i in range(len(rows)):
        if i < WINDOW - 1:
            continue

        window = rows[i - WINDOW + 1 : i + 1]

        brent_series = [get_brent(r) for r in window]

        row_out = {
            "date": rows[i]["date"]
        }

        for key in CHOKEPOINT_KEYS:
            series = [r[key] for r in window]
            corr = correlation(brent_series, series)

            row_out[f"brent_vs_{key}"] = round(corr, 3) if corr is not None else None

        result.append(row_out)

    return result


def build_latest(rows):
    if not rows:
        return {}

    latest = rows[-1]

    ranked = []
    for key in CHOKEPOINT_KEYS:
        val = latest.get(f"brent_vs_{key}")
        if val is not None:
            ranked.append({
                "key": key,
                "correlation": val
            })

    ranked.sort(key=lambda x: abs(x["correlation"]), reverse=True)

    leader = ranked[0] if ranked else None

    return {
        "leader": leader["key"] if leader else None,
        "leader_correlation": leader["correlation"] if leader else None,
        "ranked": ranked
    }


def main():
    data = load_json(INPUT_FILE, {"rows": []})
    rows = data.get("rows", [])

    # csak tiszta adatok
    clean = [r for r in rows if is_valid_row(r)]

    # dátum szerint rendezés
    clean.sort(key=lambda x: x["date"])

    rolling = compute_rolling(clean)
    latest = build_latest(rolling)

    payload = {
        "meta": {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "source_file": INPUT_FILE,
            "method": "rolling pearson correlation",
            "window_days": WINDOW
        },
        "latest": latest,
        "rows": rolling
    }

    save_json(OUTPUT_FILE, payload)

    print("market-correlation.json kész.")
    print(f"Felhasznált sorok: {len(clean)}")
    print(f"Rolling pontok: {len(rolling)}")
    print(f"Leader: {latest.get('leader')} ({latest.get('leader_correlation')})")


if __name__ == "__main__":
    main()
