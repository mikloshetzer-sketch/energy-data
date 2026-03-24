import json
import math
import os
from datetime import datetime, timezone

INPUT_FILE = "market-history.json"
OUTPUT_FILE = "market-correlation.json"

WINDOW = 7

CHOKEPOINT_KEYS = [
    "hormuz_impact",
    "suez_impact",
    "bab_el_mandeb_impact",
    "bosporus_impact",
]

DISPLAY_NAMES = {
    "hormuz_impact": "Hormuz",
    "suez_impact": "Suez",
    "bab_el_mandeb_impact": "Bab el-Mandeb",
    "bosporus_impact": "Bosporus",
}

MAX_LAG_DAYS = 3


def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
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
                "label": DISPLAY_NAMES.get(key, key),
                "correlation": val
            })

    ranked.sort(key=lambda x: abs(x["correlation"]), reverse=True)
    leader = ranked[0] if ranked else None

    return {
        "leader": leader["key"] if leader else None,
        "leader_label": leader["label"] if leader else None,
        "leader_correlation": leader["correlation"] if leader else None,
        "ranked": ranked
    }


def compute_series_correlation(history_rows, key, lag_days):
    paired_x = []
    paired_y = []

    for i in range(len(history_rows)):
        j = i - lag_days

        if j < 0 or j >= len(history_rows):
            continue

        brent = get_brent(history_rows[i])
        chokepoint = history_rows[j].get(key)

        if brent is None or chokepoint is None:
            continue

        paired_x.append(brent)
        paired_y.append(chokepoint)

    return correlation(paired_x, paired_y)


def classify_signal(best_lag, best_corr):
    strength = abs(best_corr) if best_corr is not None else 0.0

    if strength < 0.15:
        return "weak"

    if best_lag < 0:
        return "leading"

    if best_lag > 0:
        return "lagging"

    return "synchronous"


def build_leader_signal(history_rows):
    results = {}

    for key in CHOKEPOINT_KEYS:
        best_lag = 0
        best_corr = None

        for lag in range(-MAX_LAG_DAYS, MAX_LAG_DAYS + 1):
            c = compute_series_correlation(history_rows, key, lag)
            if c is None:
                continue

            if best_corr is None or abs(c) > abs(best_corr):
                best_corr = c
                best_lag = lag

        label = DISPLAY_NAMES.get(key, key)
        signal = classify_signal(best_lag, best_corr)

        results[key] = {
            "label": label,
            "lag_days": best_lag,
            "correlation": round(best_corr, 3) if best_corr is not None else None,
            "signal": signal
        }

    return results


def build_leader_summary(leader_signal):
    if not leader_signal:
        return None

    ranked = sorted(
        leader_signal.items(),
        key=lambda item: abs(item[1].get("correlation") or 0),
        reverse=True
    )

    top_key, top_val = ranked[0]

    lag = top_val.get("lag_days")
    signal = top_val.get("signal")
    label = top_val.get("label")
    corr = top_val.get("correlation")

    if signal == "leading":
        text = f"{label} leads Brent by {abs(lag)} day(s)"
    elif signal == "lagging":
        text = f"{label} lags Brent by {abs(lag)} day(s)"
    elif signal == "synchronous":
        text = f"{label} moves broadly in sync with Brent"
    else:
        text = f"{label} shows only weak relationship with Brent"

    return {
        "key": top_key,
        "label": label,
        "lag_days": lag,
        "correlation": corr,
        "signal": signal,
        "text": text
    }


def main():
    data = load_json(INPUT_FILE, {"rows": []})
    rows = data.get("rows", [])

    clean = [r for r in rows if is_valid_row(r)]
    clean.sort(key=lambda x: x["date"])

    rolling = compute_rolling(clean)
    latest = build_latest(rolling)
    leader_signal = build_leader_signal(clean)
    leader_summary = build_leader_summary(leader_signal)

    payload = {
        "meta": {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "source_file": INPUT_FILE,
            "method": "rolling pearson correlation + lead-lag scan",
            "window_days": WINDOW,
            "max_lag_days": MAX_LAG_DAYS
        },
        "latest": latest,
        "leader_signal": leader_signal,
        "leader_summary": leader_summary,
        "rows": rolling
    }

    save_json(OUTPUT_FILE, payload)

    print("market-correlation.json kész.")
    print(f"Felhasznált sorok: {len(clean)}")
    print(f"Rolling pontok: {len(rolling)}")
    if leader_summary:
        print(f"Leader summary: {leader_summary['text']}")
    else:
        print("Leader summary: n/a")


if __name__ == "__main__":
    main()
