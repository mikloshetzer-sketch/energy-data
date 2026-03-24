import json
import os
from datetime import datetime, timedelta

HISTORY_FILE = "market-history.json"
OUTPUT_FILE = "market-history.json"
EVENTS_FILE = "me-security-events.json"

WINDOW_DAYS = 7

CAT_W = {
    "military": 3.0,
    "security": 2.0,
    "political": 1.0,
    "other": 0.5,
}

ISW_MULT = 1.3


def safe_load_json(path, default):
    if not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            content = f.read().strip()
            if not content:
                return default
            return json.loads(content)
    except Exception as e:
        print(f"Figyelmeztetés: nem sikerült beolvasni: {path} | Hiba: {e}")
        return default


def save_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def round1(value):
    return round(value, 1)


def clamp(value, min_value, max_value):
    return max(min_value, min(value, max_value))


def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def norm_cat(category):
    category = (category or "other").strip().lower()
    return category if category in CAT_W else "other"


def source_type(event):
    src_type = (((event.get("source") or {}).get("type")) or "news").strip().lower()
    return "isw" if src_type == "isw" else "news"


def recency_weight(age_days, window_days):
    if window_days <= 1:
        return 1.0
    ratio = age_days / (window_days - 1)
    return 1.0 - 0.6 * ratio  # newest=1.0, oldest=0.4


def event_risk_score(event, age_days, window_days):
    category = norm_cat(event.get("category"))
    category_weight = CAT_W[category]
    source_weight = ISW_MULT if source_type(event) == "isw" else 1.0
    time_weight = recency_weight(age_days, window_days)
    return category_weight * source_weight * time_weight


def compute_osint_total_risk(events, target_date, window_days):
    period_start = target_date - timedelta(days=window_days - 1)
    total_risk = 0.0
    total_events = 0

    for event in events:
        date_str = event.get("date")
        if not date_str:
            continue

        try:
            event_date = parse_date(date_str)
        except Exception:
            continue

        if period_start <= event_date <= target_date:
            age_days = (target_date - event_date).days
            total_risk += event_risk_score(event, age_days, window_days)
            total_events += 1

    normalized_risk_score = min(100.0, round1((total_risk / 250.0) * 100.0))

    return {
        "total_risk": total_risk,
        "normalized_risk_score": normalized_risk_score,
        "total_events": total_events,
    }


def blend_middle_east_score(structural_score, osint_score):
    return round1(clamp((0.55 * structural_score) + (0.45 * osint_score), 0, 100))


def blend_global_trade_score(structural_score, osint_score):
    return round1(clamp((0.70 * structural_score) + (0.30 * osint_score), 0, 100))


def main():
    history = safe_load_json(HISTORY_FILE, {"rows": []})
    events = safe_load_json(EVENTS_FILE, [])

    if not isinstance(history, dict) or "rows" not in history:
        raise ValueError(f"Hibás history fájl: {HISTORY_FILE}")

    rows = history.get("rows", [])
    if not isinstance(rows, list):
        raise ValueError(f'Hibás "rows" szerkezet: {HISTORY_FILE}')

    if not isinstance(events, list):
        raise ValueError(f"Hibás events fájl: {EVENTS_FILE}")

    updated_count = 0

    for row in rows:
        if not isinstance(row, dict):
            continue

        date_str = row.get("date")
        if not date_str:
            continue

        try:
            target_date = parse_date(date_str)
        except Exception:
            continue

        structural_me = row.get("middle_east_conflict_impact")
        structural_global = row.get("global_trade_risk_index")

        if structural_me is None or structural_global is None:
            continue

        osint = compute_osint_total_risk(events, target_date, WINDOW_DAYS)

        blended_me = blend_middle_east_score(structural_me, osint["normalized_risk_score"])
        blended_global = blend_global_trade_score(structural_global, osint["normalized_risk_score"])

        row["middle_east_conflict_impact"] = blended_me
        row["global_trade_risk_index"] = blended_global
        row["osint_signal_score"] = osint["normalized_risk_score"]
        row["osint_total_risk"] = round(osint["total_risk"], 2)
        row["osint_total_events"] = osint["total_events"]

        source_mode = row.get("source_mode")
        if source_mode == "backfilled":
            row["source_mode"] = "backfilled_osint"
        elif source_mode == "live":
            row["source_mode"] = "live"
        else:
            row["source_mode"] = source_mode or "backfilled_osint"

        updated_count += 1

    save_json(OUTPUT_FILE, history)

    print(f"{OUTPUT_FILE} frissítve.")
    print(f"Frissített sorok száma: {updated_count}")
    print(f"Felhasznált eseményfájl: {EVENTS_FILE}")


if __name__ == "__main__":
    main()
