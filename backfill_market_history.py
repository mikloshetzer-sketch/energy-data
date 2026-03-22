import json
import os
from datetime import datetime, timedelta

OUTPUT_FILE = "market-history.json"

START_DATE = datetime(2026, 2, 1)
END_DATE = datetime(2026, 3, 22)

# Egyszerű eseményalapú időszakok.
# Ezeket később finomíthatod.
REGIMES = [
    {
        "start": "2026-02-01",
        "end": "2026-02-10",
        "brent_base": 78.5,
        "wti_base": 74.2,
        "global_trade_risk_index": 49.0,
        "middle_east_conflict_impact": 43.0,
        "hormuz_impact": 0.145,
        "suez_impact": 0.078,
        "bab_el_mandeb_impact": 0.087,
        "bosporus_impact": 0.029,
    },
    {
        "start": "2026-02-11",
        "end": "2026-02-20",
        "brent_base": 80.2,
        "wti_base": 76.0,
        "global_trade_risk_index": 54.0,
        "middle_east_conflict_impact": 49.0,
        "hormuz_impact": 0.162,
        "suez_impact": 0.082,
        "bab_el_mandeb_impact": 0.094,
        "bosporus_impact": 0.030,
    },
    {
        "start": "2026-02-21",
        "end": "2026-03-05",
        "brent_base": 82.1,
        "wti_base": 77.8,
        "global_trade_risk_index": 58.0,
        "middle_east_conflict_impact": 53.0,
        "hormuz_impact": 0.171,
        "suez_impact": 0.089,
        "bab_el_mandeb_impact": 0.101,
        "bosporus_impact": 0.031,
    },
    {
        "start": "2026-03-06",
        "end": "2026-03-14",
        "brent_base": 83.4,
        "wti_base": 79.1,
        "global_trade_risk_index": 61.0,
        "middle_east_conflict_impact": 57.0,
        "hormuz_impact": 0.179,
        "suez_impact": 0.094,
        "bab_el_mandeb_impact": 0.108,
        "bosporus_impact": 0.032,
    },
    {
        "start": "2026-03-15",
        "end": "2026-03-22",
        "brent_base": 84.2,
        "wti_base": 79.8,
        "global_trade_risk_index": 64.0,
        "middle_east_conflict_impact": 60.0,
        "hormuz_impact": 0.186,
        "suez_impact": 0.097,
        "bab_el_mandeb_impact": 0.111,
        "bosporus_impact": 0.033,
    },
]


def save_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_json(path, default):
    if not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            content = f.read().strip()
            if not content:
                return default
            return json.loads(content)
    except Exception:
        return default


def parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d")


def get_regime(dt):
    for regime in REGIMES:
        start = parse_date(regime["start"])
        end = parse_date(regime["end"])
        if start <= dt <= end:
            return regime
    return REGIMES[-1]


def daily_offset(day_index):
    # determinisztikus, egyszerű kis hullámzás random nélkül
    pattern = [-0.6, -0.2, 0.1, 0.4, 0.2, -0.1, 0.3]
    return pattern[day_index % len(pattern)]


def build_backfill_rows():
    rows = []
    current = START_DATE
    idx = 0

    while current <= END_DATE:
        regime = get_regime(current)
        offset = daily_offset(idx)

        row = {
            "date": current.strftime("%Y-%m-%d"),
            "updated": current.strftime("%Y-%m-%d 12:00 UTC"),
            "source_mode": "backfilled",
            "brent": round(regime["brent_base"] + offset, 2),
            "wti": round(regime["wti_base"] + (offset * 0.9), 2),
            "global_trade_risk_index": round(regime["global_trade_risk_index"] + (offset * 1.4), 2),
            "middle_east_conflict_impact": round(regime["middle_east_conflict_impact"] + (offset * 1.2), 2),
            "hormuz_impact": round(regime["hormuz_impact"] + (offset * 0.004), 4),
            "suez_impact": round(regime["suez_impact"] + (offset * 0.002), 4),
            "bab_el_mandeb_impact": round(regime["bab_el_mandeb_impact"] + (offset * 0.003), 4),
            "bosporus_impact": round(regime["bosporus_impact"] + (offset * 0.001), 4),
        }

        rows.append(row)
        current += timedelta(days=1)
        idx += 1

    return rows


def merge_rows(existing_rows, new_rows):
    merged = {row["date"]: row for row in existing_rows if isinstance(row, dict) and "date" in row}

    for row in new_rows:
        # csak akkor írjuk felül, ha nincs már live adat arra a napra
        existing = merged.get(row["date"])
        if existing and existing.get("source_mode") == "live":
            continue
        merged[row["date"]] = row

    rows = list(merged.values())
    rows.sort(key=lambda x: x.get("date", ""))
    return rows


def main():
    history = load_json(OUTPUT_FILE, {"rows": []})
    existing_rows = history.get("rows", []) if isinstance(history, dict) else []

    backfill_rows = build_backfill_rows()
    merged_rows = merge_rows(existing_rows, backfill_rows)

    payload = {"rows": merged_rows}
    save_json(OUTPUT_FILE, payload)

    print(f"{OUTPUT_FILE} backfill kész: {len(backfill_rows)} rekord generálva.")


if __name__ == "__main__":
    main()
