import json
import os
import csv
from io import StringIO
from datetime import datetime, timedelta
import requests

OUTPUT_FILE = "market-history.json"

START_DATE = datetime(2026, 2, 1)
END_DATE = datetime(2026, 3, 22)

FRED_BRENT_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILBRENTEU"
FRED_WTI_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO"

REGIMES = [
    {
        "start": "2026-02-01",
        "end": "2026-02-10",
        "global_trade_risk_index": 49.0,
        "middle_east_conflict_impact": 43.0,
        "hormuz_impact": 0.1450,
        "suez_impact": 0.0780,
        "bab_el_mandeb_impact": 0.0870,
        "bosporus_impact": 0.0290,
    },
    {
        "start": "2026-02-11",
        "end": "2026-02-20",
        "global_trade_risk_index": 54.0,
        "middle_east_conflict_impact": 49.0,
        "hormuz_impact": 0.1620,
        "suez_impact": 0.0820,
        "bab_el_mandeb_impact": 0.0940,
        "bosporus_impact": 0.0300,
    },
    {
        "start": "2026-02-21",
        "end": "2026-03-05",
        "global_trade_risk_index": 58.0,
        "middle_east_conflict_impact": 53.0,
        "hormuz_impact": 0.1710,
        "suez_impact": 0.0890,
        "bab_el_mandeb_impact": 0.1010,
        "bosporus_impact": 0.0310,
    },
    {
        "start": "2026-03-06",
        "end": "2026-03-14",
        "global_trade_risk_index": 61.0,
        "middle_east_conflict_impact": 57.0,
        "hormuz_impact": 0.1790,
        "suez_impact": 0.0940,
        "bab_el_mandeb_impact": 0.1080,
        "bosporus_impact": 0.0320,
    },
    {
        "start": "2026-03-15",
        "end": "2026-03-22",
        "global_trade_risk_index": 64.0,
        "middle_east_conflict_impact": 60.0,
        "hormuz_impact": 0.1860,
        "suez_impact": 0.0970,
        "bab_el_mandeb_impact": 0.1110,
        "bosporus_impact": 0.0330,
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
    pattern = [-0.6, -0.2, 0.1, 0.4, 0.2, -0.1, 0.3]
    return pattern[day_index % len(pattern)]


def fetch_fred_series(csv_url):
    """
    FRED CSV letöltése és {date: value} formára alakítása.
    """
    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()

    result = {}
    reader = csv.DictReader(StringIO(response.text))

    for row in reader:
        keys = list(row.keys())
        if len(keys) < 2:
            continue

        date_str = row[keys[0]]
        value_str = row[keys[1]]

        if value_str in (None, "", ".", "nan"):
            continue

        try:
            result[date_str] = float(value_str)
        except ValueError:
            continue

    return result


def fill_forward(series_map, start_date, end_date):
    """
    Hétvégére/hiányzó napra az utolsó elérhető értéket visszük tovább.
    """
    filled = {}
    current = start_date
    last_value = None

    while current <= end_date:
        key = current.strftime("%Y-%m-%d")

        if key in series_map:
            last_value = series_map[key]

        filled[key] = last_value
        current += timedelta(days=1)

    return filled


def build_backfill_rows():
    brent_raw = fetch_fred_series(FRED_BRENT_CSV)
    wti_raw = fetch_fred_series(FRED_WTI_CSV)

    brent_daily = fill_forward(brent_raw, START_DATE, END_DATE)
    wti_daily = fill_forward(wti_raw, START_DATE, END_DATE)

    rows = []
    current = START_DATE
    idx = 0

    while current <= END_DATE:
        regime = get_regime(current)
        offset = daily_offset(idx)
        date_str = current.strftime("%Y-%m-%d")

        row = {
            "date": date_str,
            "updated": current.strftime("%Y-%m-%d 12:00 UTC"),
            "source_mode": "backfilled",
            "brent": brent_daily.get(date_str),
            "wti": wti_daily.get(date_str),
            "global_trade_risk_index": round(regime["global_trade_risk_index"] + (offset * 1.4), 2),
            "middle_east_conflict_impact": round(regime["middle_east_conflict_impact"] + (offset * 1.2), 2),
            "hormuz_impact": round(regime["hormuz_impact"] + (offset * 0.0040), 4),
            "suez_impact": round(regime["suez_impact"] + (offset * 0.0020), 4),
            "bab_el_mandeb_impact": round(regime["bab_el_mandeb_impact"] + (offset * 0.0030), 4),
            "bosporus_impact": round(regime["bosporus_impact"] + (offset * 0.0010), 4),
        }

        rows.append(row)
        current += timedelta(days=1)
        idx += 1

    return rows


def normalize_history(history):
    if not isinstance(history, dict):
        return {"rows": []}

    rows = history.get("rows")
    if not isinstance(rows, list):
        rows = []

    clean_rows = []
    for row in rows:
        if isinstance(row, dict) and "date" in row:
            clean_rows.append(row)

    return {"rows": clean_rows}


def merge_rows(existing_rows, new_rows):
    merged = {
        row["date"]: row
        for row in existing_rows
        if isinstance(row, dict) and "date" in row
    }

    for row in new_rows:
        existing = merged.get(row["date"])

        if existing and existing.get("source_mode") == "live":
            continue

        merged[row["date"]] = row

    rows = list(merged.values())
    rows.sort(key=lambda x: x.get("date", ""))
    return rows


def main():
    history = normalize_history(load_json(OUTPUT_FILE, {"rows": []}))
    existing_rows = history.get("rows", [])

    backfill_rows = build_backfill_rows()
    merged_rows = merge_rows(existing_rows, backfill_rows)

    payload = {"rows": merged_rows}
    save_json(OUTPUT_FILE, payload)

    print(f"{OUTPUT_FILE} backfill kész.")
    print(f"Backfill rekordok száma: {len(backfill_rows)}")
    print(f"Összes rekord a fájlban: {len(merged_rows)}")


if __name__ == "__main__":
    main()
