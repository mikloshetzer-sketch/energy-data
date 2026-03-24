import json
import os
from datetime import datetime, timedelta

import requests

OUTPUT_FILE = "market-history.json"
EVENTS_FILE = "https://raw.githubusercontent.com/mikloshetzer-sketch/me-security-monitor/main/events.json"

START_DATE = datetime(2026, 2, 1)
END_DATE = datetime(2026, 3, 23)

YAHOO_BRENT_URL = "https://query1.finance.yahoo.com/v8/finance/chart/BZ=F?interval=1d&range=6mo"
YAHOO_WTI_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CL=F?interval=1d&range=6mo"

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
        "end": "2026-03-23",
        "global_trade_risk_index": 64.0,
        "middle_east_conflict_impact": 60.0,
        "hormuz_impact": 0.1860,
        "suez_impact": 0.0970,
        "bab_el_mandeb_impact": 0.1110,
        "bosporus_impact": 0.0330,
    },
]

WINDOW_DAYS = 7

CAT_W = {
    "military": 3.0,
    "security": 2.0,
    "political": 1.0,
    "other": 0.5,
}

ISW_MULT = 1.3


def save_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_json(path, default):
    try:
        if isinstance(path, str) and path.startswith("http"):
            response = requests.get(path, timeout=30)
            response.raise_for_status()
            return response.json()

        if not os.path.exists(path):
            return default

        with open(path, "r", encoding="utf-8-sig") as f:
            content = f.read().strip()
            if not content:
                return default
            return json.loads(content)
    except Exception as e:
        print(f"Figyelmeztetés: nem sikerült beolvasni: {path} | Hiba: {e}")
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


def round1(value):
    return round(value, 1)


def clamp(value, min_value, max_value):
    return max(min_value, min(value, max_value))


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
    return round(clamp((0.55 * structural_score) + (0.45 * osint_score), 0, 100), 2)


def blend_global_trade_score(structural_score, osint_score):
    return round(clamp((0.70 * structural_score) + (0.30 * osint_score), 0, 100), 2)


def fetch_yahoo_series(url):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    result = data.get("chart", {}).get("result", [])
    if not result:
        return {}

    item = result[0]
    timestamps = item.get("timestamp", []) or []
    closes = item.get("indicators", {}).get("quote", [{}])[0].get("close", []) or []

    series = {}
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        series[date_str] = float(close)

    return series


def fill_forward(series_map, start_date, end_date):
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


def build_backfill_rows(events):
    brent_raw = fetch_yahoo_series(YAHOO_BRENT_URL)
    wti_raw = fetch_yahoo_series(YAHOO_WTI_URL)

    brent_daily = fill_forward(brent_raw, START_DATE, END_DATE)
    wti_daily = fill_forward(wti_raw, START_DATE, END_DATE)

    rows = []
    current = START_DATE
    idx = 0

    while current <= END_DATE:
        regime = get_regime(current)
        offset = daily_offset(idx)
        date_str = current.strftime("%Y-%m-%d")

        market_brent = brent_daily.get(date_str)
        market_wti = wti_daily.get(date_str)

        structural_global = round(regime["global_trade_risk_index"] + (offset * 1.4), 2)
        structural_me = round(regime["middle_east_conflict_impact"] + (offset * 1.2), 2)

        osint = compute_osint_total_risk(events, current, WINDOW_DAYS)

        blended_global = blend_global_trade_score(structural_global, osint["normalized_risk_score"])
        blended_me = blend_middle_east_score(structural_me, osint["normalized_risk_score"])

        row = {
            "date": date_str,
            "updated": current.strftime("%Y-%m-%d 12:00 UTC"),
            "source_mode": "backfilled_osint",
            "market_brent": market_brent,
            "market_wti": market_wti,

            # kompatibilitási fallback a régebbi blokkokhoz
            "brent": market_brent,
            "wti": market_wti,

            "global_trade_risk_index": blended_global,
            "middle_east_conflict_impact": blended_me,

            # struktúrális chokepoint komponensek maradnak historikus proxyként
            "hormuz_impact": round(regime["hormuz_impact"] + (offset * 0.0040), 4),
            "suez_impact": round(regime["suez_impact"] + (offset * 0.0020), 4),
            "bab_el_mandeb_impact": round(regime["bab_el_mandeb_impact"] + (offset * 0.0030), 4),
            "bosporus_impact": round(regime["bosporus_impact"] + (offset * 0.0010), 4),

            # új OSINT historikus mezők
            "osint_signal_score": osint["normalized_risk_score"],
            "osint_total_risk": round(osint["total_risk"], 2),
            "osint_total_events": osint["total_events"],

            # opcionális transzparencia
            "structural_global_trade_risk_index": structural_global,
            "structural_middle_east_conflict_impact": structural_me,
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

        # élő sort nem írunk felül
        if existing and existing.get("source_mode") == "live":
            continue

        merged[row["date"]] = row

    rows = list(merged.values())
    rows.sort(key=lambda x: x.get("date", ""))
    return rows


def main():
    history = normalize_history(load_json(OUTPUT_FILE, {"rows": []}))
    existing_rows = history.get("rows", [])
    events = load_json(EVENTS_FILE, [])

    if not isinstance(events, list):
        print(f"Figyelmeztetés: hibás vagy hiányzó events fájl: {EVENTS_FILE}")
        events = []

    backfill_rows = build_backfill_rows(events)
    merged_rows = merge_rows(existing_rows, backfill_rows)

    payload = {"rows": merged_rows}
    save_json(OUTPUT_FILE, payload)

    print(f"{OUTPUT_FILE} backfill kész.")
    print(f"Backfill rekordok száma: {len(backfill_rows)}")
    print(f"Összes rekord a fájlban: {len(merged_rows)}")
    print(f"Felhasznált eseményfájl: {EVENTS_FILE}")
    print(f"OSINT események száma: {len(events)}")


if __name__ == "__main__":
    main()
