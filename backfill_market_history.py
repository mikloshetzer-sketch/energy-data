import json
import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import requests

OUTPUT_FILE = "market-history.json"
LOCAL_CHOKEPOINT_HISTORY_FILE = "chokepoint-impact-history.json"
EVENTS_FILE = "https://raw.githubusercontent.com/mikloshetzer-sketch/me-security-monitor/main/events.json"

START_DATE = datetime(2026, 2, 1)
TODAY_UTC = datetime.now(UTC)
END_DATE = datetime(TODAY_UTC.year, TODAY_UTC.month, TODAY_UTC.day)

YAHOO_BRENT_URL = "https://query1.finance.yahoo.com/v8/finance/chart/BZ=F?interval=1d&range=1y"
YAHOO_WTI_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CL=F?interval=1d&range=1y"

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

CHOKEPOINT_KEYS = [
    "hormuz_impact",
    "suez_impact",
    "bab_el_mandeb_impact",
    "bosporus_impact",
]


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
            return deepcopy(default)

        with open(path, "r", encoding="utf-8-sig") as f:
            content = f.read().strip()
            if not content:
                return deepcopy(default)
            return json.loads(content)
    except Exception as e:
        print(f"Figyelmeztetés: nem sikerült beolvasni: {path} | Hiba: {e}")
        return deepcopy(default)


def parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d")


def parse_snapshot_date(s):
    return datetime.strptime(s, "%Y-%m-%d")


def parse_number(value):
    if isinstance(value, (int, float)):
        return float(value)
    return None


def round1(value):
    return round(value, 1)


def clamp(value, min_value, max_value):
    return max(min_value, min(value, max_value))


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
    return 1.0 - 0.6 * ratio


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


def osint_weight_from_events(event_count):
    if event_count <= 0:
        return 0.0
    if event_count <= 4:
        return 0.10
    if event_count <= 9:
        return 0.20
    if event_count <= 19:
        return 0.30
    return 0.45


def blend_score(structural_score, osint_score, osint_events, max_osint_weight):
    if osint_events <= 0:
        return round(structural_score, 2)

    event_weight = osint_weight_from_events(osint_events)
    osint_weight = min(event_weight, max_osint_weight)
    structural_weight = 1.0 - osint_weight

    blended = (structural_weight * structural_score) + (osint_weight * osint_score)
    return round(clamp(blended, 0, 100), 2)


def blend_middle_east_score(structural_score, osint_score, osint_events):
    return blend_score(
        structural_score=structural_score,
        osint_score=osint_score,
        osint_events=osint_events,
        max_osint_weight=0.45,
    )


def blend_global_trade_score(structural_score, osint_score, osint_events):
    return blend_score(
        structural_score=structural_score,
        osint_score=osint_score,
        osint_events=osint_events,
        max_osint_weight=0.30,
    )


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
        date_str = datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d")
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

    clean_rows.sort(key=lambda x: x.get("date", ""))
    return {"rows": clean_rows}


def snapshot_name_to_key(name):
    normalized = (name or "").strip().lower()
    if normalized == "hormuzi-szoros":
        return "hormuz_impact"
    if normalized == "szuezi térség":
        return "suez_impact"
    if normalized == "bab el-mandeb":
        return "bab_el_mandeb_impact"
    if normalized == "boszporusz":
        return "bosporus_impact"
    return None


def load_exact_chokepoint_history():
    data = load_json(LOCAL_CHOKEPOINT_HISTORY_FILE, {"snapshots": []})
    snapshots = data.get("snapshots", [])
    if not isinstance(snapshots, list):
        return {}

    by_date = {}

    for snap in snapshots:
        if not isinstance(snap, dict):
            continue

        date_str = snap.get("date")
        if not date_str:
            continue

        item = {
            "date": date_str,
            "updated": snap.get("timestamp") or f"{date_str} 12:00 UTC",
            "global_trade_risk_index": parse_number(snap.get("global_trade_risk_index")),
            "middle_east_conflict_impact": parse_number(snap.get("middle_east_conflict_impact_score")),
            "hormuz_impact": None,
            "suez_impact": None,
            "bab_el_mandeb_impact": None,
            "bosporus_impact": None,
        }

        for risk in snap.get("top_risks", []) or []:
            if not isinstance(risk, dict):
                continue
            key = snapshot_name_to_key(risk.get("name"))
            if key:
                item[key] = parse_number(risk.get("estimated_impact"))

        by_date[date_str] = item

    return by_date


def nearest_exact_dates(exact_by_date):
    dates = []
    for key in exact_by_date.keys():
        try:
            dates.append(parse_snapshot_date(key))
        except Exception:
            pass
    dates.sort()
    return dates


def interpolate_gap_value(prev_value, next_value, step_index, total_steps):
    if prev_value is None and next_value is None:
        return None
    if prev_value is None:
        return next_value
    if next_value is None:
        return prev_value
    ratio = step_index / total_steps
    return round(prev_value + ((next_value - prev_value) * ratio), 4)


def build_gap_estimate(date_str, prev_row, next_exact_row):
    row = {
        "date": date_str,
        "updated": f"{date_str} 12:00 UTC",
        "source_mode": "gap_interpolated",
        "global_trade_risk_index": None,
        "middle_east_conflict_impact": None,
        "hormuz_impact": None,
        "suez_impact": None,
        "bab_el_mandeb_impact": None,
        "bosporus_impact": None,
    }

    gap_start = parse_snapshot_date("2026-03-23")
    current = parse_snapshot_date(date_str)
    next_date = parse_snapshot_date(next_exact_row["date"])

    total_steps = (next_date - gap_start).days
    step_index = (current - gap_start).days

    if prev_row:
        row["global_trade_risk_index"] = interpolate_gap_value(
            prev_row.get("global_trade_risk_index"),
            next_exact_row.get("global_trade_risk_index"),
            step_index,
            total_steps,
        )
        row["middle_east_conflict_impact"] = interpolate_gap_value(
            prev_row.get("middle_east_conflict_impact"),
            next_exact_row.get("middle_east_conflict_impact"),
            step_index,
            total_steps,
        )

        for key in CHOKEPOINT_KEYS:
            row[key] = interpolate_gap_value(
                prev_row.get(key),
                next_exact_row.get(key),
                step_index,
                total_steps,
            )

    return row


def build_legacy_structural_row(events, current, idx):
    regime = get_regime(current)
    offset = daily_offset(idx)
    date_str = current.strftime("%Y-%m-%d")

    structural_global = round(regime["global_trade_risk_index"] + (offset * 1.4), 2)
    structural_me = round(regime["middle_east_conflict_impact"] + (offset * 1.2), 2)

    osint = compute_osint_total_risk(events, current, WINDOW_DAYS)

    blended_global = blend_global_trade_score(
        structural_global,
        osint["normalized_risk_score"],
        osint["total_events"],
    )
    blended_me = blend_middle_east_score(
        structural_me,
        osint["normalized_risk_score"],
        osint["total_events"],
    )

    return {
        "date": date_str,
        "updated": current.strftime("%Y-%m-%d 12:00 UTC"),
        "source_mode": "backfilled_osint",
        "global_trade_risk_index": blended_global,
        "middle_east_conflict_impact": blended_me,
        "hormuz_impact": round(regime["hormuz_impact"] + (offset * 0.0040), 4),
        "suez_impact": round(regime["suez_impact"] + (offset * 0.0020), 4),
        "bab_el_mandeb_impact": round(regime["bab_el_mandeb_impact"] + (offset * 0.0030), 4),
        "bosporus_impact": round(regime["bosporus_impact"] + (offset * 0.0010), 4),
        "osint_signal_score": osint["normalized_risk_score"],
        "osint_total_risk": round(osint["total_risk"], 2),
        "osint_total_events": osint["total_events"],
        "osint_weight_global": min(osint_weight_from_events(osint["total_events"]), 0.30),
        "osint_weight_middle_east": min(osint_weight_from_events(osint["total_events"]), 0.45),
        "structural_global_trade_risk_index": structural_global,
        "structural_middle_east_conflict_impact": structural_me,
    }


def build_backfill_rows(events, existing_rows):
    brent_raw = fetch_yahoo_series(YAHOO_BRENT_URL)
    wti_raw = fetch_yahoo_series(YAHOO_WTI_URL)

    brent_daily = fill_forward(brent_raw, START_DATE, END_DATE)
    wti_daily = fill_forward(wti_raw, START_DATE, END_DATE)

    exact_by_date = load_exact_chokepoint_history()
    exact_dates_sorted = nearest_exact_dates(exact_by_date)

    existing_by_date = {
        row["date"]: row
        for row in existing_rows
        if isinstance(row, dict) and row.get("date")
    }

    rows = []
    current = START_DATE
    idx = 0

    last_legacy_row = existing_by_date.get("2026-03-23")

    first_exact_date = exact_dates_sorted[0] if exact_dates_sorted else None
    first_exact_row = exact_by_date.get(first_exact_date.strftime("%Y-%m-%d")) if first_exact_date else None

    while current <= END_DATE:
        date_str = current.strftime("%Y-%m-%d")
        existing_row = deepcopy(existing_by_date.get(date_str, {}))

        base_row = {
            "date": date_str,
            "updated": existing_row.get("updated", f"{date_str} 12:00 UTC"),
            "source_mode": existing_row.get("source_mode", "backfilled"),
            "market_brent": brent_daily.get(date_str),
            "market_wti": wti_daily.get(date_str),
            "brent": brent_daily.get(date_str),
            "wti": wti_daily.get(date_str),
        }

        if date_str in exact_by_date:
            exact = exact_by_date[date_str]
            base_row.update({
                "updated": exact.get("updated", base_row["updated"]),
                "source_mode": "exact_chokepoint_history",
                "global_trade_risk_index": exact.get("global_trade_risk_index"),
                "middle_east_conflict_impact": exact.get("middle_east_conflict_impact"),
                "hormuz_impact": exact.get("hormuz_impact"),
                "suez_impact": exact.get("suez_impact"),
                "bab_el_mandeb_impact": exact.get("bab_el_mandeb_impact"),
                "bosporus_impact": exact.get("bosporus_impact"),
            })
        elif current <= parse_date("2026-03-23"):
            legacy = build_legacy_structural_row(events, current, idx)
            base_row.update(legacy)
            base_row["market_brent"] = brent_daily.get(date_str)
            base_row["market_wti"] = wti_daily.get(date_str)
            base_row["brent"] = brent_daily.get(date_str)
            base_row["wti"] = wti_daily.get(date_str)
        elif first_exact_row and current < first_exact_date:
            gap_row = build_gap_estimate(date_str, last_legacy_row, first_exact_row)
            base_row.update(gap_row)
            base_row["market_brent"] = brent_daily.get(date_str)
            base_row["market_wti"] = wti_daily.get(date_str)
            base_row["brent"] = brent_daily.get(date_str)
            base_row["wti"] = wti_daily.get(date_str)
        else:
            # ha van már existing pontos/chokepointos sor, azt megtartjuk
            for key in [
                "global_trade_risk_index",
                "middle_east_conflict_impact",
                "hormuz_impact",
                "suez_impact",
                "bab_el_mandeb_impact",
                "bosporus_impact",
                "osint_signal_score",
                "osint_total_risk",
                "osint_total_events",
                "osint_weight_global",
                "osint_weight_middle_east",
                "structural_global_trade_risk_index",
                "structural_middle_east_conflict_impact",
            ]:
                if key in existing_row:
                    base_row[key] = existing_row[key]

        rows.append(base_row)
        current += timedelta(days=1)
        idx += 1

    return rows


def merge_rows(existing_rows, new_rows):
    merged = {
        row["date"]: row
        for row in existing_rows
        if isinstance(row, dict) and "date" in row
    }

    for row in new_rows:
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

    backfill_rows = build_backfill_rows(events, existing_rows)
    merged_rows = merge_rows(existing_rows, backfill_rows)

    payload = {"rows": merged_rows}
    save_json(OUTPUT_FILE, payload)

    exact_count = sum(1 for row in merged_rows if row.get("source_mode") == "exact_chokepoint_history")
    gap_count = sum(1 for row in merged_rows if row.get("source_mode") == "gap_interpolated")

    print(f"{OUTPUT_FILE} backfill kész.")
    print(f"Backfill rekordok száma: {len(backfill_rows)}")
    print(f"Összes rekord a fájlban: {len(merged_rows)}")
    print(f"Felhasznált eseményfájl: {EVENTS_FILE}")
    print(f"OSINT események száma: {len(events)}")
    print(f"Pontos chokepoint-history sorok: {exact_count}")
    print(f"Átmeneti gap-interpolált sorok: {gap_count}")


if __name__ == "__main__":
    main()
