import json
import os
import re
from copy import deepcopy
from datetime import datetime, timezone

OIL_FILE = "oil-data.json"
CHOKEPOINT_FILE = "chokepoint-impact.json"
OUTPUT_FILE = "market-history.json"

CHOKEPOINT_KEYS = [
    "hormuz_impact",
    "suez_impact",
    "bab_el_mandeb_impact",
    "bosporus_impact",
]

NAME_TO_KEY = {
    "hormuzi-szoros": "hormuz_impact",
    "hormuz": "hormuz_impact",
    "bab el-mandeb": "bab_el_mandeb_impact",
    "bab-el-mandeb": "bab_el_mandeb_impact",
    "szuezi térség": "suez_impact",
    "suez": "suez_impact",
    "boszporusz": "bosporus_impact",
    "bosporus": "bosporus_impact",
}


def safe_load_json(path, default):
    if not os.path.exists(path):
        return deepcopy(default)
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            content = f.read().strip()
            if not content:
                return deepcopy(default)
            return json.loads(content)
    except Exception:
        return deepcopy(default)


def save_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def parse_number(value):
    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        value = value.replace(",", ".")
        match = re.search(r"-?\d+(\.\d+)?", value)
        if match:
            return float(match.group(0))

    return None


def normalize_history(history):
    if not isinstance(history, dict):
        return {"rows": []}

    rows = history.get("rows")
    if not isinstance(rows, list):
        rows = []

    clean_rows = []
    for row in rows:
        if isinstance(row, dict) and row.get("date"):
            clean_rows.append(row)

    clean_rows.sort(key=lambda x: x.get("date", ""))
    return {"rows": clean_rows}


def extract_prices(oil_data):
    market = oil_data.get("market", {}) if isinstance(oil_data, dict) else {}

    brent = parse_number(market.get("brent"))
    wti = parse_number(market.get("wti"))

    return {
        "market_brent": brent,
        "market_wti": wti,
        "brent": brent,
        "wti": wti,
    }


def extract_chokepoint(cp):
    result = {
        "global_trade_risk_index": None,
        "middle_east_conflict_impact": None,
        "hormuz_impact": None,
        "suez_impact": None,
        "bab_el_mandeb_impact": None,
        "bosporus_impact": None,
    }

    if not isinstance(cp, dict):
        return result

    result["global_trade_risk_index"] = parse_number(cp.get("global_trade_risk_index"))

    me_conflict = cp.get("middle_east_conflict_impact")
    if isinstance(me_conflict, dict):
        result["middle_east_conflict_impact"] = parse_number(me_conflict.get("score"))
    else:
        result["middle_east_conflict_impact"] = parse_number(me_conflict)

    chokepoints = cp.get("chokepoints", [])
    if isinstance(chokepoints, list):
        for item in chokepoints:
            if not isinstance(item, dict):
                continue

            raw_name = (item.get("name") or item.get("label") or item.get("key") or "").strip().lower()
            target_key = NAME_TO_KEY.get(raw_name)

            if not target_key and item.get("key"):
                key_name = str(item.get("key")).strip().lower()
                if key_name == "hormuz":
                    target_key = "hormuz_impact"
                elif key_name == "suez":
                    target_key = "suez_impact"
                elif key_name == "bab_el_mandeb":
                    target_key = "bab_el_mandeb_impact"
                elif key_name == "bosporus":
                    target_key = "bosporus_impact"

            if target_key:
                result[target_key] = parse_number(item.get("estimated_impact"))

    return result


def build_today_row(existing_row, prices, chokepoint_values, date_str, updated_str):
    row = deepcopy(existing_row) if isinstance(existing_row, dict) else {}

    row["date"] = date_str
    row["updated"] = updated_str
    row["source_mode"] = "live"

    for key, value in prices.items():
        row[key] = value

    for key, value in chokepoint_values.items():
        row[key] = value

    return row


def main():
    oil = safe_load_json(OIL_FILE, {})
    cp = safe_load_json(CHOKEPOINT_FILE, {})
    history = normalize_history(safe_load_json(OUTPUT_FILE, {"rows": []}))

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    updated_str = now.strftime("%Y-%m-%d %H:%M UTC")

    prices = extract_prices(oil)
    chokepoint_values = extract_chokepoint(cp)

    rows = history.get("rows", [])
    existing_by_date = {
        row["date"]: row
        for row in rows
        if isinstance(row, dict) and row.get("date")
    }

    today_row = build_today_row(
        existing_row=existing_by_date.get(date_str),
        prices=prices,
        chokepoint_values=chokepoint_values,
        date_str=date_str,
        updated_str=updated_str,
    )

    existing_by_date[date_str] = today_row

    merged_rows = list(existing_by_date.values())
    merged_rows.sort(key=lambda x: x.get("date", ""))

    payload = {"rows": merged_rows}
    save_json(OUTPUT_FILE, payload)

    print("Market history frissítve:", date_str)
    print("Összes sor:", len(merged_rows))
    print("Mai row tartalmazza a 4 chokepoint mezőt is.")


if __name__ == "__main__":
    main()
