import json
import os
import re
from datetime import datetime, timezone

OIL_FILE = "oil-data.json"
CHOKEPOINT_FILE = "chokepoint-impact.json"
OUTPUT_FILE = "market-history.json"


def safe_load_json(path, default):
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


def parse_number(value):
    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        value = value.replace(",", ".")
        match = re.search(r"\d+(\.\d+)?", value)
        if match:
            return float(match.group(0))

    return None


def extract_prices(oil_data):
    # 🔥 ÚJ: market ár elsődleges
    market = oil_data.get("market", {})
    return {
        "market_brent": parse_number(market.get("brent")),
        "market_wti": parse_number(market.get("wti")),
    }


def extract_chokepoint(cp):
    return {
        "global_trade_risk_index": parse_number(cp.get("global_trade_risk_index")),
        "middle_east_conflict_impact": parse_number(cp.get("middle_east_conflict_impact", {}).get("score")),
    }


def main():
    oil = safe_load_json(OIL_FILE, {})
    cp = safe_load_json(CHOKEPOINT_FILE, {})
    history = safe_load_json(OUTPUT_FILE, {"rows": []})

    now = datetime.now(timezone.utc)
    date = now.strftime("%Y-%m-%d")
    updated = now.strftime("%Y-%m-%d %H:%M UTC")

    prices = extract_prices(oil)
    risk = extract_chokepoint(cp)

    new_row = {
        "date": date,
        "updated": updated,
        "market_brent": prices["market_brent"],
        "market_wti": prices["market_wti"],
        "global_trade_risk_index": risk["global_trade_risk_index"],
        "middle_east_conflict_impact": risk["middle_east_conflict_impact"],
    }

    # upsert
    rows = history.get("rows", [])
    rows = [r for r in rows if r.get("date") != date]
    rows.append(new_row)
    rows.sort(key=lambda x: x["date"])

    history["rows"] = rows
    save_json(OUTPUT_FILE, history)

    print("Market history frissítve:", date)


if __name__ == "__main__":
    main()
