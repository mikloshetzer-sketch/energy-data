import json
import os
import re
from datetime import datetime, timezone

OIL_FILE = "oil-data.json"
CHOKEPOINT_FILE = "chokepoint-impact.json"
OUTPUT_FILE = "live-market.json"


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


def parse_number(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        text = text.replace(",", ".")
        text = text.replace("$", "")
        text = text.replace("USD", "")
        text = text.replace("usd", "")
        text = text.strip()

        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if match:
            try:
                return float(match.group(0))
            except ValueError:
                return None

    return None


def extract_live_prices(oil_data):
    if not isinstance(oil_data, dict):
        return {"brent": None, "wti": None}

    candidates = []

    market = oil_data.get("market")
    if isinstance(market, dict):
        candidates.append((market.get("brent"), market.get("wti")))

    prices = oil_data.get("prices")
    if isinstance(prices, dict):
        candidates.append((prices.get("brent"), prices.get("wti")))

    candidates.append((oil_data.get("brent"), oil_data.get("wti")))

    for brent_raw, wti_raw in candidates:
        brent = parse_number(brent_raw)
        wti = parse_number(wti_raw)

        if brent is not None or wti is not None:
            return {
                "brent": brent,
                "wti": wti,
            }

    return {
        "brent": None,
        "wti": None,
    }


def extract_chokepoint_values(cp_data):
    if not isinstance(cp_data, dict):
        return {
            "global_trade_risk_index": None,
            "middle_east_conflict_impact": None,
            "daily_change": {},
            "top_risks": [],
        }

    me = cp_data.get("middle_east_conflict_impact", {}) or {}

    return {
        "global_trade_risk_index": parse_number(cp_data.get("global_trade_risk_index")),
        "middle_east_conflict_impact": parse_number(me.get("score")),
        "daily_change": cp_data.get("daily_change", {}) or {},
        "top_risks": cp_data.get("top_risks", []) or [],
    }


def main():
    oil_data = safe_load_json(OIL_FILE, {})
    cp_data = safe_load_json(CHOKEPOINT_FILE, {})

    now = datetime.now(timezone.utc)
    updated_str = now.strftime("%Y-%m-%d %H:%M UTC")

    prices = extract_live_prices(oil_data)
    cp_values = extract_chokepoint_values(cp_data)

    payload = {
        "meta": {
            "updated": updated_str,
            "source_mode": "live",
        },
        "prices": {
            "brent": prices["brent"],
            "wti": prices["wti"],
        },
        "risk": {
            "global_trade_risk_index": cp_values["global_trade_risk_index"],
            "middle_east_conflict_impact": cp_values["middle_east_conflict_impact"],
            "daily_change": cp_values["daily_change"],
            "top_risks": cp_values["top_risks"],
        }
    }

    save_json(OUTPUT_FILE, payload)
    print(f"{OUTPUT_FILE} frissítve.")
    print(f"Live Brent: {prices['brent']} | Live WTI: {prices['wti']}")


if __name__ == "__main__":
    main()
