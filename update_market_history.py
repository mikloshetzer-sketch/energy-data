import json
import os
from datetime import datetime, timezone

OIL_FILE = "oil-data.json"
CHOKEPOINT_FILE = "chokepoint-impact.json"
OUTPUT_FILE = "market-history.json"


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


def extract_oil_prices(oil_data):
    market = oil_data.get("market", {}) if isinstance(oil_data, dict) else {}

    def to_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return {
        "brent": to_float(market.get("brent")),
        "wti": to_float(market.get("wti")),
    }


def extract_chokepoint_values(cp_data):
    if not isinstance(cp_data, dict):
        return {
            "global_trade_risk_index": None,
            "middle_east_conflict_impact": None,
            "hormuz_impact": None,
            "suez_impact": None,
            "bab_el_mandeb_impact": None,
            "bosporus_impact": None,
        }

    chokepoints = cp_data.get("chokepoints", []) or []

    lookup = {item.get("key"): item for item in chokepoints if isinstance(item, dict)}

    def impact(key):
        item = lookup.get(key, {})
        return item.get("estimated_impact")

    me = cp_data.get("middle_east_conflict_impact", {}) or {}

    return {
        "global_trade_risk_index": cp_data.get("global_trade_risk_index"),
        "middle_east_conflict_impact": me.get("score"),
        "hormuz_impact": impact("hormuz"),
        "suez_impact": impact("suez"),
        "bab_el_mandeb_impact": impact("bab_el_mandeb"),
        "bosporus_impact": impact("bosporus"),
    }


def upsert_snapshot(history, snapshot):
    rows = history.get("rows", [])
    date = snapshot["date"]

    replaced = False
    for i, row in enumerate(rows):
        if row.get("date") == date:
            # ha már létezik az adott nap, felülírjuk frissebb adattal
            rows[i] = snapshot
            replaced = True
            break

    if not replaced:
        rows.append(snapshot)

    rows.sort(key=lambda x: x.get("date", ""))
    history["rows"] = rows
    return history


def main():
    oil_data = safe_load_json(OIL_FILE, {})
    cp_data = safe_load_json(CHOKEPOINT_FILE, {})
    history = safe_load_json(OUTPUT_FILE, {"rows": []})

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    updated_str = now.strftime("%Y-%m-%d %H:%M UTC")

    prices = extract_oil_prices(oil_data)
    cp_values = extract_chokepoint_values(cp_data)

    snapshot = {
        "date": date_str,
        "updated": updated_str,
        "source_mode": "live",
        "brent": prices["brent"],
        "wti": prices["wti"],
        "global_trade_risk_index": cp_values["global_trade_risk_index"],
        "middle_east_conflict_impact": cp_values["middle_east_conflict_impact"],
        "hormuz_impact": cp_values["hormuz_impact"],
        "suez_impact": cp_values["suez_impact"],
        "bab_el_mandeb_impact": cp_values["bab_el_mandeb_impact"],
        "bosporus_impact": cp_values["bosporus_impact"],
    }

    history = upsert_snapshot(history, snapshot)

    save_json(OUTPUT_FILE, history)
    print(f"{OUTPUT_FILE} frissítve: {date_str}")


if __name__ == "__main__":
    main()
