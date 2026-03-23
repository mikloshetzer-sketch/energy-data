import json
import os
import re
import csv
from io import StringIO
from datetime import datetime, timezone
import requests

OIL_FILE = "oil-data.json"
CHOKEPOINT_FILE = "chokepoint-impact.json"
OUTPUT_FILE = "market-history.json"

FRED_BRENT_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILBRENTEU"
FRED_WTI_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO"


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


def fetch_latest_fred_value(csv_url):
    """
    Utolsó elérhető FRED érték lekérése.
    Ha a mai napra még nincs adat, az utolsó elérhető üzleti napot adja.
    """
    try:
        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()

        rows = list(csv.DictReader(StringIO(response.text)))
        rows.reverse()

        for row in rows:
            keys = list(row.keys())
            if len(keys) < 2:
                continue

            value = row[keys[1]]
            parsed = parse_number(value)
            if parsed is not None:
                return parsed

    except Exception as e:
        print(f"FRED fallback hiba: {e}")

    return None


def extract_oil_prices(oil_data):
    """
    Elsődlegesen a saját oil-data.json-ból dolgozik.
    Ha ott nincs használható érték, FRED fallbacket használ.
    """
    brent = None
    wti = None

    if isinstance(oil_data, dict):
        candidates = []

        market = oil_data.get("market")
        if isinstance(market, dict):
            candidates.append((market.get("brent"), market.get("wti")))

        prices = oil_data.get("prices")
        if isinstance(prices, dict):
            candidates.append((prices.get("brent"), prices.get("wti")))

        candidates.append((oil_data.get("brent"), oil_data.get("wti")))

        for brent_raw, wti_raw in candidates:
            parsed_brent = parse_number(brent_raw)
            parsed_wti = parse_number(wti_raw)

            if parsed_brent is not None:
                brent = parsed_brent
            if parsed_wti is not None:
                wti = parsed_wti

            if brent is not None or wti is not None:
                break

    if brent is None:
        brent = fetch_latest_fred_value(FRED_BRENT_CSV)

    if wti is None:
        wti = fetch_latest_fred_value(FRED_WTI_CSV)

    return {
        "brent": brent,
        "wti": wti,
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
    lookup = {
        item.get("key"): item
        for item in chokepoints
        if isinstance(item, dict)
    }

    def impact(key):
        item = lookup.get(key, {})
        return item.get("estimated_impact")

    me = cp_data.get("middle_east_conflict_impact", {}) or {}

    return {
        "global_trade_risk_index": parse_number(cp_data.get("global_trade_risk_index")),
        "middle_east_conflict_impact": parse_number(me.get("score")),
        "hormuz_impact": parse_number(impact("hormuz")),
        "suez_impact": parse_number(impact("suez")),
        "bab_el_mandeb_impact": parse_number(impact("bab_el_mandeb")),
        "bosporus_impact": parse_number(impact("bosporus")),
    }


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


def upsert_snapshot(history, snapshot):
    rows = history.get("rows", [])
    date = snapshot["date"]

    replaced = False
    for i, row in enumerate(rows):
        if row.get("date") == date:
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
    history = normalize_history(safe_load_json(OUTPUT_FILE, {"rows": []}))

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
    print(f"Brent: {prices['brent']} | WTI: {prices['wti']}")


if __name__ == "__main__":
    main()
