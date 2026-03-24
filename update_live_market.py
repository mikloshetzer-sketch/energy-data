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


def extract_spot_prices(oil_data):
    """
    A meglévő oil-data.json-ból kinyeri a spot / EIA jellegű árakat.
    Több lehetséges JSON szerkezetet próbál.
    """
    if not isinstance(oil_data, dict):
        return {"spot_brent": None, "spot_wti": None}

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
                "spot_brent": brent,
                "spot_wti": wti,
            }

    return {
        "spot_brent": None,
        "spot_wti": None,
    }


def extract_live_prices(oil_data, spot_prices):
    """
    Jelenlegi verzió:
    - ha az oil-data.json tartalmazna külön live mezőket, azokat használnánk
    - ha nincs ilyen, fallbackként a spot árakat tesszük a live mezőbe is

    Később itt lehet bekötni külön valódi live forrást.
    """
    if not isinstance(oil_data, dict):
        return {
            "live_brent": spot_prices["spot_brent"],
            "live_wti": spot_prices["spot_wti"],
            "live_source": "spot_fallback"
        }

    live = oil_data.get("live")
    if isinstance(live, dict):
        live_brent = parse_number(live.get("brent"))
        live_wti = parse_number(live.get("wti"))

        if live_brent is not None or live_wti is not None:
            return {
                "live_brent": live_brent,
                "live_wti": live_wti,
                "live_source": "oil_data_live"
            }

    realtime = oil_data.get("realtime")
    if isinstance(realtime, dict):
        live_brent = parse_number(realtime.get("brent"))
        live_wti = parse_number(realtime.get("wti"))

        if live_brent is not None or live_wti is not None:
            return {
                "live_brent": live_brent,
                "live_wti": live_wti,
                "live_source": "oil_data_realtime"
            }

    return {
        "live_brent": spot_prices["spot_brent"],
        "live_wti": spot_prices["spot_wti"],
        "live_source": "spot_fallback"
    }


def extract_chokepoint_values(cp_data):
    if not isinstance(cp_data, dict):
        return {
            "global_trade_risk_index": None,
            "middle_east_conflict_impact": None,
            "middle_east_conflict_label": None,
            "daily_change": {},
            "top_risks": [],
            "me_components": {},
            "risk_meta": {},
        }

    me = cp_data.get("middle_east_conflict_impact", {}) or {}
    meta = cp_data.get("meta", {}) or {}

    return {
        "global_trade_risk_index": parse_number(cp_data.get("global_trade_risk_index")),
        "middle_east_conflict_impact": parse_number(me.get("score")),
        "middle_east_conflict_label": me.get("label"),
        "daily_change": cp_data.get("daily_change", {}) or {},
        "top_risks": cp_data.get("top_risks", []) or [],
        "me_components": me.get("components", {}) or {},
        "risk_meta": {
            "updated": meta.get("updated"),
            "method": meta.get("method"),
            "uses_tanker_signal": meta.get("uses_tanker_signal"),
            "uses_me_security_signal": meta.get("uses_me_security_signal"),
            "tanker_input_source": meta.get("tanker_input_source"),
            "me_security_input_source": meta.get("me_security_input_source"),
        },
    }


def main():
    oil_data = safe_load_json(OIL_FILE, {})
    cp_data = safe_load_json(CHOKEPOINT_FILE, {})

    now = datetime.now(timezone.utc)
    updated_str = now.strftime("%Y-%m-%d %H:%M UTC")

    spot_prices = extract_spot_prices(oil_data)
    live_prices = extract_live_prices(oil_data, spot_prices)
    cp_values = extract_chokepoint_values(cp_data)

    payload = {
        "meta": {
            "updated": updated_str,
            "source_mode": "live"
        },
        "prices": {
            "live_brent": live_prices["live_brent"],
            "live_wti": live_prices["live_wti"],
            "live_source": live_prices["live_source"],
            "spot_brent": spot_prices["spot_brent"],
            "spot_wti": spot_prices["spot_wti"],
            "spot_source": "oil_data_spot"
        },
        "risk": {
            "global_trade_risk_index": cp_values["global_trade_risk_index"],
            "middle_east_conflict_impact": cp_values["middle_east_conflict_impact"],
            "middle_east_conflict_label": cp_values["middle_east_conflict_label"],
            "daily_change": cp_values["daily_change"],
            "top_risks": cp_values["top_risks"],
            "me_components": cp_values["me_components"],
            "risk_meta": cp_values["risk_meta"],
        }
    }

    save_json(OUTPUT_FILE, payload)

    print(f"{OUTPUT_FILE} frissítve.")
    print(
        f"Live Brent: {live_prices['live_brent']} | "
        f"Spot Brent: {spot_prices['spot_brent']} | "
        f"Live source: {live_prices['live_source']}"
    )
    print(
        f"Risk index: {cp_values['global_trade_risk_index']} | "
        f"ME impact: {cp_values['middle_east_conflict_impact']} "
        f"({cp_values['middle_east_conflict_label']})"
    )


if __name__ == "__main__":
    main()
