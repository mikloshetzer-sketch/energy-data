import os
import json
import time
from datetime import datetime, timezone
from websocket import create_connection


API_KEY = os.getenv("AISSTREAM_API_KEY")
STREAM_URL = "wss://stream.aisstream.io/v0/stream"
OUTPUT_FILE = "tanker-data.json"

if not API_KEY:
    raise RuntimeError("Hiányzik az AISSTREAM_API_KEY környezeti változó.")


# AIS ship type tanker tartományok / tipikus tanker kódok
TANKER_TYPES = {80, 81, 82, 83, 84}

CHOKEPOINTS = {
    "hormuz": {
        "min_lat": 24.0,
        "max_lat": 28.5,
        "min_lon": 55.0,
        "max_lon": 58.8,
    },
    "suez": {
        "min_lat": 29.0,
        "max_lat": 31.8,
        "min_lon": 32.0,
        "max_lon": 33.8,
    },
    "bab_el_mandeb": {
        "min_lat": 11.0,
        "max_lat": 14.8,
        "min_lon": 42.0,
        "max_lon": 45.5,
    },
    "bosporus": {
        "min_lat": 40.8,
        "max_lat": 41.3,
        "min_lon": 28.9,
        "max_lon": 29.3,
    },
}


def in_bbox(lat, lon, box):
    return (
        box["min_lat"] <= lat <= box["max_lat"]
        and box["min_lon"] <= lon <= box["max_lon"]
    )


def classify_zone(lat, lon):
    for zone_name, box in CHOKEPOINTS.items():
        if in_bbox(lat, lon, box):
            return zone_name
    return "other"


def extract_static_info(data):
    """
    Kinyeri a statikus hajóadatokat ShipStaticData vagy StaticDataReport üzenetből.
    Kulcs: UserID (gyakorlatilag MMSI)
    """
    msg = data.get("Message", {}) or {}

    if "ShipStaticData" in msg:
        s = msg["ShipStaticData"] or {}
        user_id = s.get("UserID")
        if user_id is None:
            return None, None

        static_info = {
            "mmsi": user_id,
            "imo": s.get("ImoNumber"),
            "name": s.get("Name"),
            "destination": s.get("Destination"),
            "ship_type": s.get("Type"),
        }
        return user_id, static_info

    if "StaticDataReport" in msg:
        s = msg["StaticDataReport"] or {}
        user_id = s.get("UserID")
        if user_id is None:
            return None, None

        report_a = s.get("ReportA", {}) or {}
        report_b = s.get("ReportB", {}) or {}

        static_info = {
            "mmsi": user_id,
            "imo": None,
            "name": report_a.get("Name"),
            "destination": None,
            "ship_type": report_b.get("ShipType"),
        }
        return user_id, static_info

    return None, None


def extract_position_info(data):
    """
    Kinyeri a pozíciót PositionReport / StandardClassBPositionReport üzenetből.
    """
    msg = data.get("Message", {}) or {}

    position = None

    if "PositionReport" in msg:
        position = msg["PositionReport"] or {}
    elif "StandardClassBPositionReport" in msg:
        position = msg["StandardClassBPositionReport"] or {}
    else:
        return None, None

    user_id = position.get("UserID")
    lat = position.get("Latitude")
    lon = position.get("Longitude")

    if user_id is None or lat is None or lon is None:
        return None, None

    pos_info = {
        "mmsi": user_id,
        "lat": lat,
        "lon": lon,
        "speed": position.get("Sog"),
        "course": position.get("Cog"),
        "heading": position.get("TrueHeading"),
        "zone": classify_zone(lat, lon),
    }
    return user_id, pos_info


def merge_vessel(static_info, pos_info):
    ship_type = static_info.get("ship_type")

    if ship_type not in TANKER_TYPES:
        return None

    return {
        "name": static_info.get("name"),
        "mmsi": pos_info.get("mmsi"),
        "imo": static_info.get("imo"),
        "ship_type": ship_type,
        "lat": pos_info.get("lat"),
        "lon": pos_info.get("lon"),
        "speed": pos_info.get("speed"),
        "course": pos_info.get("course"),
        "heading": pos_info.get("heading"),
        "destination": static_info.get("destination"),
        "zone": pos_info.get("zone"),
    }


def collect_tankers(duration_seconds=90, connect_timeout=60, max_retries=3):
    last_error = None

    for attempt in range(1, max_retries + 1):
        ws = None

        try:
            print(f"AISStream kapcsolat próbálkozás {attempt}/{max_retries}")

            ws = create_connection(STREAM_URL, timeout=connect_timeout)

            subscribe_message = {
                "APIKey": API_KEY,
                "BoundingBoxes": [
                    [[11.0, 28.5], [41.5, 58.8]],
                    [[40.5, 28.5], [41.5, 29.5]],
                ],
                "FilterMessageTypes": [
                    "PositionReport",
                    "StandardClassBPositionReport",
                    "ShipStaticData",
                    "StaticDataReport"
                ],
            }

            ws.send(json.dumps(subscribe_message))
            print("Kapcsolódva. Adatgyűjtés indul.")

            static_cache = {}
            position_cache = {}

            end_time = time.time() + duration_seconds

            while time.time() < end_time:
                try:
                    raw = ws.recv()
                    data = json.loads(raw)

                    user_id, static_info = extract_static_info(data)
                    if user_id is not None and static_info is not None:
                        existing = static_cache.get(user_id, {})
                        merged = {**existing, **{k: v for k, v in static_info.items() if v not in (None, "", {})}}
                        static_cache[user_id] = merged
                        continue

                    user_id, pos_info = extract_position_info(data)
                    if user_id is not None and pos_info is not None:
                        position_cache[user_id] = pos_info
                        continue

                except Exception as inner_error:
                    print(f"Üzenet feldolgozási hiba: {inner_error}")
                    continue

            vessels = []
            for user_id, pos_info in position_cache.items():
                static_info = static_cache.get(user_id)
                if not static_info:
                    continue

                vessel = merge_vessel(static_info, pos_info)
                if vessel:
                    vessels.append(vessel)

            return vessels

        except Exception as e:
            last_error = e
            print(f"Kapcsolódási hiba: {e}")
            time.sleep(10)

        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

    raise RuntimeError(f"Nem sikerült kapcsolódni az AISStreamhez: {last_error}")


def build_summary(vessels):
    return {
        "tracked_tankers": len(vessels),
        "in_hormuz": sum(1 for v in vessels if v["zone"] == "hormuz"),
        "in_suez": sum(1 for v in vessels if v["zone"] == "suez"),
        "in_bab_el_mandeb": sum(1 for v in vessels if v["zone"] == "bab_el_mandeb"),
        "in_bosporus": sum(1 for v in vessels if v["zone"] == "bosporus"),
    }


def main():
    vessels = collect_tankers()

    payload = {
        "meta": {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "source": "AISStream",
        },
        "summary": build_summary(vessels),
        "vessels": vessels,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"{OUTPUT_FILE} frissítve. Tankerek száma: {len(vessels)}")


if __name__ == "__main__":
    main()
