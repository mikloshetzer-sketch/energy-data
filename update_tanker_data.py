import os
import json
import time
from datetime import datetime, timezone

from websocket import create_connection

API_KEY = os.getenv("AISSTREAM_API_KEY")
OUTPUT_FILE = "tanker-data.json"
STREAM_URL = "wss://stream.aisstream.io/v0/stream"

if not API_KEY:
    raise RuntimeError("Hiányzik az AISSTREAM_API_KEY környezeti változó.")


# Egyszerű bounding boxok a fő szűkületekhez
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

# Általános tanker ship type kódok
TANKER_TYPES = {80, 81, 82, 83, 84}


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


def safe_get_position(message):
    meta = message.get("MetaData", {}) or {}
    pos = message.get("Message", {}).get("PositionReport", {}) or {}

    lat = pos.get("Latitude")
    lon = pos.get("Longitude")

    if lat is None or lon is None:
        return None

    ship_type = meta.get("ShipType")
    if ship_type not in TANKER_TYPES:
        return None

    return {
        "name": meta.get("ShipName"),
        "mmsi": meta.get("MMSI"),
        "imo": meta.get("IMO"),
        "ship_type": ship_type,
        "lat": lat,
        "lon": lon,
        "speed": pos.get("Sog"),
        "course": pos.get("Cog"),
        "heading": pos.get("TrueHeading"),
        "destination": meta.get("Destination"),
        "zone": classify_zone(lat, lon),
    }


def collect_tankers(duration_seconds=60):
    ws = create_connection(STREAM_URL, timeout=20)

    subscribe_message = {
        "APIKey": API_KEY,
        "BoundingBoxes": [
            [[11.0, 28.5], [41.5, 58.8]],   # MENA + Suez + Hormuz térség
            [[40.5, 28.5], [41.5, 29.5]],   # Boszporusz
        ],
        "FilterMessageTypes": ["PositionReport"],
    }

    ws.send(json.dumps(subscribe_message))

    vessels = {}
    end_time = time.time() + duration_seconds

    while time.time() < end_time:
        try:
            raw = ws.recv()
            data = json.loads(raw)

            vessel = safe_get_position(data)
            if not vessel:
                continue

            key = str(vessel.get("mmsi") or vessel.get("imo") or vessel.get("name"))
            vessels[key] = vessel

        except Exception:
            continue

    ws.close()
    return list(vessels.values())


def build_summary(vessels):
    return {
        "tracked_tankers": len(vessels),
        "in_hormuz": sum(1 for v in vessels if v["zone"] == "hormuz"),
        "in_suez": sum(1 for v in vessels if v["zone"] == "suez"),
        "in_bab_el_mandeb": sum(1 for v in vessels if v["zone"] == "bab_el_mandeb"),
        "in_bosporus": sum(1 for v in vessels if v["zone"] == "bosporus"),
    }


def main():
    vessels = collect_tankers(duration_seconds=60)

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

    print(f"{OUTPUT_FILE} frissítve, hajók száma: {len(vessels)}")


if __name__ == "__main__":
    main()
