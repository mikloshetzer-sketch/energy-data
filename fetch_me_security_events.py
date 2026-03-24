import json
from datetime import datetime, timezone

import requests

EVENTS_URL = (
    "https://raw.githubusercontent.com/"
    "mikloshetzer-sketch/me-security-monitor/main/events.json"
)

OUTPUT_FILE = "me-security-events.json"


def save_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main():
    response = requests.get(EVENTS_URL, timeout=30)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError("A letöltött events.json nem lista formátumú.")

    save_json(OUTPUT_FILE, data)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"{OUTPUT_FILE} frissítve: {now}")
    print(f"Események száma: {len(data)}")
    print(f"Forrás: {EVENTS_URL}")


if __name__ == "__main__":
    main()
