import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

INTERPRETATION_FILE = ROOT / "docs" / "data" / "market_interpretation.json"
OUTPUT_FILE = ROOT / "docs" / "data" / "risk_drivers.json"


def safe_number(value, default=0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def main():
    if not INTERPRETATION_FILE.exists():
        raise FileNotFoundError(
            "Nem található a market_interpretation.json. "
            "Előbb fusson le a generate_market_interpretation.py."
        )

    with open(INTERPRETATION_FILE, "r", encoding="utf-8") as f:
        interpretation = json.load(f)

    components = interpretation.get("risk_components", {})

    chokepoint = safe_number(components.get("chokepoint_risk"))
    middle_east = safe_number(components.get("middle_east_conflict_impact"))
    price_risk = safe_number(components.get("brent_price_change_risk"))
    volatility_risk = safe_number(components.get("brent_volatility_risk"))

    drivers = [
        {
            "id": "middle_east_conflict",
            "label_hu": "Közel-keleti konfliktushatás",
            "label_en": "Middle East conflict impact",
            "raw_value": round(middle_east, 2),
            "weight": 0.25,
            "contribution_points": round(middle_east * 0.25, 2)
        },
        {
            "id": "chokepoint_risk",
            "label_hu": "Szoroskockázat",
            "label_en": "Chokepoint risk",
            "raw_value": round(chokepoint, 2),
            "weight": 0.40,
            "contribution_points": round(chokepoint * 0.40, 2)
        },
        {
            "id": "brent_price_change",
            "label_hu": "Brent árváltozási kockázat",
            "label_en": "Brent price-change risk",
            "raw_value": round(price_risk, 2),
            "weight": 0.20,
            "contribution_points": round(price_risk * 0.20, 2)
        },
        {
            "id": "brent_volatility",
            "label_hu": "Brent volatilitási kockázat",
            "label_en": "Brent volatility risk",
            "raw_value": round(volatility_risk, 2),
            "weight": 0.15,
            "contribution_points": round(volatility_risk * 0.15, 2)
        }
    ]

    drivers.sort(key=lambda x: x["contribution_points"], reverse=True)

    total = sum(d["contribution_points"] for d in drivers)

    for d in drivers:
        d["share_pct"] = round((d["contribution_points"] / total) * 100, 1) if total else 0

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "risk_score": interpretation.get("combined_risk_score"),
        "risk_level": interpretation.get("risk_level"),
        "risk_label_hu": interpretation.get("risk_label_hu"),
        "risk_label_en": interpretation.get("risk_label_en"),
        "method": {
            "hu": "A kockázati hozzájárulás az egyes komponensek normalizált értékéből és súlyából számított pontszám.",
            "en": "Risk contribution is calculated from each component's normalized value and model weight."
        },
        "drivers": drivers
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("Risk drivers generated")
    print(f"Total contribution: {total:.2f}")
    for d in drivers:
        print(f"{d['label_en']}: {d['contribution_points']} points")


if __name__ == "__main__":
    main()
