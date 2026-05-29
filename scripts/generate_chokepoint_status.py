import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

INTERPRETATION_FILE = ROOT / "docs" / "data" / "market_interpretation.json"
RISK_DRIVERS_FILE = ROOT / "docs" / "data" / "risk_drivers.json"
OUTPUT_FILE = ROOT / "docs" / "data" / "chokepoint_status.json"


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def level_from_score(score):
    if score < 35:
        return "LOW", "Alacsony", "Low"
    elif score < 65:
        return "MEDIUM", "Közepes", "Medium"
    return "HIGH", "Magas", "High"


def main():
    with open(INTERPRETATION_FILE, "r", encoding="utf-8") as f:
        interpretation = json.load(f)

    with open(RISK_DRIVERS_FILE, "r", encoding="utf-8") as f:
        drivers = json.load(f)

    components = interpretation.get("risk_components", {})

    chokepoint_base = components.get("chokepoint_risk")
    middle_east = components.get("middle_east_conflict_impact")
    combined_risk = interpretation.get("combined_risk_score")

    if chokepoint_base is None:
        chokepoint_base = 40

    if middle_east is None:
        middle_east = 40

    if combined_risk is None:
        combined_risk = 50

    # Heurisztikus szűkületi bontás:
    # A teljes chokepoint és közel-keleti kockázatból képzünk regionális státuszt.
    # Cél: stratégiai térképhez és dashboardhoz stabil, átlátható jelzőrendszer.
    chokepoints = [
        {
            "id": "hormuz",
            "name_hu": "Hormuzi-szoros",
            "name_en": "Strait of Hormuz",
            "region_hu": "Perzsa-öböl",
            "region_en": "Persian Gulf",
            "score": clamp(chokepoint_base * 0.55 + middle_east * 0.35 + combined_risk * 0.10),
            "strategic_note_hu": "A Perzsa-öböl olajexportjának legérzékenyebb tengeri szűkülete.",
            "strategic_note_en": "The most sensitive maritime chokepoint for Persian Gulf oil exports."
        },
        {
            "id": "bab_el_mandeb",
            "name_hu": "Bab el-Mandeb",
            "name_en": "Bab el-Mandeb",
            "region_hu": "Vörös-tenger",
            "region_en": "Red Sea",
            "score": clamp(chokepoint_base * 0.45 + middle_east * 0.45 + combined_risk * 0.10),
            "strategic_note_hu": "A Vörös-tenger és a Szuezi-csatorna felé tartó forgalom kritikus pontja.",
            "strategic_note_en": "A critical point for traffic toward the Red Sea and the Suez Canal."
        },
        {
            "id": "suez",
            "name_hu": "Szuezi-csatorna",
            "name_en": "Suez Canal",
            "region_hu": "Egyiptom",
            "region_en": "Egypt",
            "score": clamp(chokepoint_base * 0.35 + middle_east * 0.25 + combined_risk * 0.10),
            "strategic_note_hu": "Európa és Ázsia közötti tengeri energia- és áruforgalom kulcsútvonala.",
            "strategic_note_en": "A key route for maritime energy and goods trade between Europe and Asia."
        },
        {
            "id": "malacca",
            "name_hu": "Malaka-szoros",
            "name_en": "Strait of Malacca",
            "region_hu": "Délkelet-Ázsia",
            "region_en": "Southeast Asia",
            "score": clamp(chokepoint_base * 0.25 + combined_risk * 0.15),
            "strategic_note_hu": "Kína és Kelet-Ázsia energiaellátásának egyik legfontosabb tengeri kapuja.",
            "strategic_note_en": "One of the most important maritime gateways for China and East Asia’s energy supply."
        }
    ]

    for item in chokepoints:
        code, label_hu, label_en = level_from_score(item["score"])
        item["score"] = round(item["score"], 1)
        item["level"] = code
        item["level_hu"] = label_hu
        item["level_en"] = label_en

    chokepoints.sort(key=lambda x: x["score"], reverse=True)

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "method_hu": "A szűkületi státusz a globális szoroskockázat, a közel-keleti konfliktushatás és az összesített kockázati pontszám heurisztikus kombinációja.",
        "method_en": "Chokepoint status is a heuristic combination of global chokepoint risk, Middle East conflict impact and the combined risk score.",
        "drivers_reference": {
            "risk_score": drivers.get("risk_score"),
            "risk_level": drivers.get("risk_level")
        },
        "chokepoints": chokepoints
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("Chokepoint status generated")
    for item in chokepoints:
        print(f"{item['name_en']}: {item['score']} / {item['level']}")


if __name__ == "__main__":
    main()
