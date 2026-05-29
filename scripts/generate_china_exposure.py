import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

CHINA_FILE = ROOT / "china-oil-import.json"
CHOKEPOINT_FILE = ROOT / "docs" / "data" / "chokepoint_status.json"
INTERPRETATION_FILE = ROOT / "docs" / "data" / "market_interpretation.json"

OUTPUT_FILE = ROOT / "docs" / "data" / "china_exposure.json"


def exposure_level(score):
    if score < 35:
        return "LOW", "Alacsony", "Low"
    elif score < 65:
        return "MEDIUM", "Közepes", "Medium"
    return "HIGH", "Magas", "High"


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def main():
    with open(CHINA_FILE, "r", encoding="utf-8") as f:
        china = json.load(f)

    with open(CHOKEPOINT_FILE, "r", encoding="utf-8") as f:
        chokepoints = json.load(f)

    with open(INTERPRETATION_FILE, "r", encoding="utf-8") as f:
        interpretation = json.load(f)

    latest = china.get("summary", {}).get("latest")

    if not latest:
        latest = china["series"][-1]

    import_volume = latest.get("estimated_import_volume_mbd", 0)
    import_cost = latest.get("estimated_import_value_billion_usd", 0)
    brent_price = latest.get("brent_usd_per_barrel", 0)

    chokepoint_list = chokepoints.get("chokepoints", [])

    hormuz = next(
        (c for c in chokepoint_list if c["id"] == "hormuz"),
        None
    )

    malacca = next(
        (c for c in chokepoint_list if c["id"] == "malacca"),
        None
    )

    hormuz_score = hormuz["score"] if hormuz else 50
    malacca_score = malacca["score"] if malacca else 50

    combined_risk = interpretation.get("combined_risk_score", 50)

    # Kína importfüggőségi modell
    # 40% importmennyiség
    # 25% Hormuz
    # 20% Malaka
    # 15% globális kockázat

    volume_score = min(import_volume * 8, 100)

    exposure_score = (
        volume_score * 0.40 +
        hormuz_score * 0.25 +
        malacca_score * 0.20 +
        combined_risk * 0.15
    )

    exposure_score = round(clamp(exposure_score), 1)

    level_code, level_hu, level_en = exposure_level(exposure_score)

    drivers = []

    drivers.append({
        "name_hu": "Importmennyiség",
        "name_en": "Import volume",
        "score": round(volume_score * 0.40, 1)
    })

    drivers.append({
        "name_hu": "Hormuzi kitettség",
        "name_en": "Hormuz exposure",
        "score": round(hormuz_score * 0.25, 1)
    })

    drivers.append({
        "name_hu": "Malaka kitettség",
        "name_en": "Malacca exposure",
        "score": round(malacca_score * 0.20, 1)
    })

    drivers.append({
        "name_hu": "Globális energiapiaci kockázat",
        "name_en": "Global energy risk",
        "score": round(combined_risk * 0.15, 1)
    })

    drivers.sort(key=lambda x: x["score"], reverse=True)

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),

        "exposure_score": exposure_score,

        "exposure_level": level_code,
        "exposure_level_hu": level_hu,
        "exposure_level_en": level_en,

        "import_volume_mbd": round(import_volume, 2),
        "import_cost_billion_usd": round(import_cost, 2),
        "brent_price_usd": round(brent_price, 2),

        "summary_hu":
            f"Kína becsült napi olajimportja "
            f"{import_volume:.2f} millió hordó. "
            f"Az importköltség megközelíti a "
            f"{import_cost:.2f} milliárd USD-t naponta. "
            f"A kitettségi pontszám {exposure_score}/100, "
            f"ami {level_hu.lower()} szintnek felel meg.",

        "summary_en":
            f"China's estimated daily oil imports amount to "
            f"{import_volume:.2f} million barrels. "
            f"Estimated import cost is approximately "
            f"{import_cost:.2f} billion USD per day. "
            f"The exposure score is {exposure_score}/100, "
            f"corresponding to a {level_en.lower()} exposure level.",

        "drivers": drivers
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("China exposure generated")
    print(f"Exposure score: {exposure_score}")
    print(f"Exposure level: {level_code}")


if __name__ == "__main__":
    main()
