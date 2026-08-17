import json
from pathlib import Path
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[1]

CHINA_COST_FILE = ROOT / "china-oil-import.json"

CHINA_VOLUME_FILE = (
    ROOT
    / "docs"
    / "data"
    / "china_crude_import_volume.json"
)

CHOKEPOINT_FILE = (
    ROOT
    / "docs"
    / "data"
    / "chokepoint_status.json"
)

INTERPRETATION_FILE = (
    ROOT
    / "docs"
    / "data"
    / "market_interpretation.json"
)

OUTPUT_FILE = (
    ROOT
    / "docs"
    / "data"
    / "china_exposure.json"
)


def exposure_level(score):
    if score < 35:
        return "LOW", "Alacsony", "Low"

    elif score < 65:
        return "MEDIUM", "Közepes", "Medium"

    return "HIGH", "Magas", "High"


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def load_json(path):
    if not path.exists():
        raise RuntimeError(
            f"Required data file not found: {path}"
        )

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_latest_jodi_volume(volume_data):
    series = volume_data.get("series", [])

    if not series:
        raise RuntimeError(
            "China crude import JODI series is empty."
        )

    valid = [
        item
        for item in series
        if item.get("period")
        and item.get("import_volume_mbd") is not None
    ]

    if not valid:
        raise RuntimeError(
            "No valid China crude import observations found."
        )

    latest = max(
        valid,
        key=lambda x: x["period"],
    )

    return {
        "period": latest["period"],
        "import_volume_mbd": float(
            latest["import_volume_mbd"]
        ),
        "import_volume_kbd": latest.get(
            "import_volume_kbd"
        ),
        "assessment_code": latest.get(
            "assessment_code"
        ),
        "assessment_label": latest.get(
            "assessment_label"
        ),
    }


def get_latest_cost_data(cost_data):
    latest = (
        cost_data
        .get("summary", {})
        .get("latest")
    )

    if not latest:
        series = cost_data.get("series", [])

        if not series:
            raise RuntimeError(
                "China oil import cost series is empty."
            )

        latest = series[-1]

    return latest


def main():
    china_cost = load_json(
        CHINA_COST_FILE
    )

    china_volume = load_json(
        CHINA_VOLUME_FILE
    )

    chokepoints = load_json(
        CHOKEPOINT_FILE
    )

    interpretation = load_json(
        INTERPRETATION_FILE
    )

    # ------------------------------------------------
    # 1. Hivatalos havi importvolumen – JODI
    # ------------------------------------------------

    jodi_latest = get_latest_jodi_volume(
        china_volume
    )

    import_volume = (
        jodi_latest["import_volume_mbd"]
    )

    import_volume_period = (
        jodi_latest["period"]
    )

    # ------------------------------------------------
    # 2. Napi importköltség és Brent
    # ------------------------------------------------

    cost_latest = get_latest_cost_data(
        china_cost
    )

    import_cost = float(
        cost_latest.get(
            "estimated_import_value_billion_usd",
            0,
        )
    )

    brent_price = float(
        cost_latest.get(
            "brent_usd_per_barrel",
            0,
        )
    )

    market_date = cost_latest.get(
        "date"
    )

    cost_volume_period = cost_latest.get(
        "volume_period"
    )

    cost_volume_status = cost_latest.get(
        "volume_status"
    )

    # ------------------------------------------------
    # 3. Chokepoint adatok
    # ------------------------------------------------

    chokepoint_list = chokepoints.get(
        "chokepoints",
        [],
    )

    hormuz = next(
        (
            c
            for c in chokepoint_list
            if c.get("id") == "hormuz"
        ),
        None,
    )

    malacca = next(
        (
            c
            for c in chokepoint_list
            if c.get("id") == "malacca"
        ),
        None,
    )

    hormuz_score = (
        float(hormuz["score"])
        if hormuz
        else 50
    )

    malacca_score = (
        float(malacca["score"])
        if malacca
        else 50
    )

    combined_risk = float(
        interpretation.get(
            "combined_risk_score",
            50,
        )
    )

    # ------------------------------------------------
    # 4. China Exposure modell
    #
    # 40% importmennyiség
    # 25% Hormuz
    # 20% Malacca
    # 15% globális energiapiaci kockázat
    # ------------------------------------------------

    volume_score = min(
        import_volume * 8,
        100,
    )

    exposure_score = (
        volume_score * 0.40
        + hormuz_score * 0.25
        + malacca_score * 0.20
        + combined_risk * 0.15
    )

    exposure_score = round(
        clamp(exposure_score),
        1,
    )

    (
        level_code,
        level_hu,
        level_en,
    ) = exposure_level(
        exposure_score
    )

    # ------------------------------------------------
    # 5. Driver bontás
    # ------------------------------------------------

    drivers = [
        {
            "name_hu": "Importmennyiség",
            "name_en": "Import volume",
            "raw_score": round(
                volume_score,
                1,
            ),
            "weighted_score": round(
                volume_score * 0.40,
                1,
            ),
            "weight": 0.40,
            "source": "JODI Oil World Database",
        },
        {
            "name_hu": "Hormuzi kitettség",
            "name_en": "Hormuz exposure",
            "raw_score": round(
                hormuz_score,
                1,
            ),
            "weighted_score": round(
                hormuz_score * 0.25,
                1,
            ),
            "weight": 0.25,
        },
        {
            "name_hu": "Malaka kitettség",
            "name_en": "Malacca exposure",
            "raw_score": round(
                malacca_score,
                1,
            ),
            "weighted_score": round(
                malacca_score * 0.20,
                1,
            ),
            "weight": 0.20,
        },
        {
            "name_hu": (
                "Globális energiapiaci kockázat"
            ),
            "name_en": (
                "Global energy risk"
            ),
            "raw_score": round(
                combined_risk,
                1,
            ),
            "weighted_score": round(
                combined_risk * 0.15,
                1,
            ),
            "weight": 0.15,
        },
    ]

    drivers.sort(
        key=lambda x: x[
            "weighted_score"
        ],
        reverse=True,
    )

    # ------------------------------------------------
    # 6. Output
    # ------------------------------------------------

    output = {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "methodology": {
            "description": (
                "China Exposure combines official "
                "JODI crude-oil import volume with "
                "Hormuz exposure, Malacca exposure "
                "and global energy-market risk."
            ),

            "weights": {
                "import_volume": 0.40,
                "hormuz": 0.25,
                "malacca": 0.20,
                "global_energy_risk": 0.15,
            },

            "volume_score_formula": (
                "min(import_volume_mbd * 8, 100)"
            ),
        },

        "exposure_score": exposure_score,

        "exposure_level": level_code,

        "exposure_level_hu": level_hu,

        "exposure_level_en": level_en,

        "import_volume_mbd": round(
            import_volume,
            3,
        ),

        "import_volume_period": (
            import_volume_period
        ),

        "import_volume_source": (
            "JODI Oil World Database"
        ),

        "jodi_assessment_code": (
            jodi_latest[
                "assessment_code"
            ]
        ),

        "jodi_assessment_label": (
            jodi_latest[
                "assessment_label"
            ]
        ),

        "import_cost_billion_usd": round(
            import_cost,
            3,
        ),

        "brent_price_usd": round(
            brent_price,
            2,
        ),

        "market_date": market_date,

        "cost_volume_period": (
            cost_volume_period
        ),

        "cost_volume_status": (
            cost_volume_status
        ),

        "summary_hu": (
            f"Kína legfrissebb JODI szerinti "
            f"nyersolajimportja "
            f"{import_volume:.2f} millió hordó/nap "
            f"({import_volume_period}). "
            f"A legfrissebb piaci becslés szerint "
            f"az importköltség "
            f"{import_cost:.2f} milliárd USD/nap. "
            f"A China Exposure pontszám "
            f"{exposure_score}/100, "
            f"ami {level_hu.lower()} "
            f"kitettségi szintnek felel meg."
        ),

        "summary_en": (
            f"China's latest JODI crude-oil "
            f"import volume is "
            f"{import_volume:.2f} million barrels "
            f"per day ({import_volume_period}). "
            f"The latest estimated daily import "
            f"cost is approximately "
            f"{import_cost:.2f} billion USD. "
            f"The China Exposure score is "
            f"{exposure_score}/100, "
            f"corresponding to a "
            f"{level_en.lower()} exposure level."
        ),

        "drivers": drivers,
    }

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            output,
            f,
            ensure_ascii=False,
            indent=2,
        )

    print("China exposure generated")

    print(
        f"JODI volume: "
        f"{import_volume:.3f} mb/d "
        f"({import_volume_period})"
    )

    print(
        f"Brent: "
        f"{brent_price:.2f} USD/bbl "
        f"({market_date})"
    )

    print(
        f"Import cost: "
        f"{import_cost:.3f} bn USD/day"
    )

    print(
        f"Exposure score: "
        f"{exposure_score}"
    )

    print(
        f"Exposure level: "
        f"{level_code}"
    )


if __name__ == "__main__":
    main()
