#!/usr/bin/env python3
"""
Top olajtermelők és fő nyersolaj-importőrök adatmodulja.

Kimenet:
    docs/data/oil_country_flows.json

A modul célja:
- 2023–2026 közötti éves országos összehasonlítás;
- külön termelői és importőri rangsor;
- közös évválasztó támogatása a dashboardon;
- a történeti és becsült adatok egyértelmű megkülönböztetése.

Mértékegység:
    millió hordó/nap (mb/d)

Fontos:
- A "production_mbd" nyersolaj- és kondenzátumtermelést jelöl.
- Az "imports_mbd" bruttó nyersolajimportot jelöl.
- 2025 és 2026 becslés, ezért a dashboardon ESTIMATE jelölést kap.
- Az értékek frissítése ehhez a fájlhoz kötött, ellenőrzött
  adatkarbantartással történik. A script nem talál ki automatikusan
  országos előrejelzéseket.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_FILE = ROOT / "docs" / "data" / "oil_country_flows.json"


# ------------------------------------------------------------------
# ELLENŐRZÖTT ÉVES ADATKÉSZLET
# ------------------------------------------------------------------
#
# A 2023–2024-es értékek történeti éves átlagok.
# A 2025–2026-os értékek becslések.
#
# A becslések célja a fő szerkezeti trendek bemutatása, nem pedig
# napi piaci vagy kereskedelmi elszámolás.
#
# A listák sorrendje nem számít: a script minden évnél automatikusan
# csökkenő sorrendbe rendezi az országokat.
#

COUNTRY_FLOW_DATA: dict[int, dict[str, Any]] = {
    2023: {
        "status": "historical",
        "producers": [
            {"country": "Egyesült Államok", "country_en": "United States", "value_mbd": 12.93},
            {"country": "Oroszország", "country_en": "Russia", "value_mbd": 10.10},
            {"country": "Szaúd-Arábia", "country_en": "Saudi Arabia", "value_mbd": 9.61},
            {"country": "Kanada", "country_en": "Canada", "value_mbd": 4.76},
            {"country": "Irak", "country_en": "Iraq", "value_mbd": 4.34},
            {"country": "Kína", "country_en": "China", "value_mbd": 4.18},
            {"country": "Brazília", "country_en": "Brazil", "value_mbd": 3.40},
        ],
        "importers": [
            {"country": "Kína", "country_en": "China", "value_mbd": 11.30},
            {"country": "Egyesült Államok", "country_en": "United States", "value_mbd": 6.48},
            {"country": "India", "country_en": "India", "value_mbd": 4.63},
            {"country": "Dél-Korea", "country_en": "South Korea", "value_mbd": 2.75},
            {"country": "Japán", "country_en": "Japan", "value_mbd": 2.54},
            {"country": "Németország", "country_en": "Germany", "value_mbd": 1.70},
            {"country": "Hollandia", "country_en": "Netherlands", "value_mbd": 1.62},
        ],
    },
    2024: {
        "status": "historical",
        "producers": [
            {"country": "Egyesült Államok", "country_en": "United States", "value_mbd": 13.21},
            {"country": "Oroszország", "country_en": "Russia", "value_mbd": 9.86},
            {"country": "Szaúd-Arábia", "country_en": "Saudi Arabia", "value_mbd": 8.96},
            {"country": "Kanada", "country_en": "Canada", "value_mbd": 4.95},
            {"country": "Irak", "country_en": "Iraq", "value_mbd": 4.28},
            {"country": "Kína", "country_en": "China", "value_mbd": 4.26},
            {"country": "Brazília", "country_en": "Brazil", "value_mbd": 3.36},
        ],
        "importers": [
            {"country": "Kína", "country_en": "China", "value_mbd": 11.07},
            {"country": "Egyesült Államok", "country_en": "United States", "value_mbd": 6.59},
            {"country": "India", "country_en": "India", "value_mbd": 4.80},
            {"country": "Dél-Korea", "country_en": "South Korea", "value_mbd": 2.76},
            {"country": "Japán", "country_en": "Japan", "value_mbd": 2.32},
            {"country": "Németország", "country_en": "Germany", "value_mbd": 1.69},
            {"country": "Hollandia", "country_en": "Netherlands", "value_mbd": 1.30},
        ],
    },
    2025: {
        "status": "estimate",
        "producers": [
            {"country": "Egyesült Államok", "country_en": "United States", "value_mbd": 13.46},
            {"country": "Oroszország", "country_en": "Russia", "value_mbd": 9.35},
            {"country": "Szaúd-Arábia", "country_en": "Saudi Arabia", "value_mbd": 9.18},
            {"country": "Kanada", "country_en": "Canada", "value_mbd": 5.08},
            {"country": "Irak", "country_en": "Iraq", "value_mbd": 4.20},
            {"country": "Kína", "country_en": "China", "value_mbd": 4.30},
            {"country": "Brazília", "country_en": "Brazil", "value_mbd": 3.55},
        ],
        "importers": [
            {"country": "Kína", "country_en": "China", "value_mbd": 11.02},
            {"country": "Egyesült Államok", "country_en": "United States", "value_mbd": 6.20},
            {"country": "India", "country_en": "India", "value_mbd": 4.98},
            {"country": "Dél-Korea", "country_en": "South Korea", "value_mbd": 2.82},
            {"country": "Japán", "country_en": "Japan", "value_mbd": 2.22},
            {"country": "Németország", "country_en": "Germany", "value_mbd": 1.62},
            {"country": "Hollandia", "country_en": "Netherlands", "value_mbd": 1.34},
        ],
    },
    2026: {
        "status": "estimate",
        "producers": [
            {"country": "Egyesült Államok", "country_en": "United States", "value_mbd": 13.32},
            {"country": "Oroszország", "country_en": "Russia", "value_mbd": 8.90},
            {"country": "Szaúd-Arábia", "country_en": "Saudi Arabia", "value_mbd": 8.75},
            {"country": "Kanada", "country_en": "Canada", "value_mbd": 5.12},
            {"country": "Kína", "country_en": "China", "value_mbd": 4.31},
            {"country": "Irak", "country_en": "Iraq", "value_mbd": 4.05},
            {"country": "Brazília", "country_en": "Brazil", "value_mbd": 3.70},
        ],
        "importers": [
            {"country": "Kína", "country_en": "China", "value_mbd": 10.82},
            {"country": "Egyesült Államok", "country_en": "United States", "value_mbd": 5.85},
            {"country": "India", "country_en": "India", "value_mbd": 5.10},
            {"country": "Dél-Korea", "country_en": "South Korea", "value_mbd": 2.87},
            {"country": "Japán", "country_en": "Japan", "value_mbd": 2.12},
            {"country": "Németország", "country_en": "Germany", "value_mbd": 1.55},
            {"country": "Hollandia", "country_en": "Netherlands", "value_mbd": 1.32},
        ],
    },
}


def utc_now_iso() -> str:
    """Aktuális UTC idő ISO-formátumban."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def round_value(value: float, digits: int = 2) -> float:
    """Numerikus érték egységes kerekítése."""
    return round(float(value), digits)


def sort_and_rank(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Országok csökkenő sorrendbe rendezése és rangsorolása.
    """
    sorted_rows = sorted(
        rows,
        key=lambda item: float(item["value_mbd"]),
        reverse=True,
    )

    ranked_rows: list[dict[str, Any]] = []

    for rank, row in enumerate(sorted_rows, start=1):
        ranked_rows.append(
            {
                "rank": rank,
                "country": row["country"],
                "country_en": row["country_en"],
                "value_mbd": round_value(row["value_mbd"]),
            }
        )

    return ranked_rows


def calculate_year_change(
    current_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """
    Előző évhez viszonyított változás hozzáadása országonként.
    """
    if previous_rows is None:
        return [
            {
                **row,
                "change_mbd": None,
                "change_percent": None,
            }
            for row in current_rows
        ]

    previous_map = {
        row["country_en"]: float(row["value_mbd"])
        for row in previous_rows
    }

    output: list[dict[str, Any]] = []

    for row in current_rows:
        previous_value = previous_map.get(row["country_en"])
        current_value = float(row["value_mbd"])

        if previous_value is None or previous_value == 0:
            change_mbd = None
            change_percent = None
        else:
            change_mbd = round_value(current_value - previous_value)
            change_percent = round_value(
                ((current_value - previous_value) / previous_value) * 100
            )

        output.append(
            {
                **row,
                "change_mbd": change_mbd,
                "change_percent": change_percent,
            }
        )

    return output


def validate_source_data() -> None:
    """A forrásadat alapvető szerkezeti ellenőrzése."""
    required_years = {2023, 2024, 2025, 2026}
    available_years = set(COUNTRY_FLOW_DATA)

    missing_years = required_years - available_years

    if missing_years:
        raise ValueError(
            f"Hiányzó évek a COUNTRY_FLOW_DATA objektumból: "
            f"{sorted(missing_years)}"
        )

    for year, year_data in COUNTRY_FLOW_DATA.items():
        if year_data.get("status") not in {"historical", "estimate", "forecast"}:
            raise ValueError(f"Érvénytelen státusz {year} évnél.")

        for category in ("producers", "importers"):
            rows = year_data.get(category)

            if not isinstance(rows, list) or not rows:
                raise ValueError(
                    f"Hiányzó vagy üres lista: {year} / {category}"
                )

            seen_countries: set[str] = set()

            for row in rows:
                required_keys = {"country", "country_en", "value_mbd"}
                missing_keys = required_keys - row.keys()

                if missing_keys:
                    raise ValueError(
                        f"Hiányzó mezők {year} / {category}: "
                        f"{sorted(missing_keys)}"
                    )

                country_key = str(row["country_en"])

                if country_key in seen_countries:
                    raise ValueError(
                        f"Duplikált ország {year} / {category}: "
                        f"{country_key}"
                    )

                seen_countries.add(country_key)

                if float(row["value_mbd"]) <= 0:
                    raise ValueError(
                        f"Nem pozitív érték {year} / {category}: "
                        f"{country_key}"
                    )


def build_year_summary(
    year: int,
    status: str,
    producers: list[dict[str, Any]],
    importers: list[dict[str, Any]],
) -> dict[str, Any]:
    """Automatikus rövid elemző összefoglaló."""
    top_producer = producers[0]
    top_importer = importers[0]

    fastest_producer = max(
        (
            row
            for row in producers
            if row["change_percent"] is not None
        ),
        key=lambda item: item["change_percent"],
        default=None,
    )

    fastest_importer = max(
        (
            row
            for row in importers
            if row["change_percent"] is not None
        ),
        key=lambda item: item["change_percent"],
        default=None,
    )

    findings_hu = [
        (
            f"A legnagyobb termelő {year}-ban: "
            f"{top_producer['country']} "
            f"({top_producer['value_mbd']:.2f} mb/d)."
        ),
        (
            f"A legnagyobb nyersolaj-importőr {year}-ban: "
            f"{top_importer['country']} "
            f"({top_importer['value_mbd']:.2f} mb/d)."
        ),
    ]

    findings_en = [
        (
            f"The largest producer in {year} is "
            f"{top_producer['country_en']} "
            f"({top_producer['value_mbd']:.2f} mb/d)."
        ),
        (
            f"The largest crude oil importer in {year} is "
            f"{top_importer['country_en']} "
            f"({top_importer['value_mbd']:.2f} mb/d)."
        ),
    ]

    if fastest_producer is not None:
        findings_hu.append(
            f"A vizsgált termelők közül a legnagyobb éves növekedést "
            f"{fastest_producer['country']} mutatja "
            f"({fastest_producer['change_percent']:+.2f}%)."
        )
        findings_en.append(
            f"Among the selected producers, "
            f"{fastest_producer['country_en']} records the strongest "
            f"annual increase ({fastest_producer['change_percent']:+.2f}%)."
        )

    if fastest_importer is not None:
        findings_hu.append(
            f"A vizsgált importőrök közül a legnagyobb éves növekedést "
            f"{fastest_importer['country']} mutatja "
            f"({fastest_importer['change_percent']:+.2f}%)."
        )
        findings_en.append(
            f"Among the selected importers, "
            f"{fastest_importer['country_en']} records the strongest "
            f"annual increase ({fastest_importer['change_percent']:+.2f}%)."
        )

    if status != "historical":
        findings_hu.append(
            "Az adott év országos értékei becslések, ezért később módosulhatnak."
        )
        findings_en.append(
            "Country-level values for this year are estimates and may be revised."
        )

    return {
        "findings_hu": findings_hu,
        "findings_en": findings_en,
    }


def build_output() -> dict[str, Any]:
    """A végleges JSON-struktúra előállítása."""
    validate_source_data()

    years_output: dict[str, Any] = {}

    previous_producers: list[dict[str, Any]] | None = None
    previous_importers: list[dict[str, Any]] | None = None

    for year in sorted(COUNTRY_FLOW_DATA):
        year_data = COUNTRY_FLOW_DATA[year]

        ranked_producers = sort_and_rank(year_data["producers"])
        ranked_importers = sort_and_rank(year_data["importers"])

        producers = calculate_year_change(
            ranked_producers,
            previous_producers,
        )

        importers = calculate_year_change(
            ranked_importers,
            previous_importers,
        )

        summary = build_year_summary(
            year=year,
            status=year_data["status"],
            producers=producers,
            importers=importers,
        )

        years_output[str(year)] = {
            "year": year,
            "status": year_data["status"],
            "producers": producers,
            "importers": importers,
            "summary": summary,
        }

        previous_producers = producers
        previous_importers = importers

    return {
        "generated_at": utc_now_iso(),
        "dataset": "oil_country_production_and_imports",
        "title_hu": "Fő olajtermelők és nyersolaj-importőrök",
        "title_en": "Top Oil Producers and Crude Oil Importers",
        "unit": "million_barrels_per_day",
        "unit_short": "mb/d",
        "default_year": 2026,
        "available_years": [2023, 2024, 2025, 2026],
        "methodology_hu": (
            "A termelői rangsor nyersolaj- és kondenzátumtermelést, "
            "az importőri rangsor bruttó nyersolajimportot mutat. "
            "A két mutató eltérő piaci oldalt mér, ezért külön diagramon "
            "jelenik meg. A 2025–2026-os országos értékek becslések."
        ),
        "methodology_en": (
            "The producer ranking covers crude oil and condensate production, "
            "while the importer ranking covers gross crude oil imports. "
            "The metrics represent different sides of the market and are "
            "therefore displayed in separate charts. Country-level values "
            "for 2025–2026 are estimates."
        ),
        "years": years_output,
        "sources": [
            {
                "name": "U.S. Energy Information Administration",
                "used_for": (
                    "Country crude oil production series and international "
                    "petroleum market comparison."
                ),
            },
            {
                "name": "National customs and energy statistics",
                "used_for": (
                    "Country crude oil import benchmarks and annualized "
                    "trade estimates."
                ),
            },
        ],
        "disclaimer_hu": (
            "A 2025–2026-os értékek becslések. Nem használhatók kereskedelmi, "
            "adózási vagy hivatalos statisztikai elszámolásra."
        ),
        "disclaimer_en": (
            "Values for 2025–2026 are estimates and should not be used for "
            "commercial, tax or official statistical reporting."
        ),
    }


def main() -> None:
    """JSON-fájl létrehozása."""
    output = build_output()

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        json.dump(
            output,
            file,
            ensure_ascii=False,
            indent=2,
        )

    print(f"Elkészült: {OUTPUT_FILE.relative_to(ROOT)}")

    for year in output["available_years"]:
        row = output["years"][str(year)]

        top_producer = row["producers"][0]
        top_importer = row["importers"][0]

        print(
            f"{year} [{row['status']}]: "
            f"producer={top_producer['country_en']} "
            f"{top_producer['value_mbd']:.2f} mb/d; "
            f"importer={top_importer['country_en']} "
            f"{top_importer['value_mbd']:.2f} mb/d"
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Hiba: {error}", file=sys.stderr)
        sys.exit(1)
