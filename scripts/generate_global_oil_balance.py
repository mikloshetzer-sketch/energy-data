#!/usr/bin/env python3

"""
Globális olajpiaci kínálat–kereslet adatmodul.

Feladata:
1. Letölti az EIA Short-Term Energy Outlook aktuális
   "World liquid fuels production and consumption" Excel-fájlját.
2. A havi értékekből éves átlagot számol.
3. Létrehozza a 2023–2026 közötti globális kínálati,
   keresleti és piaci egyenleg adatokat.
4. Elmenti az eredményt ide:

   docs/data/global_oil_balance.json

Mértékegység:
millió hordó/nap, azaz million barrels per day (mb/d)
"""

from __future__ import annotations

import json
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------
# ELÉRÉSI UTAK
# ---------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_FILE = (
    ROOT
    / "docs"
    / "data"
    / "global_oil_balance.json"
)

EIA_EXCEL_URL = (
    "https://www.eia.gov/outlooks/steo/xls/Fig3.xlsx"
)


# ---------------------------------------------------------
# 2023-AS BÁZISADAT
# ---------------------------------------------------------
#
# Az aktuális EIA Fig3.xlsx adatfájl jelenleg 2024-től
# tartalmaz havi értékeket.
#
# Emiatt a 2023-as éves bázisérték külön szerepel.
# Ezek world petroleum and other liquid fuels adatok.
#
# supply: világ folyékonyüzemanyag-termelése
# demand: világ folyékonyüzemanyag-fogyasztása
#

BASELINE_2023 = {
    "year": 2023,
    "supply_mbd": 101.84,
    "demand_mbd": 101.78,
    "status": "historical",
    "source_type": "annual_baseline",
}


# ---------------------------------------------------------
# SEGÉDFÜGGVÉNYEK
# ---------------------------------------------------------

def utc_now_iso() -> str:
    """Aktuális UTC idő ISO-formátumban."""
    return datetime.now(timezone.utc).replace(
        microsecond=0
    ).isoformat()


def round_value(value: float, digits: int = 2) -> float:
    """Lebegőpontos érték biztonságos kerekítése."""
    return round(float(value), digits)


def download_file(url: str, destination: Path) -> None:
    """
    Fájl letöltése egyszerű böngészőazonosítóval.

    Az EIA bizonyos szerverei visszautasíthatják az olyan
    kéréseket, amelyeknél nincs User-Agent fejléc.
    """

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 "
                "Energy-Intelligence-Dashboard/1.0"
            )
        },
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=60,
        ) as response:
            destination.write_bytes(response.read())

    except Exception as exc:
        raise RuntimeError(
            f"Az EIA Excel-fájl letöltése sikertelen: {exc}"
        ) from exc


def detect_data_rows(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Megkeresi az EIA Excel-fájl havi adatokat tartalmazó sorait.

    A jelenlegi munkalap szerkezete:

    1. oszlop: dátum
    2. oszlop: non-OPEC termelés
    3. oszlop: OPEC termelés
    6. oszlop: OECD fogyasztás
    7. oszlop: non-OECD fogyasztás

    A pozíciók nulla alapú indexként:
    1, 2, 3, 6, 7
    """

    if raw.shape[1] < 8:
        raise RuntimeError(
            "Az EIA Excel-fájl szerkezete megváltozott: "
            "nem található elegendő oszlop."
        )

    parsed_dates = pd.to_datetime(
        raw.iloc[:, 1],
        errors="coerce",
    )

    valid_rows = raw.loc[parsed_dates.notna()].copy()

    if valid_rows.empty:
        raise RuntimeError(
            "Nem találhatók havi dátumsorok az EIA fájlban."
        )

    data = pd.DataFrame(
        {
            "date": pd.to_datetime(
                valid_rows.iloc[:, 1],
                errors="coerce",
            ),
            "non_opec_production": pd.to_numeric(
                valid_rows.iloc[:, 2],
                errors="coerce",
            ),
            "opec_production": pd.to_numeric(
                valid_rows.iloc[:, 3],
                errors="coerce",
            ),
            "oecd_consumption": pd.to_numeric(
                valid_rows.iloc[:, 6],
                errors="coerce",
            ),
            "non_oecd_consumption": pd.to_numeric(
                valid_rows.iloc[:, 7],
                errors="coerce",
            ),
        }
    )

    data = data.dropna(
        subset=[
            "date",
            "non_opec_production",
            "opec_production",
            "oecd_consumption",
            "non_oecd_consumption",
        ]
    )

    if data.empty:
        raise RuntimeError(
            "Az EIA fájlban nem található feldolgozható "
            "termelési és fogyasztási adat."
        )

    return data


def calculate_annual_values(
    monthly_data: pd.DataFrame,
    start_year: int = 2024,
    end_year: int = 2026,
) -> list[dict[str, Any]]:
    """
    Havi adatokból éves átlagok számítása.

    Supply:
        non-OPEC termelés + OPEC termelés

    Demand:
        OECD fogyasztás + non-OECD fogyasztás

    Balance:
        supply - demand

    Pozitív balance:
        kínálati többlet

    Negatív balance:
        kínálati hiány / készletcsökkenési nyomás
    """

    data = monthly_data.copy()

    data["year"] = data["date"].dt.year

    data["supply_mbd"] = (
        data["non_opec_production"]
        + data["opec_production"]
    )

    data["demand_mbd"] = (
        data["oecd_consumption"]
        + data["non_oecd_consumption"]
    )

    annual_rows: list[dict[str, Any]] = []

    for year in range(start_year, end_year + 1):
        year_data = data[data["year"] == year]

        if year_data.empty:
            print(
                f"Figyelmeztetés: nincs EIA adat {year} évre.",
                file=sys.stderr,
            )
            continue

        supply = float(year_data["supply_mbd"].mean())
        demand = float(year_data["demand_mbd"].mean())
        balance = supply - demand

        month_count = int(year_data["date"].dt.month.nunique())

        status = (
            "forecast"
            if year >= datetime.now().year
            else "historical"
        )

        annual_rows.append(
            {
                "year": year,
                "supply_mbd": round_value(supply),
                "demand_mbd": round_value(demand),
                "balance_mbd": round_value(balance),
                "balance_status": get_balance_status(balance),
                "months_available": month_count,
                "status": status,
                "source_type": "eia_monthly_average",
            }
        )

    return annual_rows


def get_balance_status(balance: float) -> str:
    """
    Piaci egyenleg egyszerű kategorizálása.

    ±0,20 mb/d alatt a piacot közel kiegyensúlyozottnak
    tekintjük.
    """

    if balance > 0.20:
        return "surplus"

    if balance < -0.20:
        return "deficit"

    return "balanced"


def calculate_changes(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Éves változások hozzáadása.

    Kiszámolja:
    - kínálat éves változását;
    - kereslet éves változását;
    - egyenleg éves változását.
    """

    sorted_rows = sorted(
        rows,
        key=lambda item: item["year"],
    )

    previous: dict[str, Any] | None = None

    for row in sorted_rows:
        if previous is None:
            row["supply_change_mbd"] = None
            row["demand_change_mbd"] = None
            row["balance_change_mbd"] = None

        else:
            row["supply_change_mbd"] = round_value(
                row["supply_mbd"]
                - previous["supply_mbd"]
            )

            row["demand_change_mbd"] = round_value(
                row["demand_mbd"]
                - previous["demand_mbd"]
            )

            row["balance_change_mbd"] = round_value(
                row["balance_mbd"]
                - previous["balance_mbd"]
            )

        previous = row

    return sorted_rows


def build_output(
    annual_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """A dashboard számára használható végleges JSON felépítése."""

    latest_row = max(
        annual_rows,
        key=lambda item: item["year"],
    )

    return {
        "generated_at": utc_now_iso(),
        "dataset": "global_oil_supply_demand",
        "title_hu": "Globális olajkínálat és kereslet",
        "title_en": "Global Oil Supply and Demand",
        "unit": "million_barrels_per_day",
        "unit_short": "mb/d",
        "coverage": {
            "start_year": min(
                row["year"] for row in annual_rows
            ),
            "end_year": max(
                row["year"] for row in annual_rows
            ),
        },
        "latest_year": latest_row["year"],
        "latest_balance_mbd": latest_row["balance_mbd"],
        "latest_balance_status": (
            latest_row["balance_status"]
        ),
        "methodology_hu": (
            "A kínálat az OPEC- és nem OPEC-országok "
            "folyékonyüzemanyag-termelésének összege. "
            "A kereslet az OECD- és nem OECD-országok "
            "folyékonyüzemanyag-fogyasztásának összege. "
            "Az éves értékek a havi adatok számtani átlagai. "
            "A piaci egyenleg a kínálat és a kereslet "
            "különbsége."
        ),
        "methodology_en": (
            "Supply is calculated as OPEC plus non-OPEC "
            "liquid fuels production. Demand is calculated "
            "as OECD plus non-OECD liquid fuels consumption. "
            "Annual values are arithmetic averages of monthly "
            "data. Market balance equals supply minus demand."
        ),
        "interpretation_hu": {
            "surplus": (
                "A pozitív érték kínálati többletet jelez, "
                "ami általában lefelé irányuló árnyomást "
                "okozhat."
            ),
            "balanced": (
                "A nullához közeli érték közel "
                "kiegyensúlyozott piacot jelez."
            ),
            "deficit": (
                "A negatív érték kínálati hiányt vagy "
                "készletleépülést jelezhet, ami árfelhajtó "
                "hatású lehet."
            ),
        },
        "interpretation_en": {
            "surplus": (
                "A positive value indicates a supply surplus "
                "that can create downward price pressure."
            ),
            "balanced": (
                "A value close to zero indicates a broadly "
                "balanced market."
            ),
            "deficit": (
                "A negative value indicates a supply deficit "
                "or inventory draw, which can support prices."
            ),
        },
        "series": annual_rows,
        "sources": [
            {
                "name": (
                    "U.S. Energy Information Administration"
                ),
                "report": (
                    "Short-Term Energy Outlook – "
                    "World liquid fuels production "
                    "and consumption"
                ),
                "url": EIA_EXCEL_URL,
                "used_for": "2024-2026 monthly series",
            },
            {
                "name": (
                    "U.S. Energy Information Administration"
                ),
                "report": (
                    "Historical world petroleum and "
                    "other liquid fuels annual data"
                ),
                "used_for": "2023 annual baseline",
            },
        ],
        "disclaimer_hu": (
            "A 2026-os adat előrejelzés. Az értékek az EIA "
            "következő havi STEO kiadásában módosulhatnak. "
            "A mutató petroleum and other liquid fuels "
            "kategóriát használ, ezért nem kizárólag nyersolajat "
            "tartalmaz."
        ),
        "disclaimer_en": (
            "The 2026 value is a forecast and may change in "
            "subsequent EIA STEO releases. The dataset covers "
            "petroleum and other liquid fuels, not crude oil only."
        ),
    }


# ---------------------------------------------------------
# FŐ PROGRAM
# ---------------------------------------------------------

def main() -> None:
    """Adatok letöltése, feldolgozása és mentése."""

    print("Global oil balance generation started.")

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        excel_file = Path(temp_dir) / "eia_fig3.xlsx"

        print(f"EIA adat letöltése: {EIA_EXCEL_URL}")

        download_file(
            EIA_EXCEL_URL,
            excel_file,
        )

        try:
            raw_data = pd.read_excel(
                excel_file,
                sheet_name=0,
                header=None,
                engine="openpyxl",
            )

        except Exception as exc:
            raise RuntimeError(
                f"Az EIA Excel-fájl nem olvasható: {exc}"
            ) from exc

        monthly_data = detect_data_rows(raw_data)

        calculated_rows = calculate_annual_values(
            monthly_data,
            start_year=2024,
            end_year=2026,
        )

    baseline_supply = BASELINE_2023["supply_mbd"]
    baseline_demand = BASELINE_2023["demand_mbd"]
    baseline_balance = (
        baseline_supply - baseline_demand
    )

    baseline_row = {
        **BASELINE_2023,
        "balance_mbd": round_value(
            baseline_balance
        ),
        "balance_status": get_balance_status(
            baseline_balance
        ),
        "months_available": 12,
    }

    annual_rows = calculate_changes(
        [baseline_row, *calculated_rows]
    )

    output = build_output(annual_rows)

    with OUTPUT_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            output,
            file,
            ensure_ascii=False,
            indent=2,
        )

    print(
        f"Elkészült: {OUTPUT_FILE.relative_to(ROOT)}"
    )

    for row in annual_rows:
        print(
            f"{row['year']}: "
            f"supply={row['supply_mbd']:.2f} mb/d, "
            f"demand={row['demand_mbd']:.2f} mb/d, "
            f"balance={row['balance_mbd']:+.2f} mb/d, "
            f"status={row['status']}"
        )


if __name__ == "__main__":
    try:
        main()

    except Exception as error:
        print(
            f"Hiba: {error}",
            file=sys.stderr,
        )
        sys.exit(1)
