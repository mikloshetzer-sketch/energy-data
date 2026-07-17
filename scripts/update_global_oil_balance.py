#!/usr/bin/env python3
"""
EIA STEO-alapú globális olajpiaci fizikai mérleg.

Kimenet:
    docs/data/global_oil_balance.json

A fő dashboardérték mindig az aktuális naptári hónaphoz tartozó STEO-adat.
A teljes idősor ettől függetlenül tartalmazza a történeti és előrejelzési
hónapokat is.

Mérleg:
    balance_mbd = global_supply_mbd - global_demand_mbd

Pozitív érték: kínálati többlet.
Negatív érték: kínálati hiány / készletlehívási igény.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_FILE = ROOT / "docs" / "data" / "global_oil_balance.json"

EIA_API_KEY = os.environ.get("EIA_API_KEY")
EIA_STEO_URL = "https://api.eia.gov/v2/steo/data/"

SUPPLY_SERIES_ID = "PAPR_WORLD"
DEMAND_SERIES_ID = "PATC_WORLD"

SERIES_NAMES = {
    SUPPLY_SERIES_ID: "World petroleum and other liquid fuels production",
    DEMAND_SERIES_ID: "World petroleum and other liquid fuels consumption",
}

REQUEST_HEADERS = {
    "User-Agent": (
        "energy-data-dashboard/2.1 "
        "(https://github.com/mikloshetzer-sketch/energy-data)"
    )
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def current_month(value: datetime) -> str:
    return value.strftime("%Y-%m")


def save_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")

    with temporary.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
        file.write("\n")

    temporary.replace(path)


def request_steo_series(series_id: str, length: int = 60) -> list[dict[str, Any]]:
    if not EIA_API_KEY:
        raise RuntimeError("Hiányzik az EIA_API_KEY GitHub secret.")

    x_params = {
        "frequency": "monthly",
        "data": ["value"],
        "facets": {"seriesId": [series_id]},
        "sort": [{"column": "period", "direction": "desc"}],
        "offset": 0,
        "length": length,
    }

    response = requests.get(
        EIA_STEO_URL,
        params={"api_key": EIA_API_KEY},
        headers={
            **REQUEST_HEADERS,
            "X-Params": json.dumps(x_params),
        },
        timeout=45,
    )
    response.raise_for_status()

    payload = response.json()
    rows = payload.get("response", {}).get("data", [])

    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"Nincs EIA STEO adat ehhez a sorozathoz: {series_id}")

    cleaned: list[dict[str, Any]] = []

    for row in rows:
        period = row.get("period")
        raw_value = row.get("value")

        if not period or raw_value in (None, "", "NA", "--"):
            continue

        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue

        cleaned.append(
            {
                "period": str(period),
                "value": round(value, 4),
                "series_id": series_id,
            }
        )

    if not cleaned:
        raise RuntimeError(f"Nincs használható EIA STEO érték: {series_id}")

    return cleaned


def by_period(rows: list[dict[str, Any]]) -> dict[str, float]:
    return {row["period"]: float(row["value"]) for row in rows}


def classify_balance(balance: float) -> tuple[str, str, str]:
    if balance <= -1.0:
        return "deficit_high", "Jelentős hiány", "Material deficit"
    if balance <= -0.25:
        return "deficit", "Hiány", "Deficit"
    if balance < 0.25:
        return "balanced", "Közel egyensúly", "Near balance"
    if balance < 1.0:
        return "surplus", "Többlet", "Surplus"
    return "surplus_high", "Jelentős többlet", "Material surplus"


def build_summary_hu(label: str, row: dict[str, Any]) -> str:
    supply = row["global_supply_mbd"]
    demand = row["global_demand_mbd"]
    balance = row["balance_mbd"]
    period = row["period"]

    if balance < 0:
        return (
            f"Az EIA STEO {period} időszakra {supply:.2f} millió hordó/nap "
            f"globális folyékonyüzemanyag-kínálatot és {demand:.2f} millió "
            f"hordó/nap fogyasztást jelez. A {label} fizikai mérleg "
            f"{balance:.2f} millió hordó/nap, ami hiányt és készletlehívási "
            "igényt jelezhet."
        )

    return (
        f"Az EIA STEO {period} időszakra {supply:.2f} millió hordó/nap "
        f"globális folyékonyüzemanyag-kínálatot és {demand:.2f} millió "
        f"hordó/nap fogyasztást jelez. A {label} fizikai mérleg "
        f"+{balance:.2f} millió hordó/nap, ami kínálati többletet és "
        "készletépítési lehetőséget jelezhet."
    )


def build_summary_en(label: str, row: dict[str, Any]) -> str:
    supply = row["global_supply_mbd"]
    demand = row["global_demand_mbd"]
    balance = row["balance_mbd"]
    period = row["period"]

    if balance < 0:
        return (
            f"For {period}, the EIA STEO indicates global liquid-fuels supply "
            f"of {supply:.2f} million barrels per day and consumption of "
            f"{demand:.2f} million barrels per day. The {label} physical "
            f"balance is {balance:.2f} mb/d, indicating a deficit and "
            "potential inventory draw."
        )

    return (
        f"For {period}, the EIA STEO indicates global liquid-fuels supply "
        f"of {supply:.2f} million barrels per day and consumption of "
        f"{demand:.2f} million barrels per day. The {label} physical "
        f"balance is +{balance:.2f} mb/d, indicating a surplus and "
        "potential inventory build."
    )


def main() -> None:
    generated_at = utc_now()
    current_period = current_month(generated_at)

    supply_rows = request_steo_series(SUPPLY_SERIES_ID)
    demand_rows = request_steo_series(DEMAND_SERIES_ID)

    supply_map = by_period(supply_rows)
    demand_map = by_period(demand_rows)
    common_periods = sorted(set(supply_map) & set(demand_map))

    if not common_periods:
        raise RuntimeError(
            "A kínálati és keresleti EIA sorozatnak nincs közös időszaka."
        )

    series: list[dict[str, Any]] = []

    for period in common_periods:
        supply = supply_map[period]
        demand = demand_map[period]
        balance = round(supply - demand, 4)
        state, state_hu, state_en = classify_balance(balance)

        if period < current_period:
            period_type = "historical_or_estimate"
        elif period == current_period:
            period_type = "current_month_estimate"
        else:
            period_type = "forecast"

        series.append(
            {
                "period": period,
                "period_type": period_type,
                "global_supply_mbd": round(supply, 3),
                "global_demand_mbd": round(demand, 3),
                "balance_mbd": round(balance, 3),
                "balance_state": state,
                "balance_state_hu": state_hu,
                "balance_state_en": state_en,
            }
        )

    series_by_period = {row["period"]: row for row in series}

    if current_period in series_by_period:
        current = series_by_period[current_period]
        current_selection_method = "exact_current_month"
    else:
        non_future_rows = [row for row in series if row["period"] <= current_period]

        if not non_future_rows:
            raise RuntimeError(
                "Nincs az aktuális hónaphoz vagy azt megelőző időszakhoz "
                "tartozó közös EIA STEO adat."
            )

        current = non_future_rows[-1]
        current_selection_method = "latest_available_not_after_current_month"

    forecast_rows = [row for row in series if row["period"] > current["period"]]
    latest_forecast = forecast_rows[-1] if forecast_rows else None
    next_month_forecast = forecast_rows[0] if forecast_rows else None

    output = {
        "meta": {
            "generated_at": iso_utc(generated_at),
            "generator": "scripts/update_global_oil_balance.py",
            "generator_version": "1.1.0",
            "source": "U.S. Energy Information Administration (EIA), STEO",
            "source_url": "https://api.eia.gov/v2/steo/data/",
            "frequency": "monthly",
            "unit": "million barrels per day",
            "is_forecast_series": True,
            "method_version": "physical_balance_v2_current_period",
            "requested_current_period": current_period,
            "current_selection_method": current_selection_method,
        },
        "series_definition": {
            "supply": {
                "series_id": SUPPLY_SERIES_ID,
                "name": SERIES_NAMES[SUPPLY_SERIES_ID],
            },
            "demand": {
                "series_id": DEMAND_SERIES_ID,
                "name": SERIES_NAMES[DEMAND_SERIES_ID],
            },
            "balance_formula": "global_supply_mbd - global_demand_mbd",
            "interpretation": {
                "positive": "surplus / potential inventory build",
                "negative": "deficit / potential inventory draw",
            },
        },

        # Elsődleges, jelenlegi dashboardérték.
        "current": current,
        "current_period": current["period"],
        "current_global_supply_mbd": current["global_supply_mbd"],
        "current_global_demand_mbd": current["global_demand_mbd"],
        "current_balance_mbd": current["balance_mbd"],
        "current_balance_state": current["balance_state"],

        # Előrejelzési referencia.
        "next_month_forecast": next_month_forecast,
        "latest_forecast": latest_forecast,
        "forecast_until": (
            latest_forecast["period"] if latest_forecast is not None else None
        ),

        # Visszafelé kompatibilis fő mezők. Ezek most már a jelenlegi hónapot
        # jelentik, nem az idősor utolsó, távoli előrejelzési hónapját.
        "latest": current,
        "period": current["period"],
        "global_supply_mbd": current["global_supply_mbd"],
        "global_demand_mbd": current["global_demand_mbd"],
        "balance_mbd": current["balance_mbd"],
        "balance_state": current["balance_state"],
        "balance_state_hu": current["balance_state_hu"],
        "balance_state_en": current["balance_state_en"],

        "summary_hu": build_summary_hu("jelenlegi havi", current),
        "summary_en": build_summary_en("current-month", current),
        "forecast_summary_hu": (
            build_summary_hu("előrejelzési", latest_forecast)
            if latest_forecast is not None
            else None
        ),
        "forecast_summary_en": (
            build_summary_en("forecast", latest_forecast)
            if latest_forecast is not None
            else None
        ),
        "series": series,
    }

    save_json_atomic(OUTPUT_FILE, output)

    print(f"{OUTPUT_FILE.relative_to(ROOT)} frissítve.")
    print(
        f"Current period: {current['period']} | "
        f"supply={current['global_supply_mbd']:.3f}, "
        f"demand={current['global_demand_mbd']:.3f}, "
        f"balance={current['balance_mbd']:+.3f} mb/d"
    )

    if latest_forecast is not None:
        print(
            f"Forecast horizon: {latest_forecast['period']} | "
            f"balance={latest_forecast['balance_mbd']:+.3f} mb/d"
        )


if __name__ == "__main__":
    main()
