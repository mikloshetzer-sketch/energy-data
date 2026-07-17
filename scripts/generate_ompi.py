#!/usr/bin/env python3

"""
Oil Market Pressure Index – OMPI v2

Input files:
- docs/data/global_oil_balance.json
- docs/data/inventory_stress.json
- docs/data/chokepoint_status.json
- docs/data/market_interpretation.json
- china-oil-import.json

Output files:
- docs/data/ompi.json
- docs/data/ompi-history.json

Fontos:
- A bemeneti JSON-fájlokat a script nem módosítja.
- A Brent ára nem része az OMPI pontszámának.
- A kínai komponens kizárólag az importvolumenből készül.
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "docs" / "data"

BALANCE_PATH = DATA_DIR / "global_oil_balance.json"
INVENTORY_PATH = DATA_DIR / "inventory_stress.json"
CHOKEPOINT_PATH = DATA_DIR / "chokepoint_status.json"
GEOPOLITICAL_PATH = DATA_DIR / "market_interpretation.json"

OMPI_PATH = DATA_DIR / "ompi.json"
OMPI_HISTORY_PATH = DATA_DIR / "ompi-history.json"

CHINA_CANDIDATE_PATHS = [
    ROOT / "china-oil-import.json",
    DATA_DIR / "china-oil-import.json",
    DATA_DIR / "china_oil_import.json",
    ROOT / "data" / "china-oil-import.json",
]

WEIGHTS = {
    "physical_balance": 0.35,
    "inventory_stress": 0.20,
    "opec_buffer": 0.15,
    "geopolitical_risk": 0.15,
    "china_import_momentum": 0.10,
    "chokepoint_risk": 0.05,
}

ROUTE_WEIGHTS = {
    "hormuz": 0.40,
    "bab_el_mandeb": 0.25,
    "suez": 0.20,
    "malacca": 0.15,
}

METHOD_VERSION = "ompi_v2_fundamental_2026_07_china_volume"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_timestamp() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def today_utc() -> str:
    return utc_now().strftime("%Y-%m-%d")


def clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def round_number(value: float | int | None, digits: int = 1) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def is_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def safe_float(value: Any, default: float | None = None) -> float | None:
    if is_number(value):
        return float(value)

    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        try:
            result = float(cleaned)
            if math.isfinite(result):
                return result
        except ValueError:
            pass

    return default


def load_json(path: Path, required: bool = True) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Hiányzó bemeneti fájl: {path}")
        return {}

    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Hibás JSON-fájl: {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise RuntimeError(f"A JSON gyökérelem nem objektum: {path}")

    return data


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
        )
        handle.write("\n")


def first_number(data: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = safe_float(data.get(key))
        if value is not None:
            return value
    return None


def first_text(data: dict[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def find_china_path() -> Path | None:
    for path in CHINA_CANDIDATE_PATHS:
        if path.exists():
            return path
    return None


def extract_latest_balance_record(data: dict[str, Any]) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []

    for key in (
        "latest",
        "latest_year_data",
        "latest_period",
        "summary",
    ):
        value = data.get(key)
        if isinstance(value, dict):
            candidates.append(value)

    for key in (
        "data",
        "history",
        "series",
        "annual_data",
        "monthly_data",
        "records",
    ):
        value = data.get(key)
        if isinstance(value, list):
            candidates.extend(
                row for row in value
                if isinstance(row, dict)
            )

    candidates.append(data)

    def record_period(record: dict[str, Any]) -> str:
        for key in ("period", "date", "month", "year"):
            value = record.get(key)
            if value is not None:
                return str(value)
        return ""

    candidates.sort(key=record_period)

    for record in reversed(candidates):
        balance = first_number(
            record,
            [
                "balance_mbd",
                "market_balance_mbd",
                "latest_balance_mbd",
                "balance",
            ],
        )

        supply = first_number(
            record,
            [
                "global_supply_mbd",
                "supply_mbd",
                "supply",
            ],
        )

        demand = first_number(
            record,
            [
                "global_demand_mbd",
                "demand_mbd",
                "demand",
            ],
        )

        if balance is None and supply is not None and demand is not None:
            balance = supply - demand

        if balance is not None:
            result = dict(record)
            result["_balance_mbd"] = balance
            result["_supply_mbd"] = supply
            result["_demand_mbd"] = demand
            return result

    raise RuntimeError(
        "Nem található felhasználható piaci mérleg "
        "a global_oil_balance.json fájlban."
    )


def build_physical_balance(data: dict[str, Any]) -> dict[str, Any]:
    record = extract_latest_balance_record(data)

    balance_mbd = float(record["_balance_mbd"])
    supply_mbd = record.get("_supply_mbd")
    demand_mbd = record.get("_demand_mbd")

    score = clamp(50.0 - balance_mbd * 25.0)

    period = (
        record.get("period")
        or record.get("month")
        or record.get("year")
        or record.get("date")
        or data.get("latest_year")
        or today_utc()
    )

    weight = WEIGHTS["physical_balance"]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "period": str(period),
        "global_supply_mbd": round_number(supply_mbd, 3),
        "global_demand_mbd": round_number(demand_mbd, 3),
        "source_generated_at": (
            data.get("generated_at")
            or data.get("updated_at")
        ),
        "data_quality": "AVAILABLE",
        "balance_mbd": round(balance_mbd, 3),
        "method": "clamp_50_minus_balance_times_25",
    }


def build_inventory_stress(data: dict[str, Any]) -> dict[str, Any]:
    score = first_number(
        data,
        [
            "inventory_stress_score",
            "score",
        ],
    )

    data_quality = first_text(
        data,
        [
            "data_quality",
            "quality",
            "status",
        ],
    ) or "UNKNOWN"

    if score is None:
        score = 50.0
        data_quality = "MISSING_NEUTRAL_FALLBACK"

    score = clamp(score)
    weight = WEIGHTS["inventory_stress"]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "data_quality": data_quality,
        "level": data.get("inventory_stress_level") or data.get("level"),
        "level_hu": (
            data.get("inventory_stress_level_hu")
            or data.get("level_hu")
        ),
        "level_en": (
            data.get("inventory_stress_level_en")
            or data.get("level_en")
        ),
        "source_generated_at": (
            data.get("generated_at")
            or data.get("updated_at")
        ),
        "summary_hu": data.get("summary_hu"),
        "summary_en": data.get("summary_en"),
    }


def get_opec_spare_capacity() -> float:
    raw_value = os.getenv(
        "OPEC_EFFECTIVE_SPARE_CAPACITY_MBD",
        "0.17",
    )

    value = safe_float(raw_value, 0.17)

    if value is None or value < 0:
        return 0.17

    return value


def build_opec_buffer(balance_component: dict[str, Any]) -> dict[str, Any]:
    balance_mbd = safe_float(
        balance_component.get("balance_mbd"),
        0.0,
    ) or 0.0

    deficit_mbd = max(0.0, -balance_mbd)
    spare_capacity_mbd = get_opec_spare_capacity()

    if deficit_mbd <= 0:
        coverage_ratio = 1.0
        score = 25.0
        method = "no_physical_deficit_low_pressure"
    else:
        coverage_ratio = spare_capacity_mbd / deficit_mbd

        # Ha az OPEC-puffer teljesen fedezi a hiányt, a nyomás alacsony.
        # Ha csak kis részét fedezi, a nyomás magas.
        score = clamp(100.0 - coverage_ratio * 80.0)
        method = "effective_spare_capacity_divided_by_deficit"

    weight = WEIGHTS["opec_buffer"]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "effective_spare_capacity_mbd": round(
            spare_capacity_mbd,
            3,
        ),
        "physical_deficit_mbd": round(deficit_mbd, 3),
        "coverage_ratio": round(coverage_ratio, 4),
        "coverage_pct": round(coverage_ratio * 100, 1),
        "method": method,
        "parameter_status": "TEMPORARY_STATIC_INPUT",
        "data_quality": "PARTIAL_STATIC_PARAMETER",
    }


def build_geopolitical_risk(data: dict[str, Any]) -> dict[str, Any]:
    risk_components = data.get("risk_components")

    score = None

    if isinstance(risk_components, dict):
        score = safe_float(
            risk_components.get(
                "middle_east_conflict_impact"
            )
        )

    data_quality = "AVAILABLE"

    if score is None:
        score = 50.0
        data_quality = "MISSING_NEUTRAL_FALLBACK"

    score = clamp(score)
    weight = WEIGHTS["geopolitical_risk"]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "source_field": (
            "risk_components.middle_east_conflict_impact"
        ),
        "data_quality": data_quality,
        "source_generated_at": (
            data.get("generated_at")
            or data.get("updated_at")
        ),
        "excluded_fields": [
            "combined_risk_score",
            "risk_components.chokepoint_risk",
            "risk_components.brent_price_change_risk",
            "risk_components.inventory_stress",
            "risk_components.brent_volatility_risk",
        ],
    }


def normalize_route_id(value: Any) -> str:
    if not isinstance(value, str):
        return ""

    return (
        value.strip()
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
        .replace("strait_of_", "")
        .replace("bab_al_mandeb", "bab_el_mandeb")
        .replace("malaka", "malacca")
    )


def extract_route_records(data: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for key in (
        "routes",
        "chokepoints",
        "route_status",
        "route_scores",
    ):
        value = data.get(key)

        if isinstance(value, list):
            records.extend(
                row for row in value
                if isinstance(row, dict)
            )

        elif isinstance(value, dict):
            for route_id, route_data in value.items():
                if isinstance(route_data, dict):
                    row = dict(route_data)
                    row.setdefault("id", route_id)
                else:
                    row = {
                        "id": route_id,
                        "score": route_data,
                    }
                records.append(row)

    for route_id in ROUTE_WEIGHTS:
        direct = data.get(route_id)

        if isinstance(direct, dict):
            row = dict(direct)
            row.setdefault("id", route_id)
            records.append(row)
        elif is_number(direct):
            records.append({
                "id": route_id,
                "score": direct,
            })

    return records


def build_chokepoint_risk(data: dict[str, Any]) -> dict[str, Any]:
    raw_records = extract_route_records(data)

    route_map: dict[str, dict[str, Any]] = {}

    for row in raw_records:
        route_id = normalize_route_id(
            row.get("id")
            or row.get("route")
            or row.get("name")
            or row.get("key")
        )

        if route_id not in ROUTE_WEIGHTS:
            continue

        score = first_number(
            row,
            [
                "score",
                "risk_score",
                "route_score",
                "value",
            ],
        )

        if score is None:
            continue

        route_map[route_id] = {
            "id": route_id,
            "score": clamp(score),
            "level": (
                row.get("level")
                or row.get("risk_level")
            ),
        }

    weighted_sum = 0.0
    available_weight = 0.0

    for route_id, route_weight in ROUTE_WEIGHTS.items():
        route = route_map.get(route_id)

        if route is None:
            continue

        weighted_sum += route["score"] * route_weight
        available_weight += route_weight

    if available_weight > 0:
        score = weighted_sum / available_weight
        data_quality = "AVAILABLE"
    else:
        score = 50.0
        data_quality = "MISSING_NEUTRAL_FALLBACK"

    weight = WEIGHTS["chokepoint_risk"]

    ordered_routes = [
        route_map[route_id]
        for route_id in ROUTE_WEIGHTS
        if route_id in route_map
    ]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "data_quality": data_quality,
        "source_generated_at": (
            data.get("generated_at")
            or data.get("updated_at")
        ),
        "method": "weighted_route_scores",
        "route_weights": ROUTE_WEIGHTS,
        "available_route_weight": round(
            available_weight,
            2,
        ),
        "routes": ordered_routes,
        "model_overlap": "PARTIAL",
        "temporary_reduced_weight": True,
    }


def extract_china_monthly_observations(
    data: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    A kínai importköltség-JSON módosítása nélkül olvassa ki:

    monthly_inputs[].month
    monthly_inputs[].estimated_import_volume_mbd

    A Brent-árat és az importköltséget nem használja.
    """

    rows = data.get("monthly_inputs")

    if not isinstance(rows, list):
        return []

    observations: list[dict[str, Any]] = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        month = row.get("month")
        volume = safe_float(
            row.get("estimated_import_volume_mbd")
        )

        if not isinstance(month, str) or not month.strip():
            continue

        if volume is None or volume <= 0:
            continue

        observations.append({
            "month": month.strip(),
            "volume_mbd": float(volume),
        })

    observations.sort(key=lambda item: item["month"])

    unique: dict[str, dict[str, Any]] = {}

    for observation in observations:
        unique[observation["month"]] = observation

    return list(unique.values())


def build_china_import_momentum(
    data: dict[str, Any],
    source_path: Path | None,
) -> dict[str, Any]:
    observations = extract_china_monthly_observations(data)
    observation_count = len(observations)
    weight = WEIGHTS["china_import_momentum"]

    if observation_count < 4:
        score = 50.0

        return {
            "score": score,
            "weight": weight,
            "weight_pct": weight * 100,
            "contribution": round(score * weight, 2),
            "data_quality": "INSUFFICIENT_HISTORY_FALLBACK",
            "observation_count": observation_count,
            "method": "neutral_fallback",
            "source_file": (
                str(source_path.relative_to(ROOT))
                if source_path is not None
                else None
            ),
            "excluded_source_fields": [
                "series[].brent_usd_per_barrel",
                "series[].estimated_import_value_billion_usd",
                "summary.latest.brent_usd_per_barrel",
                "summary.latest.estimated_import_value_billion_usd",
            ],
        }

    # Rövid idősor esetén az utolsó két hónapot hasonlítjuk
    # az azt megelőző hónapok átlagához.
    recent_rows = observations[-2:]
    reference_rows = observations[:-2]

    recent_average = sum(
        row["volume_mbd"]
        for row in recent_rows
    ) / len(recent_rows)

    reference_average = sum(
        row["volume_mbd"]
        for row in reference_rows
    ) / len(reference_rows)

    if reference_average <= 0:
        change_pct = 0.0
    else:
        change_pct = (
            (recent_average - reference_average)
            / reference_average
            * 100.0
        )

    # 50 pont a semleges helyzet.
    # Például -20%-os importmomentum körülbelül 30 pontot ad.
    score = clamp(50.0 + change_pct)

    latest = observations[-1]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "latest_month": latest["month"],
        "latest_import_volume_mbd": round(
            latest["volume_mbd"],
            3,
        ),
        "recent_average_mbd": round(
            recent_average,
            3,
        ),
        "reference_average_mbd": round(
            reference_average,
            3,
        ),
        "short_term_change_pct": round(
            change_pct,
            1,
        ),
        "observation_count": observation_count,
        "data_quality": "PARTIAL_SHORT_HISTORY",
        "method": "latest_2m_average_vs_previous_months_average",
        "source_file": (
            str(source_path.relative_to(ROOT))
            if source_path is not None
            else None
        ),
        "source_fields": [
            "monthly_inputs[].month",
            "monthly_inputs[].estimated_import_volume_mbd",
        ],
        "excluded_source_fields": [
            "series[].brent_usd_per_barrel",
            "series[].estimated_import_value_billion_usd",
            "summary.latest.brent_usd_per_barrel",
            "summary.latest.estimated_import_value_billion_usd",
        ],
    }


def classify_score(score: float) -> dict[str, str]:
    if score >= 80:
        return {
            "level": "VERY_HIGH",
            "level_hu": "Nagyon magas",
            "level_en": "Very high",
            "direction": "STRONGLY_BULLISH",
            "direction_hu": "Erősen felfelé mutató",
            "direction_en": "Strongly bullish",
            "description_hu": (
                "Rendkívül erős felfelé irányuló árnyomás "
                "és nagyon korlátozott piaci alkalmazkodóképesség."
            ),
            "description_en": (
                "Exceptionally strong upward price pressure "
                "and very limited market adjustment capacity."
            ),
        }

    if score >= 65:
        return {
            "level": "HIGH",
            "level_hu": "Magas",
            "level_en": "High",
            "direction": "BULLISH",
            "direction_hu": "Felfelé mutató",
            "direction_en": "Bullish",
            "description_hu": (
                "Erős felfelé irányuló árnyomás és "
                "korlátozott piaci alkalmazkodóképesség."
            ),
            "description_en": (
                "Strong upward price pressure and "
                "limited market adjustment capacity."
            ),
        }

    if score >= 45:
        return {
            "level": "MEDIUM",
            "level_hu": "Közepes",
            "level_en": "Medium",
            "direction": "NEUTRAL",
            "direction_hu": "Semleges",
            "direction_en": "Neutral",
            "description_hu": (
                "Kiegyensúlyozott, de érzékeny olajpiaci helyzet."
            ),
            "description_en": (
                "Balanced but sensitive oil-market conditions."
            ),
        }

    if score >= 30:
        return {
            "level": "LOW",
            "level_hu": "Alacsony",
            "level_en": "Low",
            "direction": "BEARISH",
            "direction_hu": "Lefelé mutató",
            "direction_en": "Bearish",
            "description_hu": (
                "Mérsékelt lefelé irányuló árnyomás és "
                "kedvezőbb piaci alkalmazkodóképesség."
            ),
            "description_en": (
                "Moderate downward price pressure and "
                "improved market adjustment capacity."
            ),
        }

    return {
        "level": "VERY_LOW",
        "level_hu": "Nagyon alacsony",
        "level_en": "Very low",
        "direction": "STRONGLY_BEARISH",
        "direction_hu": "Erősen lefelé mutató",
        "direction_en": "Strongly bearish",
        "description_hu": (
            "Erős lefelé irányuló árnyomás és "
            "jelentős piaci tartalék."
        ),
        "description_en": (
            "Strong downward price pressure and "
            "substantial market buffer capacity."
        ),
    }


def calculate_data_quality(
    components: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    missing_components: list[str] = []
    fallback_components: list[str] = []
    partial_components: list[str] = []

    confidence_penalty = 0.0
    covered_weight = 0.0

    for name, component in components.items():
        quality = str(
            component.get("data_quality", "UNKNOWN")
        ).upper()

        weight = safe_float(
            component.get("weight"),
            0.0,
        ) or 0.0

        if "MISSING" in quality:
            missing_components.append(name)
            confidence_penalty += weight * 100.0
            continue

        if "FALLBACK" in quality:
            fallback_components.append(name)
            confidence_penalty += weight * 50.0
            covered_weight += weight
            continue

        if (
            "PARTIAL" in quality
            or "SHORT_HISTORY" in quality
            or "STATIC" in quality
        ):
            partial_components.append(name)
            confidence_penalty += weight * 25.0
            covered_weight += weight
            continue

        covered_weight += weight

    coverage_pct = clamp(covered_weight * 100.0)
    confidence_score = clamp(100.0 - confidence_penalty)

    if missing_components:
        status = "LIMITED"
    elif fallback_components or partial_components:
        status = "PARTIAL"
    else:
        status = "GOOD"

    return {
        "status": status,
        "confidence_score": round(confidence_score, 1),
        "coverage_pct": round(coverage_pct, 1),
        "missing_components": missing_components,
        "fallback_components": fallback_components,
        "partial_components": partial_components,
    }


def build_summaries(
    score: float,
    classification: dict[str, str],
    components: dict[str, dict[str, Any]],
    data_quality: dict[str, Any],
) -> tuple[str, str]:
    balance = safe_float(
        components["physical_balance"].get("balance_mbd"),
        0.0,
    ) or 0.0

    largest_name, largest_component = max(
        components.items(),
        key=lambda item: safe_float(
            item[1].get("contribution"),
            0.0,
        ) or 0.0,
    )

    component_names_hu = {
        "physical_balance": "fizikai kínálat–keresleti mérleg",
        "inventory_stress": "készlethelyzet",
        "opec_buffer": "OPEC-puffer",
        "geopolitical_risk": "geopolitikai kockázat",
        "china_import_momentum": "kínai importmomentum",
        "chokepoint_risk": "tengeri szoroskockázat",
    }

    component_names_en = {
        "physical_balance": "physical supply-demand balance",
        "inventory_stress": "inventory conditions",
        "opec_buffer": "OPEC buffer",
        "geopolitical_risk": "geopolitical risk",
        "china_import_momentum": "Chinese import momentum",
        "chokepoint_risk": "maritime chokepoint risk",
    }

    summary_hu = (
        f"Az OMPI értéke {score:.1f}/100, ami "
        f"{classification['level_hu'].lower()} olajpiaci nyomást jelez. "
        f"A fizikai mérleg {balance:.2f} millió hordó/nap. "
        f"A legnagyobb súlyozott hozzájárulást jelenleg a "
        f"{component_names_hu[largest_name]} adja. "
        f"Az adatbizalmi státusz: {data_quality['status']} "
        f"({data_quality['confidence_score']:.1f}/100)."
    )

    summary_en = (
        f"The OMPI stands at {score:.1f}/100, indicating "
        f"{classification['level_en'].lower()} oil-market pressure. "
        f"The physical balance is {balance:.2f} million barrels per day. "
        f"The largest weighted contribution currently comes from the "
        f"{component_names_en[largest_name]}. "
        f"Data-confidence status: {data_quality['status']} "
        f"({data_quality['confidence_score']:.1f}/100)."
    )

    return summary_hu, summary_en


def load_history() -> list[dict[str, Any]]:
    if not OMPI_HISTORY_PATH.exists():
        return []

    try:
        with OMPI_HISTORY_PATH.open(
            "r",
            encoding="utf-8",
        ) as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return []

    if isinstance(data, dict):
        history = data.get("history")
        if isinstance(history, list):
            return [
                row for row in history
                if isinstance(row, dict)
            ]

    if isinstance(data, list):
        return [
            row for row in data
            if isinstance(row, dict)
        ]

    return []


def update_history(ompi: dict[str, Any]) -> dict[str, Any]:
    history = load_history()

    history_row = {
        "date": today_utc(),
        "generated_at": ompi["generated_at"],
        "score": ompi["score"],
        "level": ompi["level"],
        "direction": ompi["direction"],
        "data_quality": ompi["data_quality"]["status"],
        "confidence_score": (
            ompi["data_quality"]["confidence_score"]
        ),
        "components": {
            name: {
                "score": component.get("score"),
                "contribution": component.get("contribution"),
                "data_quality": component.get("data_quality"),
            }
            for name, component in ompi["components"].items()
        },
    }

    history = [
        row for row in history
        if row.get("date") != history_row["date"]
    ]

    history.append(history_row)
    history.sort(key=lambda row: str(row.get("date", "")))

    # Legfeljebb három évnyi napi rekord.
    history = history[-1095:]

    return {
        "generated_at": ompi["generated_at"],
        "index": "OMPI",
        "method_version": METHOD_VERSION,
        "history": history,
    }


def validate_weights() -> None:
    total = sum(WEIGHTS.values())

    if abs(total - 1.0) > 0.000001:
        raise RuntimeError(
            f"Az OMPI-súlyok összege nem 1.0: {total}"
        )


def main() -> None:
    validate_weights()

    balance_data = load_json(BALANCE_PATH)
    inventory_data = load_json(INVENTORY_PATH)
    chokepoint_data = load_json(CHOKEPOINT_PATH)
    geopolitical_data = load_json(GEOPOLITICAL_PATH)

    china_path = find_china_path()

    if china_path is not None:
        china_data = load_json(china_path)
    else:
        china_data = {}

    physical_balance = build_physical_balance(
        balance_data
    )

    components = {
        "physical_balance": physical_balance,
        "inventory_stress": build_inventory_stress(
            inventory_data
        ),
        "opec_buffer": build_opec_buffer(
            physical_balance
        ),
        "geopolitical_risk": build_geopolitical_risk(
            geopolitical_data
        ),
        "china_import_momentum": build_china_import_momentum(
            china_data,
            china_path,
        ),
        "chokepoint_risk": build_chokepoint_risk(
            chokepoint_data
        ),
    }

    score = round(
        sum(
            safe_float(
                component.get("contribution"),
                0.0,
            ) or 0.0
            for component in components.values()
        ),
        1,
    )

    classification = classify_score(score)
    data_quality = calculate_data_quality(components)

    summary_hu, summary_en = build_summaries(
        score,
        classification,
        components,
        data_quality,
    )

    ompi = {
        "generated_at": utc_timestamp(),
        "index": "OMPI",
        "index_name": "Oil Market Pressure Index",
        "method_version": METHOD_VERSION,
        "score": score,
        **classification,
        "weights": WEIGHTS,
        "components": components,
        "data_quality": data_quality,
        "market_confirmation": {
            "included_in_ompi": False,
            "brent_momentum": None,
            "status": "NOT_CALCULATED",
            "note_hu": (
                "A Brent-momentum nem része az OMPI pontszámának. "
                "Később külön piaci megerősítési rétegként használható."
            ),
            "note_en": (
                "Brent momentum is not part of the OMPI score. "
                "It may later be used as a separate "
                "market-confirmation layer."
            ),
        },
        "model_notes": {
            "chokepoint_weight_temporary": True,
            "chokepoint_overlap_warning": True,
            "opec_input_temporary_static": True,
            "brent_excluded_from_score": True,
            "china_source_files_modified": False,
            "china_exposure_score_excluded": True,
            "china_import_cost_excluded": True,
            "china_brent_price_excluded": True,
        },
        "summary_hu": summary_hu,
        "summary_en": summary_en,
    }

    history_output = update_history(ompi)

    write_json(OMPI_PATH, ompi)
    write_json(OMPI_HISTORY_PATH, history_output)

    china_component = components["china_import_momentum"]

    print("=" * 68)
    print("OMPI generálás sikeres")
    print("=" * 68)
    print(f"OMPI: {ompi['score']}/100")
    print(f"Szint: {ompi['level']}")
    print(f"Irány: {ompi['direction']}")
    print(
        "Adatbizalom: "
        f"{data_quality['status']} "
        f"({data_quality['confidence_score']}/100)"
    )
    print("-" * 68)
    print(
        "Kína importmomentum: "
        f"{china_component['score']}/100"
    )
    print(
        "Kínai megfigyelések: "
        f"{china_component['observation_count']}"
    )
    print(
        "Kínai adatminőség: "
        f"{china_component['data_quality']}"
    )
    print("-" * 68)
    print(f"Létrehozva: {OMPI_PATH}")
    print(f"Létrehozva: {OMPI_HISTORY_PATH}")
    print("=" * 68)


if __name__ == "__main__":
    main()
