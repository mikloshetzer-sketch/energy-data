#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "docs" / "data"

BALANCE_PATH = DATA_DIR / "global_crude_oil_fundamentals.json"
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

METHOD_VERSION = "ompi_v3_jodi_physical_tightness_2026_07"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_timestamp() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def today_utc() -> str:
    return utc_now().strftime("%Y-%m-%d")


def current_month_utc() -> str:
    return utc_now().strftime("%Y-%m")


def clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def safe_float(value: Any, default: float | None = None) -> float | None:
    if is_number(value):
        return float(value)
    if isinstance(value, str):
        try:
            result = float(value.strip().replace(" ", "").replace(",", "."))
            return result if math.isfinite(result) else default
        except ValueError:
            return default
    return default


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


def load_json(path: Path, required: bool = True) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Hiányzó bemeneti fájl: {path}")
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise RuntimeError(f"A JSON gyökérelem nem objektum: {path}")
    return data


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def normalize_period_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, int) and 1900 <= value <= 2200:
        return f"{value}-12"

    text = str(value).strip().replace("/", "-").replace(".", "-").replace("_", "-")
    if not text:
        return None

    match = re.match(r"^(\d{4})-(\d{1,2})(?:-(\d{1,2}))?", text)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        if 1900 <= year <= 2200 and 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"

    match = re.match(r"^(\d{4})-?Q([1-4])$", text.upper())
    if match:
        year, quarter = int(match.group(1)), int(match.group(2))
        return f"{year:04d}-{quarter * 3:02d}"

    if re.match(r"^\d{4}$", text):
        year = int(text)
        if 1900 <= year <= 2200:
            return f"{year:04d}-12"
    return None


def get_record_period(record: dict[str, Any]) -> str | None:
    for key in (
        "period", "date", "month", "year_month", "year", "timestamp",
        "datetime", "latest_period", "latest_month", "latest_year",
    ):
        period = normalize_period_value(record.get(key))
        if period:
            return period
    return None


def is_forecast_record(record: dict[str, Any]) -> bool:
    for key in ("is_forecast", "forecast", "projected", "is_projection", "is_estimate"):
        value = record.get(key)
        if value is True:
            return True
        if isinstance(value, str) and value.strip().lower() in {
            "true", "yes", "forecast", "projection", "projected", "estimate", "estimated"
        }:
            return True

    terms = ("forecast", "projection", "projected", "outlook", "scenario", "estimate", "estimated", "modelled", "modeled")
    for key in ("data_type", "type", "status", "category", "record_type", "method", "source_type"):
        value = record.get(key)
        if isinstance(value, str) and any(term in value.strip().lower() for term in terms):
            return True

    source_section = str(record.get("_source_section", "")).lower()
    return any(term in source_section for term in ("forecast", "projection", "outlook", "scenario"))


def extract_balance_values(record: dict[str, Any]) -> dict[str, Any] | None:
    balance = first_number(record, [
        "balance_mbd", "market_balance_mbd", "latest_balance_mbd",
        "supply_demand_balance_mbd", "net_balance_mbd", "balance",
    ])
    supply = first_number(record, [
        "global_supply_mbd", "supply_mbd", "total_supply_mbd", "oil_supply_mbd", "supply",
    ])
    demand = first_number(record, [
        "global_demand_mbd", "demand_mbd", "total_demand_mbd", "oil_demand_mbd", "demand",
    ])

    if balance is None and supply is not None and demand is not None:
        balance = supply - demand
    if balance is None:
        return None

    return {
        "balance_mbd": float(balance),
        "supply_mbd": float(supply) if supply is not None else None,
        "demand_mbd": float(demand) if demand is not None else None,
    }


def add_candidate_records(container: Any, source_section: str, target: list[dict[str, Any]]) -> None:
    if isinstance(container, list):
        for index, item in enumerate(container):
            add_candidate_records(item, f"{source_section}[{index}]", target)
        return
    if not isinstance(container, dict):
        return

    values = extract_balance_values(container)
    period = get_record_period(container)
    if values is not None and period is not None:
        record = dict(container)
        record.update({
            "_source_section": source_section,
            "_period": period,
            "_balance_mbd": values["balance_mbd"],
            "_supply_mbd": values["supply_mbd"],
            "_demand_mbd": values["demand_mbd"],
        })
        record["_is_forecast"] = is_forecast_record(record)
        target.append(record)

    for key, value in container.items():
        if isinstance(value, (dict, list)):
            add_candidate_records(value, f"{source_section}.{key}", target)


def collect_balance_candidates(data: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    add_candidate_records(data, "root", candidates)

    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    for record in candidates:
        key = (
            record["_period"],
            round(record["_balance_mbd"], 6),
            round(record["_supply_mbd"], 6) if record["_supply_mbd"] is not None else None,
            round(record["_demand_mbd"], 6) if record["_demand_mbd"] is not None else None,
        )
        existing = unique.get(key)
        if existing is None or (existing.get("_is_forecast") and not record.get("_is_forecast")):
            unique[key] = record
    return list(unique.values())


def candidate_priority(record: dict[str, Any]) -> tuple[int, int, int]:
    actual = 1 if not record.get("_is_forecast") else 0
    complete = 1 if record.get("_supply_mbd") is not None and record.get("_demand_mbd") is not None else 0
    source = str(record.get("_source_section", "")).lower()
    timeseries = 1 if any(term in source for term in ("series", "monthly", "history", "records", "data")) else 0
    return actual, complete, timeseries


def extract_latest_balance_record(data: dict[str, Any]) -> dict[str, Any]:
    candidates = collect_balance_candidates(data)
    if not candidates:
        raise RuntimeError("Nem található használható fizikai olajmérleg-rekord.")

    current_period = current_month_utc()
    non_future = [record for record in candidates if record["_period"] <= current_period]
    future = [record for record in candidates if record["_period"] > current_period]

    if not non_future:
        periods = sorted({record["_period"] for record in candidates})
        raise RuntimeError(
            f"Nincs jelenlegi vagy múltbeli mérlegrekord. Aktuális időszak: {current_period}. Elérhető: {periods}"
        )

    latest_period = max(record["_period"] for record in non_future)
    latest_candidates = [record for record in non_future if record["_period"] == latest_period]
    selected = dict(max(latest_candidates, key=candidate_priority))

    selected["_selection_method"] = (
        "latest_non_future_estimate" if selected.get("_is_forecast") else "latest_non_future_actual"
    )
    selected["_current_period"] = current_period
    selected["_future_records_excluded"] = len(future)
    selected["_candidate_count"] = len(candidates)
    selected["_non_future_candidate_count"] = len(non_future)
    return selected


def select_latest_jodi_annual_record(data: dict[str, Any]) -> dict[str, Any]:
    rows = data.get("annual_series")
    if not isinstance(rows, list):
        raise RuntimeError("A JODI fundamentals fájl nem tartalmaz annual_series listát.")

    usable = [row for row in rows if isinstance(row, dict) and safe_float(row.get("year")) is not None]
    if not usable:
        raise RuntimeError("A JODI fundamentals annual_series listája üres vagy hibás.")

    return max(usable, key=lambda row: int(safe_float(row.get("year"), 0) or 0))


def build_physical_balance(data: dict[str, Any]) -> dict[str, Any]:
    """Build a JODI-based physical-market tightness proxy.

    Compatibility note: the component remains under the ``physical_balance`` key
    because the dashboard and downstream generators already expect that name.
    ``balance_mbd`` is no longer presented as a measured global supply-demand
    balance. It is a capped stress-equivalent deficit derived from the JODI
    common-country production-minus-refinery-intake gap.
    """
    record = select_latest_jodi_annual_record(data)

    year = int(safe_float(record.get("year"), 0) or 0)
    period_label = str(record.get("period_label") or year)
    provisional = bool(record.get("provisional")) or str(record.get("period_type", "")).lower() == "ytd"
    months_available = int(safe_float(record.get("months_available"), 0) or 0)

    production = first_number(record, [
        "preliminary_average_reported_production_mbd",
        "average_reported_production_mbd",
    ])
    refinery_intake = first_number(record, [
        "preliminary_average_reported_refinery_intake_mbd",
        "average_reported_refinery_intake_mbd",
    ])
    common_gap = first_number(record, [
        "preliminary_average_common_country_gap_mbd",
        "average_common_country_gap_mbd",
    ])

    production_change = safe_float(record.get("production_change_same_period_mbd"), 0.0) or 0.0
    intake_change = safe_float(record.get("refinery_intake_change_same_period_mbd"), 0.0) or 0.0
    gap_change = safe_float(record.get("common_country_gap_change_same_period_mbd"), 0.0) or 0.0

    if production is None or refinery_intake is None or common_gap is None:
        raise RuntimeError("A legfrissebb JODI éves/YTD rekordból hiányzik termelési, finomítói vagy gap adat.")

    # Sub-scores: 50 is neutral, 100 indicates stronger upward price pressure.
    # A deeper negative common-country gap raises structural tightness, but it is
    # deliberately capped and is not treated as a literal global shortage.
    gap_level_score = clamp(50.0 + (-common_gap / 30.0) * 35.0)
    production_trend_score = clamp(50.0 - production_change * 8.0)
    intake_trend_score = clamp(50.0 + intake_change * 6.0)
    gap_change_score = clamp(50.0 - gap_change * 6.0)

    score = clamp(
        gap_level_score * 0.40
        + production_trend_score * 0.30
        + intake_trend_score * 0.20
        + gap_change_score * 0.10
    )

    # Compatibility proxy for OPEC coverage and dashboard stress modules.
    # One tenth of the common-country gap is used as a stress equivalent and
    # capped at 3 mb/d. This is explicitly not a measured global deficit.
    stress_deficit = clamp(max(0.0, -common_gap) / 10.0, 0.0, 3.0)
    balance_proxy = -stress_deficit

    comparison_months = int(safe_float(record.get("comparison_months"), 0) or 0)
    confidence = clamp(min(months_available, 12) / 12.0 * 100.0)
    quality = "PARTIAL_YTD_ESTIMATE" if provisional else "AVAILABLE"

    weight = WEIGHTS["physical_balance"]
    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "period": period_label,
        "year": year,
        "period_type": record.get("period_type") or ("ytd" if provisional else "full_year"),
        "provisional": provisional,
        "months_available": months_available,
        "comparison_year": record.get("comparison_year"),
        "comparison_months": comparison_months,
        "reported_production_mbd": round(production, 3),
        "reported_refinery_intake_mbd": round(refinery_intake, 3),
        "common_country_gap_mbd": round(common_gap, 3),
        "production_change_same_period_mbd": round(production_change, 3),
        "refinery_intake_change_same_period_mbd": round(intake_change, 3),
        "common_country_gap_change_same_period_mbd": round(gap_change, 3),
        "subscores": {
            "gap_level": round(gap_level_score, 1),
            "production_trend": round(production_trend_score, 1),
            "refinery_intake_trend": round(intake_trend_score, 1),
            "gap_change": round(gap_change_score, 1),
        },
        "subscore_weights": {
            "gap_level": 0.40,
            "production_trend": 0.30,
            "refinery_intake_trend": 0.20,
            "gap_change": 0.10,
        },
        "stress_equivalent_deficit_mbd": round(stress_deficit, 3),
        "balance_mbd": round(balance_proxy, 3),
        "global_supply_mbd": None,
        "global_demand_mbd": None,
        "source_generated_at": data.get("generated_at") or data.get("updated_at"),
        "source_file": "docs/data/global_crude_oil_fundamentals.json",
        "data_quality": quality,
        "confidence_score": round(confidence, 1),
        "selection_method": "latest_jodi_annual_or_ytd_record",
        "future_records_excluded": 0,
        "candidate_count": len(data.get("annual_series", [])) if isinstance(data.get("annual_series"), list) else 0,
        "non_future_candidate_count": len(data.get("annual_series", [])) if isinstance(data.get("annual_series"), list) else 0,
        "is_forecast": False,
        "method": "jodi_physical_tightness_v1",
        "interpretation_limit_hu": "A balance_mbd stresszegyenértékű proxy, nem mért globális kínálat–keresleti hiány. A common-country gap a közös jelentő országok termelése és finomítói betáplálása közötti különbség.",
        "interpretation_limit_en": "balance_mbd is a stress-equivalent proxy, not a measured global supply-demand deficit. The common-country gap compares production and refinery intake across the same reporting countries.",
    }


def build_inventory_stress(data: dict[str, Any]) -> dict[str, Any]:
    score = first_number(data, ["inventory_stress_score", "score"])
    quality = first_text(data, ["data_quality", "quality", "status"]) or "UNKNOWN"
    if score is None:
        score = 50.0
        quality = "MISSING_NEUTRAL_FALLBACK"

    score = clamp(score)
    weight = WEIGHTS["inventory_stress"]
    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "data_quality": quality,
        "level": data.get("inventory_stress_level") or data.get("level"),
        "level_hu": data.get("inventory_stress_level_hu") or data.get("level_hu"),
        "level_en": data.get("inventory_stress_level_en") or data.get("level_en"),
        "source_generated_at": data.get("generated_at") or data.get("updated_at"),
        "summary_hu": data.get("summary_hu"),
        "summary_en": data.get("summary_en"),
    }


def get_opec_spare_capacity() -> float:
    value = safe_float(os.getenv("OPEC_EFFECTIVE_SPARE_CAPACITY_MBD", "0.17"), 0.17)
    return 0.17 if value is None or value < 0 else value


def build_opec_buffer(balance_component: dict[str, Any]) -> dict[str, Any]:
    balance = safe_float(balance_component.get("balance_mbd"), 0.0) or 0.0
    deficit = max(0.0, -balance)
    spare = get_opec_spare_capacity()

    if deficit <= 0:
        coverage_ratio = 1.0
        score = 25.0
        method = "no_physical_deficit_low_pressure"
    else:
        coverage_ratio = spare / deficit
        score = clamp(100.0 - coverage_ratio * 80.0)
        method = "effective_spare_capacity_divided_by_deficit"

    weight = WEIGHTS["opec_buffer"]
    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "effective_spare_capacity_mbd": round(spare, 3),
        "physical_deficit_mbd": round(deficit, 3),
        "coverage_ratio": round(coverage_ratio, 4),
        "coverage_pct": round(coverage_ratio * 100, 1),
        "method": method,
        "parameter_status": "TEMPORARY_STATIC_INPUT",
        "data_quality": "PARTIAL_STATIC_PARAMETER",
    }


def build_geopolitical_risk(data: dict[str, Any]) -> dict[str, Any]:
    components = data.get("risk_components")
    score = safe_float(components.get("middle_east_conflict_impact")) if isinstance(components, dict) else None
    quality = "AVAILABLE"
    if score is None:
        score = 50.0
        quality = "MISSING_NEUTRAL_FALLBACK"

    score = clamp(score)
    weight = WEIGHTS["geopolitical_risk"]
    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "source_field": "risk_components.middle_east_conflict_impact",
        "data_quality": quality,
        "source_generated_at": data.get("generated_at") or data.get("updated_at"),
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
        value.strip().lower().replace("-", "_").replace(" ", "_")
        .replace("strait_of_", "").replace("bab_al_mandeb", "bab_el_mandeb")
        .replace("bab_el_mandab", "bab_el_mandeb").replace("malaka", "malacca")
    )


def extract_route_records(data: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for key in ("routes", "chokepoints", "route_status", "route_scores"):
        value = data.get(key)
        if isinstance(value, list):
            records.extend(row for row in value if isinstance(row, dict))
        elif isinstance(value, dict):
            for route_id, route_data in value.items():
                if isinstance(route_data, dict):
                    row = dict(route_data)
                    row.setdefault("id", route_id)
                else:
                    row = {"id": route_id, "score": route_data}
                records.append(row)

    for route_id in ROUTE_WEIGHTS:
        direct = data.get(route_id)
        if isinstance(direct, dict):
            row = dict(direct)
            row.setdefault("id", route_id)
            records.append(row)
        elif is_number(direct):
            records.append({"id": route_id, "score": direct})
    return records


def build_chokepoint_risk(data: dict[str, Any]) -> dict[str, Any]:
    route_map: dict[str, dict[str, Any]] = {}
    for row in extract_route_records(data):
        route_id = normalize_route_id(row.get("id") or row.get("route") or row.get("name") or row.get("key"))
        if route_id not in ROUTE_WEIGHTS:
            continue
        score = first_number(row, ["score", "risk_score", "route_score", "value"])
        if score is None:
            continue
        route_map[route_id] = {
            "id": route_id,
            "score": clamp(score),
            "level": row.get("level") or row.get("risk_level"),
        }

    weighted_sum = 0.0
    available_weight = 0.0
    for route_id, route_weight in ROUTE_WEIGHTS.items():
        route = route_map.get(route_id)
        if route is None:
            continue
        weighted_sum += route["score"] * route_weight
        available_weight += route_weight

    score = weighted_sum / available_weight if available_weight > 0 else 50.0
    quality = "AVAILABLE" if available_weight > 0 else "MISSING_NEUTRAL_FALLBACK"
    weight = WEIGHTS["chokepoint_risk"]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "data_quality": quality,
        "source_generated_at": data.get("generated_at") or data.get("updated_at"),
        "method": "weighted_route_scores",
        "route_weights": ROUTE_WEIGHTS,
        "available_route_weight": round(available_weight, 2),
        "routes": [route_map[key] for key in ROUTE_WEIGHTS if key in route_map],
        "model_overlap": "PARTIAL",
        "temporary_reduced_weight": True,
    }


def find_china_path() -> Path | None:
    for path in CHINA_CANDIDATE_PATHS:
        if path.exists():
            return path
    return None


def extract_china_monthly_observations(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = data.get("monthly_inputs")
    if not isinstance(rows, list):
        return []

    observations: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        month = normalize_period_value(row.get("month"))
        volume = safe_float(row.get("estimated_import_volume_mbd"))
        if month and volume is not None and volume > 0:
            observations[month] = {"month": month, "volume_mbd": float(volume)}
    return sorted(observations.values(), key=lambda item: item["month"])


def build_china_import_momentum(data: dict[str, Any], source_path: Path | None) -> dict[str, Any]:
    observations = extract_china_monthly_observations(data)
    count = len(observations)
    weight = WEIGHTS["china_import_momentum"]

    source_file = None
    if source_path is not None:
        try:
            source_file = str(source_path.relative_to(ROOT))
        except ValueError:
            source_file = str(source_path)

    if count < 4:
        score = 50.0
        return {
            "score": score,
            "weight": weight,
            "weight_pct": weight * 100,
            "contribution": round(score * weight, 2),
            "data_quality": "INSUFFICIENT_HISTORY_FALLBACK",
            "observation_count": count,
            "method": "neutral_fallback",
            "source_file": source_file,
        }

    recent = observations[-2:]
    reference = observations[:-2]
    recent_avg = sum(row["volume_mbd"] for row in recent) / len(recent)
    reference_avg = sum(row["volume_mbd"] for row in reference) / len(reference)
    change_pct = ((recent_avg - reference_avg) / reference_avg * 100.0) if reference_avg > 0 else 0.0
    score = clamp(50.0 + change_pct)
    latest = observations[-1]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "latest_month": latest["month"],
        "latest_import_volume_mbd": round(latest["volume_mbd"], 3),
        "recent_average_mbd": round(recent_avg, 3),
        "reference_average_mbd": round(reference_avg, 3),
        "short_term_change_pct": round(change_pct, 1),
        "observation_count": count,
        "data_quality": "PARTIAL_SHORT_HISTORY",
        "method": "latest_2m_average_vs_previous_months_average",
        "source_file": source_file,
        "source_fields": [
            "monthly_inputs[].month",
            "monthly_inputs[].estimated_import_volume_mbd",
        ],
        "excluded_source_fields": [
            "series[].brent_usd_per_barrel",
            "series[].estimated_import_value_billion_usd",
            "summary.latest.brent_usd_per_barrel",
            "summary.latest.estimated_import_value_billion_usd",
            "exposure_score",
        ],
    }


def classify_score(score: float) -> dict[str, str]:
    if score >= 80:
        return {"level": "VERY_HIGH", "level_hu": "Nagyon magas", "level_en": "Very high", "direction": "STRONGLY_BULLISH", "direction_hu": "Erősen felfelé mutató", "direction_en": "Strongly bullish", "description_hu": "Rendkívül erős felfelé irányuló árnyomás és nagyon korlátozott piaci alkalmazkodóképesség.", "description_en": "Exceptionally strong upward price pressure and very limited market adjustment capacity."}
    if score >= 65:
        return {"level": "HIGH", "level_hu": "Magas", "level_en": "High", "direction": "BULLISH", "direction_hu": "Felfelé mutató", "direction_en": "Bullish", "description_hu": "Erős felfelé irányuló árnyomás és korlátozott piaci alkalmazkodóképesség.", "description_en": "Strong upward price pressure and limited market adjustment capacity."}
    if score >= 45:
        return {"level": "MEDIUM", "level_hu": "Közepes", "level_en": "Medium", "direction": "NEUTRAL", "direction_hu": "Semleges", "direction_en": "Neutral", "description_hu": "Kiegyensúlyozott, de érzékeny olajpiaci helyzet.", "description_en": "Balanced but sensitive oil-market conditions."}
    if score >= 30:
        return {"level": "LOW", "level_hu": "Alacsony", "level_en": "Low", "direction": "BEARISH", "direction_hu": "Lefelé mutató", "direction_en": "Bearish", "description_hu": "Mérsékelt lefelé irányuló árnyomás és kedvezőbb piaci alkalmazkodóképesség.", "description_en": "Moderate downward price pressure and improved market adjustment capacity."}
    return {"level": "VERY_LOW", "level_hu": "Nagyon alacsony", "level_en": "Very low", "direction": "STRONGLY_BEARISH", "direction_hu": "Erősen lefelé mutató", "direction_en": "Strongly bearish", "description_hu": "Erős lefelé irányuló árnyomás és jelentős piaci tartalék.", "description_en": "Strong downward price pressure and substantial market buffer capacity."}


def calculate_data_quality(components: dict[str, dict[str, Any]]) -> dict[str, Any]:
    missing: list[str] = []
    fallback: list[str] = []
    partial: list[str] = []
    penalty = 0.0
    covered = 0.0

    for name, component in components.items():
        quality = str(component.get("data_quality", "UNKNOWN")).upper()
        weight = safe_float(component.get("weight"), 0.0) or 0.0

        if "MISSING" in quality:
            missing.append(name)
            penalty += weight * 100
        elif "FALLBACK" in quality:
            fallback.append(name)
            penalty += weight * 50
            covered += weight
        elif any(term in quality for term in ("PARTIAL", "SHORT_HISTORY", "STATIC", "ESTIMATE")):
            partial.append(name)
            penalty += weight * 25
            covered += weight
        else:
            covered += weight

    status = "LIMITED" if missing else "PARTIAL" if fallback or partial else "GOOD"
    return {
        "status": status,
        "confidence_score": round(clamp(100.0 - penalty), 1),
        "coverage_pct": round(clamp(covered * 100.0), 1),
        "missing_components": missing,
        "fallback_components": fallback,
        "partial_components": partial,
    }


def build_summaries(score: float, classification: dict[str, str], components: dict[str, dict[str, Any]], quality: dict[str, Any]) -> tuple[str, str]:
    balance = safe_float(components["physical_balance"].get("balance_mbd"), 0.0) or 0.0
    period = components["physical_balance"].get("period") or "ismeretlen"
    largest = max(components, key=lambda name: safe_float(components[name].get("contribution"), 0.0) or 0.0)

    names_hu = {
        "physical_balance": "fizikai kínálat–keresleti mérleg",
        "inventory_stress": "készlethelyzet",
        "opec_buffer": "OPEC-puffer",
        "geopolitical_risk": "geopolitikai kockázat",
        "china_import_momentum": "kínai importmomentum",
        "chokepoint_risk": "tengeri szoroskockázat",
    }
    names_en = {
        "physical_balance": "physical supply-demand balance",
        "inventory_stress": "inventory conditions",
        "opec_buffer": "OPEC buffer",
        "geopolitical_risk": "geopolitical risk",
        "china_import_momentum": "Chinese import momentum",
        "chokepoint_risk": "maritime chokepoint risk",
    }

    hu = (
        f"Az OMPI értéke {score:.1f}/100, ami {classification['level_hu'].lower()} olajpiaci nyomást jelez. "
        f"A {period} időszak JODI-alapú stresszegyenértéke {balance:.2f} millió hordó/nap. "
        f"A legnagyobb súlyozott hozzájárulást jelenleg a {names_hu[largest]} adja. "
        f"Az adatbizalmi státusz: {quality['status']} ({quality['confidence_score']:.1f}/100)."
    )
    en = (
        f"The OMPI stands at {score:.1f}/100, indicating {classification['level_en'].lower()} oil-market pressure. "
        f"The JODI-based stress equivalent for {period} is {balance:.2f} million barrels per day. "
        f"The largest weighted contribution currently comes from the {names_en[largest]}. "
        f"Data-confidence status: {quality['status']} ({quality['confidence_score']:.1f}/100)."
    )
    return hu, en


def load_history() -> list[dict[str, Any]]:
    if not OMPI_HISTORY_PATH.exists():
        return []
    try:
        with OMPI_HISTORY_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return []
    if isinstance(data, dict) and isinstance(data.get("history"), list):
        return [row for row in data["history"] if isinstance(row, dict)]
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def update_history(ompi: dict[str, Any]) -> dict[str, Any]:
    history = [row for row in load_history() if row.get("date") != today_utc()]
    history.append({
        "date": today_utc(),
        "generated_at": ompi["generated_at"],
        "score": ompi["score"],
        "level": ompi["level"],
        "direction": ompi["direction"],
        "data_quality": ompi["data_quality"]["status"],
        "confidence_score": ompi["data_quality"]["confidence_score"],
        "physical_balance_period": ompi["components"]["physical_balance"].get("period"),
        "components": {
            name: {
                "score": component.get("score"),
                "contribution": component.get("contribution"),
                "data_quality": component.get("data_quality"),
            }
            for name, component in ompi["components"].items()
        },
    })
    history.sort(key=lambda row: str(row.get("date", "")))
    return {
        "generated_at": ompi["generated_at"],
        "index": "OMPI",
        "method_version": METHOD_VERSION,
        "history": history[-1095:],
    }


def main() -> None:
    if abs(sum(WEIGHTS.values()) - 1.0) > 0.000001:
        raise RuntimeError("Az OMPI-súlyok összege nem 1.0.")

    balance_data = load_json(BALANCE_PATH)
    inventory_data = load_json(INVENTORY_PATH)
    chokepoint_data = load_json(CHOKEPOINT_PATH)
    geopolitical_data = load_json(GEOPOLITICAL_PATH)

    china_path = find_china_path()
    china_data = load_json(china_path) if china_path else {}

    physical = build_physical_balance(balance_data)
    components = {
        "physical_balance": physical,
        "inventory_stress": build_inventory_stress(inventory_data),
        "opec_buffer": build_opec_buffer(physical),
        "geopolitical_risk": build_geopolitical_risk(geopolitical_data),
        "china_import_momentum": build_china_import_momentum(china_data, china_path),
        "chokepoint_risk": build_chokepoint_risk(chokepoint_data),
    }

    score = round(sum(safe_float(component.get("contribution"), 0.0) or 0.0 for component in components.values()), 1)
    classification = classify_score(score)
    data_quality = calculate_data_quality(components)
    summary_hu, summary_en = build_summaries(score, classification, components, data_quality)

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
            "note_hu": "A Brent-momentum nem része az OMPI pontszámának. Külön piaci megerősítési rétegként használható.",
            "note_en": "Brent momentum is not part of the OMPI score. It may be used as a separate market-confirmation layer.",
        },
        "model_notes": {
            "future_balance_records_excluded": False,
            "physical_balance_uses_latest_non_future_period": False,
            "physical_balance_source": "JODI global_crude_oil_fundamentals.json",
            "physical_balance_is_stress_proxy": True,
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

    write_json(OMPI_PATH, ompi)
    write_json(OMPI_HISTORY_PATH, update_history(ompi))

    print("=" * 68)
    print("OMPI generálás sikeres")
    print(f"OMPI: {score}/100 | Szint: {ompi['level']} | Irány: {ompi['direction']}")
    print(f"Fizikai mérleg időszaka: {physical['period']}")
    print(f"Kizárt jövőbeli rekordok: {physical.get('future_records_excluded', 0)}")
    print(f"Létrehozva: {OMPI_PATH}")
    print(f"Létrehozva: {OMPI_HISTORY_PATH}")
    print("=" * 68)


if __name__ == "__main__":
    main()


