#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Live AIS Collector v1.1
==============================

Purpose
-------
Fetch and analyse live / near-live AIS-derived Strait of Hormuz data
from the public Hormuz API.

Output:
    hormuz-live-ais.json

IMPORTANT
---------
This collector remains independent from:

    fetch_hormuz_flow.py
    hormuz-flow.json
    hormuz_risk_model.py
    hormuz-risk.json
    OMPI

It does NOT modify or overwrite any existing model output.

The Hormuz API is treated as a LIVE AIS observation source.

Estimated oil-export values are AIS-derived estimates,
NOT independently measured physical oil throughput.

Version 1.1 adds:
- complete-day detection
- 7 / 14 / 30 day crossing statistics
- 7 / 30 day oil-export proxy statistics
- missing oil-export day detection
- trend calculations
- data-quality assessment
- normalized model-role metadata

No final Flow Risk Score is calculated here.
"""

from __future__ import annotations

import json
import sys

from collections import defaultdict
from datetime import datetime, timezone, date
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# ============================================================
# CONFIG
# ============================================================

MODEL_NAME = "Hormuz Live AIS Collector"
MODEL_VERSION = "1.1-live-analysis"

BASE_URL = "https://hormuz.data-tracking.net"

OUTPUT_FILE = Path("hormuz-live-ais.json")

HTTP_TIMEOUT_SECONDS = 30

SUMMARY_HOURS = 24
DAILY_DAYS = 30

ENDPOINTS = {
    "summary": f"/api/summary?hours={SUMMARY_HOURS}",
    "crossings_by_type": (
        f"/api/crossings/by_type?hours={SUMMARY_HOURS}"
    ),
    "daily_crossings": (
        f"/api/crossings/daily?days={DAILY_DAYS}"
    ),
    "daily_oil_export": (
        f"/api/oil_export/daily?days={DAILY_DAYS}"
    ),
}


# ============================================================
# BASIC HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_float(
    value: Any,
    default: Optional[float] = None,
) -> Optional[float]:

    if value is None:
        return default

    try:
        return float(value)

    except (TypeError, ValueError):
        return default


def safe_int(
    value: Any,
    default: Optional[int] = None,
) -> Optional[int]:

    if value is None:
        return default

    try:
        return int(float(value))

    except (TypeError, ValueError):
        return default


def round_or_none(
    value: Optional[float],
    digits: int = 2,
) -> Optional[float]:

    if value is None:
        return None

    return round(value, digits)


def pct_change(
    current: Optional[float],
    baseline: Optional[float],
) -> Optional[float]:

    if (
        current is None
        or baseline is None
        or baseline == 0
    ):
        return None

    return (
        (current - baseline)
        / baseline
        * 100.0
    )


def average(
    values: List[float],
) -> Optional[float]:

    if not values:
        return None

    return mean(values)


def parse_day(
    value: Any,
) -> Optional[date]:

    if not value:
        return None

    try:
        return datetime.strptime(
            str(value),
            "%Y-%m-%d",
        ).date()

    except ValueError:
        return None


# ============================================================
# HTTP
# ============================================================

def fetch_json(
    endpoint: str,
) -> Any:

    url = BASE_URL + endpoint

    request = Request(
        url,
        headers={
            "User-Agent": (
                "energy-data-hormuz-live-ais/1.1"
            ),
            "Accept": "application/json",
        },
    )

    try:

        with urlopen(
            request,
            timeout=HTTP_TIMEOUT_SECONDS,
        ) as response:

            status = getattr(
                response,
                "status",
                200,
            )

            raw = (
                response
                .read()
                .decode("utf-8")
            )

            if status != 200:

                raise RuntimeError(
                    f"HTTP {status} for {endpoint}"
                )

            data = json.loads(raw)

            if not isinstance(
                data,
                (dict, list),
            ):

                raise RuntimeError(
                    f"Unexpected JSON type "
                    f"for {endpoint}"
                )

            return data

    except HTTPError as exc:

        raise RuntimeError(
            f"HTTP error for {endpoint}: "
            f"{exc.code} {exc.reason}"
        ) from exc

    except URLError as exc:

        raise RuntimeError(
            f"URL error for {endpoint}: "
            f"{exc.reason}"
        ) from exc

    except TimeoutError as exc:

        raise RuntimeError(
            f"Timeout for {endpoint}"
        ) from exc

    except json.JSONDecodeError as exc:

        raise RuntimeError(
            f"Invalid JSON from {endpoint}"
        ) from exc


def safe_fetch(
    name: str,
    endpoint: str,
) -> Dict[str, Any]:

    try:

        data = fetch_json(endpoint)

        return {
            "status": "OK",
            "endpoint": endpoint,
            "data": data,
            "error": None,
        }

    except Exception as exc:

        return {
            "status": "ERROR",
            "endpoint": endpoint,
            "data": None,
            "error": (
                f"{type(exc).__name__}: {exc}"
            ),
        }


# ============================================================
# SUMMARY NORMALIZATION
# ============================================================

def normalize_summary(
    raw: Any,
) -> Dict[str, Any]:

    if not isinstance(raw, dict):

        return {
            "available": False,
            "raw": raw,
        }

    return {
        "available": True,

        "period_hours": safe_int(
            raw.get("period_hours")
        ),

        "last_poll": raw.get(
            "last_poll"
        ),

        "total_ships": safe_int(
            raw.get("total_ships")
        ),

        "persian_gulf_ships": safe_int(
            raw.get("persian_gulf_ships")
        ),

        "gulf_of_oman_ships": safe_int(
            raw.get("gulf_of_oman_ships")
        ),

        "in_strait": safe_int(
            raw.get("in_strait")
        ),

        "crossings": {
            "inbound": safe_int(
                raw.get("inbound")
            ),

            "outbound": safe_int(
                raw.get("outbound")
            ),

            "total": safe_int(
                raw.get("total_crossings")
            ),
        },

        "estimated_oil_export": {
            "barrels": safe_float(
                raw.get("oil_export_barrels")
            ),

            "crude_barrels": safe_float(
                raw.get(
                    "oil_export_crude_barrels"
                )
            ),

            "petrochem_barrels": safe_float(
                raw.get(
                    "oil_export_petrochem_barrels"
                )
            ),

            "measurement_role": (
                "AIS-derived oil export estimate"
            ),

            "physical_flow_measurement": False,
        },

        "raw": raw,
    }


# ============================================================
# SOURCE STATUS
# ============================================================

def evaluate_source_status(
    results: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:

    total = len(results)

    successful = sum(
        1
        for result in results.values()
        if result.get("status") == "OK"
    )

    failed = total - successful

    if successful == total:
        status = "AVAILABLE"
        confidence = "HIGH"

    elif successful >= 2:
        status = "PARTIAL"
        confidence = "MODERATE"

    elif successful == 1:
        status = "LIMITED"
        confidence = "LOW"

    else:
        status = "UNAVAILABLE"
        confidence = "NONE"

    return {
        "status": status,
        "confidence": confidence,
        "endpoints_total": total,
        "endpoints_successful": successful,
        "endpoints_failed": failed,
    }


# ============================================================
# CROSSING NORMALIZATION
# ============================================================

def normalize_crossings(
    raw: Any,
    current_utc_day: date,
) -> Dict[str, Dict[str, int]]:

    daily: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {
            "in_strait": 0,
            "inbound": 0,
            "outbound": 0,
        }
    )

    if not isinstance(raw, list):
        return {}

    for row in raw:

        if not isinstance(row, dict):
            continue

        day = row.get("day")
        direction = row.get("direction")
        count = safe_int(
            row.get("count"),
            0,
        )

        parsed = parse_day(day)

        if parsed is None:
            continue

        if direction not in {
            "in_strait",
            "inbound",
            "outbound",
        }:
            continue

        daily[str(parsed)][direction] = (
            count or 0
        )

    return dict(
        sorted(
            daily.items()
        )
    )


def complete_crossing_days(
    daily: Dict[str, Dict[str, int]],
    current_utc_day: date,
) -> List[str]:

    return [
        day
        for day in sorted(daily.keys())
        if parse_day(day) is not None
        and parse_day(day) < current_utc_day
    ]


def crossing_window(
    daily: Dict[str, Dict[str, int]],
    complete_days: List[str],
    window: int,
) -> Dict[str, Any]:

    selected = complete_days[-window:]

    if not selected:

        return {
            "requested_days": window,
            "observed_days": 0,
        }

    inbound = [
        daily[d]["inbound"]
        for d in selected
    ]

    outbound = [
        daily[d]["outbound"]
        for d in selected
    ]

    in_strait = [
        daily[d]["in_strait"]
        for d in selected
    ]

    total_crossings = [
        daily[d]["inbound"]
        + daily[d]["outbound"]
        for d in selected
    ]

    return {
        "requested_days": window,
        "observed_days": len(selected),
        "start_day": selected[0],
        "end_day": selected[-1],

        "inbound_avg": round_or_none(
            average(inbound)
        ),

        "outbound_avg": round_or_none(
            average(outbound)
        ),

        "total_crossings_avg": round_or_none(
            average(total_crossings)
        ),

        "in_strait_avg": round_or_none(
            average(in_strait)
        ),

        "inbound_total": sum(inbound),
        "outbound_total": sum(outbound),

        "total_crossings": sum(
            total_crossings
        ),
    }


# ============================================================
# OIL EXPORT NORMALIZATION
# ============================================================

def normalize_oil_days(
    raw: Any,
) -> Dict[str, Dict[str, Any]]:

    if not isinstance(raw, dict):
        return {}

    rows = raw.get("days")

    if not isinstance(rows, list):
        return {}

    normalized = {}

    for row in rows:

        if not isinstance(row, dict):
            continue

        parsed = parse_day(
            row.get("day")
        )

        if parsed is None:
            continue

        crude = row.get(
            "crude",
            {},
        )

        petrochem = row.get(
            "petrochem",
            {},
        )

        if not isinstance(crude, dict):
            crude = {}

        if not isinstance(
            petrochem,
            dict,
        ):
            petrochem = {}

        normalized[str(parsed)] = {
            "crude_ships": safe_int(
                crude.get("ships"),
                0,
            ),

            "crude_barrels": safe_float(
                crude.get("barrels"),
                0.0,
            ),

            "petrochem_ships": safe_int(
                petrochem.get("ships"),
                0,
            ),

            "petrochem_barrels": safe_float(
                petrochem.get("barrels"),
                0.0,
            ),

            "total_barrels": safe_float(
                row.get("total_barrels"),
                0.0,
            ),
        }

    return dict(
        sorted(
            normalized.items()
        )
    )


def oil_export_window(
    oil_days: Dict[str, Dict[str, Any]],
    crossing_days: List[str],
    window: int,
) -> Dict[str, Any]:

    expected_days = crossing_days[-window:]

    if not expected_days:

        return {
            "requested_days": window,
            "expected_complete_days": 0,
            "observed_oil_days": 0,
            "missing_days": [],
        }

    observed_days = [
        day
        for day in expected_days
        if day in oil_days
    ]

    missing_days = [
        day
        for day in expected_days
        if day not in oil_days
    ]

    # IMPORTANT:
    # Missing oil-export days are NOT converted to zero.
    # Only explicitly observed days are used here.

    total_values = [
        oil_days[d]["total_barrels"]
        for d in observed_days
        if oil_days[d]["total_barrels"]
        is not None
    ]

    crude_values = [
        oil_days[d]["crude_barrels"]
        for d in observed_days
        if oil_days[d]["crude_barrels"]
        is not None
    ]

    petro_values = [
        oil_days[d]["petrochem_barrels"]
        for d in observed_days
        if oil_days[d]["petrochem_barrels"]
        is not None
    ]

    crude_ships = sum(
        oil_days[d]["crude_ships"] or 0
        for d in observed_days
    )

    petro_ships = sum(
        oil_days[d]["petrochem_ships"] or 0
        for d in observed_days
    )

    completeness = (
        len(observed_days)
        / len(expected_days)
        * 100.0
    )

    return {
        "requested_days": window,

        "expected_complete_days": len(
            expected_days
        ),

        "observed_oil_days": len(
            observed_days
        ),

        "missing_day_count": len(
            missing_days
        ),

        "missing_days": missing_days,

        "completeness_pct": round(
            completeness,
            2,
        ),

        "observed_day_average_barrels": (
            round_or_none(
                average(total_values)
            )
        ),

        "observed_day_average_crude_barrels": (
            round_or_none(
                average(crude_values)
            )
        ),

        "observed_day_average_petrochem_barrels": (
            round_or_none(
                average(petro_values)
            )
        ),

        "observed_total_barrels": round_or_none(
            sum(total_values)
            if total_values
            else None
        ),

        "observed_crude_barrels": round_or_none(
            sum(crude_values)
            if crude_values
            else None
        ),

        "observed_petrochem_barrels": (
            round_or_none(
                sum(petro_values)
                if petro_values
                else None
            )
        ),

        "observed_crude_ships": crude_ships,
        "observed_petrochem_ships": petro_ships,

        "missing_days_treated_as_zero": False,

        "physical_flow_measurement": False,
    }


# ============================================================
# DATA QUALITY
# ============================================================

def evaluate_data_quality(
    source_status: Dict[str, Any],
    crossings_30d: Dict[str, Any],
    oil_30d: Dict[str, Any],
) -> Dict[str, Any]:

    score = 100
    issues = []

    if source_status.get(
        "status"
    ) != "AVAILABLE":

        score -= 30

        issues.append(
            "Not all live API endpoints are available."
        )

    crossing_days = safe_int(
        crossings_30d.get(
            "observed_days"
        ),
        0,
    ) or 0

    if crossing_days < 25:

        score -= 20

        issues.append(
            "Crossing history contains fewer than "
            "25 complete days."
        )

    oil_completeness = safe_float(
        oil_30d.get(
            "completeness_pct"
        ),
        0.0,
    ) or 0.0

    if oil_completeness < 90:

        score -= 10

        issues.append(
            "Oil-export proxy has missing days."
        )

    if oil_completeness < 75:

        score -= 10

        issues.append(
            "Oil-export proxy completeness is below 75%."
        )

    score = max(
        0,
        min(
            100,
            score,
        ),
    )

    if score >= 85:
        status = "HIGH"

    elif score >= 70:
        status = "MODERATE"

    elif score >= 50:
        status = "LIMITED"

    else:
        status = "LOW"

    return {
        "score": score,
        "status": status,
        "issues": issues,
        "interpretation": (
            "Quality describes usability of the live AIS "
            "observation layer, not accuracy of physical "
            "oil throughput."
        ),
    }


# ============================================================
# LIVE ANALYSIS
# ============================================================

def build_live_analysis(
    raw_crossings: Any,
    raw_oil_export: Any,
    source_status: Dict[str, Any],
    generated_at: datetime,
) -> Dict[str, Any]:

    today = generated_at.date()

    crossings = normalize_crossings(
        raw_crossings,
        today,
    )

    complete_days = (
        complete_crossing_days(
            crossings,
            today,
        )
    )

    current_day = (
        str(today)
        if str(today) in crossings
        else None
    )

    crossing_7 = crossing_window(
        crossings,
        complete_days,
        7,
    )

    crossing_14 = crossing_window(
        crossings,
        complete_days,
        14,
    )

    crossing_30 = crossing_window(
        crossings,
        complete_days,
        30,
    )

    oil_days = normalize_oil_days(
        raw_oil_export
    )

    oil_7 = oil_export_window(
        oil_days,
        complete_days,
        7,
    )

    oil_30 = oil_export_window(
        oil_days,
        complete_days,
        30,
    )

    outbound_trend = pct_change(
        safe_float(
            crossing_7.get(
                "outbound_avg"
            )
        ),
        safe_float(
            crossing_30.get(
                "outbound_avg"
            )
        ),
    )

    total_crossing_trend = pct_change(
        safe_float(
            crossing_7.get(
                "total_crossings_avg"
            )
        ),
        safe_float(
            crossing_30.get(
                "total_crossings_avg"
            )
        ),
    )

    oil_trend = pct_change(
        safe_float(
            oil_7.get(
                "observed_day_average_barrels"
            )
        ),
        safe_float(
            oil_30.get(
                "observed_day_average_barrels"
            )
        ),
    )

    data_quality = evaluate_data_quality(
        source_status,
        crossing_30,
        oil_30,
    )

    return {
        "complete_day_logic": {
            "current_utc_day": str(today),
            "current_day_excluded_from_averages": True,
            "current_day_present_in_crossings": (
                current_day is not None
            ),
            "current_day": current_day,
            "complete_crossing_days_available": len(
                complete_days
            ),
        },

        "crossing_statistics": {
            "7d": crossing_7,
            "14d": crossing_14,
            "30d": crossing_30,

            "trend": {
                "outbound_7d_vs_30d_pct": (
                    round_or_none(
                        outbound_trend
                    )
                ),

                "total_crossings_7d_vs_30d_pct": (
                    round_or_none(
                        total_crossing_trend
                    )
                ),
            },
        },

        "oil_export_proxy": {
            "7d": oil_7,
            "30d": oil_30,

            "trend": {
                "observed_day_average_7d_vs_30d_pct": (
                    round_or_none(
                        oil_trend
                    )
                ),
            },

            "status": (
                "PARTIAL_OBSERVATION"
                if (
                    oil_30.get(
                        "missing_day_count",
                        0,
                    )
                    > 0
                )
                else "COMPLETE_OBSERVATION"
            ),

            "warning": (
                "AIS-derived tanker/DWT estimate. "
                "Missing days are not assumed to mean "
                "zero physical oil flow."
            ),

            "physical_flow_measurement": False,
        },

        "data_quality": data_quality,

        "model_role": {
            "historical_baseline_source": False,

            "live_ais_observation": True,

            "flow_disruption_role": (
                "SUPPORTING_PROXY"
            ),

            "logistics_stress_role": (
                "PRIMARY_INPUT_CANDIDATE"
            ),

            "physical_flow_role": False,

            "ready_for_direct_risk_integration": False,

            "reason": (
                "Live AIS observations are useful for "
                "traffic and logistics assessment but "
                "require comparison with PortWatch and "
                "independent physical-flow validation "
                "before risk-model integration."
            ),
        },
    }


# ============================================================
# BUILD OUTPUT
# ============================================================

def build_dataset() -> Dict[str, Any]:

    generated_at = utc_now()

    print(
        "Hormuz Live AIS Collector v1.1"
    )

    print(
        "========================================"
    )

    results: Dict[str, Dict[str, Any]] = {}

    for name, endpoint in ENDPOINTS.items():

        print(
            f"Fetching {name}: {endpoint}"
        )

        result = safe_fetch(
            name,
            endpoint,
        )

        results[name] = result

        print(
            f" -> {result['status']}"
        )

        if result.get("error"):

            print(
                f"    {result['error']}"
            )

    source_status = (
        evaluate_source_status(
            results
        )
    )

    summary_result = results.get(
        "summary",
        {},
    )

    summary = normalize_summary(
        summary_result.get("data")
        if summary_result.get(
            "status"
        ) == "OK"
        else None
    )

    raw_crossings = (
        results
        .get(
            "daily_crossings",
            {},
        )
        .get("data")
    )

    raw_oil_export = (
        results
        .get(
            "daily_oil_export",
            {},
        )
        .get("data")
    )

    live_analysis = build_live_analysis(
        raw_crossings,
        raw_oil_export,
        source_status,
        generated_at,
    )

    return {
        "meta": {
            "collector": MODEL_NAME,
            "version": MODEL_VERSION,
            "generated_at": (
                generated_at.isoformat()
            ),
            "repository": "energy-data",
            "automatic": True,
            "mode": "LIVE_AIS_OBSERVATION",
        },

        "methodology": {
            "final_flow_risk_calculated": False,

            "principles": [
                (
                    "This dataset is independent "
                    "from the existing PortWatch "
                    "Hormuz flow collector."
                ),

                (
                    "No existing Hormuz model "
                    "output is modified."
                ),

                (
                    "Hormuz API observations are "
                    "treated as live AIS-derived "
                    "traffic information."
                ),

                (
                    "Oil export values are "
                    "AIS-derived estimates and are "
                    "not direct physical throughput "
                    "measurements."
                ),

                (
                    "AIS suppression, GPS jamming, "
                    "dark transit and coverage gaps "
                    "may cause under-observation."
                ),

                (
                    "Current UTC day is excluded "
                    "from rolling daily averages."
                ),

                (
                    "Missing oil-export days are "
                    "not interpreted as zero flow."
                ),

                (
                    "This collector does not "
                    "calculate the final Hormuz "
                    "Flow Disruption Score."
                ),
            ],
        },

        "source": {
            "name": "Hormuz API",
            "base_url": BASE_URL,
            "measurement_role": (
                "Live AIS observation layer"
            ),
            "physical_flow_source": False,
        },

        "source_status": source_status,

        "summary": summary,

        "endpoint_results": {
            name: {
                "status": result.get(
                    "status"
                ),
                "endpoint": result.get(
                    "endpoint"
                ),
                "error": result.get(
                    "error"
                ),
            }
            for name, result
            in results.items()
        },

        "crossings_by_type": (
            results
            .get(
                "crossings_by_type",
                {},
            )
            .get("data")
        ),

        "daily_crossings": raw_crossings,

        "daily_oil_export": raw_oil_export,

        "live_ais_analysis": live_analysis,

        "integration": {
            "integrated_into_hormuz_flow": False,
            "integrated_into_hormuz_risk": False,
            "integrated_into_ompi": False,

            "existing_models_modified": False,

            "next_step": (
                "Compare normalized live AIS metrics "
                "with IMF PortWatch and independent "
                "physical-flow observations before "
                "risk-model integration."
            ),
        },
    }


# ============================================================
# CONSOLE SUMMARY
# ============================================================

def print_analysis_summary(
    result: Dict[str, Any],
) -> None:

    analysis = result.get(
        "live_ais_analysis",
        {},
    )

    crossing = analysis.get(
        "crossing_statistics",
        {},
    )

    oil = analysis.get(
        "oil_export_proxy",
        {},
    )

    quality = analysis.get(
        "data_quality",
        {},
    )

    print()
    print(
        "LIVE AIS ANALYSIS"
    )

    print(
        "========================================"
    )

    for window in (
        "7d",
        "14d",
        "30d",
    ):

        values = crossing.get(
            window,
            {},
        )

        print()
        print(
            f"{window.upper()} crossings"
        )

        print(
            "  Observed days:",
            values.get(
                "observed_days"
            ),
        )

        print(
            "  Inbound avg:",
            values.get(
                "inbound_avg"
            ),
        )

        print(
            "  Outbound avg:",
            values.get(
                "outbound_avg"
            ),
        )

        print(
            "  Total avg:",
            values.get(
                "total_crossings_avg"
            ),
        )

        print(
            "  In-strait avg:",
            values.get(
                "in_strait_avg"
            ),
        )

    trend = crossing.get(
        "trend",
        {},
    )

    print()
    print(
        "Outbound 7d vs 30d:",
        trend.get(
            "outbound_7d_vs_30d_pct"
        ),
        "%",
    )

    print(
        "Total crossings 7d vs 30d:",
        trend.get(
            "total_crossings_7d_vs_30d_pct"
        ),
        "%",
    )

    oil30 = oil.get(
        "30d",
        {},
    )

    print()
    print(
        "OIL EXPORT PROXY"
    )

    print(
        "========================================"
    )

    print(
        "Status:",
        oil.get("status"),
    )

    print(
        "30d expected days:",
        oil30.get(
            "expected_complete_days"
        ),
    )

    print(
        "30d observed oil days:",
        oil30.get(
            "observed_oil_days"
        ),
    )

    print(
        "30d missing days:",
        oil30.get(
            "missing_day_count"
        ),
    )

    print(
        "30d completeness:",
        oil30.get(
            "completeness_pct"
        ),
        "%",
    )

    print(
        "30d observed-day avg barrels:",
        oil30.get(
            "observed_day_average_barrels"
        ),
    )

    print()
    print(
        "DATA QUALITY"
    )

    print(
        "========================================"
    )

    print(
        "Score:",
        quality.get("score"),
    )

    print(
        "Status:",
        quality.get("status"),
    )

    for issue in quality.get(
        "issues",
        [],
    ):

        print(
            "-",
            issue,
        )


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    try:

        result = build_dataset()

        OUTPUT_FILE.write_text(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        print()
        print(
            "SOURCE STATUS"
        )

        print(
            "========================================"
        )

        status = result.get(
            "source_status",
            {},
        )

        print(
            "Status:",
            status.get("status"),
        )

        print(
            "Confidence:",
            status.get("confidence"),
        )

        print(
            "Successful endpoints:",
            status.get(
                "endpoints_successful"
            ),
            "/",
            status.get(
                "endpoints_total"
            ),
        )

        summary = result.get(
            "summary",
            {},
        )

        if summary.get(
            "available"
        ):

            print()
            print(
                "LIVE SUMMARY"
            )

            print(
                "========================================"
            )

            print(
                "Last poll:",
                summary.get(
                    "last_poll"
                ),
            )

            print(
                "Total ships:",
                summary.get(
                    "total_ships"
                ),
            )

            print(
                "In strait:",
                summary.get(
                    "in_strait"
                ),
            )

            crossings = summary.get(
                "crossings",
                {},
            )

            print(
                "Inbound:",
                crossings.get(
                    "inbound"
                ),
            )

            print(
                "Outbound:",
                crossings.get(
                    "outbound"
                ),
            )

            print(
                "Total crossings:",
                crossings.get(
                    "total"
                ),
            )

            oil = summary.get(
                "estimated_oil_export",
                {},
            )

            print(
                "Estimated oil export barrels:",
                oil.get(
                    "barrels"
                ),
            )

            print(
                "Estimated crude barrels:",
                oil.get(
                    "crude_barrels"
                ),
            )

        print_analysis_summary(
            result
        )

        print()
        print(
            "NO EXISTING MODEL FILES MODIFIED."
        )

        print(
            "Output:",
            OUTPUT_FILE,
        )

        return 0

    except Exception as exc:

        print(
            f"ERROR: "
            f"{type(exc).__name__}: "
            f"{exc}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":

    raise SystemExit(
        main()
    )
