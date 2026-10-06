#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Flow Data Collector v1.3
================================

Purpose
-------
Fetch IMF PortWatch Strait of Hormuz data and create:

    hormuz-flow.json

v1.3 introduces:

1. STRUCTURAL_BASELINE
   Full calendar year 2025.

2. PREWAR_CONTROL
   2026-02-01 -> 2026-02-27.

3. AIS OBSERVED DISRUPTION
   Measures how far currently AIS-observed tanker traffic
   is below the 2025 structural baseline.

IMPORTANT
---------
AIS Observed Disruption is NOT the final Flow Risk Score.

PortWatch is treated as an AIS-observed traffic proxy,
not as direct physical oil throughput in mb/d.

A very low PortWatch observation must therefore be validated
against independent physical-flow information before being
used as the final Hormuz Flow Disruption component.
"""

from __future__ import annotations

import json
import statistics
import sys

from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# ============================================================
# CONFIG
# ============================================================

MODEL_NAME = "Hormuz Flow Data Collector"
MODEL_VERSION = "1.3-ais-observed-disruption"

OUTPUT_FILE = Path("hormuz-flow.json")

HTTP_TIMEOUT_SECONDS = 30

PORTWATCH_SERVICE_URL = (
    "https://services9.arcgis.com/"
    "weJ1QsnbMYJlCHdG/arcgis/rest/services/"
    "Daily_Chokepoints_Data/FeatureServer/0/query"
)

HORMUZ_PORT_ID = "chokepoint6"

MAX_RECORDS = 5000
MAX_HISTORY_DAYS = 800

WAR_START_DATE = date(2026, 2, 28)

STRUCTURAL_BASELINE_START = date(2025, 1, 1)
STRUCTURAL_BASELINE_END = date(2025, 12, 31)

PREWAR_CONTROL_START = date(2026, 2, 1)
PREWAR_CONTROL_END = date(2026, 2, 27)

FRESH_MAX_DAYS = 3
RECENT_MAX_DAYS = 7
AGING_MAX_DAYS = 14


# ============================================================
# GENERIC HELPERS
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


def clamp(
    value: float,
    minimum: float = 0.0,
    maximum: float = 100.0,
) -> float:

    return max(
        minimum,
        min(maximum, value),
    )


# ============================================================
# DATE HANDLING
# ============================================================

def parse_arcgis_date(
    value: Any,
) -> Optional[datetime]:

    if value is None:
        return None

    if isinstance(value, (int, float)):

        try:
            return datetime.fromtimestamp(
                float(value) / 1000.0,
                tz=timezone.utc,
            )

        except (
            ValueError,
            OSError,
            OverflowError,
        ):
            return None

    text = str(value).strip()

    if not text:
        return None

    try:
        numeric = float(text)

        if numeric > 10000000000:

            return datetime.fromtimestamp(
                numeric / 1000.0,
                tz=timezone.utc,
            )

    except ValueError:
        pass

    try:

        dt = datetime.fromisoformat(
            text.replace(
                "Z",
                "+00:00",
            )
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.astimezone(
            timezone.utc
        )

    except ValueError:
        return None


def age_days(
    dt: Optional[datetime],
) -> Optional[int]:

    if dt is None:
        return None

    delta = utc_now() - dt

    return max(
        0,
        int(
            delta.total_seconds()
            // 86400
        ),
    )


# ============================================================
# NETWORK
# ============================================================

def build_portwatch_url() -> str:

    params = {
        "where": f"portid='{HORMUZ_PORT_ID}'",
        "outFields": "*",
        "returnGeometry": "false",
        "orderByFields": "date DESC",
        "resultRecordCount": str(MAX_RECORDS),
        "f": "json",
    }

    return (
        PORTWATCH_SERVICE_URL
        + "?"
        + urlencode(params)
    )


def fetch_json(
    url: str,
) -> Dict[str, Any]:

    request = Request(
        url,
        headers={
            "User-Agent": (
                "energy-data-hormuz-flow/1.3"
            ),
            "Accept": "application/json",
        },
    )

    try:

        with urlopen(
            request,
            timeout=HTTP_TIMEOUT_SECONDS,
        ) as response:

            raw = (
                response
                .read()
                .decode("utf-8")
            )

            return json.loads(raw)

    except HTTPError as exc:

        raise RuntimeError(
            f"PortWatch HTTP error "
            f"{exc.code}: {exc.reason}"
        ) from exc

    except URLError as exc:

        raise RuntimeError(
            f"PortWatch URL error: "
            f"{exc.reason}"
        ) from exc

    except TimeoutError as exc:

        raise RuntimeError(
            "PortWatch request timeout"
        ) from exc

    except json.JSONDecodeError as exc:

        raise RuntimeError(
            "PortWatch returned invalid JSON"
        ) from exc


# ============================================================
# RESPONSE HANDLING
# ============================================================

def extract_features(
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:

    if not isinstance(payload, dict):

        raise RuntimeError(
            "Unexpected PortWatch response type"
        )

    if "error" in payload:

        raise RuntimeError(
            f"ArcGIS error: "
            f"{payload.get('error')}"
        )

    features = payload.get(
        "features",
        [],
    )

    if not isinstance(features, list):

        raise RuntimeError(
            "No valid PortWatch features"
        )

    return features


def first_existing(
    attributes: Dict[str, Any],
    candidates: List[str],
) -> Any:

    for field in candidates:

        if field in attributes:
            return attributes.get(field)

    return None


def detect_available_fields(
    features: List[Dict[str, Any]],
) -> List[str]:

    fields = set()

    for feature in features[:30]:

        attributes = feature.get(
            "attributes",
            {},
        )

        if isinstance(attributes, dict):
            fields.update(
                attributes.keys()
            )

    return sorted(fields)


# ============================================================
# NORMALIZATION
# ============================================================

def normalize_feature(
    feature: Dict[str, Any],
) -> Optional[Dict[str, Any]]:

    attributes = feature.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    raw_date = first_existing(
        attributes,
        ["date", "Date", "DATE"],
    )

    date_dt = parse_arcgis_date(
        raw_date
    )

    if date_dt is None:
        return None

    portid = first_existing(
        attributes,
        [
            "portid",
            "port_id",
            "PortID",
        ],
    )

    if (
        portid is not None
        and str(portid) != HORMUZ_PORT_ID
    ):
        return None

    n_total = safe_int(
        first_existing(
            attributes,
            [
                "n_total",
                "n_tot",
                "total",
            ],
        )
    )

    n_tanker = safe_int(
        first_existing(
            attributes,
            [
                "n_tanker",
                "tankers",
                "tanker",
            ],
        )
    )

    n_cargo = safe_int(
        first_existing(
            attributes,
            [
                "n_cargo",
                "cargo",
            ],
        )
    )

    capacity_total = safe_float(
        first_existing(
            attributes,
            [
                "capacity",
                "total_capacity",
                "capacity_total",
            ],
        )
    )

    capacity_tanker = safe_float(
        first_existing(
            attributes,
            [
                "capacity_tanker",
                "tanker_capacity",
            ],
        )
    )

    tanker_capacity_valid = True
    tanker_capacity_issue = None

    if n_tanker is None:

        tanker_capacity_valid = False

        tanker_capacity_issue = (
            "tanker_count_missing"
        )

    elif n_tanker > 0:

        if (
            capacity_tanker is None
            or capacity_tanker <= 0
        ):

            tanker_capacity_valid = False

            tanker_capacity_issue = (
                "positive_tanker_count_"
                "but_zero_or_missing_capacity"
            )

    elif n_tanker == 0:

        if capacity_tanker is None:
            capacity_tanker = 0.0

    observation_date = date_dt.date()

    return {
        "date": observation_date.isoformat(),
        "timestamp": date_dt.isoformat(),

        "year": observation_date.year,
        "month": observation_date.month,

        "year_month": (
            observation_date.strftime(
                "%Y-%m"
            )
        ),

        "period": (
            "PRE_WAR"
            if observation_date
            < WAR_START_DATE
            else "POST_WAR"
        ),

        "n_total": n_total,
        "n_tanker": n_tanker,
        "n_cargo": n_cargo,

        "capacity_total": (
            round_or_none(
                capacity_total
            )
        ),

        "capacity_tanker": (
            round_or_none(
                capacity_tanker
            )
        ),

        "quality_flags": {
            "tanker_capacity_valid": (
                tanker_capacity_valid
            ),

            "tanker_capacity_issue": (
                tanker_capacity_issue
            ),
        },
    }


def normalize_features(
    features: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    observations = []
    seen_dates = set()

    for feature in features:

        item = normalize_feature(
            feature
        )

        if item is None:
            continue

        observation_date = (
            item["date"]
        )

        if observation_date in seen_dates:
            continue

        seen_dates.add(
            observation_date
        )

        observations.append(
            item
        )

    observations.sort(
        key=lambda item: item["date"],
        reverse=True,
    )

    return observations[
        :MAX_HISTORY_DAYS
    ]


# ============================================================
# STATISTICS
# ============================================================

def numeric_values(
    observations: List[Dict[str, Any]],
    field: str,
) -> List[float]:

    result = []

    for item in observations:

        value = item.get(field)

        if isinstance(
            value,
            (int, float),
        ):
            result.append(
                float(value)
            )

    return result


def valid_tanker_capacity_values(
    observations: List[Dict[str, Any]],
) -> List[float]:

    result = []

    for item in observations:

        flags = item.get(
            "quality_flags",
            {},
        )

        if not flags.get(
            "tanker_capacity_valid",
            False,
        ):
            continue

        value = item.get(
            "capacity_tanker"
        )

        if isinstance(
            value,
            (int, float),
        ):
            result.append(
                float(value)
            )

    return result


def metric_stats(
    values: List[float],
) -> Dict[str, Any]:

    if not values:

        return {
            "observations": 0,
            "average": None,
            "median": None,
            "min": None,
            "max": None,
        }

    return {
        "observations": len(values),

        "average": round(
            statistics.mean(values),
            2,
        ),

        "median": round(
            statistics.median(values),
            2,
        ),

        "min": round(
            min(values),
            2,
        ),

        "max": round(
            max(values),
            2,
        ),
    }


def build_period_stats(
    observations: List[Dict[str, Any]],
) -> Dict[str, Any]:

    invalid_capacity_days = sum(
        1
        for item in observations
        if not item.get(
            "quality_flags",
            {},
        ).get(
            "tanker_capacity_valid",
            False,
        )
    )

    valid_capacity_values = (
        valid_tanker_capacity_values(
            observations
        )
    )

    return {
        "days": len(observations),

        "n_total": metric_stats(
            numeric_values(
                observations,
                "n_total",
            )
        ),

        "n_tanker": metric_stats(
            numeric_values(
                observations,
                "n_tanker",
            )
        ),

        "n_cargo": metric_stats(
            numeric_values(
                observations,
                "n_cargo",
            )
        ),

        "capacity_total": metric_stats(
            numeric_values(
                observations,
                "capacity_total",
            )
        ),

        "capacity_tanker_valid_only": (
            metric_stats(
                valid_capacity_values
            )
        ),

        "capacity_quality": {
            "valid_days": len(
                valid_capacity_values
            ),

            "invalid_days": (
                invalid_capacity_days
            ),

            "valid_pct": (
                round(
                    (
                        len(
                            valid_capacity_values
                        )
                        / len(observations)
                    )
                    * 100.0,
                    2,
                )
                if observations
                else None
            ),
        },
    }


# ============================================================
# PERIOD SELECTION
# ============================================================

def select_period(
    observations: List[Dict[str, Any]],
    start: date,
    end: date,
) -> List[Dict[str, Any]]:

    result = []

    for item in observations:

        try:

            item_date = date.fromisoformat(
                item["date"]
            )

        except (
            KeyError,
            ValueError,
        ):
            continue

        if start <= item_date <= end:
            result.append(item)

    return result


# ============================================================
# REFERENCE PERIODS
# ============================================================

def build_reference_periods(
    observations: List[Dict[str, Any]],
) -> Dict[str, Any]:

    structural = select_period(
        observations,
        STRUCTURAL_BASELINE_START,
        STRUCTURAL_BASELINE_END,
    )

    prewar_control = select_period(
        observations,
        PREWAR_CONTROL_START,
        PREWAR_CONTROL_END,
    )

    structural_stats = build_period_stats(
        structural
    )

    control_stats = build_period_stats(
        prewar_control
    )

    structural_complete = (
        structural_stats.get(
            "days",
            0,
        ) >= 365
    )

    control_complete = (
        control_stats.get(
            "days",
            0,
        ) >= 27
    )

    return {
        "structural_baseline": {
            "name": (
                "STRUCTURAL_BASELINE"
            ),

            "start_date": (
                STRUCTURAL_BASELINE_START
                .isoformat()
            ),

            "end_date": (
                STRUCTURAL_BASELINE_END
                .isoformat()
            ),

            "status": (
                "FIXED"
                if structural_complete
                else "INCOMPLETE"
            ),

            "purpose": (
                "Long-run normal AIS-observed "
                "Hormuz traffic reference"
            ),

            **structural_stats,
        },

        "prewar_control": {
            "name": (
                "PREWAR_CONTROL"
            ),

            "start_date": (
                PREWAR_CONTROL_START
                .isoformat()
            ),

            "end_date": (
                PREWAR_CONTROL_END
                .isoformat()
            ),

            "status": (
                "VALID"
                if control_complete
                else "INCOMPLETE"
            ),

            "purpose": (
                "Immediate pre-war control "
                "for baseline validation"
            ),

            **control_stats,
        },
    }


# ============================================================
# RECENT WINDOWS
# ============================================================

def build_recent_windows(
    observations: List[Dict[str, Any]],
) -> Dict[str, Any]:

    return {
        "7d": build_period_stats(
            observations[:7]
        ),

        "30d": build_period_stats(
            observations[:30]
        ),

        "90d": build_period_stats(
            observations[:90]
        ),
    }


# ============================================================
# AIS OBSERVED DISRUPTION
# ============================================================

def calculate_disruption_from_ratio(
    current: Optional[float],
    baseline: Optional[float],
) -> Dict[str, Any]:

    if (
        current is None
        or baseline is None
        or baseline <= 0
    ):

        return {
            "ratio": None,
            "disruption_score": None,
        }

    traffic_ratio = (
        current / baseline
    )

    # 1.00 baseline traffic = 0 disruption
    # 0.00 observed traffic = 100 disruption
    #
    # Values above baseline do not create
    # negative disruption.

    disruption = (
        1.0 - traffic_ratio
    ) * 100.0

    disruption = clamp(
        disruption
    )

    return {
        "ratio": round(
            traffic_ratio,
            4,
        ),

        "disruption_score": round(
            disruption,
            2,
        ),
    }


def classify_disruption(
    score: Optional[float],
) -> Optional[str]:

    if score is None:
        return None

    if score < 15:
        return "NORMAL"

    if score < 35:
        return "LOW"

    if score < 55:
        return "MODERATE"

    if score < 75:
        return "HIGH"

    return "SEVERE"


def build_ais_observed_disruption(
    recent: Dict[str, Any],
    references: Dict[str, Any],
) -> Dict[str, Any]:

    baseline = references.get(
        "structural_baseline",
        {},
    )

    control = references.get(
        "prewar_control",
        {},
    )

    current_7d_tanker = (
        recent
        .get("7d", {})
        .get("n_tanker", {})
        .get("average")
    )

    current_30d_tanker = (
        recent
        .get("30d", {})
        .get("n_tanker", {})
        .get("average")
    )

    baseline_tanker = (
        baseline
        .get("n_tanker", {})
        .get("average")
    )

    control_tanker = (
        control
        .get("n_tanker", {})
        .get("average")
    )

    seven_day = (
        calculate_disruption_from_ratio(
            current_7d_tanker,
            baseline_tanker,
        )
    )

    thirty_day = (
        calculate_disruption_from_ratio(
            current_30d_tanker,
            baseline_tanker,
        )
    )

    control_check = (
        calculate_disruption_from_ratio(
            control_tanker,
            baseline_tanker,
        )
    )

    score_7d = seven_day.get(
        "disruption_score"
    )

    return {
        "indicator_name": (
            "AIS_OBSERVED_DISRUPTION"
        ),

        "status": (
            "VALIDATION_REQUIRED"
        ),

        "role": (
            "AIS traffic observation layer only"
        ),

        "final_flow_risk": False,

        "scale": {
            "minimum": 0,
            "maximum": 100,

            "meaning": (
                "0 = observed traffic at or above "
                "structural baseline; "
                "100 = no AIS-observed tanker traffic"
            ),
        },

        "structural_baseline_tankers_per_day": (
            baseline_tanker
        ),

        "prewar_control_tankers_per_day": (
            control_tanker
        ),

        "current_7d_tankers_per_day": (
            current_7d_tanker
        ),

        "current_30d_tankers_per_day": (
            current_30d_tanker
        ),

        "seven_day": {
            **seven_day,

            "level": (
                classify_disruption(
                    score_7d
                )
            ),
        },

        "thirty_day": {
            **thirty_day,

            "level": (
                classify_disruption(
                    thirty_day.get(
                        "disruption_score"
                    )
                )
            ),
        },

        "baseline_control_check": {
            "prewar_control_vs_structural_ratio": (
                control_check.get(
                    "ratio"
                )
            ),

            "difference_score": (
                control_check.get(
                    "disruption_score"
                )
            ),

            "interpretation": (
                "Used to verify that the "
                "structural baseline is broadly "
                "consistent with the immediate "
                "pre-war period."
            ),
        },

        "validation_warning": (
            "AIS-observed disruption must not be "
            "interpreted as an equivalent decline "
            "in physical oil throughput. "
            "Independent physical-flow validation "
            "is required."
        ),
    }


# ============================================================
# MONTHLY DIAGNOSTICS
# ============================================================

def build_monthly_diagnostics(
    observations: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    grouped: Dict[
        str,
        List[Dict[str, Any]]
    ] = defaultdict(list)

    for item in observations:

        key = item.get(
            "year_month"
        )

        if key:
            grouped[key].append(
                item
            )

    result = []

    for month in sorted(
        grouped.keys()
    ):

        rows = grouped[
            month
        ]

        result.append(
            {
                "month": month,
                **build_period_stats(rows),
            }
        )

    return result


# ============================================================
# FRESHNESS
# ============================================================

def determine_freshness(
    latest_dt: Optional[datetime],
) -> Dict[str, Any]:

    age = age_days(
        latest_dt
    )

    if age is None:

        return {
            "status": "UNKNOWN",
            "age_days": None,
        }

    if age <= FRESH_MAX_DAYS:
        status = "FRESH"

    elif age <= RECENT_MAX_DAYS:
        status = "RECENT"

    elif age <= AGING_MAX_DAYS:
        status = "AGING"

    else:
        status = "STALE"

    return {
        "status": status,
        "age_days": age,
    }


# ============================================================
# DATA QUALITY
# ============================================================

def evaluate_data_quality(
    observations: List[Dict[str, Any]],
    freshness: Dict[str, Any],
    references: Dict[str, Any],
) -> Dict[str, Any]:

    score = 100
    issues = []

    freshness_status = freshness.get(
        "status"
    )

    if freshness_status == "RECENT":

        score -= 10

    elif freshness_status == "AGING":

        score -= 30

        issues.append(
            "Latest PortWatch observation is aging"
        )

    elif freshness_status == "STALE":

        score -= 55

        issues.append(
            "Latest PortWatch observation is stale"
        )

    recent = observations[:30]

    invalid_recent = sum(
        1
        for item in recent
        if not item.get(
            "quality_flags",
            {},
        ).get(
            "tanker_capacity_valid",
            False,
        )
    )

    if invalid_recent:

        invalid_pct = (
            invalid_recent
            / max(
                1,
                len(recent),
            )
        )

        if invalid_pct >= 0.50:
            score -= 20

        elif invalid_pct >= 0.25:
            score -= 10

        else:
            score -= 5

        issues.append(
            f"{invalid_recent} of "
            f"{len(recent)} recent days "
            f"have invalid tanker-capacity values"
        )

    structural = references.get(
        "structural_baseline",
        {},
    )

    if structural.get(
        "status"
    ) != "FIXED":

        score -= 20

        issues.append(
            "Structural baseline is incomplete"
        )

    structural_capacity_pct = (
        structural
        .get("capacity_quality", {})
        .get("valid_pct")
    )

    if (
        structural_capacity_pct
        is not None
        and structural_capacity_pct < 90
    ):

        score -= 10

        issues.append(
            "Structural baseline tanker-capacity "
            "quality is below 90%"
        )

    score = max(
        0,
        min(
            100,
            int(score),
        ),
    )

    if score >= 85:
        level = "GOOD"

    elif score >= 65:
        level = "MODERATE"

    elif score >= 40:
        level = "LIMITED"

    else:
        level = "POOR"

    return {
        "score": score,
        "level": level,
        "issues": issues,
    }


# ============================================================
# BUILD DATASET
# ============================================================

def build_flow_dataset() -> Dict[str, Any]:

    generated_at = utc_now()

    print(
        "Fetching IMF PortWatch Hormuz data..."
    )

    payload = fetch_json(
        build_portwatch_url()
    )

    features = extract_features(
        payload
    )

    print(
        "PortWatch features received:",
        len(features),
    )

    available_fields = (
        detect_available_fields(
            features
        )
    )

    observations = normalize_features(
        features
    )

    if not observations:

        raise RuntimeError(
            "No valid Hormuz observations"
        )

    latest = observations[0]

    latest_dt = parse_arcgis_date(
        latest.get(
            "timestamp"
        )
    )

    freshness = determine_freshness(
        latest_dt
    )

    references = (
        build_reference_periods(
            observations
        )
    )

    recent = build_recent_windows(
        observations
    )

    ais_disruption = (
        build_ais_observed_disruption(
            recent,
            references,
        )
    )

    monthly = (
        build_monthly_diagnostics(
            observations
        )
    )

    data_quality = (
        evaluate_data_quality(
            observations,
            freshness,
            references,
        )
    )

    return {
        "meta": {
            "collector": MODEL_NAME,
            "version": MODEL_VERSION,

            "generated_at": (
                generated_at.isoformat()
            ),

            "automatic": True,
            "repository": "energy-data",

            "mode": (
                "AIS_FLOW_OBSERVATION"
            ),
        },

        "methodology": {
            "war_start_reference": (
                WAR_START_DATE.isoformat()
            ),

            "structural_baseline": (
                "2025-01-01/2025-12-31"
            ),

            "prewar_control": (
                "2026-02-01/2026-02-27"
            ),

            "final_flow_risk_calculated": (
                False
            ),

            "principles": [
                (
                    "PortWatch is treated as an "
                    "AIS-observed traffic proxy."
                ),

                (
                    "AIS-observed traffic is not "
                    "equivalent to physical oil "
                    "throughput in mb/d."
                ),

                (
                    "The full calendar year 2025 "
                    "is the fixed structural baseline."
                ),

                (
                    "February 1-27 2026 is retained "
                    "as an immediate pre-war control."
                ),

                (
                    "AIS Observed Disruption is an "
                    "observation layer, not the final "
                    "Flow Risk Score."
                ),

                (
                    "Independent physical-flow "
                    "validation is required before "
                    "integration into Hormuz Risk."
                ),
            ],
        },

        "source": {
            "provider": "IMF PortWatch",

            "service": (
                "Daily_Chokepoints_Data"
            ),

            "portid": HORMUZ_PORT_ID,

            "measurement_role": (
                "AIS-observed vessel traffic proxy"
            ),
        },

        "latest_observation": {
            **latest,
            "freshness": freshness,
        },

        "freshness": freshness,

        "data_quality": data_quality,

        "reference_periods": references,

        "recent_windows": recent,

        "ais_observed_disruption": (
            ais_disruption
        ),

        "monthly_diagnostics": monthly,

        "diagnostics": {
            "raw_features_received": (
                len(features)
            ),

            "normalized_observations": (
                len(observations)
            ),

            "available_fields": (
                available_fields
            ),

            "oldest_observation": (
                observations[-1]["date"]
            ),

            "newest_observation": (
                observations[0]["date"]
            ),
        },

        "history": observations,
    }


# ============================================================
# CONSOLE
# ============================================================

def main() -> int:

    try:

        result = build_flow_dataset()

        OUTPUT_FILE.write_text(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        references = result.get(
            "reference_periods",
            {},
        )

        structural = references.get(
            "structural_baseline",
            {},
        )

        control = references.get(
            "prewar_control",
            {},
        )

        disruption = result.get(
            "ais_observed_disruption",
            {},
        )

        freshness = result.get(
            "freshness",
            {},
        )

        quality = result.get(
            "data_quality",
            {},
        )

        print()
        print(
            "Hormuz Flow Collector v1.3"
        )

        print(
            "========================================"
        )

        print(
            "Latest observation:",
            result
            .get("latest_observation", {})
            .get("date"),
        )

        print(
            "Freshness:",
            freshness.get("status"),
        )

        print(
            "Age days:",
            freshness.get("age_days"),
        )

        print()
        print(
            "STRUCTURAL BASELINE"
        )

        print(
            "========================================"
        )

        print(
            "Status:",
            structural.get("status"),
        )

        print(
            "Days:",
            structural.get("days"),
        )

        print(
            "Tanker/day:",
            structural
            .get("n_tanker", {})
            .get("average"),
        )

        print(
            "Total vessels/day:",
            structural
            .get("n_total", {})
            .get("average"),
        )

        print()
        print(
            "PREWAR CONTROL"
        )

        print(
            "========================================"
        )

        print(
            "Status:",
            control.get("status"),
        )

        print(
            "Tanker/day:",
            control
            .get("n_tanker", {})
            .get("average"),
        )

        print(
            "Total vessels/day:",
            control
            .get("n_total", {})
            .get("average"),
        )

        print()
        print(
            "AIS OBSERVED DISRUPTION"
        )

        print(
            "========================================"
        )

        print(
            "7d tankers/day:",
            disruption.get(
                "current_7d_tankers_per_day"
            ),
        )

        print(
            "30d tankers/day:",
            disruption.get(
                "current_30d_tankers_per_day"
            ),
        )

        print(
            "7d baseline ratio:",
            disruption
            .get("seven_day", {})
            .get("ratio"),
        )

        print(
            "7d observed disruption:",
            disruption
            .get("seven_day", {})
            .get("disruption_score"),
        )

        print(
            "7d level:",
            disruption
            .get("seven_day", {})
            .get("level"),
        )

        print(
            "30d observed disruption:",
            disruption
            .get("thirty_day", {})
            .get("disruption_score"),
        )

        print(
            "Prewar control / structural ratio:",
            disruption
            .get("baseline_control_check", {})
            .get(
                "prewar_control_vs_structural_ratio"
            ),
        )

        print()
        print(
            "IMPORTANT:"
        )

        print(
            "AIS Observed Disruption is NOT "
            "the final Flow Risk Score."
        )

        print(
            "Independent physical-flow "
            "validation is required."
        )

        print()
        print(
            "DATA QUALITY"
        )

        print(
            "========================================"
        )

        print(
            quality.get("level"),
            f"({quality.get('score')}/100)",
        )

        for issue in quality.get(
            "issues",
            [],
        ):

            print(
                " -",
                issue,
            )

        print()
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
