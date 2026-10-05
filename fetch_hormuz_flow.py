#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Flow Data Collector v1.1
================================

Purpose
-------
Fetch Strait of Hormuz transit data from IMF PortWatch and create:

    hormuz-flow.json

Main methodological changes in v1.1
-----------------------------------
1. Fixed pre-disruption baseline.
2. Moving 90-day average is NOT used as the normal baseline.
3. Tanker count and tanker capacity are evaluated separately.
4. Suspicious tanker-capacity zero values are flagged.
5. Capacity averages exclude invalid/suspicious observations.
6. Baseline coverage and confidence are reported explicitly.
7. No final Hormuz Flow Risk Score is calculated yet.

Important:
PortWatch is a traffic/capacity proxy.
It is NOT direct physical oil throughput in mb/d.
"""

from __future__ import annotations

import json
import statistics
import sys
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
MODEL_VERSION = "1.1-portwatch-baseline"

OUTPUT_FILE = Path("hormuz-flow.json")

HTTP_TIMEOUT_SECONDS = 30

PORTWATCH_SERVICE_URL = (
    "https://services9.arcgis.com/"
    "weJ1QsnbMYJlCHdG/arcgis/rest/services/"
    "Daily_Chokepoints_Data/FeatureServer/0/query"
)

HORMUZ_PORT_ID = "chokepoint6"

MAX_RECORDS = 5000
MAX_HISTORY_DAYS = 365

# ------------------------------------------------------------
# FIXED BASELINE
# ------------------------------------------------------------
#
# The baseline must represent the pre-disruption operating
# regime and must NOT move forward with the crisis.
#
# Current initial baseline:
# 2026-05-01 -> 2026-06-07
#
# We deliberately end the baseline before the major June
# disruption visible in the PortWatch series.
#
# This is configurable and will be validated from the actual
# observations returned by PortWatch.
# ------------------------------------------------------------

BASELINE_START = date(2026, 5, 1)
BASELINE_END = date(2026, 6, 7)

MIN_BASELINE_DAYS_GOOD = 28
MIN_BASELINE_DAYS_USABLE = 14

# ------------------------------------------------------------
# Freshness
# ------------------------------------------------------------

FRESH_MAX_DAYS = 3
RECENT_MAX_DAYS = 7
AGING_MAX_DAYS = 14


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
        "where": (
            f"portid='{HORMUZ_PORT_ID}'"
        ),
        "outFields": "*",
        "returnGeometry": "false",
        "orderByFields": "date DESC",
        "resultRecordCount": str(
            MAX_RECORDS
        ),
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
                "energy-data-hormuz-flow/1.1"
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
# PORTWATCH RESPONSE
# ============================================================

def extract_features(
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:

    if not isinstance(
        payload,
        dict,
    ):
        raise RuntimeError(
            "Unexpected PortWatch response type"
        )

    if "error" in payload:

        error = payload.get(
            "error",
            {},
        )

        if isinstance(
            error,
            dict,
        ):

            message = error.get(
                "message",
                "Unknown ArcGIS error",
            )

            details = error.get(
                "details",
                [],
            )

            raise RuntimeError(
                f"ArcGIS error: {message}; "
                f"details={details}"
            )

        raise RuntimeError(
            f"ArcGIS error: {error}"
        )

    features = payload.get(
        "features",
        [],
    )

    if not isinstance(
        features,
        list,
    ):
        raise RuntimeError(
            "PortWatch response contains "
            "no valid feature list"
        )

    return features


# ============================================================
# FIELD HELPERS
# ============================================================

def first_existing(
    attributes: Dict[str, Any],
    candidates: List[str],
) -> Any:

    for field in candidates:

        if field in attributes:
            return attributes.get(
                field
            )

    return None


def detect_available_fields(
    features: List[Dict[str, Any]],
) -> List[str]:

    fields = set()

    for feature in features[:20]:

        attributes = feature.get(
            "attributes",
            {},
        )

        if isinstance(
            attributes,
            dict,
        ):
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

    if not isinstance(
        attributes,
        dict,
    ):
        return None

    raw_date = first_existing(
        attributes,
        [
            "date",
            "Date",
            "DATE",
        ],
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
        and str(portid)
        != HORMUZ_PORT_ID
    ):
        return None

    n_total = safe_int(
        first_existing(
            attributes,
            [
                "n_total",
                "n_tot",
                "total",
                "total_transits",
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
                "tanker_transits",
            ],
        )
    )

    n_cargo = safe_int(
        first_existing(
            attributes,
            [
                "n_cargo",
                "cargo",
                "cargo_transits",
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
                "dwt",
                "total_dwt",
            ],
        )
    )

    capacity_tanker = safe_float(
        first_existing(
            attributes,
            [
                "capacity_tanker",
                "tanker_capacity",
                "tanker_dwt",
            ],
        )
    )

    capacity_cargo = safe_float(
        first_existing(
            attributes,
            [
                "capacity_cargo",
                "cargo_capacity",
                "cargo_dwt",
            ],
        )
    )

    # --------------------------------------------------------
    # Tanker capacity quality flag
    # --------------------------------------------------------

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

        # Zero tanker count + zero capacity is logically valid.
        if capacity_tanker is None:
            capacity_tanker = 0.0

    return {
        "date": (
            date_dt.date().isoformat()
        ),
        "timestamp": (
            date_dt.isoformat()
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

        "capacity_cargo": (
            round_or_none(
                capacity_cargo
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

        normalized = (
            normalize_feature(
                feature
            )
        )

        if normalized is None:
            continue

        observation_date = (
            normalized["date"]
        )

        if observation_date in seen_dates:
            continue

        seen_dates.add(
            observation_date
        )

        observations.append(
            normalized
        )

    observations.sort(
        key=lambda item: item["date"],
        reverse=True,
    )

    return observations[
        :MAX_HISTORY_DAYS
    ]


# ============================================================
# GENERIC STATISTICS
# ============================================================

def numeric_values(
    observations: List[Dict[str, Any]],
    field: str,
) -> List[float]:

    values = []

    for item in observations:

        value = item.get(
            field
        )

        if isinstance(
            value,
            (int, float),
        ):
            values.append(
                float(value)
            )

    return values


def valid_tanker_capacity_values(
    observations: List[Dict[str, Any]],
) -> List[float]:

    values = []

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

        if not isinstance(
            value,
            (int, float),
        ):
            continue

        values.append(
            float(value)
        )

    return values


def average(
    values: List[float],
) -> Optional[float]:

    if not values:
        return None

    return round(
        statistics.mean(values),
        2,
    )


def median(
    values: List[float],
) -> Optional[float]:

    if not values:
        return None

    return round(
        statistics.median(values),
        2,
    )


def minimum(
    values: List[float],
) -> Optional[float]:

    if not values:
        return None

    return round(
        min(values),
        2,
    )


def maximum(
    values: List[float],
) -> Optional[float]:

    if not values:
        return None

    return round(
        max(values),
        2,
    )


def metric_stats(
    values: List[float],
) -> Dict[str, Any]:

    return {
        "observations": len(values),
        "average": average(
            values
        ),
        "median": median(
            values
        ),
        "min": minimum(
            values
        ),
        "max": maximum(
            values
        ),
    }


# ============================================================
# WINDOW STATISTICS
# ============================================================

def build_window_stats(
    observations: List[Dict[str, Any]],
    days: int,
) -> Dict[str, Any]:

    window = observations[:days]

    tanker_capacity_values = (
        valid_tanker_capacity_values(
            window
        )
    )

    invalid_tanker_capacity_days = sum(
        1
        for item in window
        if not item.get(
            "quality_flags",
            {},
        ).get(
            "tanker_capacity_valid",
            False,
        )
    )

    return {
        "requested_days": days,
        "available_days": len(
            window
        ),

        "n_total": metric_stats(
            numeric_values(
                window,
                "n_total",
            )
        ),

        "n_tanker": metric_stats(
            numeric_values(
                window,
                "n_tanker",
            )
        ),

        "n_cargo": metric_stats(
            numeric_values(
                window,
                "n_cargo",
            )
        ),

        "capacity_total": metric_stats(
            numeric_values(
                window,
                "capacity_total",
            )
        ),

        "capacity_tanker_valid_only": (
            metric_stats(
                tanker_capacity_values
            )
        ),

        "capacity_quality": {
            "valid_days": len(
                tanker_capacity_values
            ),
            "invalid_days": (
                invalid_tanker_capacity_days
            ),
        },
    }


# ============================================================
# FIXED PRE-DISRUPTION BASELINE
# ============================================================

def get_fixed_baseline_observations(
    observations: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    result = []

    for item in observations:

        try:
            observation_date = (
                date.fromisoformat(
                    item["date"]
                )
            )

        except (
            KeyError,
            ValueError,
        ):
            continue

        if (
            BASELINE_START
            <= observation_date
            <= BASELINE_END
        ):
            result.append(
                item
            )

    result.sort(
        key=lambda item: item["date"]
    )

    return result


def determine_baseline_confidence(
    baseline: List[Dict[str, Any]],
) -> Dict[str, Any]:

    available_days = len(
        baseline
    )

    valid_capacity_days = sum(
        1
        for item in baseline
        if item.get(
            "quality_flags",
            {},
        ).get(
            "tanker_capacity_valid",
            False,
        )
    )

    if (
        available_days
        >= MIN_BASELINE_DAYS_GOOD
    ):
        coverage_level = "GOOD"

    elif (
        available_days
        >= MIN_BASELINE_DAYS_USABLE
    ):
        coverage_level = "USABLE"

    else:
        coverage_level = "POOR"

    if available_days > 0:

        capacity_valid_pct = round(
            (
                valid_capacity_days
                / available_days
            )
            * 100.0,
            2,
        )

    else:
        capacity_valid_pct = 0.0

    if (
        coverage_level == "GOOD"
        and capacity_valid_pct >= 80
    ):
        confidence = "HIGH"

    elif (
        coverage_level
        in {"GOOD", "USABLE"}
        and capacity_valid_pct >= 50
    ):
        confidence = "MEDIUM"

    else:
        confidence = "LOW"

    return {
        "coverage_level": (
            coverage_level
        ),
        "confidence": confidence,
        "available_days": (
            available_days
        ),
        "valid_tanker_capacity_days": (
            valid_capacity_days
        ),
        "valid_tanker_capacity_pct": (
            capacity_valid_pct
        ),
    }


def build_fixed_baseline(
    observations: List[Dict[str, Any]],
) -> Dict[str, Any]:

    baseline = (
        get_fixed_baseline_observations(
            observations
        )
    )

    confidence = (
        determine_baseline_confidence(
            baseline
        )
    )

    tanker_capacity_values = (
        valid_tanker_capacity_values(
            baseline
        )
    )

    return {
        "type": (
            "fixed_pre_disruption"
        ),

        "start_date": (
            BASELINE_START.isoformat()
        ),

        "end_date": (
            BASELINE_END.isoformat()
        ),

        "methodological_reason": (
            "Fixed pre-disruption baseline prevents "
            "the reference level from drifting downward "
            "during a prolonged disruption."
        ),

        "confidence": confidence,

        "metrics": {
            "n_total": metric_stats(
                numeric_values(
                    baseline,
                    "n_total",
                )
            ),

            "n_tanker": metric_stats(
                numeric_values(
                    baseline,
                    "n_tanker",
                )
            ),

            "capacity_total": (
                metric_stats(
                    numeric_values(
                        baseline,
                        "capacity_total",
                    )
                )
            ),

            "capacity_tanker_valid_only": (
                metric_stats(
                    tanker_capacity_values
                )
            ),
        },
    }


# ============================================================
# COMPARISONS
# ============================================================

def percentage_change(
    current: Optional[float],
    baseline: Optional[float],
) -> Optional[float]:

    if (
        current is None
        or baseline is None
        or baseline == 0
    ):
        return None

    return round(
        (
            (current - baseline)
            / baseline
        )
        * 100.0,
        2,
    )


def ratio_to_baseline(
    current: Optional[float],
    baseline: Optional[float],
) -> Optional[float]:

    if (
        current is None
        or baseline is None
        or baseline == 0
    ):
        return None

    return round(
        current / baseline,
        4,
    )


def build_baseline_comparison(
    stats_7d: Dict[str, Any],
    stats_30d: Dict[str, Any],
    baseline: Dict[str, Any],
) -> Dict[str, Any]:

    baseline_metrics = baseline.get(
        "metrics",
        {},
    )

    tanker_baseline = (
        baseline_metrics
        .get("n_tanker", {})
        .get("average")
    )

    total_baseline = (
        baseline_metrics
        .get("n_total", {})
        .get("average")
    )

    tanker_capacity_baseline = (
        baseline_metrics
        .get(
            "capacity_tanker_valid_only",
            {},
        )
        .get("average")
    )

    tanker_7d = (
        stats_7d
        .get("n_tanker", {})
        .get("average")
    )

    tanker_30d = (
        stats_30d
        .get("n_tanker", {})
        .get("average")
    )

    total_7d = (
        stats_7d
        .get("n_total", {})
        .get("average")
    )

    total_30d = (
        stats_30d
        .get("n_total", {})
        .get("average")
    )

    tanker_capacity_7d = (
        stats_7d
        .get(
            "capacity_tanker_valid_only",
            {},
        )
        .get("average")
    )

    tanker_capacity_30d = (
        stats_30d
        .get(
            "capacity_tanker_valid_only",
            {},
        )
        .get("average")
    )

    return {
        "tanker_count": {
            "baseline_average": (
                tanker_baseline
            ),

            "7d_average": (
                tanker_7d
            ),

            "30d_average": (
                tanker_30d
            ),

            "7d_vs_baseline_pct": (
                percentage_change(
                    tanker_7d,
                    tanker_baseline,
                )
            ),

            "30d_vs_baseline_pct": (
                percentage_change(
                    tanker_30d,
                    tanker_baseline,
                )
            ),

            "7d_baseline_ratio": (
                ratio_to_baseline(
                    tanker_7d,
                    tanker_baseline,
                )
            ),

            "30d_baseline_ratio": (
                ratio_to_baseline(
                    tanker_30d,
                    tanker_baseline,
                )
            ),
        },

        "total_vessels": {
            "baseline_average": (
                total_baseline
            ),

            "7d_average": (
                total_7d
            ),

            "30d_average": (
                total_30d
            ),

            "7d_vs_baseline_pct": (
                percentage_change(
                    total_7d,
                    total_baseline,
                )
            ),

            "30d_vs_baseline_pct": (
                percentage_change(
                    total_30d,
                    total_baseline,
                )
            ),

            "7d_baseline_ratio": (
                ratio_to_baseline(
                    total_7d,
                    total_baseline,
                )
            ),
        },

        "tanker_capacity": {
            "baseline_average_valid_only": (
                tanker_capacity_baseline
            ),

            "7d_average_valid_only": (
                tanker_capacity_7d
            ),

            "30d_average_valid_only": (
                tanker_capacity_30d
            ),

            "7d_vs_baseline_pct": (
                percentage_change(
                    tanker_capacity_7d,
                    tanker_capacity_baseline,
                )
            ),

            "30d_vs_baseline_pct": (
                percentage_change(
                    tanker_capacity_30d,
                    tanker_capacity_baseline,
                )
            ),

            "7d_baseline_ratio": (
                ratio_to_baseline(
                    tanker_capacity_7d,
                    tanker_capacity_baseline,
                )
            ),
        },
    }


# ============================================================
# MOVING TREND
# ============================================================

def build_moving_trend(
    stats_7d: Dict[str, Any],
    stats_30d: Dict[str, Any],
    stats_90d: Dict[str, Any],
) -> Dict[str, Any]:

    tanker_7 = (
        stats_7d
        .get("n_tanker", {})
        .get("average")
    )

    tanker_30 = (
        stats_30d
        .get("n_tanker", {})
        .get("average")
    )

    tanker_90 = (
        stats_90d
        .get("n_tanker", {})
        .get("average")
    )

    return {
        "role": (
            "short_term_trend_only"
        ),

        "warning": (
            "The moving 90-day average is NOT "
            "used as the normal baseline."
        ),

        "tanker_7d_vs_30d_pct": (
            percentage_change(
                tanker_7,
                tanker_30,
            )
        ),

        "tanker_7d_vs_90d_pct": (
            percentage_change(
                tanker_7,
                tanker_90,
            )
        ),
    }


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
    baseline: Dict[str, Any],
) -> Dict[str, Any]:

    score = 100
    issues = []

    if not observations:

        return {
            "score": 0,
            "level": "POOR",
            "issues": [
                "No PortWatch observations available"
            ],
        }

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

    elif freshness_status == "UNKNOWN":

        score -= 40

        issues.append(
            "Latest observation age "
            "cannot be determined"
        )

    recent = observations[:30]

    invalid_capacity = sum(
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

    if invalid_capacity > 0:

        invalid_pct = (
            invalid_capacity
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
            f"{invalid_capacity} of "
            f"{len(recent)} recent days "
            f"have invalid/suspicious "
            f"tanker-capacity values"
        )

    baseline_confidence = (
        baseline
        .get("confidence", {})
        .get("confidence")
    )

    if baseline_confidence == "LOW":

        score -= 20

        issues.append(
            "Fixed baseline confidence is LOW"
        )

    elif baseline_confidence == "MEDIUM":

        score -= 5

        issues.append(
            "Fixed baseline confidence is MEDIUM"
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
# MODEL
# ============================================================

def build_flow_dataset() -> Dict[str, Any]:

    generated_at = utc_now()

    query_url = (
        build_portwatch_url()
    )

    print(
        "Fetching IMF PortWatch Hormuz data..."
    )

    payload = fetch_json(
        query_url
    )

    features = extract_features(
        payload
    )

    print(
        f"PortWatch features received: "
        f"{len(features)}"
    )

    available_fields = (
        detect_available_fields(
            features
        )
    )

    print(
        "Available PortWatch fields:"
    )

    print(
        ", ".join(
            available_fields
        )
    )

    observations = (
        normalize_features(
            features
        )
    )

    if not observations:

        raise RuntimeError(
            "No valid Hormuz observations "
            "could be normalized"
        )

    latest = observations[0]

    latest_dt = parse_arcgis_date(
        latest.get(
            "timestamp"
        )
    )

    freshness = (
        determine_freshness(
            latest_dt
        )
    )

    stats_7d = build_window_stats(
        observations,
        7,
    )

    stats_30d = build_window_stats(
        observations,
        30,
    )

    stats_90d = build_window_stats(
        observations,
        90,
    )

    baseline = (
        build_fixed_baseline(
            observations
        )
    )

    baseline_comparison = (
        build_baseline_comparison(
            stats_7d,
            stats_30d,
            baseline,
        )
    )

    moving_trend = (
        build_moving_trend(
            stats_7d,
            stats_30d,
            stats_90d,
        )
    )

    data_quality = (
        evaluate_data_quality(
            observations,
            freshness,
            baseline,
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
        },

        "source": {
            "provider": "IMF PortWatch",
            "service": (
                "Daily_Chokepoints_Data"
            ),
            "portid": HORMUZ_PORT_ID,
            "role": (
                "traffic_and_capacity_proxy"
            ),
            "methodological_warning": (
                "PortWatch vessel traffic and "
                "capacity are proxies and must "
                "not be interpreted directly as "
                "physical oil throughput in mb/d."
            ),
        },

        "latest_observation": {
            **latest,
            "freshness": freshness,
        },

        "freshness": freshness,

        "data_quality": data_quality,

        "fixed_baseline": baseline,

        "statistics": {
            "7d": stats_7d,
            "30d": stats_30d,
            "90d": stats_90d,
        },

        "baseline_comparison": (
            baseline_comparison
        ),

        "moving_trend": (
            moving_trend
        ),

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
        },

        "history": observations,
    }


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    try:

        result = (
            build_flow_dataset()
        )

        OUTPUT_FILE.write_text(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        latest = result.get(
            "latest_observation",
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

        baseline = result.get(
            "fixed_baseline",
            {},
        )

        comparison = result.get(
            "baseline_comparison",
            {},
        )

        tanker_comparison = (
            comparison.get(
                "tanker_count",
                {},
            )
        )

        capacity_comparison = (
            comparison.get(
                "tanker_capacity",
                {},
            )
        )

        print()
        print(
            "Hormuz Flow Data Collector "
            "v1.1 completed"
        )

        print(
            "------------------------------------"
        )

        print(
            "Latest observation:",
            latest.get("date"),
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
            "FIXED PRE-DISRUPTION BASELINE"
        )

        print(
            "Baseline period:",
            baseline.get("start_date"),
            "->",
            baseline.get("end_date"),
        )

        print(
            "Baseline confidence:",
            baseline
            .get("confidence", {})
            .get("confidence"),
        )

        print(
            "Baseline available days:",
            baseline
            .get("confidence", {})
            .get("available_days"),
        )

        print()
        print(
            "Tanker baseline average:",
            tanker_comparison.get(
                "baseline_average"
            ),
        )

        print(
            "Tanker 7d average:",
            tanker_comparison.get(
                "7d_average"
            ),
        )

        print(
            "Tanker 7d vs baseline:",
            tanker_comparison.get(
                "7d_vs_baseline_pct"
            ),
            "%",
        )

        print(
            "Tanker 7d baseline ratio:",
            tanker_comparison.get(
                "7d_baseline_ratio"
            ),
        )

        print()
        print(
            "Tanker capacity baseline:",
            capacity_comparison.get(
                "baseline_average_valid_only"
            ),
        )

        print(
            "Tanker capacity 7d:",
            capacity_comparison.get(
                "7d_average_valid_only"
            ),
        )

        print(
            "Tanker capacity 7d vs baseline:",
            capacity_comparison.get(
                "7d_vs_baseline_pct"
            ),
            "%",
        )

        print()
        print(
            "Data quality:",
            quality.get("level"),
            f"({quality.get('score')}/100)",
        )

        issues = quality.get(
            "issues",
            [],
        )

        if issues:

            print(
                "Data quality issues:"
            )

            for issue in issues:

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
