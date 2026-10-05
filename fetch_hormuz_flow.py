#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Flow Data Collector v1.0
===============================

Purpose
-------
Fetch Strait of Hormuz transit data from IMF PortWatch's public
ArcGIS Feature Service and create a normalized local JSON file:

    hormuz-flow.json

This module does NOT calculate the final Hormuz Risk Score.

Its only responsibilities are:

1. Download PortWatch data.
2. Select Strait of Hormuz (chokepoint6).
3. Normalize daily observations.
4. Calculate 7 / 30 / 90 day statistics.
5. Measure data freshness.
6. Detect obvious data-quality problems.
7. Write hormuz-flow.json.

Important methodological rule
-----------------------------
PortWatch vessel counts are traffic / flow proxies.

They MUST NOT be interpreted directly as physical crude-oil
throughput in million barrels per day.

AIS / vessel count != physical oil flow.
"""

from __future__ import annotations

import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# ============================================================
# CONFIG
# ============================================================

MODEL_NAME = "Hormuz Flow Data Collector"
MODEL_VERSION = "1.0-portwatch"

OUTPUT_FILE = Path("hormuz-flow.json")

HTTP_TIMEOUT_SECONDS = 30

PORTWATCH_SERVICE_URL = (
    "https://services9.arcgis.com/"
    "weJ1QsnbMYJlCHdG/arcgis/rest/services/"
    "Daily_Chokepoints_Data/FeatureServer/0/query"
)

HORMUZ_PORT_ID = "chokepoint6"

# Maximum number of observations requested from ArcGIS.
MAX_RECORDS = 5000

# We keep this much history in the normalized JSON.
MAX_HISTORY_DAYS = 365

# Freshness thresholds.
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

    """
    ArcGIS frequently returns dates as Unix epoch milliseconds.

    This function also accepts ISO date strings as fallback.
    """

    if value is None:
        return None

    # --------------------------------------------------------
    # Epoch milliseconds
    # --------------------------------------------------------

    if isinstance(value, (int, float)):

        try:
            return datetime.fromtimestamp(
                float(value) / 1000.0,
                tz=timezone.utc,
            )

        except (ValueError, OSError, OverflowError):
            return None

    # --------------------------------------------------------
    # String
    # --------------------------------------------------------

    text = str(value).strip()

    if not text:
        return None

    # Numeric string containing epoch milliseconds.
    try:
        numeric = float(text)

        if numeric > 10000000000:
            return datetime.fromtimestamp(
                numeric / 1000.0,
                tz=timezone.utc,
            )

    except ValueError:
        pass

    # ISO fallback.
    try:

        dt = datetime.fromisoformat(
            text.replace("Z", "+00:00")
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


def iso_date(
    value: Any,
) -> Optional[str]:

    dt = parse_arcgis_date(value)

    if dt is None:
        return None

    return dt.date().isoformat()


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

    """
    Build the ArcGIS FeatureServer query.

    We request all available attributes because PortWatch's
    field structure may evolve. The normalization step below
    extracts only the fields we need.
    """

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
                "energy-data-hormuz-flow/1.0"
            ),
            "Accept": "application/json",
        },
    )

    try:

        with urlopen(
            request,
            timeout=HTTP_TIMEOUT_SECONDS,
        ) as response:

            raw = response.read().decode(
                "utf-8"
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

    """
    Extract ArcGIS features and check for ArcGIS-level errors.
    """

    if not isinstance(payload, dict):

        raise RuntimeError(
            "Unexpected PortWatch response type"
        )

    if "error" in payload:

        error = payload.get(
            "error",
            {}
        )

        if isinstance(error, dict):

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
        []
    )

    if not isinstance(features, list):

        raise RuntimeError(
            "PortWatch response contains "
            "no valid feature list"
        )

    return features


# ============================================================
# FIELD DETECTION
# ============================================================

def first_existing(
    attributes: Dict[str, Any],
    candidates: List[str],
) -> Any:

    """
    PortWatch schemas can change field naming slightly.

    This helper makes the collector more tolerant by checking
    several possible names.
    """

    for field in candidates:

        if field in attributes:
            return attributes.get(field)

    return None


def detect_available_fields(
    features: List[Dict[str, Any]],
) -> List[str]:

    fields = set()

    for feature in features[:20]:

        attributes = feature.get(
            "attributes",
            {}
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
        {}
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

    # PortWatch datasets may contain tonnage / capacity fields.
    # We preserve them if present, but do not assume they exist.
    total_capacity = safe_float(
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

    tanker_capacity = safe_float(
        first_existing(
            attributes,
            [
                "tanker_capacity",
                "capacity_tanker",
                "tanker_dwt",
            ],
        )
    )

    cargo_capacity = safe_float(
        first_existing(
            attributes,
            [
                "cargo_capacity",
                "capacity_cargo",
                "cargo_dwt",
            ],
        )
    )

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
                total_capacity,
                2,
            )
        ),
        "capacity_tanker": (
            round_or_none(
                tanker_capacity,
                2,
            )
        ),
        "capacity_cargo": (
            round_or_none(
                cargo_capacity,
                2,
            )
        ),
    }


def normalize_features(
    features: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    observations = []

    seen_dates = set()

    for feature in features:

        normalized = normalize_feature(
            feature
        )

        if normalized is None:
            continue

        date = normalized["date"]

        # One observation per day.
        if date in seen_dates:
            continue

        seen_dates.add(date)

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
# STATISTICS
# ============================================================

def numeric_values(
    observations: List[Dict[str, Any]],
    field: str,
) -> List[float]:

    values = []

    for item in observations:

        value = item.get(field)

        if isinstance(
            value,
            (int, float),
        ):
            values.append(
                float(value)
            )

    return values


def calculate_average(
    observations: List[Dict[str, Any]],
    field: str,
) -> Optional[float]:

    values = numeric_values(
        observations,
        field,
    )

    if not values:
        return None

    return round(
        statistics.mean(values),
        2,
    )


def calculate_median(
    observations: List[Dict[str, Any]],
    field: str,
) -> Optional[float]:

    values = numeric_values(
        observations,
        field,
    )

    if not values:
        return None

    return round(
        statistics.median(values),
        2,
    )


def calculate_min(
    observations: List[Dict[str, Any]],
    field: str,
) -> Optional[float]:

    values = numeric_values(
        observations,
        field,
    )

    if not values:
        return None

    return round(
        min(values),
        2,
    )


def calculate_max(
    observations: List[Dict[str, Any]],
    field: str,
) -> Optional[float]:

    values = numeric_values(
        observations,
        field,
    )

    if not values:
        return None

    return round(
        max(values),
        2,
    )


def build_window_stats(
    observations: List[Dict[str, Any]],
    days: int,
) -> Dict[str, Any]:

    window = observations[:days]

    return {
        "requested_days": days,
        "available_days": len(window),

        "n_total": {
            "average": calculate_average(
                window,
                "n_total",
            ),
            "median": calculate_median(
                window,
                "n_total",
            ),
            "min": calculate_min(
                window,
                "n_total",
            ),
            "max": calculate_max(
                window,
                "n_total",
            ),
        },

        "n_tanker": {
            "average": calculate_average(
                window,
                "n_tanker",
            ),
            "median": calculate_median(
                window,
                "n_tanker",
            ),
            "min": calculate_min(
                window,
                "n_tanker",
            ),
            "max": calculate_max(
                window,
                "n_tanker",
            ),
        },

        "n_cargo": {
            "average": calculate_average(
                window,
                "n_cargo",
            ),
            "median": calculate_median(
                window,
                "n_cargo",
            ),
            "min": calculate_min(
                window,
                "n_cargo",
            ),
            "max": calculate_max(
                window,
                "n_cargo",
            ),
        },

        "capacity_total": {
            "average": calculate_average(
                window,
                "capacity_total",
            ),
        },

        "capacity_tanker": {
            "average": calculate_average(
                window,
                "capacity_tanker",
            ),
        },
    }


# ============================================================
# BASELINE COMPARISON
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


def build_trend_comparison(
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

    total_7 = (
        stats_7d
        .get("n_total", {})
        .get("average")
    )

    total_30 = (
        stats_30d
        .get("n_total", {})
        .get("average")
    )

    total_90 = (
        stats_90d
        .get("n_total", {})
        .get("average")
    )

    return {
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

        "total_7d_vs_30d_pct": (
            percentage_change(
                total_7,
                total_30,
            )
        ),

        "total_7d_vs_90d_pct": (
            percentage_change(
                total_7,
                total_90,
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
            "Latest observation age cannot be determined"
        )

    # --------------------------------------------------------
    # Missing values
    # --------------------------------------------------------

    recent = observations[:30]

    missing_total = sum(
        1
        for item in recent
        if item.get("n_total") is None
    )

    missing_tanker = sum(
        1
        for item in recent
        if item.get("n_tanker") is None
    )

    if missing_total > 5:

        score -= 10

        issues.append(
            "Multiple recent n_total values are missing"
        )

    if missing_tanker > 5:

        score -= 15

        issues.append(
            "Multiple recent n_tanker values are missing"
        )

    # --------------------------------------------------------
    # All-zero detection
    # --------------------------------------------------------

    tanker_values = numeric_values(
        recent,
        "n_tanker",
    )

    if (
        tanker_values
        and all(
            value == 0
            for value in tanker_values
        )
    ):

        score -= 30

        issues.append(
            "Recent tanker observations are all zero; "
            "possible AIS/data coverage issue"
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

    query_url = build_portwatch_url()

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

    observations = normalize_features(
        features
    )

    if not observations:

        raise RuntimeError(
            "No valid Hormuz observations "
            "could be normalized"
        )

    latest = observations[0]

    latest_dt = parse_arcgis_date(
        latest.get("timestamp")
    )

    freshness = determine_freshness(
        latest_dt
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

    trend = build_trend_comparison(
        stats_7d,
        stats_30d,
        stats_90d,
    )

    data_quality = (
        evaluate_data_quality(
            observations,
            freshness,
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
                "PortWatch vessel traffic must not "
                "be interpreted directly as physical "
                "oil throughput in million barrels/day."
            ),
        },

        "latest_observation": {
            **latest,
            "freshness": freshness,
        },

        "freshness": freshness,

        "data_quality": data_quality,

        "statistics": {
            "7d": stats_7d,
            "30d": stats_30d,
            "90d": stats_90d,
        },

        "trend": trend,

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

        stats = result.get(
            "statistics",
            {},
        )

        stats_7d = stats.get(
            "7d",
            {},
        )

        print()
        print(
            "Hormuz Flow Data Collector completed"
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

        print(
            "Latest total vessels:",
            latest.get("n_total"),
        )

        print(
            "Latest tankers:",
            latest.get("n_tanker"),
        )

        print(
            "7d average tankers:",
            stats_7d
            .get("n_tanker", {})
            .get("average"),
        )

        print(
            "7d average total:",
            stats_7d
            .get("n_total", {})
            .get("average"),
        )

        print(
            "Data quality:",
            quality.get("level"),
            f"({quality.get('score')}/100)",
        )

        print(
            "Normalized observations:",
            result.get(
                "diagnostics",
                {},
            ).get(
                "normalized_observations"
            ),
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
    raise SystemExit(main())
