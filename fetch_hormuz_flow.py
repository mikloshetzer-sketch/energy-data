#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Flow Data Collector v1.2
===============================

DIAGNOSTIC VERSION

Purpose
-------
Fetch IMF PortWatch Strait of Hormuz data and create:

    hormuz-flow.json

Version 1.2 does NOT calculate a Flow Risk Score.

Main purpose:
1. Inspect the structure of the PortWatch Hormuz series.
2. Produce monthly diagnostics.
3. Compare:
      - calendar year 2025
      - 2026-02-01 -> 2026-02-27
      - post-war observations
4. Detect tanker-capacity quality problems.
5. Help select the final baseline empirically.

Important dates
---------------
War / major Hormuz disruption reference:
    2026-02-28

Important methodological rule
-----------------------------
PortWatch data are AIS-derived vessel transit/capacity proxies.

They are NOT direct physical oil throughput in mb/d.
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
MODEL_VERSION = "1.2-monthly-diagnostics"

OUTPUT_FILE = Path("hormuz-flow.json")

HTTP_TIMEOUT_SECONDS = 30

PORTWATCH_SERVICE_URL = (
    "https://services9.arcgis.com/"
    "weJ1QsnbMYJlCHdG/arcgis/rest/services/"
    "Daily_Chokepoints_Data/FeatureServer/0/query"
)

HORMUZ_PORT_ID = "chokepoint6"

MAX_RECORDS = 5000

# Keep enough history to include all of 2025.
MAX_HISTORY_DAYS = 800

WAR_START_DATE = date(2026, 2, 28)

PREWAR_2026_START = date(2026, 2, 1)
PREWAR_2026_END = date(2026, 2, 27)

BASELINE_2025_START = date(2025, 1, 1)
BASELINE_2025_END = date(2025, 12, 31)

FRESH_MAX_DAYS = 3
RECENT_MAX_DAYS = 7
AGING_MAX_DAYS = 14


# ============================================================
# HELPERS
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
                "energy-data-hormuz-flow/1.2"
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
# RESPONSE
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

    # --------------------------------------------------------
    # Capacity quality
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

        if capacity_tanker is None:
            capacity_tanker = 0.0

    observation_date = (
        date_dt.date()
    )

    return {
        "date": (
            observation_date.isoformat()
        ),

        "timestamp": (
            date_dt.isoformat()
        ),

        "year": (
            observation_date.year
        ),

        "month": (
            observation_date.month
        ),

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
        "days": len(
            observations
        ),

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

        "capacity_total": (
            metric_stats(
                numeric_values(
                    observations,
                    "capacity_total",
                )
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

        stats = build_period_stats(
            rows
        )

        result.append(
            {
                "month": month,

                "period": (
                    "PRE_WAR"
                    if month < "2026-03"
                    else "POST_WAR"
                ),

                **stats,
            }
        )

    return result


# ============================================================
# SPECIFIC PERIODS
# ============================================================

def select_period(
    observations: List[Dict[str, Any]],
    start: date,
    end: date,
) -> List[Dict[str, Any]]:

    result = []

    for item in observations:

        try:
            item_date = (
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
            start
            <= item_date
            <= end
        ):
            result.append(
                item
            )

    return result


def build_reference_periods(
    observations: List[Dict[str, Any]],
) -> Dict[str, Any]:

    full_2025 = select_period(
        observations,
        BASELINE_2025_START,
        BASELINE_2025_END,
    )

    feb_prewar = select_period(
        observations,
        PREWAR_2026_START,
        PREWAR_2026_END,
    )

    postwar = []

    for item in observations:

        try:

            item_date = (
                date.fromisoformat(
                    item["date"]
                )
            )

        except (
            KeyError,
            ValueError,
        ):
            continue

        if item_date >= WAR_START_DATE:

            postwar.append(
                item
            )

    return {
        "calendar_2025": {
            "start_date": (
                BASELINE_2025_START.isoformat()
            ),

            "end_date": (
                BASELINE_2025_END.isoformat()
            ),

            "role": (
                "candidate_normal_baseline"
            ),

            **build_period_stats(
                full_2025
            ),
        },

        "february_2026_prewar": {
            "start_date": (
                PREWAR_2026_START.isoformat()
            ),

            "end_date": (
                PREWAR_2026_END.isoformat()
            ),

            "role": (
                "immediate_prewar_reference"
            ),

            **build_period_stats(
                feb_prewar
            ),
        },

        "postwar_since_2026_02_28": {
            "start_date": (
                WAR_START_DATE.isoformat()
            ),

            "role": (
                "disruption_period"
            ),

            **build_period_stats(
                postwar
            ),
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
# COMPARISON
# ============================================================

def ratio(
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


def build_diagnostic_comparison(
    recent: Dict[str, Any],
    references: Dict[str, Any],
) -> Dict[str, Any]:

    current_7d = recent.get(
        "7d",
        {}
    )

    baseline_2025 = (
        references.get(
            "calendar_2025",
            {},
        )
    )

    feb_prewar = (
        references.get(
            "february_2026_prewar",
            {},
        )
    )

    current_tankers = (
        current_7d
        .get("n_tanker", {})
        .get("average")
    )

    baseline_2025_tankers = (
        baseline_2025
        .get("n_tanker", {})
        .get("average")
    )

    feb_tankers = (
        feb_prewar
        .get("n_tanker", {})
        .get("average")
    )

    current_total = (
        current_7d
        .get("n_total", {})
        .get("average")
    )

    baseline_2025_total = (
        baseline_2025
        .get("n_total", {})
        .get("average")
    )

    feb_total = (
        feb_prewar
        .get("n_total", {})
        .get("average")
    )

    return {
        "tanker_count_7d": {
            "current": (
                current_tankers
            ),

            "calendar_2025_average": (
                baseline_2025_tankers
            ),

            "feb_2026_prewar_average": (
                feb_tankers
            ),

            "current_vs_2025_pct": (
                percentage_change(
                    current_tankers,
                    baseline_2025_tankers,
                )
            ),

            "current_vs_2025_ratio": (
                ratio(
                    current_tankers,
                    baseline_2025_tankers,
                )
            ),

            "current_vs_feb_prewar_pct": (
                percentage_change(
                    current_tankers,
                    feb_tankers,
                )
            ),

            "current_vs_feb_prewar_ratio": (
                ratio(
                    current_tankers,
                    feb_tankers,
                )
            ),
        },

        "total_vessels_7d": {
            "current": (
                current_total
            ),

            "calendar_2025_average": (
                baseline_2025_total
            ),

            "feb_2026_prewar_average": (
                feb_total
            ),

            "current_vs_2025_pct": (
                percentage_change(
                    current_total,
                    baseline_2025_total,
                )
            ),

            "current_vs_2025_ratio": (
                ratio(
                    current_total,
                    baseline_2025_total,
                )
            ),

            "current_vs_feb_prewar_pct": (
                percentage_change(
                    current_total,
                    feb_total,
                )
            ),

            "current_vs_feb_prewar_ratio": (
                ratio(
                    current_total,
                    feb_total,
                )
            ),
        },
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
    references: Dict[str, Any],
) -> Dict[str, Any]:

    score = 100
    issues = []

    freshness_status = (
        freshness.get(
            "status"
        )
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

    baseline_days = (
        references
        .get("calendar_2025", {})
        .get("days", 0)
    )

    if baseline_days < 300:

        score -= 20

        issues.append(
            "Calendar 2025 reference coverage "
            "is incomplete"
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

    observations = (
        normalize_features(
            features
        )
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

    monthly = (
        build_monthly_diagnostics(
            observations
        )
    )

    references = (
        build_reference_periods(
            observations
        )
    )

    recent = (
        build_recent_windows(
            observations
        )
    )

    comparison = (
        build_diagnostic_comparison(
            recent,
            references,
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

            "repository": (
                "energy-data"
            ),

            "mode": (
                "DIAGNOSTIC"
            ),
        },

        "methodology": {
            "war_start_reference": (
                WAR_START_DATE.isoformat()
            ),

            "warning": (
                "No final Flow Risk Score is "
                "calculated in this version."
            ),

            "baseline_status": (
                "UNDER_REVIEW"
            ),

            "principles": [
                (
                    "PortWatch transit counts are "
                    "AIS-derived traffic proxies."
                ),
                (
                    "PortWatch capacity is not direct "
                    "physical oil throughput in mb/d."
                ),
                (
                    "Calendar 2025 and immediate "
                    "pre-war February 2026 are compared "
                    "before selecting the final baseline."
                ),
                (
                    "Suspicious zero tanker-capacity "
                    "values are excluded from valid "
                    "capacity statistics."
                ),
            ],
        },

        "source": {
            "provider": (
                "IMF PortWatch"
            ),

            "service": (
                "Daily_Chokepoints_Data"
            ),

            "portid": (
                HORMUZ_PORT_ID
            ),
        },

        "latest_observation": {
            **latest,
            "freshness": freshness,
        },

        "freshness": freshness,

        "data_quality": (
            data_quality
        ),

        "reference_periods": (
            references
        ),

        "recent_windows": (
            recent
        ),

        "diagnostic_comparison": (
            comparison
        ),

        "monthly_diagnostics": (
            monthly
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
# CONSOLE DIAGNOSTICS
# ============================================================

def print_monthly_table(
    monthly: List[Dict[str, Any]],
) -> None:

    print()
    print(
        "MONTHLY PORTWATCH DIAGNOSTICS"
    )

    print(
        "============================================================"
    )

    print(
        f"{'Month':<9}"
        f"{'Days':>6}"
        f"{'Tank avg':>11}"
        f"{'Tank med':>11}"
        f"{'Tank max':>11}"
        f"{'Total avg':>12}"
        f"{'Cap avg':>14}"
        f"{'Bad cap':>9}"
    )

    print(
        "------------------------------------------------------------"
        "----------------"
    )

    for item in monthly:

        tanker = item.get(
            "n_tanker",
            {},
        )

        total = item.get(
            "n_total",
            {},
        )

        capacity = item.get(
            "capacity_tanker_valid_only",
            {},
        )

        quality = item.get(
            "capacity_quality",
            {},
        )

        print(
            f"{item.get('month', ''):<9}"
            f"{item.get('days', 0):>6}"
            f"{str(tanker.get('average')):>11}"
            f"{str(tanker.get('median')):>11}"
            f"{str(tanker.get('max')):>11}"
            f"{str(total.get('average')):>12}"
            f"{str(capacity.get('average')):>14}"
            f"{quality.get('invalid_days', 0):>9}"
        )


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

        references = result.get(
            "reference_periods",
            {},
        )

        comparison = result.get(
            "diagnostic_comparison",
            {},
        )

        quality = result.get(
            "data_quality",
            {},
        )

        print()
        print(
            "Hormuz Flow Collector v1.2"
        )

        print(
            "========================================"
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
            "REFERENCE PERIODS"
        )

        print(
            "========================================"
        )

        baseline_2025 = (
            references.get(
                "calendar_2025",
                {},
            )
        )

        feb_prewar = (
            references.get(
                "february_2026_prewar",
                {},
            )
        )

        print(
            "2025 days:",
            baseline_2025.get(
                "days"
            ),
        )

        print(
            "2025 tanker/day average:",
            baseline_2025
            .get("n_tanker", {})
            .get("average"),
        )

        print(
            "2025 total/day average:",
            baseline_2025
            .get("n_total", {})
            .get("average"),
        )

        print()
        print(
            "2026 Feb 1-27 days:",
            feb_prewar.get(
                "days"
            ),
        )

        print(
            "Feb pre-war tanker/day:",
            feb_prewar
            .get("n_tanker", {})
            .get("average"),
        )

        print(
            "Feb pre-war total/day:",
            feb_prewar
            .get("n_total", {})
            .get("average"),
        )

        print()
        print(
            "CURRENT 7D VS REFERENCES"
        )

        print(
            "========================================"
        )

        tanker_compare = (
            comparison.get(
                "tanker_count_7d",
                {},
            )
        )

        print(
            "Current 7d tanker/day:",
            tanker_compare.get(
                "current"
            ),
        )

        print(
            "vs 2025:",
            tanker_compare.get(
                "current_vs_2025_pct"
            ),
            "%",
        )

        print(
            "vs Feb pre-war:",
            tanker_compare.get(
                "current_vs_feb_prewar_pct"
            ),
            "%",
        )

        print_monthly_table(
            result.get(
                "monthly_diagnostics",
                [],
            )
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
