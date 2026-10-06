#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Vessel Monitor v1.0
==========================

Fetch individual vessel observations from the public Hormuz API.

Output:
    hormuz-vessels.json

Purpose:
- maintain a current vessel-level observation layer
- identify oil-related tankers
- classify tanker size
- preserve identifiers for later run-to-run tracking
- identify potentially relevant large tanker movements
- prepare vessel-level data for daily Hormuz monitoring

IMPORTANT:
- AIS observations are not physical oil-flow measurements.
- Missing AIS vessels do not mean missing physical traffic.
- GPS/AIS disruption, dark transit and coverage gaps are possible.
- This script does NOT calculate the final Hormuz risk score.
- This script does NOT modify PortWatch, Live AIS, OMPI or risk outputs.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# ============================================================
# CONFIG
# ============================================================

MODEL_NAME = "Hormuz Vessel Monitor"
MODEL_VERSION = "1.0-vessel-observation"

BASE_URL = "https://hormuz.data-tracking.net"
SHIPS_ENDPOINT = "/api/ships"

OUTPUT_FILE = Path("hormuz-vessels.json")

HTTP_TIMEOUT_SECONDS = 45


# ============================================================
# TANKER DEFINITIONS
# ============================================================

OIL_RELATED_CATEGORIES = {
    "crude oil tanker",
    "tanker",
    "vlcc/ulcc",
    "oil tanker",
    "oil/chemical tanker",
    "oil products tanker",
    "product tanker",
    "product/chemical tanker",
    "product/chem tanker",
    "chemical/oil products tanker",
    "asphalt/bitumen tanker",
}

CRUDE_RELATED_CATEGORIES = {
    "crude oil tanker",
    "vlcc/ulcc",
}

PRODUCT_RELATED_CATEGORIES = {
    "oil/chemical tanker",
    "oil products tanker",
    "product tanker",
    "product/chemical tanker",
    "product/chem tanker",
    "chemical/oil products tanker",
    "asphalt/bitumen tanker",
}


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


def normalize_text(
    value: Any,
) -> str:

    if value is None:
        return ""

    return str(value).strip()


def normalize_category(
    value: Any,
) -> str:

    return normalize_text(
        value
    ).lower()


def first_value(
    row: Dict[str, Any],
    keys: List[str],
) -> Any:

    for key in keys:

        value = row.get(key)

        if value is not None:
            return value

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
                "energy-data-hormuz-vessel-monitor/1.0"
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
                    f"HTTP {status}"
                )

            return json.loads(raw)

    except HTTPError as exc:

        raise RuntimeError(
            f"HTTP error: "
            f"{exc.code} {exc.reason}"
        ) from exc

    except URLError as exc:

        raise RuntimeError(
            f"URL error: {exc.reason}"
        ) from exc

    except TimeoutError as exc:

        raise RuntimeError(
            "Hormuz API timeout"
        ) from exc

    except json.JSONDecodeError as exc:

        raise RuntimeError(
            "Invalid JSON from Hormuz API"
        ) from exc


# ============================================================
# RESPONSE EXTRACTION
# ============================================================

def extract_ship_rows(
    raw: Any,
) -> List[Dict[str, Any]]:

    if isinstance(raw, list):

        return [
            row
            for row in raw
            if isinstance(row, dict)
        ]

    if not isinstance(raw, dict):
        return []

    possible_keys = [
        "ships",
        "vessels",
        "data",
        "results",
        "items",
    ]

    for key in possible_keys:

        value = raw.get(key)

        if isinstance(value, list):

            return [
                row
                for row in value
                if isinstance(row, dict)
            ]

    return []


# ============================================================
# VESSEL CLASSIFICATION
# ============================================================

def classify_tanker_size(
    dwt: Optional[float],
) -> str:

    if dwt is None:
        return "UNKNOWN"

    if dwt >= 320000:
        return "ULCC"

    if dwt >= 200000:
        return "VLCC"

    if dwt >= 120000:
        return "SUEZMAX"

    if dwt >= 80000:
        return "AFRAMAX"

    if dwt >= 55000:
        return "PANAMAX_LR1"

    if dwt >= 35000:
        return "HANDYMAX_MR"

    if dwt > 0:
        return "SMALL"

    return "UNKNOWN"


def tanker_relevance(
    category: str,
) -> Dict[str, Any]:

    normalized = normalize_category(
        category
    )

    oil_related = (
        normalized
        in OIL_RELATED_CATEGORIES
    )

    crude_related = (
        normalized
        in CRUDE_RELATED_CATEGORIES
    )

    product_related = (
        normalized
        in PRODUCT_RELATED_CATEGORIES
    )

    if crude_related:
        role = "CRUDE"

    elif product_related:
        role = "PRODUCT_OR_CHEMICAL"

    elif oil_related:
        role = "OIL_RELATED"

    else:
        role = "NON_OIL"

    return {
        "oil_related": oil_related,
        "crude_related": crude_related,
        "product_related": product_related,
        "energy_role": role,
    }


# ============================================================
# VESSEL NORMALIZATION
# ============================================================

def normalize_ship(
    row: Dict[str, Any],
) -> Dict[str, Any]:

    category = normalize_text(
        first_value(
            row,
            [
                "ship_category",
                "category",
                "ship_type",
                "vessel_type",
                "type",
            ],
        )
    )

    dwt = safe_float(
        first_value(
            row,
            [
                "dwt",
                "deadweight",
                "deadweight_tonnage",
            ],
        )
    )

    mmsi = normalize_text(
        first_value(
            row,
            [
                "mmsi",
                "MMSI",
            ],
        )
    )

    imo = normalize_text(
        first_value(
            row,
            [
                "imo",
                "imo_number",
                "IMO",
            ],
        )
    )

    name = normalize_text(
        first_value(
            row,
            [
                "name",
                "ship_name",
                "vessel_name",
            ],
        )
    )

    latitude = safe_float(
        first_value(
            row,
            [
                "lat",
                "latitude",
            ],
        )
    )

    longitude = safe_float(
        first_value(
            row,
            [
                "lon",
                "lng",
                "longitude",
            ],
        )
    )

    speed = safe_float(
        first_value(
            row,
            [
                "speed",
                "sog",
                "speed_over_ground",
            ],
        )
    )

    course = safe_float(
        first_value(
            row,
            [
                "course",
                "cog",
                "course_over_ground",
            ],
        )
    )

    heading = safe_float(
        first_value(
            row,
            [
                "heading",
                "true_heading",
            ],
        )
    )

    direction = normalize_text(
        first_value(
            row,
            [
                "direction",
                "movement",
                "crossing_direction",
            ],
        )
    )

    region = normalize_text(
        first_value(
            row,
            [
                "region",
                "zone",
                "area",
                "location",
            ],
        )
    )

    destination = normalize_text(
        first_value(
            row,
            [
                "destination",
                "dest",
            ],
        )
    )

    timestamp = normalize_text(
        first_value(
            row,
            [
                "timestamp",
                "last_seen",
                "position_time",
                "updated_at",
                "last_update",
            ],
        )
    )

    flag = normalize_text(
        first_value(
            row,
            [
                "flag",
                "flag_country",
                "country",
            ],
        )
    )

    relevance = tanker_relevance(
        category
    )

    tanker_size = (
        classify_tanker_size(dwt)
        if relevance["oil_related"]
        else "NOT_APPLICABLE"
    )

    vessel_id = None

    if imo:
        vessel_id = f"IMO:{imo}"

    elif mmsi:
        vessel_id = f"MMSI:{mmsi}"

    elif name:
        vessel_id = f"NAME:{name}"

    return {
        "vessel_id": vessel_id,
        "name": name or None,
        "imo": imo or None,
        "mmsi": mmsi or None,
        "flag": flag or None,

        "ship_category": (
            category or None
        ),

        "dwt": dwt,

        "tanker_size": tanker_size,

        "oil_related": (
            relevance["oil_related"]
        ),

        "crude_related": (
            relevance["crude_related"]
        ),

        "product_related": (
            relevance["product_related"]
        ),

        "energy_role": (
            relevance["energy_role"]
        ),

        "position": {
            "latitude": latitude,
            "longitude": longitude,
            "region": region or None,
        },

        "navigation": {
            "direction": (
                direction or None
            ),
            "speed": speed,
            "course": course,
            "heading": heading,
            "destination": (
                destination or None
            ),
        },

        "last_observation": (
            timestamp or None
        ),

        "raw": row,
    }


# ============================================================
# ANALYSIS
# ============================================================

def build_analysis(
    vessels: List[Dict[str, Any]],
) -> Dict[str, Any]:

    total = len(vessels)

    identified = [
        vessel
        for vessel in vessels
        if vessel.get("vessel_id")
    ]

    oil = [
        vessel
        for vessel in vessels
        if vessel.get("oil_related")
    ]

    crude = [
        vessel
        for vessel in oil
        if vessel.get("crude_related")
    ]

    products = [
        vessel
        for vessel in oil
        if vessel.get("product_related")
    ]

    large_tankers = [
        vessel
        for vessel in oil
        if vessel.get("tanker_size")
        in {
            "AFRAMAX",
            "SUEZMAX",
            "VLCC",
            "ULCC",
        }
    ]

    vlcc_ulcc = [
        vessel
        for vessel in oil
        if vessel.get("tanker_size")
        in {
            "VLCC",
            "ULCC",
        }
    ]

    size_counts: Dict[str, int] = {}

    for vessel in oil:

        size = vessel.get(
            "tanker_size",
            "UNKNOWN",
        )

        size_counts[size] = (
            size_counts.get(
                size,
                0,
            )
            + 1
        )

    category_counts: Dict[str, int] = {}

    for vessel in vessels:

        category = (
            vessel.get(
                "ship_category"
            )
            or "UNKNOWN"
        )

        category_counts[category] = (
            category_counts.get(
                category,
                0,
            )
            + 1
        )

    direction_counts: Dict[str, int] = {}

    for vessel in oil:

        direction = (
            vessel
            .get(
                "navigation",
                {},
            )
            .get("direction")
            or "UNKNOWN"
        )

        direction_counts[direction] = (
            direction_counts.get(
                direction,
                0,
            )
            + 1
        )

    known_dwt = [
        vessel.get("dwt")
        for vessel in oil
        if vessel.get("dwt")
        is not None
    ]

    total_oil_dwt = (
        round(
            sum(known_dwt),
            2,
        )
        if known_dwt
        else None
    )

    return {
        "total_vessels": total,

        "identified_vessels": len(
            identified
        ),

        "oil_related_vessels": len(
            oil
        ),

        "crude_related_vessels": len(
            crude
        ),

        "product_related_vessels": len(
            products
        ),

        "large_oil_tankers": len(
            large_tankers
        ),

        "vlcc_ulcc_count": len(
            vlcc_ulcc
        ),

        "known_oil_tanker_dwt_count": len(
            known_dwt
        ),

        "total_observed_oil_tanker_dwt": (
            total_oil_dwt
        ),

        "tanker_size_counts": dict(
            sorted(
                size_counts.items()
            )
        ),

        "oil_tanker_direction_counts": dict(
            sorted(
                direction_counts.items()
            )
        ),

        "ship_category_counts": dict(
            sorted(
                category_counts.items(),
                key=lambda item: (
                    -item[1],
                    item[0],
                ),
            )
        ),

        "large_tanker_observations": [
            {
                "vessel_id": (
                    vessel.get(
                        "vessel_id"
                    )
                ),
                "name": (
                    vessel.get("name")
                ),
                "ship_category": (
                    vessel.get(
                        "ship_category"
                    )
                ),
                "dwt": (
                    vessel.get("dwt")
                ),
                "tanker_size": (
                    vessel.get(
                        "tanker_size"
                    )
                ),
                "direction": (
                    vessel
                    .get(
                        "navigation",
                        {},
                    )
                    .get("direction")
                ),
                "region": (
                    vessel
                    .get(
                        "position",
                        {},
                    )
                    .get("region")
                ),
                "destination": (
                    vessel
                    .get(
                        "navigation",
                        {},
                    )
                    .get("destination")
                ),
                "last_observation": (
                    vessel.get(
                        "last_observation"
                    )
                ),
            }
            for vessel in large_tankers
        ],
    }


# ============================================================
# DATA QUALITY
# ============================================================

def evaluate_quality(
    vessels: List[Dict[str, Any]],
    analysis: Dict[str, Any],
) -> Dict[str, Any]:

    score = 100
    issues = []

    total = len(vessels)

    if total == 0:

        return {
            "score": 0,
            "status": "UNAVAILABLE",
            "issues": [
                "No vessel observations returned."
            ],
        }

    with_id = sum(
        1
        for vessel in vessels
        if vessel.get("vessel_id")
    )

    id_pct = (
        with_id
        / total
        * 100.0
    )

    if id_pct < 95:

        score -= 10

        issues.append(
            "Some vessels lack a stable "
            "IMO/MMSI/name identifier."
        )

    if id_pct < 75:

        score -= 15

    oil_count = analysis.get(
        "oil_related_vessels",
        0,
    )

    oil_with_dwt = analysis.get(
        "known_oil_tanker_dwt_count",
        0,
    )

    if oil_count > 0:

        dwt_pct = (
            oil_with_dwt
            / oil_count
            * 100.0
        )

        if dwt_pct < 80:

            score -= 10

            issues.append(
                "Some oil-related vessels "
                "lack DWT data."
            )

        if dwt_pct < 50:

            score -= 15

    else:

        issues.append(
            "No oil-related tanker was "
            "identified in the current snapshot."
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

        "identifier_coverage_pct": round(
            id_pct,
            2,
        ),

        "interpretation": (
            "Quality describes the usability "
            "of the vessel-level AIS snapshot. "
            "It does not measure physical "
            "oil-flow accuracy."
        ),
    }


# ============================================================
# BUILD OUTPUT
# ============================================================

def build_dataset() -> Dict[str, Any]:

    generated_at = utc_now()

    print(
        "Hormuz Vessel Monitor v1.0"
    )

    print(
        "========================================"
    )

    print(
        f"Fetching {SHIPS_ENDPOINT}"
    )

    raw = fetch_json(
        SHIPS_ENDPOINT
    )

    rows = extract_ship_rows(
        raw
    )

    print(
        "Raw vessel rows:",
        len(rows),
    )

    vessels = [
        normalize_ship(row)
        for row in rows
    ]

    analysis = build_analysis(
        vessels
    )

    quality = evaluate_quality(
        vessels,
        analysis,
    )

    return {
        "meta": {
            "collector": MODEL_NAME,
            "version": MODEL_VERSION,
            "generated_at": (
                generated_at.isoformat()
            ),
            "repository": "energy-data",
            "mode": (
                "VESSEL_LEVEL_AIS_OBSERVATION"
            ),
        },

        "methodology": {
            "final_flow_risk_calculated": False,

            "principles": [
                (
                    "Individual vessels are "
                    "AIS observations."
                ),
                (
                    "Oil-related tanker "
                    "classification is based "
                    "on reported ship category."
                ),
                (
                    "Tanker size classification "
                    "uses reported DWT."
                ),
                (
                    "AIS absence is not evidence "
                    "of physical vessel absence."
                ),
                (
                    "AIS suppression, GPS jamming, "
                    "dark transit and coverage gaps "
                    "may affect observations."
                ),
                (
                    "No physical oil throughput "
                    "is calculated from this "
                    "snapshot."
                ),
                (
                    "No final Hormuz risk score "
                    "is calculated here."
                ),
            ],
        },

        "source": {
            "name": "Hormuz API",
            "base_url": BASE_URL,
            "endpoint": SHIPS_ENDPOINT,
            "measurement_role": (
                "Vessel-level live AIS "
                "observation"
            ),
            "physical_flow_source": False,
        },

        "analysis": analysis,

        "data_quality": quality,

        "vessels": vessels,

        "integration": {
            "integrated_into_live_ais": False,
            "integrated_into_hormuz_flow": False,
            "integrated_into_hormuz_risk": False,
            "integrated_into_ompi": False,

            "existing_models_modified": False,

            "model_role": {
                "daily_vessel_monitoring": (
                    "PRIMARY_INPUT"
                ),
                "logistics_stress": (
                    "SUPPORTING_INPUT"
                ),
                "flow_disruption": (
                    "SUPPORTING_PROXY"
                ),
                "physical_flow": False,
            },

            "next_step": (
                "Validate the vessel endpoint "
                "structure and identifiers, then "
                "add snapshot history and "
                "run-to-run vessel movement "
                "detection."
            ),
        },
    }


# ============================================================
# CONSOLE
# ============================================================

def print_summary(
    dataset: Dict[str, Any],
) -> None:

    analysis = dataset.get(
        "analysis",
        {},
    )

    quality = dataset.get(
        "data_quality",
        {},
    )

    print()
    print(
        "VESSEL SUMMARY"
    )

    print(
        "========================================"
    )

    print(
        "Total vessels:",
        analysis.get(
            "total_vessels"
        ),
    )

    print(
        "Oil-related vessels:",
        analysis.get(
            "oil_related_vessels"
        ),
    )

    print(
        "Crude-related vessels:",
        analysis.get(
            "crude_related_vessels"
        ),
    )

    print(
        "Product-related vessels:",
        analysis.get(
            "product_related_vessels"
        ),
    )

    print(
        "Large oil tankers:",
        analysis.get(
            "large_oil_tankers"
        ),
    )

    print(
        "VLCC / ULCC:",
        analysis.get(
            "vlcc_ulcc_count"
        ),
    )

    print(
        "Observed oil tanker DWT:",
        analysis.get(
            "total_observed_oil_tanker_dwt"
        ),
    )

    print()
    print(
        "TANKER SIZE COUNTS"
    )

    print(
        "========================================"
    )

    for size, count in (
        analysis
        .get(
            "tanker_size_counts",
            {},
        )
        .items()
    ):

        print(
            f"{size}: {count}"
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
        quality.get(
            "score"
        ),
    )

    print(
        "Status:",
        quality.get(
            "status"
        ),
    )

    print(
        "Identifier coverage:",
        quality.get(
            "identifier_coverage_pct"
        ),
        "%",
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

        dataset = build_dataset()

        OUTPUT_FILE.write_text(
            json.dumps(
                dataset,
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        print_summary(
            dataset
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
