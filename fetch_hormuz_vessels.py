#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Vessel Monitor v1.1
==========================

Individual-vessel AIS monitoring with run-to-run comparison.

Output:
    hormuz-vessels.json

New in v1.1:
- loads previous hormuz-vessels.json before overwrite
- compares vessels by stable vessel_id
- detects new / disappeared vessels
- detects zone changes
- identifies tanker zone changes
- identifies large tanker and VLCC/ULCC movements
- counts tankers by zone
- creates movement signals
- preserves compact snapshot history
- does NOT calculate physical oil flow
- does NOT calculate final Hormuz risk
"""

from __future__ import annotations

import json
import math
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
MODEL_VERSION = "1.1-vessel-change-monitor"

BASE_URL = "https://hormuz.data-tracking.net"
SHIPS_ENDPOINT = "/api/ships"

OUTPUT_FILE = Path("hormuz-vessels.json")

HTTP_TIMEOUT_SECONDS = 45

MAX_HISTORY_SNAPSHOTS = 30


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

LARGE_TANKER_CLASSES = {
    "AFRAMAX",
    "SUEZMAX",
    "VLCC",
    "ULCC",
}

VERY_LARGE_CLASSES = {
    "VLCC",
    "ULCC",
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


def normalize_text(value: Any) -> str:

    if value is None:
        return ""

    return str(value).strip()


def normalize_category(value: Any) -> str:
    return normalize_text(value).lower()


def first_value(
    row: Dict[str, Any],
    keys: List[str],
) -> Any:

    for key in keys:

        value = row.get(key)

        if value is not None:
            return value

    return None


def normalize_zone(value: Any) -> Optional[str]:

    text = normalize_text(value).lower()

    if not text:
        return None

    replacements = {
        "persian gulf": "persian_gulf",
        "persian_gulf": "persian_gulf",
        "gulf of oman": "gulf_of_oman",
        "gulf_of_oman": "gulf_of_oman",
        "strait": "strait",
        "hormuz": "strait",
        "strait_of_hormuz": "strait",
    }

    return replacements.get(
        text,
        text.replace(" ", "_"),
    )


# ============================================================
# HTTP
# ============================================================

def fetch_json(endpoint: str) -> Any:

    url = BASE_URL + endpoint

    request = Request(
        url,
        headers={
            "User-Agent":
                "energy-data-hormuz-vessel-monitor/1.1",
            "Accept": "application/json",
        },
    )

    try:

        with urlopen(
            request,
            timeout=HTTP_TIMEOUT_SECONDS,
        ) as response:

            raw = response.read().decode("utf-8")

            return json.loads(raw)

    except HTTPError as exc:

        raise RuntimeError(
            f"HTTP error: {exc.code} {exc.reason}"
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
# PREVIOUS SNAPSHOT
# ============================================================

def load_previous_dataset() -> Optional[Dict[str, Any]]:

    if not OUTPUT_FILE.exists():
        return None

    try:

        return json.loads(
            OUTPUT_FILE.read_text(
                encoding="utf-8"
            )
        )

    except Exception as exc:

        print(
            "WARNING: previous dataset could not "
            f"be loaded: {exc}"
        )

        return None


# ============================================================
# RESPONSE EXTRACTION
# ============================================================

def extract_ship_rows(raw: Any) -> List[Dict[str, Any]]:

    if isinstance(raw, list):

        return [
            row
            for row in raw
            if isinstance(row, dict)
        ]

    if not isinstance(raw, dict):
        return []

    for key in [
        "ships",
        "vessels",
        "data",
        "results",
        "items",
    ]:

        value = raw.get(key)

        if isinstance(value, list):

            return [
                row
                for row in value
                if isinstance(row, dict)
            ]

    return []


# ============================================================
# CLASSIFICATION
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

    normalized = normalize_category(category)

    oil_related = (
        normalized in OIL_RELATED_CATEGORIES
    )

    crude_related = (
        normalized in CRUDE_RELATED_CATEGORIES
    )

    product_related = (
        normalized in PRODUCT_RELATED_CATEGORIES
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
            ["mmsi", "MMSI"],
        )
    )

    imo = normalize_text(
        first_value(
            row,
            ["imo", "imo_number", "IMO"],
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

    latitude = safe_float(
        first_value(
            row,
            ["lat", "latitude"],
        )
    )

    longitude = safe_float(
        first_value(
            row,
            ["lon", "lng", "longitude"],
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
                "hdg",
                "true_heading",
            ],
        )
    )

    destination = normalize_text(
        first_value(
            row,
            ["destination", "dest"],
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

    zone = normalize_zone(
        first_value(
            row,
            [
                "zone",
                "region",
                "area",
                "location",
            ],
        )
    )

    relevance = tanker_relevance(category)

    tanker_size = (
        classify_tanker_size(dwt)
        if relevance["oil_related"]
        else "NOT_APPLICABLE"
    )

    vessel_id = None

    # Keep same ID logic as v1.0 for
    # backwards-compatible comparisons.
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

        "oil_related":
            relevance["oil_related"],

        "crude_related":
            relevance["crude_related"],

        "product_related":
            relevance["product_related"],

        "energy_role":
            relevance["energy_role"],

        "position": {
            "latitude": latitude,
            "longitude": longitude,
            "region": zone,
        },

        "navigation": {
            # API does not currently provide
            # reliable direction in /api/ships.
            "direction": None,
            "speed": speed,
            "course": course,
            "heading": heading,
            "destination":
                destination or None,
        },

        "last_observation":
            timestamp or None,

        "raw": row,
    }


# ============================================================
# DISTANCE
# ============================================================

def haversine_km(
    lat1: Optional[float],
    lon1: Optional[float],
    lat2: Optional[float],
    lon2: Optional[float],
) -> Optional[float]:

    if None in {
        lat1,
        lon1,
        lat2,
        lon2,
    }:
        return None

    radius = 6371.0

    p1 = math.radians(lat1)
    p2 = math.radians(lat2)

    delta_lat = math.radians(
        lat2 - lat1
    )

    delta_lon = math.radians(
        lon2 - lon1
    )

    a = (
        math.sin(delta_lat / 2) ** 2
        +
        math.cos(p1)
        *
        math.cos(p2)
        *
        math.sin(delta_lon / 2) ** 2
    )

    c = 2 * math.atan2(
        math.sqrt(a),
        math.sqrt(1 - a),
    )

    return round(
        radius * c,
        2,
    )


# ============================================================
# CURRENT SNAPSHOT ANALYSIS
# ============================================================

def count_by_zone(
    vessels: List[Dict[str, Any]],
) -> Dict[str, int]:

    result: Dict[str, int] = {}

    for vessel in vessels:

        zone = (
            vessel
            .get("position", {})
            .get("region")
            or "unknown"
        )

        result[zone] = (
            result.get(zone, 0) + 1
        )

    return dict(
        sorted(result.items())
    )


def build_analysis(
    vessels: List[Dict[str, Any]],
) -> Dict[str, Any]:

    identified = [
        v for v in vessels
        if v.get("vessel_id")
    ]

    oil = [
        v for v in vessels
        if v.get("oil_related")
    ]

    crude = [
        v for v in oil
        if v.get("crude_related")
    ]

    products = [
        v for v in oil
        if v.get("product_related")
    ]

    large = [
        v for v in oil
        if v.get("tanker_size")
        in LARGE_TANKER_CLASSES
    ]

    very_large = [
        v for v in oil
        if v.get("tanker_size")
        in VERY_LARGE_CLASSES
    ]

    size_counts: Dict[str, int] = {}

    for vessel in oil:

        size = vessel.get(
            "tanker_size",
            "UNKNOWN",
        )

        size_counts[size] = (
            size_counts.get(size, 0)
            + 1
        )

    category_counts: Dict[str, int] = {}

    for vessel in vessels:

        category = (
            vessel.get("ship_category")
            or "UNKNOWN"
        )

        category_counts[category] = (
            category_counts.get(
                category,
                0,
            )
            + 1
        )

    known_dwt = [
        v.get("dwt")
        for v in oil
        if v.get("dwt") is not None
    ]

    return {
        "total_vessels": len(vessels),

        "identified_vessels":
            len(identified),

        "oil_related_vessels":
            len(oil),

        "crude_related_vessels":
            len(crude),

        "product_related_vessels":
            len(products),

        "large_oil_tankers":
            len(large),

        "vlcc_ulcc_count":
            len(very_large),

        "known_oil_tanker_dwt_count":
            len(known_dwt),

        "total_observed_oil_tanker_dwt": (
            round(sum(known_dwt), 2)
            if known_dwt
            else None
        ),

        "all_vessels_by_zone":
            count_by_zone(vessels),

        "oil_tankers_by_zone":
            count_by_zone(oil),

        "crude_tankers_by_zone":
            count_by_zone(crude),

        "vlcc_ulcc_by_zone":
            count_by_zone(very_large),

        "tanker_size_counts":
            dict(sorted(
                size_counts.items()
            )),

        "ship_category_counts":
            dict(sorted(
                category_counts.items(),
                key=lambda item: (
                    -item[1],
                    item[0],
                ),
            )),

        "large_tanker_observations": [
            compact_vessel(v)
            for v in large
        ],
    }


# ============================================================
# COMPACT VESSEL
# ============================================================

def compact_vessel(
    vessel: Dict[str, Any],
) -> Dict[str, Any]:

    return {
        "vessel_id":
            vessel.get("vessel_id"),

        "name":
            vessel.get("name"),

        "ship_category":
            vessel.get("ship_category"),

        "energy_role":
            vessel.get("energy_role"),

        "dwt":
            vessel.get("dwt"),

        "tanker_size":
            vessel.get("tanker_size"),

        "region":
            vessel
            .get("position", {})
            .get("region"),

        "latitude":
            vessel
            .get("position", {})
            .get("latitude"),

        "longitude":
            vessel
            .get("position", {})
            .get("longitude"),

        "speed":
            vessel
            .get("navigation", {})
            .get("speed"),

        "course":
            vessel
            .get("navigation", {})
            .get("course"),

        "destination":
            vessel
            .get("navigation", {})
            .get("destination"),

        "last_observation":
            vessel.get(
                "last_observation"
            ),
    }


# ============================================================
# RUN-TO-RUN COMPARISON
# ============================================================

def build_vessel_map(
    vessels: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:

    return {
        v["vessel_id"]: v
        for v in vessels
        if v.get("vessel_id")
    }


def compare_snapshots(
    previous_dataset: Optional[
        Dict[str, Any]
    ],
    current_vessels: List[
        Dict[str, Any]
    ],
) -> Dict[str, Any]:

    if not previous_dataset:

        return {
            "comparison_available": False,
            "reason":
                "No previous vessel snapshot available.",
            "previous_generated_at": None,
            "new_vessels": [],
            "disappeared_vessels": [],
            "zone_changes": [],
            "oil_tanker_zone_changes": [],
            "large_tanker_zone_changes": [],
            "vlcc_ulcc_zone_changes": [],
        }

    previous_vessels = (
        previous_dataset.get(
            "vessels",
            [],
        )
    )

    previous_map = build_vessel_map(
        previous_vessels
    )

    current_map = build_vessel_map(
        current_vessels
    )

    previous_ids = set(
        previous_map.keys()
    )

    current_ids = set(
        current_map.keys()
    )

    new_ids = (
        current_ids - previous_ids
    )

    disappeared_ids = (
        previous_ids - current_ids
    )

    common_ids = (
        previous_ids & current_ids
    )

    new_vessels = [
        compact_vessel(
            current_map[vessel_id]
        )
        for vessel_id in sorted(
            new_ids
        )
    ]

    disappeared_vessels = [
        compact_vessel(
            previous_map[vessel_id]
        )
        for vessel_id in sorted(
            disappeared_ids
        )
    ]

    zone_changes = []

    for vessel_id in sorted(
        common_ids
    ):

        old = previous_map[vessel_id]
        new = current_map[vessel_id]

        old_zone = (
            old
            .get("position", {})
            .get("region")
        )

        new_zone = (
            new
            .get("position", {})
            .get("region")
        )

        old_lat = (
            old
            .get("position", {})
            .get("latitude")
        )

        old_lon = (
            old
            .get("position", {})
            .get("longitude")
        )

        new_lat = (
            new
            .get("position", {})
            .get("latitude")
        )

        new_lon = (
            new
            .get("position", {})
            .get("longitude")
        )

        distance = haversine_km(
            old_lat,
            old_lon,
            new_lat,
            new_lon,
        )

        if (
            old_zone
            and new_zone
            and old_zone != new_zone
        ):

            zone_changes.append({
                "vessel_id":
                    vessel_id,

                "name":
                    new.get("name"),

                "ship_category":
                    new.get(
                        "ship_category"
                    ),

                "energy_role":
                    new.get(
                        "energy_role"
                    ),

                "oil_related":
                    new.get(
                        "oil_related",
                        False,
                    ),

                "crude_related":
                    new.get(
                        "crude_related",
                        False,
                    ),

                "tanker_size":
                    new.get(
                        "tanker_size"
                    ),

                "dwt":
                    new.get("dwt"),

                "from_region":
                    old_zone,

                "to_region":
                    new_zone,

                "distance_since_previous_km":
                    distance,

                "current_speed":
                    new
                    .get(
                        "navigation",
                        {},
                    )
                    .get("speed"),

                "current_course":
                    new
                    .get(
                        "navigation",
                        {},
                    )
                    .get("course"),

                "destination":
                    new
                    .get(
                        "navigation",
                        {},
                    )
                    .get("destination"),

                "previous_observation":
                    old.get(
                        "last_observation"
                    ),

                "current_observation":
                    new.get(
                        "last_observation"
                    ),
            })

    oil_changes = [
        item
        for item in zone_changes
        if item.get("oil_related")
    ]

    large_changes = [
        item
        for item in oil_changes
        if item.get("tanker_size")
        in LARGE_TANKER_CLASSES
    ]

    very_large_changes = [
        item
        for item in oil_changes
        if item.get("tanker_size")
        in VERY_LARGE_CLASSES
    ]

    return {
        "comparison_available": True,

        "previous_generated_at":
            previous_dataset
            .get("meta", {})
            .get("generated_at"),

        "previous_version":
            previous_dataset
            .get("meta", {})
            .get("version"),

        "previous_vessel_count":
            len(previous_vessels),

        "current_vessel_count":
            len(current_vessels),

        "common_vessel_count":
            len(common_ids),

        "new_vessel_count":
            len(new_ids),

        "disappeared_vessel_count":
            len(disappeared_ids),

        "zone_change_count":
            len(zone_changes),

        "oil_tanker_zone_change_count":
            len(oil_changes),

        "large_tanker_zone_change_count":
            len(large_changes),

        "vlcc_ulcc_zone_change_count":
            len(very_large_changes),

        "new_vessels":
            new_vessels,

        "disappeared_vessels":
            disappeared_vessels,

        "zone_changes":
            zone_changes,

        "oil_tanker_zone_changes":
            oil_changes,

        "large_tanker_zone_changes":
            large_changes,

        "vlcc_ulcc_zone_changes":
            very_large_changes,
    }


# ============================================================
# MOVEMENT SIGNALS
# ============================================================

def build_movement_signals(
    comparison: Dict[str, Any],
) -> Dict[str, Any]:

    if not comparison.get(
        "comparison_available"
    ):

        return {
            "available": False,
            "signals": [],
        }

    oil_changes = comparison.get(
        "oil_tanker_zone_changes",
        [],
    )

    signals = []

    transition_counts: Dict[str, int] = {}

    for item in oil_changes:

        key = (
            f"{item.get('from_region')}"
            " -> "
            f"{item.get('to_region')}"
        )

        transition_counts[key] = (
            transition_counts.get(
                key,
                0,
            )
            + 1
        )

    for transition, count in sorted(
        transition_counts.items()
    ):

        signals.append({
            "type":
                "TANKER_ZONE_TRANSITION",

            "transition":
                transition,

            "count":
                count,
        })

    vlcc_changes = comparison.get(
        "vlcc_ulcc_zone_changes",
        [],
    )

    if vlcc_changes:

        signals.append({
            "type":
                "VLCC_ULCC_ZONE_MOVEMENT",

            "count":
                len(vlcc_changes),

            "vessels": [
                {
                    "name":
                        item.get("name"),

                    "vessel_id":
                        item.get(
                            "vessel_id"
                        ),

                    "size":
                        item.get(
                            "tanker_size"
                        ),

                    "from":
                        item.get(
                            "from_region"
                        ),

                    "to":
                        item.get(
                            "to_region"
                        ),

                    "dwt":
                        item.get("dwt"),
                }
                for item in vlcc_changes
            ],
        })

    return {
        "available": True,

        "interpretation": (
            "Zone changes are observed AIS "
            "transitions between consecutive "
            "snapshots. They are not automatically "
            "equivalent to confirmed full Hormuz "
            "crossings."
        ),

        "signals":
            signals,
    }


# ============================================================
# HISTORY
# ============================================================

def build_history_entry(
    generated_at: datetime,
    analysis: Dict[str, Any],
    comparison: Dict[str, Any],
) -> Dict[str, Any]:

    return {
        "generated_at":
            generated_at.isoformat(),

        "total_vessels":
            analysis.get(
                "total_vessels"
            ),

        "oil_related_vessels":
            analysis.get(
                "oil_related_vessels"
            ),

        "crude_related_vessels":
            analysis.get(
                "crude_related_vessels"
            ),

        "large_oil_tankers":
            analysis.get(
                "large_oil_tankers"
            ),

        "vlcc_ulcc_count":
            analysis.get(
                "vlcc_ulcc_count"
            ),

        "oil_tankers_by_zone":
            analysis.get(
                "oil_tankers_by_zone"
            ),

        "vlcc_ulcc_by_zone":
            analysis.get(
                "vlcc_ulcc_by_zone"
            ),

        "new_vessel_count":
            comparison.get(
                "new_vessel_count"
            ),

        "disappeared_vessel_count":
            comparison.get(
                "disappeared_vessel_count"
            ),

        "oil_tanker_zone_change_count":
            comparison.get(
                "oil_tanker_zone_change_count"
            ),

        "vlcc_ulcc_zone_change_count":
            comparison.get(
                "vlcc_ulcc_zone_change_count"
            ),
    }


def build_history(
    previous_dataset: Optional[
        Dict[str, Any]
    ],
    new_entry: Dict[str, Any],
) -> List[Dict[str, Any]]:

    history = []

    if previous_dataset:

        old_history = (
            previous_dataset.get(
                "snapshot_history",
                [],
            )
        )

        if isinstance(
            old_history,
            list,
        ):
            history.extend(
                old_history
            )

        # v1.0 had no history.
        # Preserve its summary as the first
        # historical snapshot when possible.
        if (
            not old_history
            and previous_dataset
            .get("meta", {})
            .get("generated_at")
        ):

            old_analysis = (
                previous_dataset.get(
                    "analysis",
                    {},
                )
            )

            history.append({
                "generated_at":
                    previous_dataset
                    .get("meta", {})
                    .get("generated_at"),

                "total_vessels":
                    old_analysis.get(
                        "total_vessels"
                    ),

                "oil_related_vessels":
                    old_analysis.get(
                        "oil_related_vessels"
                    ),

                "crude_related_vessels":
                    old_analysis.get(
                        "crude_related_vessels"
                    ),

                "large_oil_tankers":
                    old_analysis.get(
                        "large_oil_tankers"
                    ),

                "vlcc_ulcc_count":
                    old_analysis.get(
                        "vlcc_ulcc_count"
                    ),

                "oil_tankers_by_zone":
                    old_analysis.get(
                        "oil_tankers_by_zone"
                    ),

                "vlcc_ulcc_by_zone":
                    old_analysis.get(
                        "vlcc_ulcc_by_zone"
                    ),

                "legacy_snapshot":
                    True,
            })

    history.append(
        new_entry
    )

    return history[
        -MAX_HISTORY_SNAPSHOTS:
    ]


# ============================================================
# DATA QUALITY
# ============================================================

def evaluate_quality(
    vessels: List[Dict[str, Any]],
    analysis: Dict[str, Any],
) -> Dict[str, Any]:

    if not vessels:

        return {
            "score": 0,
            "status": "UNAVAILABLE",
            "issues": [
                "No vessel observations returned."
            ],
        }

    score = 100
    issues = []

    total = len(vessels)

    identified = sum(
        1
        for v in vessels
        if v.get("vessel_id")
    )

    identifier_pct = (
        identified / total * 100
    )

    if identifier_pct < 95:

        score -= 15

        issues.append(
            "Stable vessel identifier "
            "coverage below 95%."
        )

    oil_count = analysis.get(
        "oil_related_vessels",
        0,
    )

    oil_dwt = analysis.get(
        "known_oil_tanker_dwt_count",
        0,
    )

    if oil_count:

        dwt_pct = (
            oil_dwt
            / oil_count
            * 100
        )

        if dwt_pct < 80:

            score -= 10

            issues.append(
                "Oil tanker DWT coverage "
                "below 80%."
            )

    zone_known = sum(
        1
        for v in vessels
        if v.get(
            "position",
            {},
        ).get("region")
    )

    zone_pct = (
        zone_known
        / total
        * 100
    )

    if zone_pct < 95:

        score -= 10

        issues.append(
            "Some vessels lack zone data."
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
        "score":
            max(0, score),

        "status":
            status,

        "issues":
            issues,

        "identifier_coverage_pct":
            round(identifier_pct, 2),

        "zone_coverage_pct":
            round(zone_pct, 2),

        "interpretation": (
            "Quality measures usability of "
            "the AIS vessel observation layer. "
            "It does not measure physical "
            "oil-flow accuracy."
        ),
    }


# ============================================================
# BUILD DATASET
# ============================================================

def build_dataset() -> Dict[str, Any]:

    generated_at = utc_now()

    print(
        "Hormuz Vessel Monitor v1.1"
    )

    print(
        "========================================"
    )

    previous_dataset = (
        load_previous_dataset()
    )

    if previous_dataset:

        print(
            "Previous snapshot:",
            previous_dataset
            .get("meta", {})
            .get("generated_at"),
        )

    else:

        print(
            "Previous snapshot: NONE"
        )

    print(
        f"Fetching {SHIPS_ENDPOINT}"
    )

    raw = fetch_json(
        SHIPS_ENDPOINT
    )

    rows = extract_ship_rows(raw)

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

    comparison = compare_snapshots(
        previous_dataset,
        vessels,
    )

    movement_signals = (
        build_movement_signals(
            comparison
        )
    )

    quality = evaluate_quality(
        vessels,
        analysis,
    )

    history_entry = (
        build_history_entry(
            generated_at,
            analysis,
            comparison,
        )
    )

    history = build_history(
        previous_dataset,
        history_entry,
    )

    return {
        "meta": {
            "collector":
                MODEL_NAME,

            "version":
                MODEL_VERSION,

            "generated_at":
                generated_at.isoformat(),

            "repository":
                "energy-data",

            "mode":
                "VESSEL_CHANGE_MONITOR",
        },

        "methodology": {
            "final_flow_risk_calculated":
                False,

            "physical_flow_calculated":
                False,

            "principles": [
                (
                    "Individual vessel observations "
                    "come from public AIS-derived data."
                ),
                (
                    "Current snapshot is compared "
                    "with the previous repository "
                    "snapshot before overwrite."
                ),
                (
                    "Zone transitions describe "
                    "observed AIS movement between "
                    "snapshots."
                ),
                (
                    "A zone transition is not "
                    "automatically treated as a "
                    "confirmed full Hormuz crossing."
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
                    "DWT and vessel counts are "
                    "logistics indicators, not direct "
                    "physical oil throughput."
                ),
            ],
        },

        "source": {
            "name":
                "Hormuz API",

            "base_url":
                BASE_URL,

            "endpoint":
                SHIPS_ENDPOINT,

            "measurement_role":
                "Vessel-level live AIS observation",

            "physical_flow_source":
                False,
        },

        "analysis":
            analysis,

        "snapshot_comparison":
            comparison,

        "movement_signals":
            movement_signals,

        "snapshot_history":
            history,

        "data_quality":
            quality,

        "vessels":
            vessels,

        "integration": {
            "integrated_into_live_ais":
                False,

            "integrated_into_hormuz_flow":
                False,

            "integrated_into_hormuz_risk":
                False,

            "integrated_into_ompi":
                False,

            "existing_models_modified":
                False,

            "model_role": {
                "daily_vessel_monitoring":
                    "PRIMARY_INPUT",

                "logistics_stress":
                    "PRIMARY_INPUT_CANDIDATE",

                "flow_disruption":
                    "SUPPORTING_PROXY",

                "physical_flow":
                    False,
            },

            "ready_for_logistics_model":
                False,

            "reason": (
                "Run-to-run vessel movement must "
                "first be observed over multiple "
                "snapshots before Logistics Stress "
                "scoring is calibrated."
            ),

            "next_step": (
                "Collect multiple snapshots and "
                "evaluate tanker and VLCC/ULCC "
                "zone transitions."
            ),
        },
    }


# ============================================================
# CONSOLE SUMMARY
# ============================================================

def print_summary(
    dataset: Dict[str, Any],
) -> None:

    analysis = dataset.get(
        "analysis",
        {},
    )

    comparison = dataset.get(
        "snapshot_comparison",
        {},
    )

    quality = dataset.get(
        "data_quality",
        {},
    )

    print()
    print(
        "CURRENT SNAPSHOT"
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
        "Oil tankers:",
        analysis.get(
            "oil_related_vessels"
        ),
    )

    print(
        "Crude tankers:",
        analysis.get(
            "crude_related_vessels"
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

    print()
    print(
        "OIL TANKERS BY ZONE"
    )

    print(
        "========================================"
    )

    for zone, count in (
        analysis
        .get(
            "oil_tankers_by_zone",
            {},
        )
        .items()
    ):

        print(
            f"{zone}: {count}"
        )

    print()
    print(
        "VLCC / ULCC BY ZONE"
    )

    print(
        "========================================"
    )

    for zone, count in (
        analysis
        .get(
            "vlcc_ulcc_by_zone",
            {},
        )
        .items()
    ):

        print(
            f"{zone}: {count}"
        )

    print()
    print(
        "SNAPSHOT COMPARISON"
    )

    print(
        "========================================"
    )

    print(
        "Available:",
        comparison.get(
            "comparison_available"
        ),
    )

    if comparison.get(
        "comparison_available"
    ):

        print(
            "Previous snapshot:",
            comparison.get(
                "previous_generated_at"
            ),
        )

        print(
            "Common vessels:",
            comparison.get(
                "common_vessel_count"
            ),
        )

        print(
            "New vessels:",
            comparison.get(
                "new_vessel_count"
            ),
        )

        print(
            "Disappeared vessels:",
            comparison.get(
                "disappeared_vessel_count"
            ),
        )

        print(
            "All zone changes:",
            comparison.get(
                "zone_change_count"
            ),
        )

        print(
            "Oil tanker zone changes:",
            comparison.get(
                "oil_tanker_zone_change_count"
            ),
        )

        print(
            "Large tanker zone changes:",
            comparison.get(
                "large_tanker_zone_change_count"
            ),
        )

        print(
            "VLCC / ULCC zone changes:",
            comparison.get(
                "vlcc_ulcc_zone_change_count"
            ),
        )

        print()
        print(
            "OIL TANKER MOVEMENTS"
        )

        print(
            "========================================"
        )

        changes = comparison.get(
            "oil_tanker_zone_changes",
            [],
        )

        if not changes:

            print(
                "No tanker zone change "
                "detected."
            )

        for item in changes[:30]:

            print(
                item.get("name"),
                "|",
                item.get(
                    "tanker_size"
                ),
                "|",
                item.get(
                    "from_region"
                ),
                "->",
                item.get(
                    "to_region"
                ),
                "| DWT:",
                item.get("dwt"),
                "| km:",
                item.get(
                    "distance_since_previous_km"
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
            "NO EXISTING RISK MODEL "
            "FILES MODIFIED."
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
