#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Live AIS Collector v1.0
==============================

Purpose
-------
Fetch live / near-live AIS-derived Strait of Hormuz data
from the public Hormuz API.

Output:
    hormuz-live-ais.json

IMPORTANT
---------
This collector is completely independent from:

    fetch_hormuz_flow.py
    hormuz-flow.json
    hormuz_risk_model.py
    hormuz-risk.json
    OMPI

It does NOT modify or overwrite any existing model output.

The Hormuz API is treated as a LIVE AIS observation source.

Estimated oil-export values from the API are AIS-derived estimates,
NOT independently measured physical oil throughput.

No final Flow Risk Score is calculated here.
"""

from __future__ import annotations

import json
import sys

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# ============================================================
# CONFIG
# ============================================================

MODEL_NAME = "Hormuz Live AIS Collector"
MODEL_VERSION = "1.0"

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


# ============================================================
# HTTP
# ============================================================

def fetch_json(
    endpoint: str,
) -> Dict[str, Any]:

    url = BASE_URL + endpoint

    request = Request(
        url,
        headers={
            "User-Agent": (
                "energy-data-hormuz-live-ais/1.0"
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


# ============================================================
# SAFE ENDPOINT FETCH
# ============================================================

def safe_fetch(
    name: str,
    endpoint: str,
) -> Dict[str, Any]:

    try:

        data = fetch_json(
            endpoint
        )

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

    if not isinstance(
        raw,
        dict,
    ):

        return {
            "available": False,
            "raw": raw,
        }

    return {
        "available": True,

        "period_hours": safe_int(
            raw.get(
                "period_hours"
            )
        ),

        "last_poll": raw.get(
            "last_poll"
        ),

        "total_ships": safe_int(
            raw.get(
                "total_ships"
            )
        ),

        "persian_gulf_ships": safe_int(
            raw.get(
                "persian_gulf_ships"
            )
        ),

        "gulf_of_oman_ships": safe_int(
            raw.get(
                "gulf_of_oman_ships"
            )
        ),

        "in_strait": safe_int(
            raw.get(
                "in_strait"
            )
        ),

        "crossings": {
            "inbound": safe_int(
                raw.get(
                    "inbound"
                )
            ),

            "outbound": safe_int(
                raw.get(
                    "outbound"
                )
            ),

            "total": safe_int(
                raw.get(
                    "total_crossings"
                )
            ),
        },

        "estimated_oil_export": {
            "barrels": safe_float(
                raw.get(
                    "oil_export_barrels"
                )
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
# QUALITY / STATUS
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
# BUILD OUTPUT
# ============================================================

def build_dataset() -> Dict[str, Any]:

    generated_at = utc_now()

    print(
        "Hormuz Live AIS Collector v1.0"
    )

    print(
        "========================================"
    )

    results = {}

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
        summary_result.get(
            "data"
        )
        if summary_result.get(
            "status"
        ) == "OK"
        else None
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

        "daily_crossings": (
            results
            .get(
                "daily_crossings",
                {},
            )
            .get("data")
        ),

        "daily_oil_export": (
            results
            .get(
                "daily_oil_export",
                {},
            )
            .get("data")
        ),

        "integration": {
            "integrated_into_hormuz_flow": False,
            "integrated_into_hormuz_risk": False,
            "integrated_into_ompi": False,

            "next_step": (
                "Validate live API output and "
                "compare with IMF PortWatch before "
                "any model integration."
            ),
        },
    }


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
