
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Risk Integration Engine
Version: 3.2-observation-quality

Inputs:
    hormuz-risk.json
    hormuz-flow.json
    hormuz-live-ais.json
    hormuz-vessels.json

Output:
    hormuz-risk-composite.json

Important:
AIS observations are not direct measurements
of physical oil throughput.
"""

import json
import math
import sys
from datetime import datetime, timezone, date, time, timedelta
from pathlib import Path

VERSION = "3.2-observation-quality"
OUTPUT = "hormuz-risk-composite.json"

INPUTS = {
    "security": "hormuz-risk.json",
    "portwatch": "hormuz-flow.json",
    "live_ais": "hormuz-live-ais.json",
    "vessels": "hormuz-vessels.json",
}

WEIGHTS = {
    "security": 0.40,
    "physical_flow": 0.35,
    "logistics": 0.25,
}

MAX_AGE_HOURS = {
    "security": 6,
    "portwatch": 48,
    "live_ais": 2,
    "vessels": 2,
}


def now_utc():
    return datetime.now(timezone.utc)


def iso_now():
    return now_utc().isoformat()


def get_nested(data, *keys, default=None):
    current = data

    for key in keys:
        if not isinstance(current, dict):
            return default

        current = current.get(key)

        if current is None:
            return default

    return current


def safe_number(value):
    if isinstance(value, bool):
        return None

    if not isinstance(value, (int, float)):
        return None

    if not math.isfinite(value):
        return None

    return float(value)


def parse_datetime(value):
    if not isinstance(value, str):
        return None

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            return None

        return parsed.astimezone(timezone.utc)

    except ValueError:
        return None


def load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError(
            f"{path}: JSON root must be an object"
        )

    return data


def find_generated_at(data):
    candidates = [
        get_nested(data, "meta", "generated_at"),
        data.get("generated_at"),
        get_nested(data, "metadata", "generated_at"),
    ]

    for value in candidates:
        if parse_datetime(value):
            return value

    return None



def observation_time(name, data):
    """Return actual observation timestamp, not the file publication timestamp."""
    if name == "live_ais":
        value = get_nested(data, "summary", "last_poll")
        return parse_datetime(value)
    if name == "portwatch":
        value = get_nested(data, "latest_observation", "date")
        if isinstance(value, str):
            try:
                # Date-only observation covers its UTC day, not midnight alone.
                day = date.fromisoformat(value[:10])
                return datetime.combine(day, time.max, tzinfo=timezone.utc)
            except ValueError:
                return None
    if name == "vessels":
        for keys in (("source", "last_poll"), ("source", "last_observation"),
                     ("source", "observed_at"), ("observation", "timestamp")):
            candidate = parse_datetime(get_nested(data, *keys))
            if candidate:
                return candidate
    return None


def bounded_score(value):
    number = safe_number(value)
    return number if number is not None and 0 <= number <= 100 else None


def percent_change(new, baseline):
    current, old = safe_number(new), safe_number(baseline)
    if current is None or old is None or old <= 0:
        return None
    return (current / old - 1.0) * 100.0


def calculate_logistics_indicator(live, vessels):
    """Uncalibrated AIS observation indicator, NOT validated logistics risk."""
    if not live["usable_for_logistics"]:
        return {"score": None, "status": "UNAVAILABLE", "components": {},
                "reason": "Live AIS observations are missing, stale or invalid."}
    stats = live.get("crossing_statistics") or {}
    seven = stats.get("7d") or {}
    thirty = stats.get("30d") or {}
    if safe_number(seven.get("observed_days")) is None or safe_number(thirty.get("observed_days")) is None:
        return {"score": None, "status": "INSUFFICIENT_HISTORY", "components": {},
                "reason": "Historical day coverage is not reported."}
    if seven["observed_days"] < 5 or thirty["observed_days"] < 20:
        return {"score": None, "status": "INSUFFICIENT_HISTORY", "components": {},
                "reason": "At least 5/7 and 20/30 observed days are required."}
    change = percent_change(seven.get("total_crossings_avg"), thirty.get("total_crossings_avg"))
    if change is None:
        return {"score": None, "status": "INVALID_BASELINE", "components": {},
                "reason": "Crossing baseline is unavailable or zero."}
    # An unusually low observed crossing count is a *signal*, not proof of a blockade.
    # Positive changes are neutral rather than treated as verified improvement.
    traffic_stress = max(0.0, min(100.0, -change * 2.0))
    quality = bounded_score(get_nested(live, "data_quality", "score"))
    if quality is None:
        return {"score": None, "status": "UNKNOWN_DATA_QUALITY", "components": {},
                "reason": "AIS data quality score is missing."}
    # Reliability is kept separate from the stress score, to avoid equating missing data with congestion.
    return {"score": round(traffic_stress, 2), "status": "EXPERIMENTAL_UNCALIBRATED",
            "validated": False, "publish_as_validated_risk": False,
            "components": {"crossings_7d_avg": seven.get("total_crossings_avg"),
                           "crossings_30d_avg": thirty.get("total_crossings_avg"),
                           "change_7d_vs_30d_pct": round(change, 2),
                           "live_ais_quality_score": quality,
                           "vessel_source_available": vessels["usable_for_logistics"]},
            "method": "max(0, min(100, -2 * crossing_change_pct)); descriptive, not calibrated",
            "reason": "An observed AIS crossing deviation; not measured oil flow or calibrated logistics risk."}

def inspect_input(name, filename):
    path = Path(filename)

    result = {
        "file": filename,
        "available": False,
        "status": "MISSING",
        "generated_at": None,
        "age_hours": None,
        "max_age_hours": MAX_AGE_HOURS[name],
        "error": None,
        "observation_at": None,
        "observation_age_hours": None,
        "observation_status": "UNKNOWN",
    }

    if not path.is_file():
        result["error"] = "Input file does not exist"
        return None, result

    try:
        data = load_json(path)

    except (OSError, ValueError) as exc:
        result["status"] = "INVALID"
        result["error"] = str(exc)
        return None, result

    result["available"] = True
    observed = observation_time(name, data)
    if observed is not None:
        obs_age = (now_utc() - observed).total_seconds() / 3600
        result["observation_at"] = observed.isoformat()
        result["observation_age_hours"] = round(obs_age, 2)
        result["observation_status"] = (
            "INVALID_TIMESTAMP" if obs_age < -0.1 else
            "FRESH" if obs_age <= MAX_AGE_HOURS[name] else "STALE"
        )
    elif name == "live_ais":
        result["observation_status"] = "UNKNOWN_FRESHNESS"

    generated = find_generated_at(data)
    result["generated_at"] = generated

    timestamp = parse_datetime(generated)

    if timestamp is None:
        result["status"] = "UNKNOWN_FRESHNESS"
        return data, result

    age = (
        now_utc() - timestamp
    ).total_seconds() / 3600

    result["age_hours"] = round(age, 2)

    if age < -0.1:
        result["status"] = "INVALID_TIMESTAMP"

    elif age <= MAX_AGE_HOURS[name]:
        result["status"] = "FRESH"

    else:
        result["status"] = "STALE"

    if name == "live_ais" and result["status"] == "FRESH" and result["observation_status"] != "FRESH":
        result["status"] = "STALE_OBSERVATION" if result["observation_status"] == "STALE" else "UNKNOWN_OBSERVATION_FRESHNESS"
    return data, result


def analyze_security(data, source):
    layer = get_nested(
        data or {},
        "layers",
        "security",
        default={},
    )

    score = safe_number(layer.get("score"))

    valid_score = (
        score is not None
        and 0 <= score <= 100
    )

    usable = (
        source["status"] == "FRESH"
        and layer.get("status") == "ACTIVE"
        and valid_score
    )

    return {
        "weight": WEIGHTS["security"],
        "status": (
            "AVAILABLE" if usable
            else "NOT_CURRENTLY_VALIDATED"
        ),
        "score": round(score, 2) if valid_score else None,
        "level": layer.get("level"),
        "confidence": layer.get("confidence"),
        "events_7d": layer.get(
            "security_events_7d"
        ),
        "direct_hormuz_events_7d": layer.get(
            "direct_hormuz_security_events_7d"
        ),
        "maritime_attacks_7d": layer.get(
            "maritime_attack_events_7d"
        ),
        "closure_threats_7d": layer.get(
            "closure_threat_events_7d"
        ),
        "source_fresh": source["status"] == "FRESH",
        "usable_for_model": usable,
    }


def analyze_portwatch(data, source):
    data = data or {}

    disruption = data.get(
        "ais_observed_disruption",
        {}
    )

    seven = disruption.get("seven_day", {})
    thirty = disruption.get("thirty_day", {})

    seven_score = safe_number(
        seven.get("disruption_score")
    )

    thirty_score = safe_number(
        thirty.get("disruption_score")
    )

    return {
        "status": source["status"],
        "indicator": "AIS_OBSERVED_DISRUPTION",
        "latest_observation": get_nested(
            data,
            "latest_observation",
            "date",
        ),
        "observation_age_days": get_nested(
            data,
            "freshness",
            "age_days",
        ),
        "observation_freshness": get_nested(
            data,
            "freshness",
            "status",
        ),
        "structural_baseline_tankers_per_day":
            disruption.get(
                "structural_baseline_tankers_per_day"
            ),
        "current_7d_tankers_per_day":
            disruption.get(
                "current_7d_tankers_per_day"
            ),
        "current_30d_tankers_per_day":
            disruption.get(
                "current_30d_tankers_per_day"
            ),
        "ais_disruption_7d": seven_score,
        "ais_disruption_30d": thirty_score,
        "physical_flow_disruption": None,
        "physical_flow_validated": False,
        "warning": (
            "AIS-observed tanker traffic is not "
            "equivalent to actual oil throughput. "
            "Missing AIS observations must not be "
            "interpreted as zero physical traffic."
        ),
    }


def analyze_live_ais(data, source):
    data = data or {}

    summary = data.get("summary", {})
    analysis = data.get(
        "live_ais_analysis",
        {}
    )

    statistics = analysis.get(
        "crossing_statistics",
        {}
    )

    crossings = summary.get(
        "crossings",
        {}
    )

    return {
        "status": source["status"],
        "source_status": get_nested(
            data,
            "source_status",
            "status",
        ),
        "last_poll": summary.get("last_poll"),
        "ships_total": summary.get("total_ships"),
        "ships_in_strait": summary.get("in_strait"),
        "crossings_24h": {
            "inbound": crossings.get("inbound"),
            "outbound": crossings.get("outbound"),
            "total": crossings.get("total"),
        },
        "crossing_statistics": statistics,
        "oil_export_proxy": analysis.get(
            "oil_export_proxy",
            {}
        ),
        "data_quality": analysis.get(
            "data_quality",
            {}
        ),
        "usable_for_logistics": (
            source["status"] == "FRESH"
            and summary.get("available") is True
        ),
        "warning": (
            "Crossing statistics and oil export "
            "estimates are AIS-derived indicators."
        ),
    }


def analyze_vessels(data, source):
    data = data or {}

    analysis = data.get("analysis", {})
    tracking = data.get(
        "persistent_tracking",
        {}
    )

    tracking_summary = tracking.get(
        "summary",
        {}
    )

    quality = data.get(
        "data_quality",
        {}
    )

    return {
        "status": source["status"],
        "analysis": analysis,
        "tracking_summary": tracking_summary,
        "data_quality": quality,
        "usable_for_logistics": (
            source["status"] == "FRESH"
            and bool(analysis or tracking_summary)
        ),
        "warning": (
            "Vessel counts, positions and "
            "persistent identities may be affected "
            "by AIS coverage and identity churn. "
            "Confirmed transits require explicit "
            "geofence sequence validation."
        ),
    }


def build_logistics(live, vessels):
    live_usable = live["usable_for_logistics"]
    vessel_usable = vessels["usable_for_logistics"]

    available_count = sum([
        live_usable,
        vessel_usable,
    ])

    if available_count == 2:
        status = "OBSERVATIONS_AVAILABLE"

    elif available_count == 1:
        status = "PARTIAL_OBSERVATIONS"

    else:
        status = "INSUFFICIENT_DATA"

    indicator = calculate_logistics_indicator(live, vessels)

    return {
        "weight": WEIGHTS["logistics"],
        "status": status,
        "score": None,
        "score_validated": False,
        "experimental_indicator": indicator,
        "live_ais_available": live_usable,
        "vessel_tracking_available": vessel_usable,
        "observed_sources": available_count,
        "expected_sources": 2,
        "assessment": (
            "Logistics observations are available, "
            "but no calibrated logistics stress "
            "score has been established."
        ),
    }


def build_composite(security, logistics):
    physical_flow_validated = False

    security_usable = security[
        "usable_for_model"
    ]

    logistics_usable = (
        logistics["score_validated"]
        and logistics["status"]
        == "OBSERVATIONS_AVAILABLE"
    )

    weighted_coverage = 0.0

    if security_usable:
        weighted_coverage += WEIGHTS["security"]

    if physical_flow_validated:
        weighted_coverage += WEIGHTS[
            "physical_flow"
        ]

    if logistics_usable:
        weighted_coverage += WEIGHTS["logistics"]

    missing = []

    if not security_usable:
        missing.append("security")

    if not physical_flow_validated:
        missing.append("physical_flow")

    if not logistics_usable:
        missing.append("logistics_score")

    return {
        "status": "PROVISIONAL_INCOMPLETE",
        "validated_score": None,
        "validated_level": None,
        "validated": False,
        "weights": WEIGHTS,
        "validated_weight_coverage": round(
            weighted_coverage,
            2
        ),
        "missing_components": missing,
        "security_score": (
            security["score"]
            if security_usable
            else None
        ),
        "physical_flow_score": None,
        "logistics_score": None,
        "reason": (
            "A final weighted risk score cannot "
            "be published without validated "
            "physical oil-flow disruption and "
            "calibrated logistics stress."
        ),
    }


def main():
    print("=== HORMUZ RISK INTEGRATION ===")
    print("Version:", VERSION)

    datasets = {}
    sources = {}

    for name, filename in INPUTS.items():
        data, inspection = inspect_input(
            name,
            filename,
        )

        datasets[name] = data
        sources[name] = inspection

        print(
            name,
            inspection["status"],
            inspection["age_hours"],
        )

    security = analyze_security(
        datasets["security"],
        sources["security"],
    )

    portwatch = analyze_portwatch(
        datasets["portwatch"],
        sources["portwatch"],
    )

    live_ais = analyze_live_ais(
        datasets["live_ais"],
        sources["live_ais"],
    )

    vessels = analyze_vessels(
        datasets["vessels"],
        sources["vessels"],
    )

    logistics = build_logistics(
        live_ais,
        vessels,
    )

    composite = build_composite(
        security,
        logistics,
    )

    output = {
        "meta": {
            "model": "Hormuz Integrated Risk",
            "version": VERSION,
            "generated_at": iso_now(),
            "repository": "energy-data",
            "mode": "PROVISIONAL",
        },
        "methodology": {
            "security_weight": 0.40,
            "physical_flow_weight": 0.35,
            "logistics_weight": 0.25,
            "physical_flow_requirement": (
                "Independent physical oil-flow "
                "measurements are required."
            ),
            "logistics_indicator_status": "EXPERIMENTAL_UNCALIBRATED_NOT_COMPOSITE_ELIGIBLE",
            "ais_limitations": (
                "AIS observation loss cannot "
                "be converted directly into "
                "physical throughput loss."
            ),
        },
        "input_status": sources,
        "layers": {
            "security": security,
            "physical_flow": {
                "weight": WEIGHTS[
                    "physical_flow"
                ],
                "status": "VALIDATION_REQUIRED",
                "score": None,
                "validated": False,
                "portwatch_observation": portwatch,
            },
            "logistics": {
                **logistics,
                "live_ais": live_ais,
                "vessels": vessels,
            },
        },
        "composite": composite,
        "publication_guidance": {
            "security_score_publishable": security[
                "usable_for_model"
            ],
            "ais_indicators_publishable": True,
            "experimental_logistics_indicator_publishable_with_warning": True,
            "final_composite_publishable": False,
            "required_label": (
                "PROVISIONAL - NOT A VALIDATED "
                "PHYSICAL OIL-FLOW RISK SCORE"
            ),
        },
    }

    output_path = Path(OUTPUT)

    temp_path = output_path.with_suffix(
        ".json.tmp"
    )

    try:
        with temp_path.open(
            "w",
            encoding="utf-8"
        ) as handle:
            json.dump(
                output,
                handle,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            )
            handle.write("\n")

        temp_path.replace(output_path)

    except (OSError, ValueError) as exc:
        print("ERROR:", exc)
        return 1

    print()
    print("=== INTEGRATION RESULT ===")
    print("Composite:", composite["status"])
    print(
        "Validated coverage:",
        composite["validated_weight_coverage"]
    )
    print(
        "Missing:",
        ", ".join(
            composite["missing_components"]
        )
    )
    print("Output:", OUTPUT)

    return 0


if __name__ == "__main__":
    sys.exit(main())
