
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Integrated Risk - History Manager
Version: 1.0

Input:
    hormuz-risk-composite.json

Outputs:
    history/hormuz/composite-history.csv
    history/hormuz/composite-history.json
    history/hormuz/snapshots/YYYY/MM/*.json

Features:
    - Persistent history
    - Duplicate prevention
    - Atomic file writing
    - UTC timestamps
    - Data quality tracking
    - Model version tracking
    - Full historical snapshots
"""

import csv
import io
import json
import os
import hashlib
import tempfile

from datetime import datetime, timezone
from pathlib import Path


SOURCE_FILE = Path("hormuz-risk-composite.json")

HISTORY_DIR = Path("history/hormuz")

CSV_FILE = HISTORY_DIR / "composite-history.csv"
JSON_FILE = HISTORY_DIR / "composite-history.json"

SNAPSHOT_DIR = HISTORY_DIR / "snapshots"

VERSION = "1.0"


FIELDS = [
    "record_id",
    "timestamp_utc",
    "model_version",
    "composite_status",
    "composite_score",
    "composite_validated",
    "validated_weight_coverage",

    "security_score",
    "security_level",
    "security_confidence",
    "security_events_7d",
    "direct_hormuz_events_7d",
    "maritime_attacks_7d",
    "closure_threats_7d",

    "physical_flow_score",
    "physical_flow_validated",

    "ais_disruption_7d",
    "ais_disruption_30d",
    "portwatch_observation_date",
    "portwatch_observation_age_days",
    "baseline_tankers_per_day",
    "observed_tankers_7d_per_day",

    "logistics_score",
    "logistics_status",

    "ships_total",
    "ships_in_strait",
    "crossings_24h",
    "crossings_inbound_24h",
    "crossings_outbound_24h",

    "crossings_7d_avg",
    "crossings_30d_avg",
    "crossings_trend_pct",
    "outbound_trend_pct",

    "oil_export_proxy_7d",
    "oil_export_proxy_completeness_pct",

    "vessels_total",
    "oil_related_vessels",
    "crude_related_vessels",
    "large_oil_tankers",
    "vlcc_ulcc_count",

    "tankers_in_strait",
    "tankers_hormuz_core",

    "confirmed_transits",
    "confirmed_micro_transits",
    "possible_micro_transits",

    "snapshot_churn_pct",
    "vessel_data_quality_score",
    "vessel_data_quality_status",

    "security_source_status",
    "portwatch_source_status",
    "live_ais_source_status",
    "vessels_source_status",

    "security_source_age_hours",
    "portwatch_source_age_hours",
    "live_ais_source_age_hours",
    "vessels_source_age_hours",

    "security_source_generated_at",
    "portwatch_source_generated_at",
    "live_ais_source_generated_at",
    "vessels_source_generated_at",
]


def nested(data, *keys, default=None):
    current = data

    for key in keys:
        if not isinstance(current, dict):
            return default

        current = current.get(key)

        if current is None:
            return default

    return current


def parse_timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Missing or invalid timestamp")

    parsed = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        raise ValueError(
            "Timestamp must include timezone"
        )

    return parsed.astimezone(timezone.utc)


def atomic_write(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    fd, temporary_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=".tmp-",
        suffix=".part"
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline=""
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_name, path)

    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8"
    ) as handle:
        return json.load(handle)


def canonical_json(data):
    return json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False
    )


def create_record_id(data):
    content = canonical_json(data)

    return hashlib.sha256(
        content.encode("utf-8")
    ).hexdigest()[:20]


def build_record(data):
    timestamp = nested(
        data,
        "meta",
        "generated_at"
    )

    parsed = parse_timestamp(timestamp)

    sources = data.get("input_status", {})

    security = nested(
        data,
        "layers",
        "security",
        default={}
    )

    physical = nested(
        data,
        "layers",
        "physical_flow",
        default={}
    )

    logistics = nested(
        data,
        "layers",
        "logistics",
        default={}
    )

    portwatch = physical.get(
        "portwatch_observation",
        {}
    )

    live = logistics.get(
        "live_ais",
        {}
    )

    vessels = logistics.get(
        "vessels",
        {}
    )

    analysis = vessels.get(
        "analysis",
        {}
    )

    tracking = vessels.get(
        "tracking_summary",
        {}
    )

    quality = vessels.get(
        "data_quality",
        {}
    )

    crossings = live.get(
        "crossings_24h",
        {}
    )

    stats = live.get(
        "crossing_statistics",
        {}
    )

    oil_proxy = live.get(
        "oil_export_proxy",
        {}
    )

    composite = data.get(
        "composite",
        {}
    )

    record = {
        "record_id": create_record_id(data),

        "timestamp_utc": parsed.isoformat(),

        "model_version": nested(
            data,
            "meta",
            "version"
        ),

        "composite_status": composite.get(
            "status"
        ),

        "composite_score": composite.get(
            "validated_score"
        ),

        "composite_validated": composite.get(
            "validated"
        ),

        "validated_weight_coverage": composite.get(
            "validated_weight_coverage"
        ),

        "security_score": security.get(
            "score"
        ),

        "security_level": security.get(
            "level"
        ),

        "security_confidence": security.get(
            "confidence"
        ),

        "security_events_7d": security.get(
            "events_7d"
        ),

        "direct_hormuz_events_7d": security.get(
            "direct_hormuz_events_7d"
        ),

        "maritime_attacks_7d": security.get(
            "maritime_attacks_7d"
        ),

        "closure_threats_7d": security.get(
            "closure_threats_7d"
        ),

        "physical_flow_score": physical.get(
            "score"
        ),

        "physical_flow_validated": physical.get(
            "validated"
        ),

        "ais_disruption_7d": portwatch.get(
            "ais_disruption_7d"
        ),

        "ais_disruption_30d": portwatch.get(
            "ais_disruption_30d"
        ),

        "portwatch_observation_date": portwatch.get(
            "latest_observation"
        ),

        "portwatch_observation_age_days": portwatch.get(
            "observation_age_days"
        ),

        "baseline_tankers_per_day": portwatch.get(
            "structural_baseline_tankers_per_day"
        ),

        "observed_tankers_7d_per_day": portwatch.get(
            "current_7d_tankers_per_day"
        ),

        "logistics_score": logistics.get(
            "score"
        ),

        "logistics_status": logistics.get(
            "status"
        ),

        "ships_total": live.get(
            "ships_total"
        ),

        "ships_in_strait": live.get(
            "ships_in_strait"
        ),

        "crossings_24h": crossings.get(
            "total"
        ),

        "crossings_inbound_24h": crossings.get(
            "inbound"
        ),

        "crossings_outbound_24h": crossings.get(
            "outbound"
        ),

        "crossings_7d_avg": nested(
            stats,
            "7d",
            "total_crossings_avg"
        ),

        "crossings_30d_avg": nested(
            stats,
            "30d",
            "total_crossings_avg"
        ),

        "crossings_trend_pct": nested(
            stats,
            "trend",
            "total_crossings_7d_vs_30d_pct"
        ),

        "outbound_trend_pct": nested(
            stats,
            "trend",
            "outbound_7d_vs_30d_pct"
        ),

        "oil_export_proxy_7d": nested(
            oil_proxy,
            "7d",
            "observed_day_average_barrels"
        ),

        "oil_export_proxy_completeness_pct": nested(
            oil_proxy,
            "7d",
            "completeness_pct"
        ),

        "vessels_total": analysis.get(
            "total_vessels"
        ),

        "oil_related_vessels": analysis.get(
            "oil_related_vessels"
        ),

        "crude_related_vessels": analysis.get(
            "crude_related_vessels"
        ),

        "large_oil_tankers": analysis.get(
            "large_oil_tankers"
        ),

        "vlcc_ulcc_count": analysis.get(
            "vlcc_ulcc_count"
        ),

        "tankers_in_strait": nested(
            analysis,
            "oil_tankers_by_zone",
            "strait"
        ),

        "tankers_hormuz_core": nested(
            analysis,
            "oil_tankers_by_micro_zone",
            "hormuz_core"
        ),

        "confirmed_transits": tracking.get(
            "confirmed_transits_this_run"
        ),

        "confirmed_micro_transits": tracking.get(
            "confirmed_micro_transits_this_run"
        ),

        "possible_micro_transits": tracking.get(
            "possible_micro_skipped_core_transits_this_run"
        ),

        "snapshot_churn_pct": quality.get(
            "snapshot_churn_pct"
        ),

        "vessel_data_quality_score": quality.get(
            "score"
        ),

        "vessel_data_quality_status": quality.get(
            "status"
        ),

        "security_source_status": nested(
            sources,
            "security",
            "status"
        ),

        "portwatch_source_status": nested(
            sources,
            "portwatch",
            "status"
        ),

        "live_ais_source_status": nested(
            sources,
            "live_ais",
            "status"
        ),

        "vessels_source_status": nested(
            sources,
            "vessels",
            "status"
        ),

        "security_source_age_hours": nested(
            sources,
            "security",
            "age_hours"
        ),

        "portwatch_source_age_hours": nested(
            sources,
            "portwatch",
            "age_hours"
        ),

        "live_ais_source_age_hours": nested(
            sources,
            "live_ais",
            "age_hours"
        ),

        "vessels_source_age_hours": nested(
            sources,
            "vessels",
            "age_hours"
        ),

        "security_source_generated_at": nested(
            sources,
            "security",
            "generated_at"
        ),

        "portwatch_source_generated_at": nested(
            sources,
            "portwatch",
            "generated_at"
        ),

        "live_ais_source_generated_at": nested(
            sources,
            "live_ais",
            "generated_at"
        ),

        "vessels_source_generated_at": nested(
            sources,
            "vessels",
            "generated_at"
        ),
    }

    return record, parsed


def load_existing_history():
    if not JSON_FILE.exists():
        return []

    data = read_json(JSON_FILE)

    if not isinstance(data, list):
        raise ValueError(
            "Existing history must be a JSON list"
        )

    return data


def write_csv(records):
    buffer = io.StringIO()

    writer = csv.DictWriter(
        buffer,
        fieldnames=FIELDS,
        extrasaction="ignore"
    )

    writer.writeheader()

    for record in records:
        writer.writerow(record)

    atomic_write(
        CSV_FILE,
        buffer.getvalue()
    )


def save_snapshot(data, timestamp, record_id):
    directory = (
        SNAPSHOT_DIR
        / timestamp.strftime("%Y")
        / timestamp.strftime("%m")
    )

    filename = (
        timestamp.strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + record_id
        + ".json"
    )

    path = directory / filename

    if path.exists():
        return path

    content = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        allow_nan=False
    )

    atomic_write(
        path,
        content + "\n"
    )

    return path


def main():
    print("=== HORMUZ HISTORY MANAGER ===")

    if not SOURCE_FILE.is_file():
        raise SystemExit(
            "ERROR: Composite JSON not found"
        )

    data = read_json(SOURCE_FILE)

    record, timestamp = build_record(data)

    HISTORY_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    records = load_existing_history()

    existing_ids = {
        item.get("record_id")
        for item in records
    }

    record_id = record["record_id"]

    if record_id in existing_ids:
        print("Record already exists.")
        print("Duplicate prevented.")
        return

    records.append(record)

    records.sort(
        key=lambda item: item.get(
            "timestamp_utc",
            ""
        )
    )

    snapshot = save_snapshot(
        data,
        timestamp,
        record_id
    )

    atomic_write(
        JSON_FILE,
        json.dumps(
            records,
            ensure_ascii=False,
            indent=2,
            allow_nan=False
        ) + "\n"
    )

    write_csv(records)

    print("History record saved.")
    print("Record ID:", record_id)
    print("Timestamp:", record["timestamp_utc"])
    print("Total records:", len(records))
    print("Snapshot:", snapshot)
    print("CSV:", CSV_FILE)
    print("JSON:", JSON_FILE)


if __name__ == "__main__":
    main()
