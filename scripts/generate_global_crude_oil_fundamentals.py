#!/usr/bin/env python3
"""Generate global crude-oil fundamentals from JODI Oil World data.

Outputs:
- docs/data/global_crude_oil_fundamentals.json
- docs/data/jodi_crude_code_audit.json

Configured JODI filters:
- CRUDEOIL + INDPROD  + KBD  = crude-oil production
- CRUDEOIL + REFINOBS + KBD  = refinery crude intake
- CRUDEOIL + STOCKCH  + KBBL = reported monthly stock change
- CRUDEOIL + CLOSTLV  + KBBL = reported closing stocks

The production-minus-refinery-intake gap is not a complete global
supply-demand balance. It is a coverage-dependent availability indicator.
"""

from __future__ import annotations

import calendar
import csv
import io
import json
import math
import os
import sys
import tempfile
import urllib.request
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_FILE = ROOT / "docs" / "data" / "global_crude_oil_fundamentals.json"
AUDIT_FILE = ROOT / "docs" / "data" / "jodi_crude_code_audit.json"

DEFAULT_JODI_URL = (
    "https://www.jodidata.org/_resources/files/downloads/"
    "oil-data/world_primary_csv.zip?iid=24"
)
JODI_URL = os.environ.get("JODI_OIL_PRIMARY_CSV_URL", DEFAULT_JODI_URL)

ENERGY_PRODUCT = "CRUDEOIL"
FLOW_CONFIG = {
    "production": {"code": "INDPROD", "unit": "KBD"},
    "refinery_intake": {"code": "REFINOBS", "unit": "KBD"},
    "stock_change": {"code": "STOCKCH", "unit": "KBBL"},
    "closing_stocks": {"code": "CLOSTLV", "unit": "KBBL"},
}

REQUIRED_COLUMNS = {
    "REF_AREA",
    "TIME_PERIOD",
    "ENERGY_PRODUCT",
    "FLOW_BREAKDOWN",
    "UNIT_MEASURE",
    "OBS_VALUE",
    "ASSESSMENT_CODE",
}

MONTHS_TO_KEEP = 120
ANNUAL_START_YEAR = 2023
MIN_COVERAGE_RATIO = 0.85
COVERAGE_LOOKBACK_MONTHS = 24
USER_AGENT = "Energy-Intelligence-Dashboard/1.0"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def norm(value: Any) -> str:
    return "" if value is None else str(value).strip().upper()


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"-", "X", "N/A", "NA", "NULL", "NONE", "..", "..."}:
        return None
    try:
        number = float(text.replace(",", ""))
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def valid_period(value: Any) -> str | None:
    text = str(value or "").strip()
    if len(text) != 7 or text[4] != "-":
        return None
    try:
        year = int(text[:4])
        month = int(text[5:7])
    except ValueError:
        return None
    if year < 1900 or not 1 <= month <= 12:
        return None
    return f"{year:04d}-{month:02d}"


def period_key(period: str) -> tuple[int, int]:
    return int(period[:4]), int(period[5:7])


def days_in_period(period: str) -> int:
    year, month = period_key(period)
    return calendar.monthrange(year, month)[1]


def round_or_none(value: float | None, digits: int = 3) -> float | None:
    return None if value is None else round(float(value), digits)


def safe_mean(values: Iterable[float | None]) -> float | None:
    usable = [float(value) for value in values if value is not None]
    return mean(usable) if usable else None


def pct_change(old: float | None, new: float | None) -> float | None:
    if old is None or new is None or old == 0:
        return None
    return ((new - old) / old) * 100.0


def save_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    temporary.replace(path)


def download_archive(url: str, destination: Path) -> None:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/zip,application/octet-stream,text/csv,*/*;q=0.8",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            content = response.read()
    except Exception as exc:
        raise RuntimeError(f"JODI archive download failed: {exc}") from exc

    if not content:
        raise RuntimeError("The downloaded JODI archive is empty.")

    destination.write_bytes(content)
    if not zipfile.is_zipfile(destination):
        preview = content[:200].decode("utf-8", errors="replace")
        raise RuntimeError(
            "The downloaded JODI response is not a ZIP archive. "
            f"Response preview: {preview!r}"
        )


def choose_csv_member(archive: zipfile.ZipFile) -> str:
    members = [
        name
        for name in archive.namelist()
        if not name.endswith("/") and name.lower().endswith(".csv")
    ]
    if not members:
        raise RuntimeError("No CSV file was found inside the JODI ZIP archive.")

    def score(name: str) -> tuple[int, int]:
        lowered = name.lower()
        points = 0
        if "primary" in lowered:
            points += 10
        if "newprocedure" in lowered or "newformat" in lowered:
            points += 5
        if "secondary" in lowered:
            points -= 20
        return points, -len(name)

    return max(members, key=score)


def decode_csv(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise RuntimeError("The JODI CSV encoding could not be decoded.")


def read_jodi_rows(
    archive_path: Path,
) -> tuple[list[dict[str, str]], str, list[str]]:
    with zipfile.ZipFile(archive_path) as archive:
        member = choose_csv_member(archive)
        raw = archive.read(member)

    reader = csv.DictReader(io.StringIO(decode_csv(raw), newline=""))
    if reader.fieldnames is None:
        raise RuntimeError("The JODI CSV does not contain a header row.")

    fieldnames = [str(name or "").strip().lstrip("\ufeff") for name in reader.fieldnames]
    reader.fieldnames = fieldnames

    missing = sorted(REQUIRED_COLUMNS - set(fieldnames))
    if missing:
        raise RuntimeError("Required JODI columns are missing: " + ", ".join(missing))

    rows: list[dict[str, str]] = []
    for source_row in reader:
        rows.append(
            {
                str(key or "").strip().lstrip("\ufeff"): str(value or "").strip()
                for key, value in source_row.items()
            }
        )

    if not rows:
        raise RuntimeError("The JODI CSV contains no data rows.")
    return rows, member, fieldnames


def build_audit(
    all_rows: list[dict[str, str]],
    archive_member: str,
    fieldnames: list[str],
) -> dict[str, Any]:
    crude_rows = [
        row for row in all_rows if norm(row.get("ENERGY_PRODUCT")) == ENERGY_PRODUCT
    ]

    flow_counts: dict[str, int] = defaultdict(int)
    unit_counts: dict[str, int] = defaultdict(int)
    assessment_counts: dict[str, int] = defaultdict(int)
    areas: set[str] = set()

    for row in crude_rows:
        flow_counts[norm(row.get("FLOW_BREAKDOWN"))] += 1
        unit_counts[norm(row.get("UNIT_MEASURE"))] += 1
        assessment_counts[norm(row.get("ASSESSMENT_CODE"))] += 1
        area = norm(row.get("REF_AREA"))
        if area:
            areas.add(area)

    configured: dict[str, Any] = {}
    for label, config in FLOW_CONFIG.items():
        code = config["code"]
        unit = config["unit"]
        matches = [
            row
            for row in crude_rows
            if norm(row.get("FLOW_BREAKDOWN")) == code
            and norm(row.get("UNIT_MEASURE")) == unit
            and parse_number(row.get("OBS_VALUE")) is not None
        ]
        periods = sorted(
            {
                period
                for row in matches
                if (period := valid_period(row.get("TIME_PERIOD")))
            },
            key=period_key,
        )
        configured[label] = {
            "code": code,
            "unit": unit,
            "valid_observations": len(matches),
            "reporting_areas": len(
                {
                    norm(row.get("REF_AREA"))
                    for row in matches
                    if norm(row.get("REF_AREA"))
                }
            ),
            "start_period": periods[0] if periods else None,
            "end_period": periods[-1] if periods else None,
        }

    return {
        "generated_at": utc_now_iso(),
        "dataset": "jodi_crude_code_audit",
        "source": {
            "name": "JODI Oil World Database",
            "download_url": JODI_URL,
            "archive_member": archive_member,
        },
        "csv_columns": fieldnames,
        "row_counts": {
            "all_rows": len(all_rows),
            "crude_oil_rows": len(crude_rows),
        },
        "required_filters": {
            "ENERGY_PRODUCT": ENERGY_PRODUCT,
            "flows": FLOW_CONFIG,
        },
        "configured_flows": configured,
        "available_flow_breakdown_values": [
            {"code": key, "row_count": flow_counts[key]}
            for key in sorted(flow_counts)
            if key
        ],
        "available_unit_measure_values": [
            {"code": key, "row_count": unit_counts[key]}
            for key in sorted(unit_counts)
            if key
        ],
        "assessment_code_values": [
            {"code": key, "row_count": assessment_counts[key]}
            for key in sorted(assessment_counts)
            if key
        ],
        "crude_reporting_areas": sorted(areas),
    }


def validate_required_flows(audit: dict[str, Any]) -> None:
    missing: list[str] = []
    for label, details in audit["configured_flows"].items():
        if details["valid_observations"] == 0:
            missing.append(f"{label} ({details['code']} + {details['unit']})")
    if missing:
        raise RuntimeError(
            "Configured JODI crude-oil flows were not found: " + ", ".join(missing)
        )


def assessment_rank(code: str) -> int:
    try:
        return int(norm(code))
    except ValueError:
        return 99


def select_rows(all_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    lookup = {
        (config["code"], config["unit"]): label
        for label, config in FLOW_CONFIG.items()
    }
    deduplicated: dict[tuple[str, str, str], dict[str, Any]] = {}

    for row in all_rows:
        if norm(row.get("ENERGY_PRODUCT")) != ENERGY_PRODUCT:
            continue

        flow = norm(row.get("FLOW_BREAKDOWN"))
        unit = norm(row.get("UNIT_MEASURE"))
        label = lookup.get((flow, unit))
        if label is None:
            continue

        area = norm(row.get("REF_AREA"))
        period = valid_period(row.get("TIME_PERIOD"))
        value = parse_number(row.get("OBS_VALUE"))
        if not area or period is None or value is None:
            continue

        prepared = {
            "area": area,
            "period": period,
            "label": label,
            "flow": flow,
            "unit": unit,
            "value": value,
            "assessment_code": str(row.get("ASSESSMENT_CODE") or "").strip(),
        }
        key = (area, period, label)
        existing = deduplicated.get(key)
        if existing is None or assessment_rank(prepared["assessment_code"]) < assessment_rank(
            existing["assessment_code"]
        ):
            deduplicated[key] = prepared

    rows = list(deduplicated.values())
    rows.sort(key=lambda item: (period_key(item["period"]), item["area"], item["label"]))
    return rows


def build_monthly(
    selected: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    grouped: dict[str, dict[str, dict[str, dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for row in selected:
        grouped[row["period"]][row["label"]][row["area"]] = row

    monthly: list[dict[str, Any]] = []
    for period in sorted(grouped, key=period_key):
        data = grouped[period]
        production = data.get("production", {})
        intake = data.get("refinery_intake", {})
        stock_change = data.get("stock_change", {})
        closing_stocks = data.get("closing_stocks", {})

        production_areas = set(production)
        intake_areas = set(intake)
        common_areas = production_areas & intake_areas

        production_total_kbd = (
            sum(item["value"] for item in production.values()) if production else None
        )
        intake_total_kbd = sum(item["value"] for item in intake.values()) if intake else None
        common_production_kbd = (
            sum(production[area]["value"] for area in common_areas)
            if common_areas
            else None
        )
        common_intake_kbd = (
            sum(intake[area]["value"] for area in common_areas)
            if common_areas
            else None
        )
        common_gap_kbd = (
            common_production_kbd - common_intake_kbd
            if common_production_kbd is not None and common_intake_kbd is not None
            else None
        )

        stock_change_total_kbbl = (
            sum(item["value"] for item in stock_change.values())
            if stock_change
            else None
        )
        closing_stocks_total_kbbl = (
            sum(item["value"] for item in closing_stocks.values())
            if closing_stocks
            else None
        )
        days = days_in_period(period)
        stock_change_average_kbd = (
            stock_change_total_kbbl / days
            if stock_change_total_kbbl is not None
            else None
        )

        monthly.append(
            {
                "period": period,
                "date": f"{period}-01",
                "days_in_month": days,
                "reported_production_kbd": round_or_none(production_total_kbd, 1),
                "reported_production_mbd": round_or_none(
                    production_total_kbd / 1000
                    if production_total_kbd is not None
                    else None
                ),
                "reported_refinery_intake_kbd": round_or_none(intake_total_kbd, 1),
                "reported_refinery_intake_mbd": round_or_none(
                    intake_total_kbd / 1000 if intake_total_kbd is not None else None
                ),
                "common_country_production_mbd": round_or_none(
                    common_production_kbd / 1000
                    if common_production_kbd is not None
                    else None
                ),
                "common_country_refinery_intake_mbd": round_or_none(
                    common_intake_kbd / 1000 if common_intake_kbd is not None else None
                ),
                "common_country_gap_mbd": round_or_none(
                    common_gap_kbd / 1000 if common_gap_kbd is not None else None
                ),
                "reported_stock_change_kbbl": round_or_none(
                    stock_change_total_kbbl, 1
                ),
                "reported_stock_change_average_kbd": round_or_none(
                    stock_change_average_kbd, 1
                ),
                "reported_stock_change_average_mbd": round_or_none(
                    stock_change_average_kbd / 1000
                    if stock_change_average_kbd is not None
                    else None
                ),
                "reported_closing_stocks_kbbl": round_or_none(
                    closing_stocks_total_kbbl, 1
                ),
                "reported_closing_stocks_million_barrels": round_or_none(
                    closing_stocks_total_kbbl / 1000
                    if closing_stocks_total_kbbl is not None
                    else None
                ),
                "adjusted_crude_balance_mbd": round_or_none(
                    (
                        common_gap_kbd / 1000
                        - stock_change_average_kbd / 1000
                    )
                    if common_gap_kbd is not None
                    and stock_change_average_kbd is not None
                    else None
                ),
                "coverage": {
                    "production_reporters": len(production_areas),
                    "refinery_intake_reporters": len(intake_areas),
                    "common_reporters": len(common_areas),
                    "stock_change_reporters": len(stock_change),
                    "closing_stock_reporters": len(closing_stocks),
                },
            }
        )

    if not monthly:
        raise RuntimeError("No usable monthly JODI crude-oil rows were produced.")

    reference_window = monthly[-COVERAGE_LOOKBACK_MONTHS:]
    references = {
        field: max(row["coverage"][field] for row in reference_window)
        for field in (
            "production_reporters",
            "refinery_intake_reporters",
            "common_reporters",
            "stock_change_reporters",
            "closing_stock_reporters",
        )
    }

    for row in monthly:
        coverage = row["coverage"]

        def ratio(field: str) -> float:
            reference = references[field]
            if not reference:
                return 0.0
            return min(1.0, coverage[field] / reference)

        production_ratio = ratio("production_reporters")
        intake_ratio = ratio("refinery_intake_reporters")
        common_ratio = ratio("common_reporters")

        row["coverage_quality"] = {
            "production_ratio": round(production_ratio, 3),
            "refinery_intake_ratio": round(intake_ratio, 3),
            "common_ratio": round(common_ratio, 3),
            "stock_change_ratio": round(ratio("stock_change_reporters"), 3),
            "closing_stock_ratio": round(ratio("closing_stock_reporters"), 3),
            "headline_usable": (
                row["common_country_gap_mbd"] is not None
                and production_ratio >= MIN_COVERAGE_RATIO
                and intake_ratio >= MIN_COVERAGE_RATIO
                and common_ratio >= MIN_COVERAGE_RATIO
            ),
        }

    return monthly, references


def add_changes(monthly: list[dict[str, Any]]) -> None:
    by_period = {row["period"]: row for row in monthly}
    for index, row in enumerate(monthly):
        previous = monthly[index - 1] if index > 0 else None
        year = int(row["period"][:4])
        month = int(row["period"][5:7])
        previous_year = by_period.get(f"{year - 1:04d}-{month:02d}")

        row["changes"] = {
            "production_mom_pct": round_or_none(
                pct_change(
                    previous.get("reported_production_mbd") if previous else None,
                    row.get("reported_production_mbd"),
                ),
                2,
            ),
            "production_yoy_pct": round_or_none(
                pct_change(
                    previous_year.get("reported_production_mbd")
                    if previous_year
                    else None,
                    row.get("reported_production_mbd"),
                ),
                2,
            ),
            "refinery_intake_mom_pct": round_or_none(
                pct_change(
                    previous.get("reported_refinery_intake_mbd")
                    if previous
                    else None,
                    row.get("reported_refinery_intake_mbd"),
                ),
                2,
            ),
            "refinery_intake_yoy_pct": round_or_none(
                pct_change(
                    previous_year.get("reported_refinery_intake_mbd")
                    if previous_year
                    else None,
                    row.get("reported_refinery_intake_mbd"),
                ),
                2,
            ),
            "closing_stocks_mom_pct": round_or_none(
                pct_change(
                    previous.get("reported_closing_stocks_million_barrels")
                    if previous
                    else None,
                    row.get("reported_closing_stocks_million_barrels"),
                ),
                2,
            ),
            "closing_stocks_yoy_pct": round_or_none(
                pct_change(
                    previous_year.get("reported_closing_stocks_million_barrels")
                    if previous_year
                    else None,
                    row.get("reported_closing_stocks_million_barrels"),
                ),
                2,
            ),
        }


def gap_state(value: float | None) -> str:
    if value is None:
        return "unavailable"
    if value > 0.5:
        return "production_above_intake"
    if value < -0.5:
        return "intake_above_production"
    return "near_balance"


def latest_usable(monthly: list[dict[str, Any]]) -> dict[str, Any]:
    usable = [row for row in monthly if row["coverage_quality"]["headline_usable"]]
    if not usable:
        raise RuntimeError(
            "No month has sufficient common-country coverage for the headline indicator."
        )
    return usable[-1]


def build_annual(monthly: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in monthly:
        year = int(row["period"][:4])
        if year >= ANNUAL_START_YEAR:
            grouped[year].append(row)

    annual: list[dict[str, Any]] = []
    for year in sorted(grouped):
        rows = grouped[year]
        gap = safe_mean(
            row.get("common_country_gap_mbd")
            for row in rows
            if row["coverage_quality"]["headline_usable"]
        )
        adjusted_balance = safe_mean(
            row.get("adjusted_crude_balance_mbd")
            for row in rows
            if row["coverage_quality"]["headline_usable"]
        )
        stock_values = [
            row["reported_stock_change_kbbl"]
            for row in rows
            if row["reported_stock_change_kbbl"] is not None
        ]
        annual.append(
            {
                "year": year,
                "months_available": len(rows),
                "headline_usable_months": sum(
                    1 for row in rows if row["coverage_quality"]["headline_usable"]
                ),
                "average_reported_production_mbd": round_or_none(
                    safe_mean(row.get("reported_production_mbd") for row in rows)
                ),
                "average_reported_refinery_intake_mbd": round_or_none(
                    safe_mean(row.get("reported_refinery_intake_mbd") for row in rows)
                ),
                "average_common_country_gap_mbd": round_or_none(gap),
                "average_adjusted_crude_balance_mbd": round_or_none(
                    adjusted_balance
                ),
                "total_reported_stock_change_kbbl": round_or_none(
                    sum(stock_values) if stock_values else None, 1
                ),
                "average_reported_stock_change_mbd": round_or_none(
                    safe_mean(
                        row.get("reported_stock_change_average_mbd") for row in rows
                    )
                ),
                "average_reported_closing_stocks_million_barrels": round_or_none(
                    safe_mean(
                        row.get("reported_closing_stocks_million_barrels")
                        for row in rows
                    )
                ),
                "gap_state": gap_state(gap),
                "adjusted_balance_state": gap_state(adjusted_balance),
            }
        )
    return annual


def build_output(
    monthly: list[dict[str, Any]],
    annual: list[dict[str, Any]],
    latest: dict[str, Any],
    references: dict[str, int],
    archive_member: str,
    selected_count: int,
) -> dict[str, Any]:
    gap = latest["common_country_gap_mbd"]
    production = latest["reported_production_mbd"]
    intake = latest["reported_refinery_intake_mbd"]
    reporters = latest["coverage"]["common_reporters"]
    adjusted_balance = latest["adjusted_crude_balance_mbd"]

    summary_hu = (
        f"A legfrissebb megfelelő lefedettségű JODI-időszak {latest['period']}. "
        f"A jelentett nyersolajtermelés {production:.2f} mb/d, a jelentett "
        f"finomítói nyersolaj-betáplálás {intake:.2f} mb/d. Az azonos "
        f"{reporters} országkörön számított termelés–betáplálás különbsége "
        f"{gap:+.2f} mb/d. A készletváltozással mechanikusan korrigált "
        f"kísérleti mutató {adjusted_balance:+.2f} mb/d. Ez nem teljes "
        f"globális kínálat–keresleti mérleg."
    )
    summary_en = (
        f"The latest JODI period with sufficient coverage is {latest['period']}. "
        f"Reported crude-oil production is {production:.2f} mb/d and reported "
        f"refinery crude intake is {intake:.2f} mb/d. Across the same set of "
        f"{reporters} countries, the production-minus-intake gap is "
        f"{gap:+.2f} mb/d. The mechanically stock-adjusted experimental "
        f"indicator is {adjusted_balance:+.2f} mb/d. This is not a complete "
        f"global supply-demand balance."
    )

    return {
        "generated_at": utc_now_iso(),
        "dataset": "global_crude_oil_fundamentals",
        "title_hu": "Globális nyersolajpiaci fundamentumok",
        "title_en": "Global Crude Oil Fundamentals",
        "frequency": "monthly",
        "primary_flow_unit": "million_barrels_per_day",
        "primary_flow_unit_short": "mb/d",
        "stock_level_unit": "million_barrels",
        "stock_change_source_unit": "thousand_barrels_per_month",
        "source": {
            "name": "JODI Oil World Database",
            "provider": "Joint Organisations Data Initiative",
            "download_url": JODI_URL,
            "archive_member": archive_member,
            "filters": {
                "ENERGY_PRODUCT": ENERGY_PRODUCT,
                "flows": FLOW_CONFIG,
            },
        },
        "latest": {
            "period": latest["period"],
            "date": latest["date"],
            "reported_production_mbd": production,
            "reported_refinery_intake_mbd": intake,
            "common_country_production_mbd": latest[
                "common_country_production_mbd"
            ],
            "common_country_refinery_intake_mbd": latest[
                "common_country_refinery_intake_mbd"
            ],
            "common_country_gap_mbd": gap,
            "gap_state": gap_state(gap),
            "adjusted_crude_balance_mbd": adjusted_balance,
            "adjusted_balance_state": gap_state(adjusted_balance),
            "reported_stock_change_kbbl": latest["reported_stock_change_kbbl"],
            "reported_stock_change_average_kbd": latest[
                "reported_stock_change_average_kbd"
            ],
            "reported_stock_change_average_mbd": latest[
                "reported_stock_change_average_mbd"
            ],
            "reported_closing_stocks_kbbl": latest[
                "reported_closing_stocks_kbbl"
            ],
            "reported_closing_stocks_million_barrels": latest[
                "reported_closing_stocks_million_barrels"
            ],
            "coverage": latest["coverage"],
            "coverage_quality": latest["coverage_quality"],
            "changes": latest["changes"],
        },
        "coverage": {
            "start_period": monthly[0]["period"],
            "latest_source_period": monthly[-1]["period"],
            "latest_headline_period": latest["period"],
            "monthly_period_count": len(monthly),
            "selected_valid_source_rows": selected_count,
            "recent_reference_reporter_counts": references,
            "headline_minimum_coverage_ratio": MIN_COVERAGE_RATIO,
            "coverage_reference_lookback_months": COVERAGE_LOOKBACK_MONTHS,
            "coverage_reference_method": (
                "maximum reporter count observed during the latest "
                f"{COVERAGE_LOOKBACK_MONTHS} source months"
            ),
        },
        "methodology_hu": (
            "A modul a JODI Oil World Primary CSV CRUDEOIL terméksorait "
            "használja. A nyersolajtermelés INDPROD + KBD, a finomítói "
            "nyersolaj-betáplálás REFINOBS + KBD, a havi készletváltozás "
            "STOCKCH + KBBL, a zárókészlet pedig CLOSTLV + KBBL szűrésből "
            "származik. A havi készletváltozás napi átlagra történő "
            "átszámítása a hónap naptári napjainak számával történik. A "
            "termelés és a finomítói betáplálás különbségét azonos "
            "országkörön számítja. A lefedettségi arányok nevezője a legutóbbi 24 forráshónapban "
            "megfigyelt legmagasabb jelentői létszám. Az arányok legfeljebb "
            "1 értéket vehetnek fel. Az Adjusted Crude Balance a "
            "common-country gap és a napi átlagra átszámított STOCKCH "
            "mechanikus különbsége."
        ),
        "methodology_en": (
            "The module uses CRUDEOIL rows from the JODI Oil World Primary "
            "CSV. Crude-oil production uses INDPROD + KBD, refinery crude "
            "intake uses REFINOBS + KBD, monthly stock change uses STOCKCH + "
            "KBBL and closing stocks use CLOSTLV + KBBL. Monthly stock change "
            "is converted to an average daily rate using calendar days in the "
            "month. Production and refinery intake are compared across the "
            "same country set. Coverage ratios use the maximum reporter count observed during the "
            "latest 24 source months and are capped at one. Adjusted Crude Balance is the mechanical "
            "difference between the common-country gap and STOCKCH converted "
            "to an average daily rate."
        ),
        "stock_sign_note_hu": (
            "A STOCKCH előjelének értelmezését a modul semlegesen kezeli. "
            "Az érték nem kap automatikus készletépítés vagy készletleépítés "
            "minősítést."
        ),
        "stock_sign_note_en": (
            "The STOCKCH sign is handled neutrally. The value is not "
            "automatically classified as a stock build or stock draw."
        ),
        "adjusted_balance_note_hu": (
            "Az Adjusted Crude Balance kísérleti, mechanikusan számított "
            "mutató: common_country_gap_mbd mínusz "
            "reported_stock_change_average_mbd. Nem tekinthető teljes "
            "globális nyersolaj-egyenlegnek, és a STOCKCH előjelének "
            "gazdasági értelmezését nem helyettesíti."
        ),
        "adjusted_balance_note_en": (
            "Adjusted Crude Balance is an experimental optional indicator: "
            "common_country_gap_mbd minus reported_stock_change_average_mbd. "
            "It is not a complete global crude-oil balance and does not "
            "replace economic interpretation of the STOCKCH sign."
        ),
        "summary_hu": summary_hu,
        "summary_en": summary_en,
        "annual_series": annual,
        "monthly_series": monthly[-MONTHS_TO_KEEP:],
        "disclaimer_hu": (
            "A JODI-adatok országonként eltérő késéssel és lefedettséggel "
            "érkeznek. A termelés mínusz finomítói betáplálás különbsége nem "
            "azonos a teljes globális nyersolaj-kínálat és -kereslet "
            "egyenlegével."
        ),
        "disclaimer_en": (
            "JODI reporting delays and country coverage vary. Production "
            "minus refinery intake is not equivalent to a complete global "
            "crude-oil supply-demand balance."
        ),
    }


def main() -> None:
    print("Global crude-oil fundamentals generation started.")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        archive_path = Path(temp_dir) / "jodi_world_primary_csv.zip"
        print(f"Downloading JODI archive: {JODI_URL}")
        download_archive(JODI_URL, archive_path)
        all_rows, archive_member, fieldnames = read_jodi_rows(archive_path)

    audit = build_audit(all_rows, archive_member, fieldnames)
    save_json_atomic(AUDIT_FILE, audit)
    print(f"Audit written: {AUDIT_FILE.relative_to(ROOT)}")

    validate_required_flows(audit)

    selected = select_rows(all_rows)
    monthly, references = build_monthly(selected)
    add_changes(monthly)
    latest = latest_usable(monthly)
    annual = build_annual(monthly)

    output = build_output(
        monthly=monthly,
        annual=annual,
        latest=latest,
        references=references,
        archive_member=archive_member,
        selected_count=len(selected),
    )
    save_json_atomic(OUTPUT_FILE, output)

    print(f"Output written: {OUTPUT_FILE.relative_to(ROOT)}")
    print(
        f"Latest headline period: {latest['period']} | "
        f"production={latest['reported_production_mbd']:.3f} mb/d | "
        f"refinery intake={latest['reported_refinery_intake_mbd']:.3f} mb/d | "
        f"common-country gap={latest['common_country_gap_mbd']:+.3f} mb/d | "
        f"adjusted balance={latest['adjusted_crude_balance_mbd']:+.3f} mb/d"
    )
    print(
        "Latest stock fields: "
        f"change={latest['reported_stock_change_kbbl']} kbbl | "
        f"closing={latest['reported_closing_stocks_million_barrels']} million barrels"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
