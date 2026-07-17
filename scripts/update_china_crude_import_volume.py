#!/usr/bin/env python3
"""
Update China's monthly crude-oil import volume from the JODI-Oil World Database.

Output:
    docs/data/china_crude_import_volume.json

Source:
    JODI-Oil full Extended Primary CSV download.

The script does not estimate missing months. It accepts both common long-form
and wide-form JODI CSV layouts and prefers thousand barrels per day when
multiple units are available for the same month.
"""

from __future__ import annotations

import calendar
import csv
import io
import json
import os
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


OUTPUT_PATH = Path("docs/data/china_crude_import_volume.json")

JODI_PRIMARY_ZIP_URL = os.getenv(
    "JODI_PRIMARY_ZIP_URL",
    "https://www.jodidata.org/_resources/files/downloads/oil-data/"
    "world_primary_csv.zip?iid=24",
)

START_PERIOD = os.getenv("CHINA_IMPORT_START_PERIOD", "2023-01")
BARRELS_PER_METRIC_TONNE = float(
    os.getenv("CRUDE_BARRELS_PER_METRIC_TONNE", "7.33")
)
REQUEST_TIMEOUT_SECONDS = 180

MIN_MILLION_TONNES = 20.0
MAX_MILLION_TONNES = 80.0
MIN_MBD = 4.0
MAX_MBD = 20.0


@dataclass(frozen=True)
class Observation:
    period: str
    import_million_tonnes: float
    import_mbd: float
    original_value: float
    original_unit: str
    assessment_code: str | None
    unit_priority: int


def normalise(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\ufeff", "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def compact(value: Any) -> str:
    return normalise(value).replace(" ", "")


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("\u00a0", "")
    if not text or text.lower() in {"na", "n/a", "null", "none", "..", "...", "-"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def parse_period(value: Any) -> str | None:
    raw = "" if value is None else str(value).strip()
    if not raw:
        return None

    patterns = (
        (r"^(\d{4})[-/](\d{1,2})$", False),
        (r"^(\d{4})(\d{2})$", False),
        (r"^(\d{1,2})[-/](\d{4})$", True),
    )
    for pattern, reversed_order in patterns:
        match = re.fullmatch(pattern, raw)
        if not match:
            continue
        first, second = int(match.group(1)), int(match.group(2))
        year, month = (second, first) if reversed_order else (first, second)
        if 1900 <= year <= 2100 and 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"

    cleaned = normalise(raw)
    year_match = re.search(r"\b(19|20)\d{2}\b", cleaned)
    if not year_match:
        return None
    year = int(year_match.group(0))

    months = {
        "jan": 1, "january": 1, "feb": 2, "february": 2,
        "mar": 3, "march": 3, "apr": 4, "april": 4,
        "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "sept": 9,
        "september": 9, "oct": 10, "october": 10,
        "nov": 11, "november": 11, "dec": 12, "december": 12,
    }
    for name, month in months.items():
        if re.search(rf"\b{re.escape(name)}\b", cleaned):
            return f"{year:04d}-{month:02d}"
    return None


def split_period(period: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})-(\d{2})", period)
    if not match:
        raise ValueError(f"Invalid period: {period}")
    year, month = int(match.group(1)), int(match.group(2))
    if not 1 <= month <= 12:
        raise ValueError(f"Invalid period: {period}")
    return year, month


def shift_period(period: str, months: int) -> str:
    year, month = split_period(period)
    index = year * 12 + month - 1 + months
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def previous_complete_month() -> str:
    now = datetime.now(timezone.utc)
    if now.month == 1:
        return f"{now.year - 1:04d}-12"
    return f"{now.year:04d}-{now.month - 1:02d}"


def month_range(start: str, end: str) -> list[str]:
    periods: list[str] = []
    current = start
    while current <= end:
        periods.append(current)
        current = shift_period(current, 1)
    return periods


def days_in_month(period: str) -> int:
    year, month = split_period(period)
    return calendar.monthrange(year, month)[1]


def download_zip() -> bytes:
    request = Request(
        JODI_PRIMARY_ZIP_URL,
        headers={
            "User-Agent": "Mozilla/5.0 energy-data-jodi-updater/1.0",
            "Accept": "application/zip,application/octet-stream,*/*",
        },
    )
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            content = response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(
            f"JODI download failed with HTTP {exc.code}: {detail}"
        ) from exc
    except (URLError, TimeoutError) as exc:
        raise RuntimeError(f"JODI download failed: {exc}") from exc

    if not content.startswith(b"PK"):
        preview = content[:300].decode("utf-8", errors="replace")
        raise RuntimeError(
            "JODI response is not a ZIP archive. Response preview: " + preview
        )
    return content


def extract_csv(zip_bytes: bytes) -> tuple[str, str]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        csv_names = [
            name for name in archive.namelist()
            if name.lower().endswith(".csv") and not name.endswith("/")
        ]
        if not csv_names:
            raise RuntimeError("The JODI ZIP archive contains no CSV file.")

        preferred = [
            name for name in csv_names
            if "primary" in name.lower() and "secondary" not in name.lower()
        ]
        selected = preferred[0] if preferred else csv_names[0]
        raw = archive.read(selected)

    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return selected, raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return selected, raw.decode("utf-8", errors="replace")


def detect_delimiter(text: str) -> str:
    sample = text[:20000]
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        counts = {delimiter: sample.count(delimiter) for delimiter in ",;\t|"}
        return max(counts, key=counts.get)


ALIASES: dict[str, set[str]] = {
    "country": {"country", "countryname", "economy", "economyname", "reporter"},
    "product": {"product", "productname", "commodity", "commodityname"},
    "flow": {"flow", "flowname", "transaction", "activity"},
    "unit": {"unit", "unitname", "measure", "unitofmeasure"},
    "time": {"time", "timeperiod", "period", "date", "month"},
    "value": {"value", "datavalue", "obsvalue", "observationvalue", "amount"},
    "assessment": {
        "assessment", "assessmentcode", "colourcode", "colorcode",
        "qualitycode", "flag",
    },
}


def identify_column(fieldnames: Iterable[str], logical_name: str) -> str | None:
    lookup = {compact(name): name for name in fieldnames}
    for alias in ALIASES[logical_name]:
        found = lookup.get(compact(alias))
        if found:
            return found
    return None


def is_china(value: Any) -> bool:
    """
    Accept both the JODI SDMX economy code and readable country names.

    In the current JODI long-form CSV, China is stored as REF_AREA=CN.
    """
    return compact(value) in {
        "cn",
        "chn",
        "china",
        "chinamainland",
        "peoplesrepublicofchina",
        "prchina",
    }


def is_crude(value: Any) -> bool:
    """
    Accept both the JODI energy-product code and readable labels.

    In the current JODI long-form CSV, crude oil is ENERGY_PRODUCT=CRUDEOIL.
    """
    return compact(value) in {
        "crude",
        "crudeoil",
    }


def is_import(value: Any) -> bool:
    """
    Accept both the JODI flow code and readable labels.

    In the current JODI long-form CSV, imports are FLOW_BREAKDOWN=TOTIMPSB.
    """
    return compact(value) in {
        "import",
        "imports",
        "totimpsb",
    }


def classify_unit(value: Any) -> str | None:
    text = normalise(value)
    packed = compact(value)

    if packed in {"kbd", "kbpd"} or (
        "thousand" in text and "barrel" in text and "day" in text
    ):
        return "kbd"

    if packed in {"kmt", "kt", "kton", "ktons"} or (
        "thousand" in text
        and ("metric ton" in text or "metric tonne" in text)
    ):
        return "kmt"

    if packed in {"kbbl", "kbbls"} or (
        "thousand" in text and "barrel" in text and "day" not in text
    ):
        return "kbbl"

    return None


def convert(
    period: str,
    value: float,
    unit: str,
    original_unit: str,
    assessment_code: str | None,
) -> Observation | None:
    if unit == "kbd":
        mbd = value / 1000.0
        million_tonnes = mbd * days_in_month(period) / BARRELS_PER_METRIC_TONNE
        priority = 3
    elif unit == "kmt":
        million_tonnes = value / 1000.0
        mbd = million_tonnes * BARRELS_PER_METRIC_TONNE / days_in_month(period)
        priority = 2
    elif unit == "kbbl":
        mbd = value / days_in_month(period) / 1000.0
        million_tonnes = value / 1000.0 / BARRELS_PER_METRIC_TONNE
        priority = 1
    else:
        return None

    if not MIN_MILLION_TONNES <= million_tonnes <= MAX_MILLION_TONNES:
        return None
    if not MIN_MBD <= mbd <= MAX_MBD:
        return None

    return Observation(
        period=period,
        import_million_tonnes=round(million_tonnes, 3),
        import_mbd=round(mbd, 3),
        original_value=value,
        original_unit=original_unit,
        assessment_code=assessment_code or None,
        unit_priority=priority,
    )


def add_observation(
    observations: dict[str, Observation],
    observation: Observation | None,
) -> None:
    if observation is None or observation.period < START_PERIOD:
        return
    existing = observations.get(observation.period)
    if existing is None or observation.unit_priority > existing.unit_priority:
        observations[observation.period] = observation


def parse_long_form(
    rows: list[dict[str, str]],
    fieldnames: list[str],
) -> dict[str, Observation]:
    country_col = identify_column(fieldnames, "country")
    product_col = identify_column(fieldnames, "product")
    flow_col = identify_column(fieldnames, "flow")
    unit_col = identify_column(fieldnames, "unit")
    time_col = identify_column(fieldnames, "time")
    value_col = identify_column(fieldnames, "value")
    assessment_col = identify_column(fieldnames, "assessment")

    required = {
        "country": country_col,
        "product": product_col,
        "flow": flow_col,
        "unit": unit_col,
        "time": time_col,
        "value": value_col,
    }
    missing = [name for name, column in required.items() if column is None]
    if missing:
        return {}

    observations: dict[str, Observation] = {}
    for row in rows:
        if not is_china(row.get(country_col, "")):
            continue
        if not is_crude(row.get(product_col, "")):
            continue
        if not is_import(row.get(flow_col, "")):
            continue

        period = parse_period(row.get(time_col, ""))
        value = safe_float(row.get(value_col, ""))
        original_unit = row.get(unit_col, "")
        unit = classify_unit(original_unit)

        if period is None or value is None or unit is None:
            continue

        assessment = row.get(assessment_col, "") if assessment_col else ""
        add_observation(
            observations,
            convert(period, value, unit, original_unit, assessment),
        )
    return observations


def month_columns(fieldnames: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for name in fieldnames:
        period = parse_period(name)
        if period:
            result[name] = period
    return result


def parse_wide_form(
    rows: list[dict[str, str]],
    fieldnames: list[str],
) -> dict[str, Observation]:
    country_col = identify_column(fieldnames, "country")
    product_col = identify_column(fieldnames, "product")
    flow_col = identify_column(fieldnames, "flow")
    unit_col = identify_column(fieldnames, "unit")
    assessment_col = identify_column(fieldnames, "assessment")
    periods = month_columns(fieldnames)

    if not all((country_col, product_col, flow_col, unit_col)) or not periods:
        return {}

    observations: dict[str, Observation] = {}
    for row in rows:
        if not is_china(row.get(country_col, "")):
            continue
        if not is_crude(row.get(product_col, "")):
            continue
        if not is_import(row.get(flow_col, "")):
            continue

        original_unit = row.get(unit_col, "")
        unit = classify_unit(original_unit)
        if unit is None:
            continue

        assessment = row.get(assessment_col, "") if assessment_col else ""
        for column, period in periods.items():
            value = safe_float(row.get(column, ""))
            if value is None:
                continue
            add_observation(
                observations,
                convert(period, value, unit, original_unit, assessment),
            )
    return observations


def parse_jodi_csv(text: str) -> tuple[dict[str, Observation], list[str]]:
    delimiter = detect_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    if not reader.fieldnames:
        raise RuntimeError("The JODI CSV has no header row.")

    fieldnames = [str(name).strip() for name in reader.fieldnames]
    rows = list(reader)

    observations = parse_long_form(rows, fieldnames)
    layout = "long"

    if not observations:
        observations = parse_wide_form(rows, fieldnames)
        layout = "wide"

    if not observations:
        raise RuntimeError(
            "No valid China / Crude Oil / Imports observations were found. "
            f"CSV columns: {fieldnames}"
        )

    return observations, [layout, delimiter, *fieldnames]


def percentage_change(current: float, previous: float | None) -> float | None:
    if previous is None or previous == 0:
        return None
    return round((current / previous - 1.0) * 100.0, 2)


def rolling_average(
    values: dict[str, float],
    period: str,
    months: int,
) -> float | None:
    selected: list[float] = []
    for offset in range(-(months - 1), 1):
        value = values.get(shift_period(period, offset))
        if value is None:
            return None
        selected.append(value)
    return round(sum(selected) / len(selected), 3)


def build_series(
    observations: dict[str, Observation],
    requested_end: str,
) -> list[dict[str, Any]]:
    filtered = {
        period: observation
        for period, observation in observations.items()
        if START_PERIOD <= period <= requested_end
    }

    tonnes = {
        period: observation.import_million_tonnes
        for period, observation in filtered.items()
    }
    mbd = {
        period: observation.import_mbd
        for period, observation in filtered.items()
    }

    series: list[dict[str, Any]] = []
    for period in sorted(filtered):
        observation = filtered[period]
        series.append(
            {
                "period": period,
                "import_million_tonnes": observation.import_million_tonnes,
                "import_mbd": observation.import_mbd,
                "status": "reported",
                "source": "JODI-Oil World Database",
                "country": "China",
                "product": "Crude Oil",
                "flow": "Imports",
                "month_on_month_percent": percentage_change(
                    observation.import_million_tonnes,
                    tonnes.get(shift_period(period, -1)),
                ),
                "year_on_year_percent": percentage_change(
                    observation.import_million_tonnes,
                    tonnes.get(shift_period(period, -12)),
                ),
                "rolling_3m_average_mbd": rolling_average(mbd, period, 3),
                "rolling_12m_average_mbd": rolling_average(mbd, period, 12),
                "original_value": observation.original_value,
                "original_unit": observation.original_unit,
                "assessment_code": observation.assessment_code,
            }
        )
    return series


def trend_summary(series: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [
        row for row in series
        if row.get("rolling_3m_average_mbd") is not None
    ]
    if len(valid) < 2:
        return {
            "direction": "unavailable",
            "change_mbd": None,
            "note": "Three-month moving-average trend is unavailable.",
        }

    latest, previous = valid[-1], valid[-2]
    change = round(
        latest["rolling_3m_average_mbd"]
        - previous["rolling_3m_average_mbd"],
        3,
    )
    if change >= 0.15:
        direction = "strengthening"
    elif change <= -0.15:
        direction = "weakening"
    else:
        direction = "stable"

    return {
        "direction": direction,
        "change_mbd": change,
        "latest_trend_period": latest["period"],
        "note": (
            "Direction is based on the month-to-month change in the "
            "three-month moving average."
        ),
    }


def validate(series: list[dict[str, Any]]) -> None:
    if not series:
        raise RuntimeError("The generated series is empty.")

    periods = [row["period"] for row in series]
    if periods != sorted(set(periods)):
        raise RuntimeError("Periods are duplicated or not sorted.")

    for row in series:
        tonnes = row["import_million_tonnes"]
        mbd = row["import_mbd"]
        if not MIN_MILLION_TONNES <= tonnes <= MAX_MILLION_TONNES:
            raise RuntimeError(
                f"Implausible import volume for {row['period']}: {tonnes} Mt"
            )
        if not MIN_MBD <= mbd <= MAX_MBD:
            raise RuntimeError(
                f"Implausible import rate for {row['period']}: {mbd} mb/d"
            )


def main() -> None:
    requested_end = previous_complete_month()
    print(f"Downloading JODI primary dataset: {JODI_PRIMARY_ZIP_URL}")

    zip_bytes = download_zip()
    csv_name, csv_text = extract_csv(zip_bytes)
    observations, parser_info = parse_jodi_csv(csv_text)
    series = build_series(observations, requested_end)
    validate(series)

    requested_periods = month_range(START_PERIOD, requested_end)
    available_periods = [row["period"] for row in series]
    available_set = set(available_periods)
    missing_periods = [
        period for period in requested_periods if period not in available_set
    ]

    latest = series[-1]
    payload = {
        "metadata": {
            "title": "China monthly crude oil import volume",
            "description": (
                "Monthly physical crude-oil import volume reported for China."
            ),
            "frequency": "monthly",
            "country": "China",
            "product": "Crude Oil",
            "flow": "Imports",
            "primary_source": "JODI-Oil World Database",
            "source_url": JODI_PRIMARY_ZIP_URL,
            "source_file": csv_name,
            "conversion_note": (
                f"One metric tonne of crude oil is approximated as "
                f"{BARRELS_PER_METRIC_TONNE} barrels."
            ),
            "data_policy": (
                "Only reported JODI observations are published. "
                "Missing months are not estimated."
            ),
            "start_period": START_PERIOD,
            "requested_end_period": requested_end,
            "latest_available_period": latest["period"],
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "generator_version": "4.0.1-jodi",
            "parser_layout": parser_info[0],
        },
        "availability": {
            "requested_months": len(requested_periods),
            "available_months": len(series),
            "missing_months": missing_periods,
            "latest_requested_period": requested_end,
            "latest_available_period": latest["period"],
        },
        "summary": {
            "latest": latest,
            "trend": trend_summary(series),
            "interpretation_note": (
                "Import volume is a physical market indicator, but it is not "
                "by itself a complete measure of Chinese oil demand."
            ),
        },
        "series": series,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = OUTPUT_PATH.with_suffix(".json.tmp")
    temporary_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(OUTPUT_PATH)

    print("JSON generation successful.")
    print(f"Output: {OUTPUT_PATH}")
    print(f"Records: {len(series)}")
    print(f"Latest period: {latest['period']}")
    print(
        "Latest import:",
        latest["import_million_tonnes"],
        "million tonnes",
    )
    print("Latest mb/d:", latest["import_mbd"])
    print(f"Missing requested months: {len(missing_periods)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise


