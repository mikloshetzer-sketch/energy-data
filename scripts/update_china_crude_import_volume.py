#!/usr/bin/env python3
"""
China crude-oil import volume updater.

Primary automated source:
- UN Comtrade public API
- Reporter: China (M49 156)
- Flow: imports
- Commodity: HS 2709, crude petroleum oils
- Frequency: monthly

Optional local override:
- data/china_crude_import_monthly.csv

The local CSV takes precedence over API records. Use it for newer China Customs
figures that have not yet appeared in UN Comtrade, or for correcting a record.
Never enter cumulative year-to-date data as if it were a single month.
"""

from __future__ import annotations

import calendar
import csv
import json
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


OUTPUT_FILE = Path("docs/data/china_crude_import_volume.json")
OVERRIDE_FILE = Path("data/china_crude_import_monthly.csv")

REPORTER_CODE = "156"       # China
PARTNER_CODE = "0"          # World
FLOW_CODE = "M"             # Imports
COMMODITY_CODE = "2709"     # Crude petroleum oils
BARRELS_PER_METRIC_TONNE = 7.33

START_PERIOD = os.getenv("CHINA_IMPORT_START_PERIOD", "2023-01")
API_TIMEOUT_SECONDS = 90
API_RETRIES = 3
REQUEST_DELAY_SECONDS = 1.0

API_ENDPOINTS = (
    "https://comtradeapi.un.org/public/v1/preview/C/M/HS",
    "https://comtradeapi.un.org/public/v1/preview/C/M/HS",
)


@dataclass
class MonthlyRecord:
    period: str
    import_million_tonnes: float
    import_mbd: float
    trade_value_billion_usd: float | None
    status: str
    source: str
    source_url: str | None
    source_date: str | None
    note: str | None
    month_on_month_percent: float | None = None
    year_on_year_percent: float | None = None
    rolling_3m_average_mbd: float | None = None
    rolling_12m_average_mbd: float | None = None


def month_range(start_period: str, end_period: str) -> list[str]:
    start_year, start_month = map(int, start_period.split("-"))
    end_year, end_month = map(int, end_period.split("-"))

    periods: list[str] = []
    year, month = start_year, start_month

    while (year, month) <= (end_year, end_month):
        periods.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1

    return periods


def previous_complete_month() -> str:
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month - 1

    if month == 0:
        year -= 1
        month = 12

    return f"{year:04d}-{month:02d}"


def period_to_api(period: str) -> str:
    return period.replace("-", "")


def days_in_month(period: str) -> int:
    year, month = map(int, period.split("-"))
    return calendar.monthrange(year, month)[1]


def million_tonnes_to_mbd(period: str, million_tonnes: float) -> float:
    million_barrels = million_tonnes * BARRELS_PER_METRIC_TONNE
    return million_barrels / days_in_month(period)


def safe_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_json(url: str) -> dict[str, Any]:
    api_key = os.getenv("UN_COMTRADE_API_KEY", "").strip()
    headers = {
        "User-Agent": "energy-data-monitor/1.0",
        "Accept": "application/json",
        "Connection": "close",
    }

    if api_key:
        headers["Ocp-Apim-Subscription-Key"] = api_key

    last_error: Exception | None = None

    for attempt in range(1, API_RETRIES + 1):
        try:
            request = Request(url, headers=headers)
            with urlopen(request, timeout=API_TIMEOUT_SECONDS) as response:
                payload = response.read().decode("utf-8", errors="replace")
                return json.loads(payload)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
            print(f"API attempt {attempt} failed: {error}")
            time.sleep(5 * attempt)

    raise RuntimeError(f"UN Comtrade request failed: {last_error}")


def build_api_url(endpoint: str, periods: list[str]) -> str:
    params = {
        "period": ",".join(period_to_api(period) for period in periods),
        "reporterCode": REPORTER_CODE,
        "cmdCode": COMMODITY_CODE,
        "flowCode": FLOW_CODE,
        "partnerCode": PARTNER_CODE,
        "partner2Code": "0",
        "customsCode": "C00",
        "motCode": "0",
        "maxRecords": "500",
        "aggregateBy": "6",
        "breakdownMode": "classic",
        "includeDesc": "true",
    }
    return f"{endpoint}?{urlencode(params)}"


def extract_period(row: dict[str, Any]) -> str | None:
    raw_period = str(row.get("period") or "").strip()

    if len(raw_period) == 6 and raw_period.isdigit():
        return f"{raw_period[:4]}-{raw_period[4:]}"

    year = row.get("refYear")
    month = row.get("refMonth")

    try:
        return f"{int(year):04d}-{int(month):02d}"
    except (TypeError, ValueError):
        return None


def extract_million_tonnes(row: dict[str, Any]) -> float | None:
    # UN Comtrade netWgt is expressed in kilograms.
    net_weight_kg = safe_float(row.get("netWgt"))
    if net_weight_kg is not None and net_weight_kg > 0:
        return net_weight_kg / 1_000_000_000

    # Fallback only when the API explicitly reports kilograms.
    quantity = safe_float(row.get("qty"))
    unit = str(
        row.get("qtyUnitAbbr")
        or row.get("qtyUnitCode")
        or row.get("altQtyUnitAbbr")
        or ""
    ).lower()

    if quantity is not None and quantity > 0 and ("kg" in unit or unit == "8"):
        return quantity / 1_000_000_000

    return None


def parse_api_rows(payload: dict[str, Any]) -> dict[str, MonthlyRecord]:
    records: dict[str, MonthlyRecord] = {}
    rows = payload.get("data")

    if not isinstance(rows, list):
        return records

    for row in rows:
        if not isinstance(row, dict):
            continue

        period = extract_period(row)
        million_tonnes = extract_million_tonnes(row)

        if not period or million_tonnes is None:
            continue

        trade_value_usd = safe_float(
            row.get("primaryValue")
            or row.get("tradeValue")
            or row.get("TradeValue")
        )

        records[period] = MonthlyRecord(
            period=period,
            import_million_tonnes=round(million_tonnes, 3),
            import_mbd=round(
                million_tonnes_to_mbd(period, million_tonnes),
                3,
            ),
            trade_value_billion_usd=(
                round(trade_value_usd / 1_000_000_000, 3)
                if trade_value_usd is not None
                else None
            ),
            status="reported",
            source="UN Comtrade, reporter data submitted by China",
            source_url="https://comtradeplus.un.org/",
            source_date=None,
            note="HS 2709 monthly gross imports from the world.",
        )

    return records


def fetch_comtrade_records(periods: list[str]) -> dict[str, MonthlyRecord]:
    all_records: dict[str, MonthlyRecord] = {}

    # Small batches reduce URL length and make failures easier to isolate.
    batches = [periods[index:index + 12] for index in range(0, len(periods), 12)]

    for batch in batches:
        batch_success = False

        for endpoint in API_ENDPOINTS:
            url = build_api_url(endpoint, batch)
            print(f"Downloading UN Comtrade: {batch[0]} to {batch[-1]}")

            try:
                payload = fetch_json(url)
                parsed = parse_api_rows(payload)
            except RuntimeError as error:
                print(error)
                continue

            if parsed:
                all_records.update(parsed)
                batch_success = True
                break

        if not batch_success:
            print(
                f"Warning: no usable UN Comtrade data for "
                f"{batch[0]} to {batch[-1]}."
            )

        time.sleep(REQUEST_DELAY_SECONDS)

    return all_records


def load_local_overrides(path: Path) -> dict[str, MonthlyRecord]:
    records: dict[str, MonthlyRecord] = {}

    if not path.exists():
        print(f"Local override file not found: {path}")
        return records

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        required = {
            "period",
            "import_million_tonnes",
            "status",
            "source",
        }
        missing = required - set(reader.fieldnames or [])

        if missing:
            raise RuntimeError(
                f"Missing CSV columns in {path}: {sorted(missing)}"
            )

        for row_number, row in enumerate(reader, start=2):
            period = (row.get("period") or "").strip()
            tonnes = safe_float(row.get("import_million_tonnes"))
            status = (row.get("status") or "").strip().lower()

            if not period and tonnes is None:
                continue

            if len(period) != 7 or period[4] != "-":
                raise RuntimeError(
                    f"Invalid period at CSV row {row_number}: {period}"
                )

            if tonnes is None or tonnes <= 0:
                raise RuntimeError(
                    f"Invalid import volume at CSV row {row_number}"
                )

            if status not in {"reported", "provisional", "estimated"}:
                raise RuntimeError(
                    f"Invalid status at CSV row {row_number}: {status}"
                )

            trade_value = safe_float(
                row.get("trade_value_billion_usd")
            )

            records[period] = MonthlyRecord(
                period=period,
                import_million_tonnes=round(tonnes, 3),
                import_mbd=round(
                    million_tonnes_to_mbd(period, tonnes),
                    3,
                ),
                trade_value_billion_usd=(
                    round(trade_value, 3)
                    if trade_value is not None
                    else None
                ),
                status=status,
                source=(row.get("source") or "").strip(),
                source_url=(row.get("source_url") or "").strip() or None,
                source_date=(row.get("source_date") or "").strip() or None,
                note=(row.get("note") or "").strip() or None,
            )

    return records


def percentage_change(current: float, previous: float | None) -> float | None:
    if previous is None or previous == 0:
        return None
    return round(((current / previous) - 1) * 100, 2)


def period_shift(period: str, months: int) -> str:
    year, month = map(int, period.split("-"))
    index = year * 12 + (month - 1) + months
    shifted_year, shifted_month_index = divmod(index, 12)
    return f"{shifted_year:04d}-{shifted_month_index + 1:02d}"


def rolling_average(
    records_by_period: dict[str, MonthlyRecord],
    period: str,
    window: int,
) -> float | None:
    values: list[float] = []

    for offset in range(-(window - 1), 1):
        target = period_shift(period, offset)
        record = records_by_period.get(target)

        if record is None:
            return None

        values.append(record.import_mbd)

    return round(sum(values) / len(values), 3)


def enrich_records(records: dict[str, MonthlyRecord]) -> list[MonthlyRecord]:
    ordered = [records[key] for key in sorted(records)]

    for record in ordered:
        previous_month = records.get(period_shift(record.period, -1))
        previous_year = records.get(period_shift(record.period, -12))

        record.month_on_month_percent = percentage_change(
            record.import_million_tonnes,
            (
                previous_month.import_million_tonnes
                if previous_month
                else None
            ),
        )

        record.year_on_year_percent = percentage_change(
            record.import_million_tonnes,
            (
                previous_year.import_million_tonnes
                if previous_year
                else None
            ),
        )

        record.rolling_3m_average_mbd = rolling_average(
            records,
            record.period,
            3,
        )

        record.rolling_12m_average_mbd = rolling_average(
            records,
            record.period,
            12,
        )

    return ordered


def build_summary(series: list[MonthlyRecord]) -> dict[str, Any]:
    if not series:
        return {
            "latest": None,
            "trend": "unavailable",
            "trend_note": "No monthly import records are available.",
        }

    latest = series[-1]
    trend = "neutral"

    if latest.rolling_3m_average_mbd is not None:
        prior_period = period_shift(latest.period, -1)
        prior_record = next(
            (record for record in series if record.period == prior_period),
            None,
        )

        if (
            prior_record is not None
            and prior_record.rolling_3m_average_mbd is not None
        ):
            delta = (
                latest.rolling_3m_average_mbd
                - prior_record.rolling_3m_average_mbd
            )

            if delta >= 0.15:
                trend = "strengthening"
            elif delta <= -0.15:
                trend = "weakening"

    return {
        "latest": asdict(latest),
        "trend": trend,
        "trend_note": (
            "Trend is based on the direction of the three-month "
            "moving average. It is an import-volume signal, not a "
            "complete measure of Chinese oil demand."
        ),
    }


def validate_series(series: list[MonthlyRecord]) -> None:
    if not series:
        raise RuntimeError(
            "No China crude-import records were generated. "
            "Check UN Comtrade availability or add verified rows to "
            f"{OVERRIDE_FILE}."
        )

    periods = [record.period for record in series]

    if periods != sorted(set(periods)):
        raise RuntimeError("Periods are duplicated or not sorted.")

    for record in series:
        if record.import_million_tonnes <= 0:
            raise RuntimeError(
                f"Invalid import volume for {record.period}"
            )
        if record.import_mbd <= 0:
            raise RuntimeError(
                f"Invalid mb/d conversion for {record.period}"
            )


def main() -> None:
    end_period = previous_complete_month()
    periods = month_range(START_PERIOD, end_period)

    api_records = fetch_comtrade_records(periods)
    override_records = load_local_overrides(OVERRIDE_FILE)

    # Verified local rows override API rows for the same month.
    merged_records = {**api_records, **override_records}
    series = enrich_records(merged_records)
    validate_series(series)

    latest_period = series[-1].period

    output = {
        "metadata": {
            "title": "China monthly crude oil import volume",
            "description": (
                "Monthly gross crude-oil imports reported by China, "
                "converted from metric tonnes to million barrels per day."
            ),
            "dataset_scope": "Crude petroleum oils, HS 2709",
            "frequency": "monthly",
            "reporter": "China",
            "partner": "World",
            "flow": "imports",
            "primary_source": "UN Comtrade",
            "primary_source_url": "https://comtradeplus.un.org/",
            "local_override_file": str(OVERRIDE_FILE),
            "conversion_note": (
                "One metric tonne of crude oil is approximated as "
                f"{BARRELS_PER_METRIC_TONNE} barrels. The exact conversion "
                "depends on crude grade."
            ),
            "method_note": (
                "The local CSV takes precedence over API data. "
                "Cumulative year-to-date figures must not be entered as "
                "single-month observations."
            ),
            "start_period": START_PERIOD,
            "latest_period": latest_period,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "generator_version": "1.0.0",
        },
        "summary": build_summary(series),
        "series": [asdict(record) for record in series],
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        json.dump(output, file, ensure_ascii=False, indent=2)

    print(f"Created: {OUTPUT_FILE}")
    print(f"Records: {len(series)}")
    print(f"Latest period: {latest_period}")
    print(f"API records: {len(api_records)}")
    print(f"Local override records: {len(override_records)}")


if __name__ == "__main__":
    main()
