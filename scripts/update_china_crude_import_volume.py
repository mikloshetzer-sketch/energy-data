#!/usr/bin/env python3

"""
China Crude Import Volume Monitor

Forrás:
UN Comtrade

Lekérdezés:
- reporter: China (156)
- partner: World (0)
- flow: Imports (M)
- commodity: HS 2709 – Crude petroleum oils
- frequency: Monthly

Kimenet:
docs/data/china_crude_import_volume.json

A script:
- kizárólag a World összesített rekordot használja;
- nem becsül hiányzó hónapokat;
- millió tonnára és millió hordó/napra konvertál;
- MoM és YoY változást számít;
- 3 és 12 havi mozgóátlagot készít;
- kiszűri a nyilvánvalóan hibás részadatokat.
"""

from __future__ import annotations

import calendar
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


OUTPUT_FILE = Path(
    "docs/data/china_crude_import_volume.json"
)

API_URL = (
    "https://comtradeapi.un.org/"
    "public/v1/preview/C/M/HS"
)

REPORTER_CODE = "156"
PARTNER_CODE = "0"
PARTNER2_CODE = "0"
FLOW_CODE = "M"
COMMODITY_CODE = "2709"

START_PERIOD = os.getenv(
    "CHINA_IMPORT_START_PERIOD",
    "2023-01",
)

BARRELS_PER_METRIC_TONNE = 7.33

TIMEOUT_SECONDS = 90
MAX_RETRIES = 3
REQUEST_DELAY_SECONDS = 1.2

# Kína havi nyersolajimportja általában több tízmillió tonna.
# Ez a tartomány megakadályozza, hogy egy partnerország
# részadata vagy más hibás rekord kerüljön publikálásra.
PLAUSIBLE_MIN_MILLION_TONNES = 20.0
PLAUSIBLE_MAX_MILLION_TONNES = 80.0


def previous_complete_month() -> str:
    now = datetime.now(timezone.utc)

    year = now.year
    month = now.month - 1

    if month == 0:
        year -= 1
        month = 12

    return f"{year:04d}-{month:02d}"


def parse_period(
    period: str,
) -> tuple[int, int]:

    try:
        year_text, month_text = period.split("-")

        year = int(year_text)
        month = int(month_text)

    except (
        ValueError,
        AttributeError,
    ) as error:

        raise ValueError(
            f"Invalid period: {period}. "
            "Expected format: YYYY-MM."
        ) from error

    if not 1 <= month <= 12:
        raise ValueError(
            f"Invalid month in period: {period}"
        )

    return year, month


def month_range(
    start_period: str,
    end_period: str,
) -> list[str]:

    start_year, start_month = parse_period(
        start_period
    )

    end_year, end_month = parse_period(
        end_period
    )

    if (
        start_year,
        start_month,
    ) > (
        end_year,
        end_month,
    ):
        raise ValueError(
            "Start period cannot be later "
            "than end period."
        )

    periods: list[str] = []

    year = start_year
    month = start_month

    while (
        year,
        month,
    ) <= (
        end_year,
        end_month,
    ):

        periods.append(
            f"{year:04d}-{month:02d}"
        )

        month += 1

        if month == 13:
            month = 1
            year += 1

    return periods


def shift_period(
    period: str,
    months: int,
) -> str:

    year, month = parse_period(period)

    month_index = (
        year * 12
        + month
        - 1
        + months
    )

    shifted_year = month_index // 12
    shifted_month = month_index % 12 + 1

    return (
        f"{shifted_year:04d}-"
        f"{shifted_month:02d}"
    )


def days_in_month(
    period: str,
) -> int:

    year, month = parse_period(period)

    return calendar.monthrange(
        year,
        month,
    )[1]


def convert_to_mbd(
    period: str,
    million_tonnes: float,
) -> float:

    million_barrels = (
        million_tonnes
        * BARRELS_PER_METRIC_TONNE
    )

    return (
        million_barrels
        / days_in_month(period)
    )


def safe_float(
    value: Any,
) -> float | None:

    if value in (
        None,
        "",
        "null",
    ):
        return None

    try:
        return float(value)

    except (
        TypeError,
        ValueError,
    ):
        return None


def build_api_url(
    period: str,
) -> str:

    params = {
        "period": period.replace("-", ""),
        "reporterCode": REPORTER_CODE,
        "cmdCode": COMMODITY_CODE,
        "flowCode": FLOW_CODE,
        "partnerCode": PARTNER_CODE,
        "partner2Code": PARTNER2_CODE,
        "customsCode": "C00",
        "motCode": "0",
        "maxRecords": "500",
        "format": "json",
        "breakdownMode": "classic",
        "includeDesc": "true",
    }

    return (
        f"{API_URL}?"
        f"{urlencode(params)}"
    )


def fetch_json(
    url: str,
) -> dict[str, Any]:

    headers = {
        "User-Agent": (
            "energy-data-monitor/3.0"
        ),
        "Accept": "application/json",
        "Connection": "close",
    }

    api_key = os.getenv(
        "UN_COMTRADE_API_KEY",
        "",
    ).strip()

    if api_key:
        headers[
            "Ocp-Apim-Subscription-Key"
        ] = api_key

    last_error: Exception | None = None

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:
            print(
                "UN Comtrade request "
                f"attempt {attempt}"
            )

            request = Request(
                url,
                headers=headers,
            )

            with urlopen(
                request,
                timeout=TIMEOUT_SECONDS,
            ) as response:

                text = response.read().decode(
                    "utf-8",
                    errors="replace",
                )

            payload = json.loads(text)

            if not isinstance(payload, dict):
                raise RuntimeError(
                    "UN Comtrade returned an "
                    "unexpected response format."
                )

            return payload

        except HTTPError as error:

            response_body = ""

            try:
                response_body = (
                    error.read().decode(
                        "utf-8",
                        errors="replace",
                    )
                )

            except Exception:
                pass

            print(
                f"HTTP error {error.code}: "
                f"{error.reason}"
            )

            if response_body:
                print(
                    "UN Comtrade response: "
                    f"{response_body[:1000]}"
                )

            if 400 <= error.code < 500:
                raise RuntimeError(
                    "UN Comtrade rejected the "
                    f"request. HTTP {error.code}. "
                    f"Response: "
                    f"{response_body[:500]}"
                ) from error

            last_error = error

        except (
            URLError,
            TimeoutError,
        ) as error:

            last_error = error

            print(
                "Temporary request failure: "
                f"{error}"
            )

        except json.JSONDecodeError as error:

            raise RuntimeError(
                "UN Comtrade returned "
                "invalid JSON."
            ) from error

        if attempt < MAX_RETRIES:
            time.sleep(
                attempt * 5
            )

    raise RuntimeError(
        "UN Comtrade request failed. "
        f"Last error: {last_error}"
    )


def extract_period(
    row: dict[str, Any],
) -> str | None:

    raw_period = str(
        row.get("period")
        or ""
    ).strip()

    if (
        len(raw_period) == 6
        and raw_period.isdigit()
    ):
        return (
            f"{raw_period[:4]}-"
            f"{raw_period[4:]}"
        )

    try:
        year = int(
            row["refYear"]
        )

        month = int(
            row["refMonth"]
        )

        return (
            f"{year:04d}-"
            f"{month:02d}"
        )

    except (
        KeyError,
        TypeError,
        ValueError,
    ):
        return None


def extract_million_tonnes(
    row: dict[str, Any],
) -> float | None:

    # A netWgt mező mértékegysége kilogramm.
    net_weight_kg = safe_float(
        row.get("netWgt")
    )

    if (
        net_weight_kg is not None
        and net_weight_kg > 0
    ):
        return (
            net_weight_kg
            / 1_000_000_000
        )

    # Tartalék megoldás, ha a qty mező
    # kifejezetten kilogrammban szerepel.
    quantity = safe_float(
        row.get("qty")
    )

    quantity_unit = str(
        row.get("qtyUnitAbbr")
        or ""
    ).strip().lower()

    valid_kg_units = {
        "kg",
        "kilogram",
        "kilograms",
    }

    if (
        quantity is not None
        and quantity > 0
        and quantity_unit in valid_kg_units
    ):
        return (
            quantity
            / 1_000_000_000
        )

    return None


def extract_trade_value_billion_usd(
    row: dict[str, Any],
) -> float | None:

    value = safe_float(
        row.get("primaryValue")
        or row.get("tradeValue")
        or row.get("TradeValue")
    )

    if value is None:
        return None

    return (
        value
        / 1_000_000_000
    )


def is_world_partner(
    row: dict[str, Any],
) -> bool:

    partner_code = str(
        row.get("partnerCode")
        or row.get("partnerCodeM49")
        or ""
    ).strip()

    partner_description = str(
        row.get("partnerDesc")
        or row.get("partnerDescEng")
        or ""
    ).strip().lower()

    return (
        partner_code in {
            "0",
            "000",
        }
        or partner_description == "world"
    )


def is_correct_reporter(
    row: dict[str, Any],
) -> bool:

    reporter_code = str(
        row.get("reporterCode")
        or row.get("reporterCodeM49")
        or ""
    ).strip()

    reporter_description = str(
        row.get("reporterDesc")
        or row.get("reporterDescEng")
        or ""
    ).strip().lower()

    return (
        reporter_code in {
            "156",
        }
        or reporter_description == "china"
    )


def is_correct_flow(
    row: dict[str, Any],
) -> bool:

    flow_code = str(
        row.get("flowCode")
        or ""
    ).strip().upper()

    flow_description = str(
        row.get("flowDesc")
        or ""
    ).strip().lower()

    return (
        flow_code == FLOW_CODE
        or flow_description == "imports"
    )


def is_correct_commodity(
    row: dict[str, Any],
) -> bool:

    commodity_code = str(
        row.get("cmdCode")
        or row.get("commodityCode")
        or ""
    ).strip()

    return commodity_code == COMMODITY_CODE


def select_world_total_record(
    rows: list[dict[str, Any]],
    requested_period: str,
) -> dict[str, Any] | None:

    candidates: list[
        tuple[
            float,
            dict[str, Any],
        ]
    ] = []

    for row in rows:

        if not isinstance(row, dict):
            continue

        if extract_period(row) != requested_period:
            continue

        if not is_world_partner(row):
            continue

        if not is_correct_reporter(row):
            continue

        if not is_correct_flow(row):
            continue

        if not is_correct_commodity(row):
            continue

        million_tonnes = (
            extract_million_tonnes(row)
        )

        if million_tonnes is None:
            continue

        candidates.append(
            (
                million_tonnes,
                row,
            )
        )

    if not candidates:
        return None

    # Ha az API valamiért több összesített rekordot ad,
    # a legnagyobb nettó tömegű rekordot választjuk.
    candidates.sort(
        key=lambda item: item[0],
        reverse=True,
    )

    return candidates[0][1]


def parse_api_response(
    payload: dict[str, Any],
    requested_period: str,
) -> dict[str, Any] | None:

    rows = payload.get("data")

    if not isinstance(rows, list):
        return None

    row = select_world_total_record(
        rows,
        requested_period,
    )

    if row is None:
        return None

    million_tonnes = (
        extract_million_tonnes(row)
    )

    if million_tonnes is None:
        return None

    if not (
        PLAUSIBLE_MIN_MILLION_TONNES
        <= million_tonnes
        <= PLAUSIBLE_MAX_MILLION_TONNES
    ):
        raise RuntimeError(
            "Implausible China monthly crude "
            f"import value for {requested_period}: "
            f"{million_tonnes:.3f} million tonnes. "
            "The API probably returned a partial "
            "or incorrectly aggregated record."
        )

    trade_value = (
        extract_trade_value_billion_usd(
            row
        )
    )

    import_mbd = convert_to_mbd(
        requested_period,
        million_tonnes,
    )

    return {
        "period": requested_period,

        "import_million_tonnes": round(
            million_tonnes,
            3,
        ),

        "import_mbd": round(
            import_mbd,
            3,
        ),

        "trade_value_billion_usd": (
            round(
                trade_value,
                3,
            )
            if trade_value is not None
            else None
        ),

        "status": "reported",

        "source": (
            "UN Comtrade; reporter China; "
            "partner World"
        ),

        "reporter_code": REPORTER_CODE,
        "partner_code": PARTNER_CODE,
        "flow_code": FLOW_CODE,
        "commodity_code": COMMODITY_CODE,

        "commodity_description": (
            "Crude petroleum oils"
        ),
    }


def download_all_records(
    periods: list[str],
) -> dict[str, dict[str, Any]]:

    records: dict[
        str,
        dict[str, Any],
    ] = {}

    successful_requests = 0
    empty_requests = 0
    failed_requests = 0

    for index, period in enumerate(
        periods,
        start=1,
    ):

        print()
        print(
            f"Downloading period "
            f"{index}/{len(periods)}: "
            f"{period}"
        )

        try:
            url = build_api_url(period)

            payload = fetch_json(url)

            record = parse_api_response(
                payload,
                period,
            )

            if record is None:
                empty_requests += 1

                print(
                    "No valid World-total "
                    f"HS 2709 record for {period}."
                )

            else:
                records[period] = record

                successful_requests += 1

                print(
                    "Received World total: "
                    f"{record['import_million_tonnes']} "
                    "million tonnes; "
                    f"{record['import_mbd']} mb/d"
                )

        except RuntimeError as error:

            failed_requests += 1

            print(
                "Period skipped because "
                f"of error: {error}"
            )

        time.sleep(
            REQUEST_DELAY_SECONDS
        )

    print()
    print("UN Comtrade download summary")
    print("-----------------------------")
    print(
        f"Successful months: "
        f"{successful_requests}"
    )
    print(
        f"Empty months: "
        f"{empty_requests}"
    )
    print(
        f"Failed months: "
        f"{failed_requests}"
    )
    print(
        f"Generated records: "
        f"{len(records)}"
    )

    return records


def percentage_change(
    current: float,
    previous: float | None,
) -> float | None:

    if (
        previous is None
        or previous == 0
    ):
        return None

    return round(
        (
            current / previous
            - 1
        )
        * 100,
        2,
    )


def rolling_average(
    records: dict[str, dict[str, Any]],
    period: str,
    months: int,
) -> float | None:

    values: list[float] = []

    for offset in range(
        -(months - 1),
        1,
    ):

        target_period = shift_period(
            period,
            offset,
        )

        target_record = records.get(
            target_period
        )

        if target_record is None:
            return None

        values.append(
            target_record["import_mbd"]
        )

    return round(
        sum(values) / len(values),
        3,
    )


def enrich_records(
    records: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:

    series: list[
        dict[str, Any]
    ] = []

    for period in sorted(records):

        record = dict(
            records[period]
        )

        previous_month_record = records.get(
            shift_period(
                period,
                -1,
            )
        )

        previous_year_record = records.get(
            shift_period(
                period,
                -12,
            )
        )

        previous_month_value = (
            previous_month_record[
                "import_million_tonnes"
            ]
            if previous_month_record
            else None
        )

        previous_year_value = (
            previous_year_record[
                "import_million_tonnes"
            ]
            if previous_year_record
            else None
        )

        record[
            "month_on_month_percent"
        ] = percentage_change(
            record[
                "import_million_tonnes"
            ],
            previous_month_value,
        )

        record[
            "year_on_year_percent"
        ] = percentage_change(
            record[
                "import_million_tonnes"
            ],
            previous_year_value,
        )

        record[
            "rolling_3m_average_mbd"
        ] = rolling_average(
            records,
            period,
            3,
        )

        record[
            "rolling_12m_average_mbd"
        ] = rolling_average(
            records,
            period,
            12,
        )

        series.append(record)

    return series


def determine_trend(
    series: list[dict[str, Any]],
) -> dict[str, Any]:

    if len(series) < 2:
        return {
            "direction": "unavailable",
            "change_mbd": None,
            "note": (
                "Not enough data to "
                "calculate trend."
            ),
        }

    latest = series[-1]
    previous = series[-2]

    latest_average = latest.get(
        "rolling_3m_average_mbd"
    )

    previous_average = previous.get(
        "rolling_3m_average_mbd"
    )

    if (
        latest_average is None
        or previous_average is None
    ):
        return {
            "direction": "unavailable",
            "change_mbd": None,
            "note": (
                "Three-month moving average "
                "is unavailable."
            ),
        }

    change = round(
        latest_average
        - previous_average,
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
        "note": (
            "Direction is based on the "
            "change in the three-month "
            "moving average."
        ),
    }


def find_missing_periods(
    requested_periods: list[str],
    records: dict[str, dict[str, Any]],
) -> list[str]:

    return [
        period
        for period in requested_periods
        if period not in records
    ]


def validate_series(
    series: list[dict[str, Any]],
) -> None:

    if not series:
        raise RuntimeError(
            "No valid China crude import "
            "records were generated."
        )

    periods = [
        row["period"]
        for row in series
    ]

    if periods != sorted(set(periods)):
        raise RuntimeError(
            "Periods are duplicated "
            "or not sorted."
        )

    for row in series:

        million_tonnes = row[
            "import_million_tonnes"
        ]

        import_mbd = row[
            "import_mbd"
        ]

        if not (
            PLAUSIBLE_MIN_MILLION_TONNES
            <= million_tonnes
            <= PLAUSIBLE_MAX_MILLION_TONNES
        ):
            raise RuntimeError(
                "Implausible monthly import "
                f"volume for {row['period']}: "
                f"{million_tonnes} million tonnes."
            )

        if not (
            4.0
            <= import_mbd
            <= 20.0
        ):
            raise RuntimeError(
                "Implausible monthly import rate "
                f"for {row['period']}: "
                f"{import_mbd} mb/d."
            )


def main() -> None:

    end_period = previous_complete_month()

    requested_periods = month_range(
        START_PERIOD,
        end_period,
    )

    records = download_all_records(
        requested_periods
    )

    series = enrich_records(
        records
    )

    validate_series(
        series
    )

    missing_periods = (
        find_missing_periods(
            requested_periods,
            records,
        )
    )

    latest = series[-1]

    output = {
        "metadata": {
            "title": (
                "China monthly crude "
                "oil import volume"
            ),

            "description": (
                "Monthly gross crude oil "
                "imports reported by China."
            ),

            "frequency": "monthly",
            "reporter": "China",
            "reporter_code": REPORTER_CODE,
            "partner": "World",
            "partner_code": PARTNER_CODE,
            "flow": "imports",
            "flow_code": FLOW_CODE,
            "commodity_code": COMMODITY_CODE,

            "commodity_description": (
                "Crude petroleum oils"
            ),

            "primary_source": (
                "UN Comtrade"
            ),

            "source_url": (
                "https://comtradeplus.un.org/"
            ),

            "conversion_note": (
                "One metric tonne of crude oil "
                "is approximated as "
                f"{BARRELS_PER_METRIC_TONNE} barrels."
            ),

            "data_policy": (
                "Only available reported monthly "
                "World-total observations are used. "
                "Missing months are not estimated."
            ),

            "start_period": START_PERIOD,

            "requested_end_period": (
                end_period
            ),

            "latest_available_period": (
                latest["period"]
            ),

            "updated_at_utc": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),

            "generator_version": "3.0.0",
        },

        "availability": {
            "requested_months": len(
                requested_periods
            ),

            "available_months": len(
                series
            ),

            "missing_months": (
                missing_periods
            ),

            "latest_requested_period": (
                end_period
            ),

            "latest_available_period": (
                latest["period"]
            ),
        },

        "summary": {
            "latest": latest,

            "trend": determine_trend(
                series
            ),

            "interpretation_note": (
                "Import volume is a physical "
                "market indicator, but it is "
                "not by itself a complete "
                "measure of Chinese oil demand."
            ),
        },

        "series": series,
    }

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_FILE.open(
        "w",
        encoding="utf-8",
    ) as output_file:

        json.dump(
            output,
            output_file,
            ensure_ascii=False,
            indent=2,
        )

        output_file.write("\n")

    print()
    print(
        f"Created: {OUTPUT_FILE}"
    )

    print(
        f"Available months: "
        f"{len(series)}"
    )

    print(
        "Latest available period: "
        f"{latest['period']}"
    )

    print(
        "Latest import volume: "
        f"{latest['import_million_tonnes']} "
        "million tonnes"
    )

    print(
        "Latest import rate: "
        f"{latest['import_mbd']} mb/d"
    )

    if missing_periods:
        print(
            "Missing periods: "
            + ", ".join(
                missing_periods
            )
        )


if __name__ == "__main__":
    main()
