import json
import csv
import io
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError


ROOT = Path(__file__).resolve().parents[1]

OUTPUT_FILE = ROOT / "china-oil-import.json"

JODI_FILE = ROOT / "docs" / "data" / "china_crude_import_volume.json"

START_DATE = "2026-01-01"

FRED_BRENT_CSV_URL = (
    "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILBRENTEU"
)


def fetch_url(url: str, timeout: int = 90, retries: int = 3) -> str:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            print(f"Downloading data, attempt {attempt}: {url}")

            request = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 GitHubActions energy-data-monitor",
                    "Accept": "text/csv,text/plain,*/*",
                    "Connection": "close",
                },
            )

            with urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="replace")

        except (TimeoutError, URLError, HTTPError) as error:
            last_error = error
            print(f"Download failed on attempt {attempt}: {error}")
            time.sleep(8 * attempt)

    raise RuntimeError(
        f"All download attempts failed for {url}. "
        f"Last error: {last_error}"
    )


def parse_brent_data(csv_text: str):
    rows = []

    reader = csv.DictReader(io.StringIO(csv_text))

    for row in reader:
        row_date = (
            row.get("observation_date")
            or row.get("DATE")
            or row.get("Date")
            or row.get("date")
        )

        value = (
            row.get("DCOILBRENTEU")
            or row.get("Value")
            or row.get("VALUE")
            or row.get("value")
        )

        if not row_date or not value or value == ".":
            continue

        if row_date < START_DATE:
            continue

        try:
            price = float(value)
        except ValueError:
            continue

        rows.append(
            {
                "date": row_date,
                "brent_usd_per_barrel": round(price, 2),
            }
        )

    rows.sort(key=lambda x: x["date"])

    return rows


def load_jodi_import_data():
    if not JODI_FILE.exists():
        raise RuntimeError(
            f"JODI China crude import file not found: {JODI_FILE}"
        )

    with open(JODI_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if data.get("dataset") != "china_crude_import_volume":
        raise RuntimeError(
            "Unexpected dataset in China crude import JSON."
        )

    series = data.get("series")

    if not isinstance(series, list) or not series:
        raise RuntimeError(
            "China crude import JODI series is empty."
        )

    monthly = {}

    for item in series:
        period = item.get("period")
        volume_mbd = item.get("import_volume_mbd")

        if not period or volume_mbd is None:
            continue

        monthly[period] = {
            "period": period,
            "import_volume_kbd": item.get("import_volume_kbd"),
            "import_volume_mbd": float(volume_mbd),
            "assessment_code": item.get("assessment_code"),
            "assessment_label": item.get("assessment_label"),
        }

    if not monthly:
        raise RuntimeError(
            "No usable JODI China crude import observations found."
        )

    latest_period = max(monthly.keys())

    return data, monthly, latest_period


def month_key_from_date(date_string: str) -> str:
    return date_string[:7]


def get_import_volume_for_day(
    day_string: str,
    monthly_data: dict,
    latest_period: str,
):
    requested_period = month_key_from_date(day_string)

    if requested_period in monthly_data:
        item = monthly_data[requested_period]

        return {
            "import_volume_mbd": round(
                item["import_volume_mbd"], 3
            ),
            "volume_period": requested_period,
            "volume_status": "jodi_observed",
            "assessment_code": item.get("assessment_code"),
            "assessment_label": item.get("assessment_label"),
        }

    # Ha a napi Brent adat frissebb, mint a legutolsó
    # publikált JODI havi importadat, akkor a legutolsó
    # JODI értéket proxyként használjuk.
    if requested_period > latest_period:
        item = monthly_data[latest_period]

        return {
            "import_volume_mbd": round(
                item["import_volume_mbd"], 3
            ),
            "volume_period": latest_period,
            "volume_status": "latest_jodi_proxy",
            "assessment_code": item.get("assessment_code"),
            "assessment_label": item.get("assessment_label"),
        }

    return None


def build_import_series(
    brent_rows,
    monthly_data,
    latest_period,
):
    series = []

    for item in brent_rows:
        day = item["date"]
        brent_price = item["brent_usd_per_barrel"]

        volume_info = get_import_volume_for_day(
            day,
            monthly_data,
            latest_period,
        )

        if volume_info is None:
            continue

        import_mbd = volume_info["import_volume_mbd"]

        # Brent USD/hordó × millió hordó/nap
        # → milliárd USD/nap
        estimated_value_billion_usd = (
            brent_price * import_mbd / 1000
        )

        series.append(
            {
                "date": day,
                "brent_usd_per_barrel": brent_price,
                "estimated_import_volume_mbd": import_mbd,
                "volume_period": volume_info["volume_period"],
                "volume_status": volume_info["volume_status"],
                "jodi_assessment_code": volume_info[
                    "assessment_code"
                ],
                "jodi_assessment_label": volume_info[
                    "assessment_label"
                ],
                "estimated_import_value_billion_usd": round(
                    estimated_value_billion_usd, 3
                ),
            }
        )

    return series


def build_summary(series):
    if not series:
        return {
            "latest": None,
            "average_daily_import_volume_mbd": None,
            "average_daily_import_value_billion_usd": None,
            "max_daily_import_value_billion_usd": None,
            "total_estimated_import_value_billion_usd": None,
            "days_count": 0,
        }

    latest = series[-1]

    total_value = sum(
        x["estimated_import_value_billion_usd"]
        for x in series
    )

    avg_value = total_value / len(series)

    avg_volume = sum(
        x["estimated_import_volume_mbd"]
        for x in series
    ) / len(series)

    max_item = max(
        series,
        key=lambda x: x[
            "estimated_import_value_billion_usd"
        ],
    )

    proxy_days = sum(
        1
        for x in series
        if x["volume_status"] == "latest_jodi_proxy"
    )

    observed_days = sum(
        1
        for x in series
        if x["volume_status"] == "jodi_observed"
    )

    return {
        "latest": latest,
        "average_daily_import_volume_mbd": round(
            avg_volume, 3
        ),
        "average_daily_import_value_billion_usd": round(
            avg_value, 3
        ),
        "max_daily_import_value_billion_usd": round(
            max_item[
                "estimated_import_value_billion_usd"
            ],
            3,
        ),
        "max_import_value_date": max_item["date"],
        "total_estimated_import_value_billion_usd": round(
            total_value, 2
        ),
        "days_count": len(series),
        "jodi_observed_days": observed_days,
        "proxy_days": proxy_days,
    }


def build_monthly_inputs(monthly_data):
    monthly = []

    for period in sorted(monthly_data.keys()):
        if period < START_DATE[:7]:
            continue

        item = monthly_data[period]

        monthly.append(
            {
                "month": period,
                "import_volume_kbd": item[
                    "import_volume_kbd"
                ],
                "import_volume_mbd": round(
                    item["import_volume_mbd"], 3
                ),
                "assessment_code": item[
                    "assessment_code"
                ],
                "assessment_label": item[
                    "assessment_label"
                ],
                "source": "JODI Oil World Database",
            }
        )

    return monthly


def main():
    jodi_data, monthly_data, latest_period = (
        load_jodi_import_data()
    )

    print(
        f"Latest JODI China crude import period: "
        f"{latest_period}"
    )

    csv_text = fetch_url(FRED_BRENT_CSV_URL)

    brent_rows = parse_brent_data(csv_text)

    if not brent_rows:
        raise RuntimeError(
            "No Brent data rows found after parsing. "
            "Check CSV source format."
        )

    series = build_import_series(
        brent_rows,
        monthly_data,
        latest_period,
    )

    if not series:
        raise RuntimeError(
            "No China oil import series generated."
        )

    output = {
        "metadata": {
            "title": "China estimated crude oil import cost",
            "description": (
                "Estimated daily gross cost of China's crude "
                "oil imports using JODI monthly crude-import "
                "volume and daily Brent crude price."
            ),
            "method": (
                "Monthly China crude-oil import volume from "
                "JODI is combined with daily Brent price. "
                "For dates after the latest available JODI "
                "month, the latest JODI observation is carried "
                "forward as an explicitly labelled proxy."
            ),
            "start_date": START_DATE,
            "unit_note": (
                "Volume is million barrels per day. "
                "Estimated value is billion USD per day."
            ),
            "price_source": (
                "FRED DCOILBRENTEU daily Brent crude oil price."
            ),
            "volume_source": (
                "JODI Oil World Database – China crude oil "
                "total imports, CRUDEOIL / TOTIMPSB / KBD."
            ),
            "jodi_latest_period": latest_period,
            "jodi_generated_at": jodi_data.get(
                "generated_at"
            ),
            "proxy_note": (
                "When Brent observations extend beyond the "
                "latest JODI month, the latest available JODI "
                "import volume is used as a proxy and labelled "
                "latest_jodi_proxy."
            ),
            "updated_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
        },
        "monthly_inputs": build_monthly_inputs(
            monthly_data
        ),
        "summary": build_summary(series),
        "series": series,
    }

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            output,
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"Created {OUTPUT_FILE}")
    print(f"Rows: {len(series)}")
    print(f"Latest date: {series[-1]['date']}")
    print(
        f"Latest volume source period: "
        f"{series[-1]['volume_period']}"
    )
    print(
        f"Latest volume status: "
        f"{series[-1]['volume_status']}"
    )


if __name__ == "__main__":
    main()
