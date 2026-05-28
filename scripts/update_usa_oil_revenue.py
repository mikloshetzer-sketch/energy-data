import json
import csv
import io
import time
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

OUTPUT_FILE = "usa-oil-revenue.json"
START_DATE = "2026-01-01"
DEFAULT_US_PRODUCTION_MBD = 13.65

FRED_WTI_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO"
NASDAQ_WTI_CSV_URL = "https://data.nasdaq.com/api/v3/datasets/FRED/DCOILWTICO.csv"


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

    raise RuntimeError(f"All download attempts failed for {url}. Last error: {last_error}")


def fetch_wti_csv() -> str:
    try:
        return fetch_url(FRED_WTI_CSV_URL, timeout=90, retries=3)
    except Exception as fred_error:
        print(f"FRED direct CSV failed: {fred_error}")
        print("Trying backup Nasdaq Data Link FRED mirror...")
        return fetch_url(NASDAQ_WTI_CSV_URL, timeout=90, retries=2)


def parse_wti_data(csv_text: str):
    rows = []
    reader = csv.DictReader(io.StringIO(csv_text))

    for row in reader:
        date = (
            row.get("observation_date")
            or row.get("DATE")
            or row.get("Date")
            or row.get("date")
        )

        value = (
            row.get("DCOILWTICO")
            or row.get("Value")
            or row.get("VALUE")
            or row.get("value")
        )

        if not date or not value or value == ".":
            continue

        if date < START_DATE:
            continue

        try:
            price = float(value)
        except ValueError:
            continue

        rows.append({
            "date": date,
            "wti_usd_per_barrel": round(price, 2)
        })

    rows.sort(key=lambda x: x["date"])
    return rows


def build_revenue_series(wti_rows):
    series = []

    for item in wti_rows:
        price = item["wti_usd_per_barrel"]
        revenue_billion_usd = price * DEFAULT_US_PRODUCTION_MBD / 1000

        series.append({
            "date": item["date"],
            "wti_usd_per_barrel": price,
            "us_crude_production_mbd": DEFAULT_US_PRODUCTION_MBD,
            "estimated_revenue_billion_usd": round(revenue_billion_usd, 3)
        })

    return series


def build_summary(series):
    if not series:
        return {
            "latest": None,
            "average_daily_revenue_billion_usd": None,
            "max_daily_revenue_billion_usd": None,
            "total_estimated_revenue_billion_usd": None,
            "days_count": 0
        }

    latest = series[-1]
    total = sum(x["estimated_revenue_billion_usd"] for x in series)
    average = total / len(series)
    max_item = max(series, key=lambda x: x["estimated_revenue_billion_usd"])

    return {
        "latest": latest,
        "average_daily_revenue_billion_usd": round(average, 3),
        "max_daily_revenue_billion_usd": round(max_item["estimated_revenue_billion_usd"], 3),
        "max_revenue_date": max_item["date"],
        "total_estimated_revenue_billion_usd": round(total, 2),
        "days_count": len(series)
    }


def main():
    csv_text = fetch_wti_csv()
    wti_rows = parse_wti_data(csv_text)

    if not wti_rows:
        raise RuntimeError("No WTI data rows found after parsing. Check CSV source format.")

    series = build_revenue_series(wti_rows)

    output = {
        "metadata": {
            "title": "USA estimated crude oil production revenue",
            "description": "Estimated gross market value of US crude oil production from 2026-01-01.",
            "method": "WTI daily price multiplied by estimated US crude oil production.",
            "start_date": START_DATE,
            "production_assumption_mbd": DEFAULT_US_PRODUCTION_MBD,
            "production_note": "Static estimate. Later this can be replaced with EIA weekly/monthly production data.",
            "price_source": "FRED DCOILWTICO daily WTI crude oil price; Nasdaq Data Link mirror as fallback.",
            "updated_at_utc": datetime.now(timezone.utc).isoformat()
        },
        "summary": build_summary(series),
        "series": series
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Created {OUTPUT_FILE}")
    print(f"Rows: {len(series)}")
    print(f"Latest date: {series[-1]['date']}")


if __name__ == "__main__":
    main()
