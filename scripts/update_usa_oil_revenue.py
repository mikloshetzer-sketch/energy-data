import json
import csv
import io
import os
from datetime import datetime, timezone
from urllib.request import urlopen, Request

OUTPUT_FILE = "usa-oil-revenue.json"

START_DATE = "2026-01-01"

# Becsült amerikai napi kőolajtermelés.
# Egység: millió hordó / nap
# Ezt később lehet EIA heti/havi adattal automatizálni.
DEFAULT_US_PRODUCTION_MBD = 13.65

FRED_WTI_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO"


def fetch_csv(url: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 energy-data-monitor"
        }
    )

    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8")


def parse_wti_data(csv_text: str):
    rows = []
    reader = csv.DictReader(io.StringIO(csv_text))

    for row in reader:
        date = row.get("observation_date") or row.get("DATE") or row.get("date")
        value = row.get("DCOILWTICO")

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

    return rows


def build_revenue_series(wti_rows):
    series = []

    for item in wti_rows:
        price = item["wti_usd_per_barrel"]

        # képlet:
        # USD/nap = ár USD/hordó × millió hordó/nap × 1 000 000
        # milliárd USD/nap = USD/nap / 1 000 000 000
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
            "total_estimated_revenue_billion_usd": None
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
    csv_text = fetch_csv(FRED_WTI_CSV_URL)
    wti_rows = parse_wti_data(csv_text)
    series = build_revenue_series(wti_rows)

    output = {
        "metadata": {
            "title": "USA estimated crude oil production revenue",
            "description": "Estimated gross market value of US crude oil production from 2026-01-01.",
            "method": "WTI daily price multiplied by estimated US crude oil production.",
            "start_date": START_DATE,
            "production_assumption_mbd": DEFAULT_US_PRODUCTION_MBD,
            "production_note": "Static estimate. Later this can be replaced with EIA weekly/monthly production data.",
            "price_source": "FRED DCOILWTICO daily WTI crude oil price",
            "updated_at_utc": datetime.now(timezone.utc).isoformat()
        },
        "summary": build_summary(series),
        "series": series
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Created {OUTPUT_FILE}")
    print(f"Rows: {len(series)}")


if __name__ == "__main__":
    main()
