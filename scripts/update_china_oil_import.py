import json
import csv
import io
import time
import calendar
from datetime import datetime, timezone, date
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

OUTPUT_FILE = "china-oil-import.json"
START_DATE = "2026-01-01"

# Brent napi ár FRED-ből
FRED_BRENT_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILBRENTEU"

# Átváltás:
# 1 metrikus tonna nyersolaj kb. 7,33 hordó.
# Ez átlagos becslés, mert a pontos érték az olajminőségtől függ.
BARRELS_PER_METRIC_TON = 7.33

# Kínai nyersolajimport havi becslés, millió tonnában.
# Forráslogika:
# - Reuters: 2026. április = 38,5 millió tonna
# - Reuters: 2026. január-április összesen = 185,3 millió tonna
# - Január-március ezért ideiglenesen egyenlő arányban elosztva:
#   (185,3 - 38,5) / 3 = 48,933
# - Május ideiglenesen áprilisi szinten tartva, amíg nincs teljes havi vámadat.
CHINA_IMPORT_MILLION_TONNES_2026 = {
    "2026-01": 48.933,
    "2026-02": 48.933,
    "2026-03": 48.933,
    "2026-04": 38.500,
    "2026-05": 38.500
}


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

        rows.append({
            "date": row_date,
            "brent_usd_per_barrel": round(price, 2)
        })

    rows.sort(key=lambda x: x["date"])
    return rows


def month_key_from_date(date_string: str) -> str:
    return date_string[:7]


def days_in_month(year_month: str) -> int:
    year, month = map(int, year_month.split("-"))
    return calendar.monthrange(year, month)[1]


def estimate_daily_import_volume_mbd(day_string: str):
    year_month = month_key_from_date(day_string)

    if year_month not in CHINA_IMPORT_MILLION_TONNES_2026:
        return None

    monthly_million_tonnes = CHINA_IMPORT_MILLION_TONNES_2026[year_month]
    month_days = days_in_month(year_month)

    monthly_million_barrels = monthly_million_tonnes * BARRELS_PER_METRIC_TON
    daily_million_barrels = monthly_million_barrels / month_days

    return round(daily_million_barrels, 3)


def build_import_series(brent_rows):
    series = []

    for item in brent_rows:
        day = item["date"]
        brent_price = item["brent_usd_per_barrel"]

        import_mbd = estimate_daily_import_volume_mbd(day)

        if import_mbd is None:
            continue

        # Importérték:
        # Brent USD/hordó × millió hordó/nap × 1 000 000
        # milliárd USD/nap = / 1 000 000 000
        estimated_value_billion_usd = brent_price * import_mbd / 1000

        series.append({
            "date": day,
            "brent_usd_per_barrel": brent_price,
            "estimated_import_volume_mbd": import_mbd,
            "estimated_import_value_billion_usd": round(estimated_value_billion_usd, 3)
        })

    return series


def build_summary(series):
    if not series:
        return {
            "latest": None,
            "average_daily_import_volume_mbd": None,
            "average_daily_import_value_billion_usd": None,
            "max_daily_import_value_billion_usd": None,
            "total_estimated_import_value_billion_usd": None,
            "days_count": 0
        }

    latest = series[-1]

    total_value = sum(x["estimated_import_value_billion_usd"] for x in series)
    avg_value = total_value / len(series)
    avg_volume = sum(x["estimated_import_volume_mbd"] for x in series) / len(series)

    max_item = max(series, key=lambda x: x["estimated_import_value_billion_usd"])

    return {
        "latest": latest,
        "average_daily_import_volume_mbd": round(avg_volume, 3),
        "average_daily_import_value_billion_usd": round(avg_value, 3),
        "max_daily_import_value_billion_usd": round(max_item["estimated_import_value_billion_usd"], 3),
        "max_import_value_date": max_item["date"],
        "total_estimated_import_value_billion_usd": round(total_value, 2),
        "days_count": len(series)
    }


def build_monthly_inputs():
    monthly = []

    for month, million_tonnes in CHINA_IMPORT_MILLION_TONNES_2026.items():
        month_days = days_in_month(month)
        million_barrels = million_tonnes * BARRELS_PER_METRIC_TON
        mbd = million_barrels / month_days

        monthly.append({
            "month": month,
            "import_million_tonnes": round(million_tonnes, 3),
            "estimated_import_million_barrels": round(million_barrels, 2),
            "estimated_import_volume_mbd": round(mbd, 3)
        })

    return monthly


def main():
    csv_text = fetch_url(FRED_BRENT_CSV_URL)
    brent_rows = parse_brent_data(csv_text)

    if not brent_rows:
        raise RuntimeError("No Brent data rows found after parsing. Check CSV source format.")

    series = build_import_series(brent_rows)

    if not series:
        raise RuntimeError("No China oil import series generated. Check monthly import inputs.")

    output = {
        "metadata": {
            "title": "China estimated crude oil import cost",
            "description": "Estimated daily gross cost of China's crude oil imports from 2026-01-01.",
            "method": "Monthly crude oil import volume estimate converted to daily barrels and multiplied by daily Brent price.",
            "start_date": START_DATE,
            "unit_note": "Volume is estimated in million barrels per day. Value is estimated in billion USD per day.",
            "conversion_note": "1 metric tonne of crude oil is approximated as 7.33 barrels. Actual conversion varies by crude grade.",
            "price_source": "FRED DCOILBRENTEU daily Brent crude oil price.",
            "volume_source_note": "China monthly crude import estimates based on reported customs/Reuters figures. January-March are distributed from Jan-Apr cumulative data; April is reported; May is provisional.",
            "updated_at_utc": datetime.now(timezone.utc).isoformat()
        },
        "monthly_inputs": build_monthly_inputs(),
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
