import csv
import json
import statistics
from datetime import datetime
from io import StringIO
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_FILE = ROOT / "docs" / "data" / "inventory_stress.json"

# EIA weekly series CSV exports
# WCESTUS1 = Weekly U.S. Ending Stocks excluding SPR of Crude Oil
# WCSSTUS1 = Weekly U.S. Ending Stocks of Crude Oil in SPR
COMMERCIAL_STOCKS_URL = (
    "https://www.eia.gov/dnav/pet/hist_xls/WCESTUS1w.xls"
)
SPR_STOCKS_URL = (
    "https://www.eia.gov/dnav/pet/hist_xls/WCSSTUS1w.xls"
)


def fetch_text(url):
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 energy-data dashboard"
        }
    )

    with urlopen(request, timeout=60) as response:
        raw = response.read()

    return raw.decode("latin-1", errors="replace")


def parse_eia_hist_xls_text(text):
    """
    EIA hist_xls files are often tabular text inside an .xls endpoint.
    We parse rows containing a date and a numeric value.
    """

    rows = []

    for line in text.splitlines():
        parts = [p.strip().strip('"') for p in line.replace("\t", ",").split(",")]

        if len(parts) < 2:
            continue

        date_raw = parts[0]
        value_raw = parts[1]

        # Skip headers
        if "Date" in date_raw or "Week" in date_raw:
            continue

        parsed_date = None

        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y"):
            try:
                parsed_date = datetime.strptime(date_raw, fmt).date()
                break
            except ValueError:
                pass

        if parsed_date is None:
            continue

        try:
            value = float(value_raw.replace(",", ""))
        except ValueError:
            continue

        rows.append({
            "date": parsed_date.isoformat(),
            "value_thousand_barrels": value
        })

    rows.sort(key=lambda x: x["date"])
    return rows


def last_n(rows, n):
    return rows[-n:] if len(rows) >= n else rows


def pct_change(old, new):
    if old is None or new is None or old == 0:
        return 0
    return ((new - old) / old) * 100


def percentile_rank(values, current):
    values = [v for v in values if v is not None]

    if not values or current is None:
        return 50

    below_or_equal = sum(1 for v in values if v <= current)
    return (below_or_equal / len(values)) * 100


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def level_from_score(score):
    if score < 35:
        return "LOW", "Alacsony", "Low"
    elif score < 65:
        return "MEDIUM", "Közepes", "Medium"
    return "HIGH", "Magas", "High"


def main():
    commercial_text = fetch_text(COMMERCIAL_STOCKS_URL)
    spr_text = fetch_text(SPR_STOCKS_URL)

    commercial_rows = parse_eia_hist_xls_text(commercial_text)
    spr_rows = parse_eia_hist_xls_text(spr_text)

    if len(commercial_rows) < 20:
        raise RuntimeError("Nincs elég EIA commercial stocks adat.")

    if len(spr_rows) < 20:
        raise RuntimeError("Nincs elég EIA SPR stocks adat.")

    commercial_latest = commercial_rows[-1]
    spr_latest = spr_rows[-1]

    commercial_values = [r["value_thousand_barrels"] for r in commercial_rows]
    spr_values = [r["value_thousand_barrels"] for r in spr_rows]

    commercial_current = commercial_latest["value_thousand_barrels"]
    spr_current = spr_latest["value_thousand_barrels"]

    commercial_30 = last_n(commercial_values, 4)   # weekly data ≈ 1 month
    commercial_90 = last_n(commercial_values, 13)  # weekly data ≈ 1 quarter

    spr_30 = last_n(spr_values, 4)
    spr_90 = last_n(spr_values, 13)

    commercial_30_avg = statistics.mean(commercial_30)
    commercial_90_avg = statistics.mean(commercial_90)

    spr_30_avg = statistics.mean(spr_30)
    spr_90_avg = statistics.mean(spr_90)

    commercial_change_4w = pct_change(commercial_rows[-5]["value_thousand_barrels"], commercial_current) if len(commercial_rows) >= 5 else 0
    spr_change_4w = pct_change(spr_rows[-5]["value_thousand_barrels"], spr_current) if len(spr_rows) >= 5 else 0

    # Stress logic:
    # Lower commercial inventories = higher stress.
    # Lower SPR inventories = higher stress.
    # Falling inventory trend = higher stress.
    commercial_percentile = percentile_rank(commercial_values[-260:], commercial_current)
    spr_percentile = percentile_rank(spr_values[-260:], spr_current)

    commercial_low_stock_stress = 100 - commercial_percentile
    spr_low_stock_stress = 100 - spr_percentile

    commercial_draw_stress = clamp(abs(commercial_change_4w) * 8) if commercial_change_4w < 0 else 0
    spr_draw_stress = clamp(abs(spr_change_4w) * 8) if spr_change_4w < 0 else 0

    stress_score = (
        commercial_low_stock_stress * 0.45 +
        spr_low_stock_stress * 0.25 +
        commercial_draw_stress * 0.20 +
        spr_draw_stress * 0.10
    )

    stress_score = round(clamp(stress_score), 1)

    level_code, level_hu, level_en = level_from_score(stress_score)

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "source": {
            "commercial_stocks": "EIA WCESTUS1 weekly U.S. crude stocks excluding SPR",
            "spr_stocks": "EIA WCSSTUS1 weekly U.S. crude oil stocks in SPR"
        },
        "unit": "thousand barrels",
        "commercial": {
            "latest_date": commercial_latest["date"],
            "current": round(commercial_current, 1),
            "avg_4w": round(commercial_30_avg, 1),
            "avg_13w": round(commercial_90_avg, 1),
            "change_4w_pct": round(commercial_change_4w, 2),
            "low_stock_stress": round(commercial_low_stock_stress, 1)
        },
        "spr": {
            "latest_date": spr_latest["date"],
            "current": round(spr_current, 1),
            "avg_4w": round(spr_30_avg, 1),
            "avg_13w": round(spr_90_avg, 1),
            "change_4w_pct": round(spr_change_4w, 2),
            "low_stock_stress": round(spr_low_stock_stress, 1)
        },
        "inventory_stress_score": stress_score,
        "inventory_stress_level": level_code,
        "inventory_stress_level_hu": level_hu,
        "inventory_stress_level_en": level_en,
        "summary_hu": (
            f"Az USA kereskedelmi nyersolajkészlete {commercial_current/1000:.1f} millió hordó, "
            f"az SPR készlet {spr_current/1000:.1f} millió hordó. "
            f"Az Inventory Stress Index {stress_score}/100, ami {level_hu.lower()} készletoldali nyomást jelez."
        ),
        "summary_en": (
            f"U.S. commercial crude stocks stand at {commercial_current/1000:.1f} million barrels, "
            f"while SPR stocks stand at {spr_current/1000:.1f} million barrels. "
            f"The Inventory Stress Index is {stress_score}/100, indicating {level_en.lower()} inventory-side pressure."
        )
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("Inventory stress generated")
    print(f"Inventory stress score: {stress_score}")
    print(f"Inventory level: {level_code}")


if __name__ == "__main__":
    main()
