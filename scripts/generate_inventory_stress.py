import json
import statistics
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_FILE = ROOT / "docs" / "data" / "inventory_stress.json"

EIA_API_URL = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"

# EIA product/series candidates. If one fails, the script tries the next.
COMMERCIAL_SERIES = ["WCESTUS1"]
SPR_SERIES = ["WCSSTUS1"]


def fetch_eia_series(series_id):
    params = {
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": "0",
        "length": "300",
    }

    url = EIA_API_URL + "?" + urlencode(params, doseq=True)

    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 energy-data dashboard",
            "Accept": "application/json",
        },
    )

    with urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))

    data = payload.get("response", {}).get("data", [])

    rows = []
    for item in data:
        period = item.get("period")
        value = item.get("value")

        if period is None or value is None:
            continue

        try:
            value = float(value)
        except (TypeError, ValueError):
            continue

        rows.append({
            "date": str(period),
            "value_thousand_barrels": value
        })

    rows.sort(key=lambda x: x["date"])
    return rows


def load_existing_or_default():
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                old = json.load(f)

            old["generated_at"] = datetime.utcnow().strftime("%Y-%m-%d")
            old["data_quality"] = "FALLBACK_PREVIOUS"
            old["warning_hu"] = "Az EIA készletadatok friss lekérése nem sikerült, ezért az előző inventory stress érték maradt érvényben."
            old["warning_en"] = "Fresh EIA inventory data could not be fetched, so the previous inventory stress value was retained."
            return old
        except Exception:
            pass

    return {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "data_quality": "FALLBACK_DEFAULT",
        "source": {
            "commercial_stocks": "EIA weekly U.S. crude stocks excluding SPR",
            "spr_stocks": "EIA weekly U.S. crude oil stocks in SPR"
        },
        "unit": "thousand barrels",
        "inventory_stress_score": 50.0,
        "inventory_stress_level": "MEDIUM",
        "inventory_stress_level_hu": "Közepes",
        "inventory_stress_level_en": "Medium",
        "summary_hu": "Az EIA készletadatok friss lekérése nem sikerült, ezért a modell óvatos, közepes készletoldali nyomást alkalmaz.",
        "summary_en": "Fresh EIA inventory data could not be fetched, so the model applies a cautious medium inventory-side pressure estimate."
    }


def last_n(values, n):
    return values[-n:] if len(values) >= n else values


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
    if score < 65:
        return "MEDIUM", "Közepes", "Medium"
    return "HIGH", "Magas", "High"


def calculate_inventory_stress(commercial_rows, spr_rows):
    commercial_latest = commercial_rows[-1]
    spr_latest = spr_rows[-1]

    commercial_values = [r["value_thousand_barrels"] for r in commercial_rows]
    spr_values = [r["value_thousand_barrels"] for r in spr_rows]

    commercial_current = commercial_latest["value_thousand_barrels"]
    spr_current = spr_latest["value_thousand_barrels"]

    commercial_4w = last_n(commercial_values, 4)
    commercial_13w = last_n(commercial_values, 13)

    spr_4w = last_n(spr_values, 4)
    spr_13w = last_n(spr_values, 13)

    commercial_4w_avg = statistics.mean(commercial_4w)
    commercial_13w_avg = statistics.mean(commercial_13w)

    spr_4w_avg = statistics.mean(spr_4w)
    spr_13w_avg = statistics.mean(spr_13w)

    commercial_change_4w = (
        pct_change(commercial_rows[-5]["value_thousand_barrels"], commercial_current)
        if len(commercial_rows) >= 5 else 0
    )

    spr_change_4w = (
        pct_change(spr_rows[-5]["value_thousand_barrels"], spr_current)
        if len(spr_rows) >= 5 else 0
    )

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

    return {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "data_quality": "LIVE_EIA_API",
        "source": {
            "commercial_stocks": "EIA API v2 WCESTUS1 weekly U.S. crude stocks excluding SPR",
            "spr_stocks": "EIA API v2 WCSSTUS1 weekly U.S. crude oil stocks in SPR"
        },
        "unit": "thousand barrels",
        "commercial": {
            "latest_date": commercial_latest["date"],
            "current": round(commercial_current, 1),
            "avg_4w": round(commercial_4w_avg, 1),
            "avg_13w": round(commercial_13w_avg, 1),
            "change_4w_pct": round(commercial_change_4w, 2),
            "low_stock_stress": round(commercial_low_stock_stress, 1)
        },
        "spr": {
            "latest_date": spr_latest["date"],
            "current": round(spr_current, 1),
            "avg_4w": round(spr_4w_avg, 1),
            "avg_13w": round(spr_13w_avg, 1),
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


def main():
    try:
        commercial_rows = fetch_eia_series("WCESTUS1")
        spr_rows = fetch_eia_series("WCSSTUS1")

        if len(commercial_rows) < 20:
            raise RuntimeError(f"Nincs elég EIA commercial stocks adat: {len(commercial_rows)} sor.")

        if len(spr_rows) < 20:
            raise RuntimeError(f"Nincs elég EIA SPR stocks adat: {len(spr_rows)} sor.")

        output = calculate_inventory_stress(commercial_rows, spr_rows)

    except Exception as exc:
        print(f"WARNING: EIA inventory fetch failed: {exc}")
        output = load_existing_or_default()

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("Inventory stress generated")
    print(f"Data quality: {output.get('data_quality')}")
    print(f"Inventory stress score: {output.get('inventory_stress_score')}")
    print(f"Inventory level: {output.get('inventory_stress_level')}")


if __name__ == "__main__":
    main()
