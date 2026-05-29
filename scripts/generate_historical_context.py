import json
import statistics
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

USA_FILE = ROOT / "usa-oil-revenue.json"
CHINA_FILE = ROOT / "china-oil-import.json"
MARKET_FILE = ROOT / "market-history.json"
INTERPRETATION_FILE = ROOT / "docs" / "data" / "market_interpretation.json"

OUTPUT_FILE = ROOT / "docs" / "data" / "historical_context.json"


def avg(values):
    values = [v for v in values if v is not None]
    return round(statistics.mean(values), 2) if values else None


def pct_diff(current, baseline):
    if current is None or baseline is None or baseline == 0:
        return None
    return round(((current - baseline) / baseline) * 100, 2)


def last_n(values, n):
    return values[-n:] if len(values) >= n else values


def get_brent(row):
    return row.get("market_brent") if row.get("market_brent") is not None else row.get("brent")


def get_wti_from_usa(row):
    return row.get("wti_usd_per_barrel")


def classify_vs_average(diff_pct):
    if diff_pct is None:
        return "UNKNOWN", "Nincs elég adat", "Not enough data"

    if diff_pct > 10:
        return "ABOVE", "Átlag felett", "Above average"
    elif diff_pct < -10:
        return "BELOW", "Átlag alatt", "Below average"
    return "NEAR", "Átlag körül", "Near average"


def build_metric(name_hu, name_en, current, values_30, values_90, unit):
    avg_30 = avg(values_30)
    avg_90 = avg(values_90)

    diff_30 = pct_diff(current, avg_30)
    diff_90 = pct_diff(current, avg_90)

    status_code, status_hu, status_en = classify_vs_average(diff_90)

    return {
        "name_hu": name_hu,
        "name_en": name_en,
        "current": round(current, 2) if current is not None else None,
        "unit": unit,
        "avg_30d": avg_30,
        "avg_90d": avg_90,
        "diff_vs_30d_pct": diff_30,
        "diff_vs_90d_pct": diff_90,
        "status_code": status_code,
        "status_hu": status_hu,
        "status_en": status_en
    }


def main():
    with open(USA_FILE, "r", encoding="utf-8") as f:
        usa = json.load(f)

    with open(CHINA_FILE, "r", encoding="utf-8") as f:
        china = json.load(f)

    with open(MARKET_FILE, "r", encoding="utf-8") as f:
        market = json.load(f)

    with open(INTERPRETATION_FILE, "r", encoding="utf-8") as f:
        interpretation = json.load(f)

    usa_series = usa.get("series", [])
    china_series = china.get("series", [])
    market_rows = market.get("rows", [])

    market_rows = [row for row in market_rows if get_brent(row) is not None]

    if not usa_series or not china_series or not market_rows:
        raise RuntimeError("Nincs elég adat a historical context generálásához.")

    latest_usa = usa_series[-1]
    latest_china = china_series[-1]
    latest_market = market_rows[-1]

    brent_values = [get_brent(row) for row in market_rows]
    wti_values = [get_wti_from_usa(row) for row in usa_series]
    risk_values = [
        row.get("global_trade_risk_index")
        for row in market_rows
        if row.get("global_trade_risk_index") is not None
    ]

    me_values = [
        row.get("middle_east_conflict_impact")
        for row in market_rows
        if row.get("middle_east_conflict_impact") is not None
    ]

    usa_revenue_values = [
        row.get("estimated_revenue_billion_usd")
        for row in usa_series
        if row.get("estimated_revenue_billion_usd") is not None
    ]

    china_cost_values = [
        row.get("estimated_import_value_billion_usd")
        for row in china_series
        if row.get("estimated_import_value_billion_usd") is not None
    ]

    china_volume_values = [
        row.get("estimated_import_volume_mbd")
        for row in china_series
        if row.get("estimated_import_volume_mbd") is not None
    ]

    combined_risk_current = interpretation.get("combined_risk_score")

    # Nincs hosszú idősoros combined risk archive, ezért az aktuális combined risket
    # a market-history risk adatokhoz hasonlítjuk kontextusként.
    risk_context_values = risk_values

    metrics = [
        build_metric(
            "Brent ár",
            "Brent price",
            get_brent(latest_market),
            last_n(brent_values, 30),
            last_n(brent_values, 90),
            "USD/barrel"
        ),
        build_metric(
            "WTI ár",
            "WTI price",
            latest_usa.get("wti_usd_per_barrel"),
            last_n(wti_values, 30),
            last_n(wti_values, 90),
            "USD/barrel"
        ),
        build_metric(
            "Összesített kockázat",
            "Combined risk",
            combined_risk_current,
            last_n(risk_context_values, 30),
            last_n(risk_context_values, 90),
            "index"
        ),
        build_metric(
            "Közel-keleti konfliktushatás",
            "Middle East conflict impact",
            latest_market.get("middle_east_conflict_impact"),
            last_n(me_values, 30),
            last_n(me_values, 90),
            "index"
        ),
        build_metric(
            "USA olajtermelési érték",
            "US oil production value",
            latest_usa.get("estimated_revenue_billion_usd"),
            last_n(usa_revenue_values, 30),
            last_n(usa_revenue_values, 90),
            "bn USD/day"
        ),
        build_metric(
            "Kína importköltség",
            "China import cost",
            latest_china.get("estimated_import_value_billion_usd"),
            last_n(china_cost_values, 30),
            last_n(china_cost_values, 90),
            "bn USD/day"
        ),
        build_metric(
            "Kína importmennyiség",
            "China import volume",
            latest_china.get("estimated_import_volume_mbd"),
            last_n(china_volume_values, 30),
            last_n(china_volume_values, 90),
            "mbd"
        )
    ]

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "description_hu": "A historical context blokk az aktuális értékeket a 30 és 90 napos átlagokhoz viszonyítja.",
        "description_en": "The historical context block compares current values with 30-day and 90-day averages.",
        "metrics": metrics
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("Historical context generated")
    print(f"Metrics: {len(metrics)}")


if __name__ == "__main__":
    main()
