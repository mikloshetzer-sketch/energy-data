import json
import statistics
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

USA_FILE = ROOT / "usa-oil-revenue.json"
CHINA_FILE = ROOT / "china-oil-import.json"
MARKET_FILE = ROOT / "market-history.json"

OUTPUT_FILE = ROOT / "docs" / "data" / "market_interpretation.json"


def pct_change(old, new):
    if old is None or new is None or old == 0:
        return 0
    return ((new - old) / old) * 100


def trend_text(value):
    if value > 5:
        return "emelkedő"
    elif value < -5:
        return "csökkenő"
    return "stabil"


def trend_text_en(value):
    if value > 5:
        return "rising"
    elif value < -5:
        return "falling"
    return "stable"


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def normalize_price_change(change_pct):
    return clamp(abs(change_pct) * 10)


def normalize_volatility(volatility_pct):
    return clamp(volatility_pct * 12.5)


def get_brent(row):
    return row.get("market_brent") if row.get("market_brent") is not None else row.get("brent")


def combined_risk_score(chokepoint, middle_east, brent_change_pct, brent_volatility_pct):
    chokepoint = chokepoint if chokepoint is not None else 50
    middle_east = middle_east if middle_east is not None else 50

    price_risk = normalize_price_change(brent_change_pct)
    volatility_risk = normalize_volatility(brent_volatility_pct)

    score = (
        chokepoint * 0.40 +
        middle_east * 0.25 +
        price_risk * 0.20 +
        volatility_risk * 0.15
    )

    return round(clamp(score), 1), round(price_risk, 1), round(volatility_risk, 1)


def risk_level(score):
    if score < 35:
        return "LOW", "Alacsony", "Low"
    elif score < 65:
        return "MEDIUM", "Közepes", "Medium"
    else:
        return "HIGH", "Magas", "High"


def main():
    with open(USA_FILE, "r", encoding="utf-8") as f:
        usa = json.load(f)

    with open(CHINA_FILE, "r", encoding="utf-8") as f:
        china = json.load(f)

    with open(MARKET_FILE, "r", encoding="utf-8") as f:
        market = json.load(f)

    usa_series = usa.get("series", [])
    china_series = china.get("series", [])
    market_rows = market.get("rows", [])

    market_rows = [
        row for row in market_rows
        if get_brent(row) is not None
    ]

    if len(usa_series) < 2 or len(china_series) < 2 or len(market_rows) < 2:
        raise RuntimeError("Nincs elég adat az értelmezéshez.")

    lookback = min(30, len(market_rows) - 1, len(usa_series) - 1, len(china_series) - 1)

    usa_latest = usa_series[-1]
    usa_old = usa_series[-1 - lookback]

    china_latest = china_series[-1]
    china_old = china_series[-1 - lookback]

    market_latest = market_rows[-1]
    market_old = market_rows[-1 - lookback]

    brent_latest = get_brent(market_latest)
    brent_old = get_brent(market_old)

    brent_change = pct_change(brent_old, brent_latest)

    brent_window = [get_brent(row) for row in market_rows[-lookback:]]
    brent_window = [x for x in brent_window if x is not None]

    if len(brent_window) > 1:
      avg_brent = statistics.mean(brent_window)
      brent_volatility = (statistics.stdev(brent_window) / avg_brent) * 100 if avg_brent else 0
    else:
      brent_volatility = 0

    usa_change = pct_change(
        usa_old.get("estimated_revenue_billion_usd"),
        usa_latest.get("estimated_revenue_billion_usd")
    )

    china_change = pct_change(
        china_old.get("estimated_import_value_billion_usd"),
        china_latest.get("estimated_import_value_billion_usd")
    )

    chokepoint = market_latest.get("global_trade_risk_index")
    middle_east = market_latest.get("middle_east_conflict_impact")

    risk_score, price_risk, volatility_risk = combined_risk_score(
        chokepoint,
        middle_east,
        brent_change,
        brent_volatility
    )

    risk_code, risk_hu, risk_en = risk_level(risk_score)

    summary_hu = (
        f"A Brent ár az elmúlt {lookback} napban "
        f"{trend_text(brent_change)} trendet mutatott "
        f"({brent_change:+.1f}%). "
        f"Az USA becsült napi olajtermelési értéke "
        f"{usa_change:+.1f}% változást mutatott, "
        f"miközben Kína napi importköltsége "
        f"{china_change:+.1f}% mértékben változott. "
        f"Az összesített energiapiaci kockázati pontszám "
        f"{risk_score:.1f}/100, ami {risk_hu.lower()} szintnek felel meg. "
        f"A pontszám a szoroskockázatot, a közel-keleti konfliktushatást, "
        f"a Brent árváltozását és az árvolatilitást együtt veszi figyelembe."
    )

    summary_en = (
        f"Brent prices showed a "
        f"{trend_text_en(brent_change)} trend "
        f"over the last {lookback} days "
        f"({brent_change:+.1f}%). "
        f"Estimated US daily oil production value changed by "
        f"{usa_change:+.1f}%, while China's daily import cost changed by "
        f"{china_change:+.1f}%. "
        f"The combined energy-market risk score is "
        f"{risk_score:.1f}/100, corresponding to a {risk_en.lower()} risk level. "
        f"The score combines chokepoint risk, Middle East conflict impact, "
        f"Brent price movement and price volatility."
    )

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "risk_level": risk_code,
        "risk_label_hu": risk_hu,
        "risk_label_en": risk_en,
        "combined_risk_score": risk_score,
        "risk_components": {
            "chokepoint_risk": chokepoint,
            "middle_east_conflict_impact": middle_east,
            "brent_price_change_risk": price_risk,
            "brent_volatility_risk": volatility_risk
        },
        "brent_change_pct": round(brent_change, 2),
        "brent_volatility_pct": round(brent_volatility, 2),
        "usa_change_pct": round(usa_change, 2),
        "china_change_pct": round(china_change, 2),
        "summary_hu": summary_hu,
        "summary_en": summary_en
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("Market interpretation generated")
    print(f"Combined risk score: {risk_score}/100")
    print(f"Risk level: {risk_code}")


if __name__ == "__main__":
    main()
