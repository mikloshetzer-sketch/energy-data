import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

USA_FILE = ROOT / "usa-oil-revenue.json"
CHINA_FILE = ROOT / "china-oil-import.json"
MARKET_FILE = ROOT / "market-history.json"

OUTPUT_FILE = ROOT / "docs" / "data" / "market_interpretation.json"


def pct_change(old, new):
    if old == 0:
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


def risk_level(risk):
    if risk < 33:
        return "LOW", "Alacsony", "Low"
    elif risk < 66:
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

    usa_series = usa["series"]
    china_series = china["series"]
    market_rows = market["rows"]

    usa_latest = usa_series[-1]
    usa_old = usa_series[max(0, len(usa_series)-30)]

    china_latest = china_series[-1]
    china_old = china_series[max(0, len(china_series)-30)]

    market_latest = market_rows[-1]
    market_old = market_rows[max(0, len(market_rows)-30)]

    brent_change = pct_change(
        market_old["market_brent"],
        market_latest["market_brent"]
    )

    usa_change = pct_change(
        usa_old["estimated_revenue_billion_usd"],
        usa_latest["estimated_revenue_billion_usd"]
    )

    china_change = pct_change(
        china_old["estimated_import_value_billion_usd"],
        china_latest["estimated_import_value_billion_usd"]
    )

    risk = market_latest.get("global_trade_risk_index", 50)

    risk_code, risk_hu, risk_en = risk_level(risk)

    summary_hu = (
        f"A Brent ár az elmúlt 30 napban "
        f"{trend_text(brent_change)} trendet mutatott "
        f"({brent_change:+.1f}%). "
        f"Az USA becsült napi olajtermelési értéke "
        f"{usa_change:+.1f}% változást mutatott. "
        f"Kína napi importköltsége "
        f"{china_change:+.1f}% mértékben változott. "
        f"A szoroskockázati mutató jelenleg "
        f"{risk_hu.lower()} szinten áll ({risk:.1f}/100)."
    )

    summary_en = (
        f"Brent prices showed a "
        f"{trend_text_en(brent_change)} trend "
        f"over the last 30 days "
        f"({brent_change:+.1f}%). "
        f"Estimated US oil production value changed by "
        f"{usa_change:+.1f}%. "
        f"China's daily import cost changed by "
        f"{china_change:+.1f}%. "
        f"The chokepoint risk indicator currently stands at "
        f"{risk_en.lower()} level ({risk:.1f}/100)."
    )

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "risk_level": risk_code,
        "risk_label_hu": risk_hu,
        "risk_label_en": risk_en,
        "brent_change_pct": round(brent_change, 2),
        "usa_change_pct": round(usa_change, 2),
        "china_change_pct": round(china_change, 2),
        "summary_hu": summary_hu,
        "summary_en": summary_en
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("Market interpretation generated")


if __name__ == "__main__":
    main()
