import json
import statistics
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

MARKET_FILE = ROOT / "market-history.json"
INTERPRETATION_FILE = ROOT / "docs" / "data" / "market_interpretation.json"

OUTPUT_FILE = ROOT / "docs" / "data" / "energy_forecast.json"


def get_brent(row):
    return row.get("market_brent") if row.get("market_brent") is not None else row.get("brent")


def safe_mean(values):
    values = [v for v in values if v is not None]
    return statistics.mean(values) if values else None


def linear_forecast(values, days_ahead=30):
    values = [v for v in values if v is not None]

    if len(values) < 7:
        return None, None, None

    n = len(values)
    x_mean = (n - 1) / 2
    y_mean = statistics.mean(values)

    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))

    slope = numerator / denominator if denominator else 0
    forecast_mid = values[-1] + slope * days_ahead

    residuals = []
    for i, value in enumerate(values):
        predicted = y_mean + slope * (i - x_mean)
        residuals.append(value - predicted)

    volatility = statistics.stdev(residuals) if len(residuals) > 1 else 0

    low = forecast_mid - volatility
    high = forecast_mid + volatility

    return round(low, 2), round(forecast_mid, 2), round(high, 2)


def classify_trend(current, forecast_mid):
    if current is None or forecast_mid is None:
        return "UNKNOWN", "Bizonytalan", "Uncertain"

    diff = ((forecast_mid - current) / current) * 100 if current else 0

    if diff > 3:
        return "UP", "Emelkedő", "Rising"
    elif diff < -3:
        return "DOWN", "Csökkenő", "Falling"
    return "STABLE", "Stabil", "Stable"


def confidence_label(values):
    values = [v for v in values if v is not None]

    if len(values) < 14:
        return "LOW", "Alacsony", "Low"

    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0

    volatility_pct = (stdev / mean) * 100 if mean else 0

    if volatility_pct < 4:
        return "HIGH", "Magas", "High"
    elif volatility_pct < 9:
        return "MEDIUM", "Közepes", "Medium"
    return "LOW", "Alacsony", "Low"


def main():
    with open(MARKET_FILE, "r", encoding="utf-8") as f:
        market = json.load(f)

    with open(INTERPRETATION_FILE, "r", encoding="utf-8") as f:
        interpretation = json.load(f)

    rows = market.get("rows", [])
    rows = [row for row in rows if get_brent(row) is not None]

    if len(rows) < 7:
        raise RuntimeError("Nincs elég market-history adat az előrejelzéshez.")

    brent_values = [get_brent(row) for row in rows]
    risk_score = interpretation.get("combined_risk_score")

    risk_series = [
        row.get("global_trade_risk_index")
        for row in rows
        if row.get("global_trade_risk_index") is not None
    ]

    # Brent forecast 30 napra
    brent_window = brent_values[-60:] if len(brent_values) >= 60 else brent_values
    brent_low, brent_mid, brent_high = linear_forecast(brent_window, 30)

    brent_current = brent_values[-1]
    brent_trend_code, brent_trend_hu, brent_trend_en = classify_trend(brent_current, brent_mid)
    brent_conf_code, brent_conf_hu, brent_conf_en = confidence_label(brent_window)

    # Risk forecast 30 napra
    # Ha nincs elég combined risk history, akkor a global_trade_risk_index idősorát használjuk proxyként,
    # majd az aktuális combined risk pontszám köré igazítjuk.
    risk_window = risk_series[-60:] if len(risk_series) >= 60 else risk_series

    if len(risk_window) >= 7:
        risk_low_raw, risk_mid_raw, risk_high_raw = linear_forecast(risk_window, 30)

        if risk_score is not None and risk_window[-1] is not None:
            adjustment = risk_score - risk_window[-1]
        else:
            adjustment = 0

        risk_low = round(max(0, min(100, risk_low_raw + adjustment)), 1)
        risk_mid = round(max(0, min(100, risk_mid_raw + adjustment)), 1)
        risk_high = round(max(0, min(100, risk_high_raw + adjustment)), 1)
    else:
        risk_low = None
        risk_mid = risk_score
        risk_high = None

    risk_current = risk_score
    risk_trend_code, risk_trend_hu, risk_trend_en = classify_trend(risk_current, risk_mid)
    risk_conf_code, risk_conf_hu, risk_conf_en = confidence_label(risk_window)

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "method_hu": "Egyszerű lineáris trendalapú 30 napos előrejelzés, historikus volatilitási sávval. Nem befektetési ajánlás.",
        "method_en": "Simple linear 30-day trend forecast with a historical volatility band. Not investment advice.",
        "forecast_horizon_days": 30,
        "brent": {
            "current": round(brent_current, 2),
            "forecast_low": brent_low,
            "forecast_mid": brent_mid,
            "forecast_high": brent_high,
            "trend_code": brent_trend_code,
            "trend_hu": brent_trend_hu,
            "trend_en": brent_trend_en,
            "confidence_code": brent_conf_code,
            "confidence_hu": brent_conf_hu,
            "confidence_en": brent_conf_en,
            "unit": "USD/barrel"
        },
        "risk": {
            "current": round(risk_current, 1) if risk_current is not None else None,
            "forecast_low": risk_low,
            "forecast_mid": risk_mid,
            "forecast_high": risk_high,
            "trend_code": risk_trend_code,
            "trend_hu": risk_trend_hu,
            "trend_en": risk_trend_en,
            "confidence_code": risk_conf_code,
            "confidence_hu": risk_conf_hu,
            "confidence_en": risk_conf_en,
            "unit": "index"
        }
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("Energy forecast generated")
    print(f"Brent forecast: {brent_low} - {brent_high}")
    print(f"Risk forecast: {risk_low} - {risk_high}")


if __name__ == "__main__":
    main()
