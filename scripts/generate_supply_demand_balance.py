import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

MARKET_FILE = ROOT / "market-history.json"
CHINA_FILE = ROOT / "china-oil-import.json"
USA_FILE = ROOT / "usa-oil-revenue.json"
INVENTORY_FILE = ROOT / "docs" / "data" / "inventory_stress.json"
INTERPRETATION_FILE = ROOT / "docs" / "data" / "market_interpretation.json"

OUTPUT_FILE = ROOT / "docs" / "data" / "supply_demand_balance.json"


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def pct_change(old, new):
    if old is None or new is None or old == 0:
        return 0
    return ((new - old) / old) * 100


def level_from_score(score):
    if score < 35:
        return "LOW", "Alacsony", "Low"
    if score < 65:
        return "MEDIUM", "Közepes", "Medium"
    return "HIGH", "Magas", "High"


def get_brent(row):
    return row.get("market_brent") if row.get("market_brent") is not None else row.get("brent")


def pressure_direction(balance_score):
    if balance_score > 10:
        return "BULLISH", "Árfelhajtó", "Bullish"
    if balance_score < -10:
        return "BEARISH", "Árnyomás alatt", "Bearish"
    return "NEUTRAL", "Semleges", "Neutral"


def main():
    with open(MARKET_FILE, "r", encoding="utf-8") as f:
        market = json.load(f)

    with open(CHINA_FILE, "r", encoding="utf-8") as f:
        china = json.load(f)

    with open(USA_FILE, "r", encoding="utf-8") as f:
        usa = json.load(f)

    with open(INVENTORY_FILE, "r", encoding="utf-8") as f:
        inventory = json.load(f)

    with open(INTERPRETATION_FILE, "r", encoding="utf-8") as f:
        interpretation = json.load(f)

    market_rows = [
        row for row in market.get("rows", [])
        if get_brent(row) is not None
    ]

    china_series = china.get("series", [])
    usa_series = usa.get("series", [])

    if len(market_rows) < 10 or len(china_series) < 10 or len(usa_series) < 10:
        raise RuntimeError("Nincs elég adat a supply-demand balance számításához.")

    lookback = min(30, len(market_rows) - 1, len(china_series) - 1, len(usa_series) - 1)

    market_latest = market_rows[-1]
    market_old = market_rows[-1 - lookback]

    china_latest = china_series[-1]
    china_old = china_series[-1 - lookback]

    usa_latest = usa_series[-1]
    usa_old = usa_series[-1 - lookback]

    brent_latest = get_brent(market_latest)
    brent_old = get_brent(market_old)

    brent_change = pct_change(brent_old, brent_latest)

    china_volume_change = pct_change(
        china_old.get("estimated_import_volume_mbd"),
        china_latest.get("estimated_import_volume_mbd")
    )

    usa_value_change = pct_change(
        usa_old.get("estimated_revenue_billion_usd"),
        usa_latest.get("estimated_revenue_billion_usd")
    )

    inventory_stress = inventory.get("inventory_stress_score", 50)
    risk_score = interpretation.get("combined_risk_score", 50)

    # Demand pressure:
    # Kína importmennyiség-változás + Brent ármozgás + kockázati környezet.
    # A negatív importváltozás csökkenti a keresleti nyomást.
    demand_pressure = (
        clamp(50 + china_volume_change * 5) * 0.45 +
        clamp(50 + brent_change * 2) * 0.25 +
        risk_score * 0.30
    )

    # Supply pressure:
    # magas inventory stress = szűkösebb kínálati oldal
    # magas Brent = piaci szűkösségi jel
    # USA értékváltozás = termelési oldali árhatás
    supply_pressure = (
        inventory_stress * 0.45 +
        clamp(50 + brent_change * 2) * 0.25 +
        clamp(50 + usa_value_change * 2) * 0.30
    )

    demand_pressure = round(clamp(demand_pressure), 1)
    supply_pressure = round(clamp(supply_pressure), 1)

    # Pozitív érték: keresleti oldal erősebb vagy szűkösebb piac.
    # Negatív érték: lazább piac.
    balance_score = round(demand_pressure - supply_pressure, 1)

    direction_code, direction_hu, direction_en = pressure_direction(balance_score)
    demand_code, demand_hu, demand_en = level_from_score(demand_pressure)
    supply_code, supply_hu, supply_en = level_from_score(supply_pressure)

    output = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "method_hu": "A supply-demand balance egy proxy indikátor, amely a kínai importmennyiség, a Brent ármozgás, az USA termelési értéke, a készletoldali nyomás és az összesített kockázati pontszám alapján becsüli a piaci egyensúlyt.",
        "method_en": "The supply-demand balance is a proxy indicator using China import volume, Brent price movement, U.S. production value, inventory stress and the combined risk score to estimate market balance.",
        "lookback_days": lookback,
        "supply_pressure": supply_pressure,
        "supply_level": supply_code,
        "supply_level_hu": supply_hu,
        "supply_level_en": supply_en,
        "demand_pressure": demand_pressure,
        "demand_level": demand_code,
        "demand_level_hu": demand_hu,
        "demand_level_en": demand_en,
        "balance_score": balance_score,
        "market_direction": direction_code,
        "market_direction_hu": direction_hu,
        "market_direction_en": direction_en,
        "drivers": {
            "brent_change_pct": round(brent_change, 2),
            "china_import_volume_change_pct": round(china_volume_change, 2),
            "usa_production_value_change_pct": round(usa_value_change, 2),
            "inventory_stress_score": inventory_stress,
            "combined_risk_score": risk_score
        },
        "summary_hu": (
            f"A piaci egyensúly proxy mutatója szerint a keresleti nyomás {demand_pressure}/100, "
            f"a kínálati nyomás {supply_pressure}/100. "
            f"A különbség {balance_score:+.1f} pont, ami {direction_hu.lower()} piaci irányt jelez."
        ),
        "summary_en": (
            f"The market balance proxy indicates demand pressure at {demand_pressure}/100 "
            f"and supply pressure at {supply_pressure}/100. "
            f"The spread is {balance_score:+.1f} points, indicating a {direction_en.lower()} market direction."
        )
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("Supply-demand balance generated")
    print(f"Demand pressure: {demand_pressure}")
    print(f"Supply pressure: {supply_pressure}")
    print(f"Market direction: {direction_code}")


if __name__ == "__main__":
    main()
