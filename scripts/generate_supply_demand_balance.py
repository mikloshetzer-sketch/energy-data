#!/usr/bin/env python3
"""
Rövid távú olajpiaci nyomásindikátor.

A script megőrzi a korábbi supply_demand_balance.json fő mezőit, hogy a
jelenlegi dashboard ne törjön el. Az új módszertan ugyanakkor már külön
kezeli:
  1. a valódi EIA STEO fizikai mérleget;
  2. a rövid távú piaci nyomást.

A régi supply_pressure, demand_pressure és balance_score mezők
kompatibilitási mezők. A következő HTML-frissítés után az új mezőket kell
elsődlegesen megjeleníteni.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

MARKET_FILE = ROOT / "market-history.json"
LIVE_MARKET_FILE = ROOT / "live-market.json"
CHINA_FILE = ROOT / "china-oil-import.json"
INVENTORY_FILE = ROOT / "docs" / "data" / "inventory_stress.json"
INTERPRETATION_FILE = ROOT / "docs" / "data" / "market_interpretation.json"
GLOBAL_BALANCE_FILE = ROOT / "docs" / "data" / "global_oil_balance.json"
OUTPUT_FILE = ROOT / "docs" / "data" / "supply_demand_balance.json"


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default

    try:
        with path.open("r", encoding="utf-8-sig") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return default


def save_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")

    with temporary.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
        file.write("\n")

    temporary.replace(path)


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def to_float(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        cleaned = (
            value.replace("USD/hordó", "")
            .replace("USD/barrel", "")
            .replace("millió hordó/nap", "")
            .replace("mb/d", "")
            .replace("%", "")
            .replace(",", ".")
            .strip()
        )
        try:
            return float(cleaned)
        except ValueError:
            return None

    return None


def pct_change(old: float | None, new: float | None) -> float:
    if old is None or new is None or old == 0:
        return 0.0
    return ((new - old) / old) * 100.0


def get_period(row: dict[str, Any]) -> str | None:
    for key in ("period", "date", "timestamp", "updated"):
        value = row.get(key)
        if value:
            return str(value)[:10]
    return None


def get_brent(row: dict[str, Any]) -> float | None:
    for key in ("market_brent", "live_brent", "brent"):
        value = to_float(row.get(key))
        if value is not None:
            return value
    return None


def latest_and_lookback(
    rows: list[dict[str, Any]],
    value_getter,
    target_days: int = 30,
) -> tuple[float | None, float | None, int]:
    valid: list[tuple[str, float]] = []

    for row in rows:
        value = value_getter(row)
        period = get_period(row)

        if value is not None and period:
            valid.append((period, value))

    if not valid:
        return None, None, 0

    valid.sort(key=lambda item: item[0])
    latest_period, latest_value = valid[-1]

    older = valid[0]
    for item in valid:
        if item[0] <= latest_period:
            older = item
        if len(valid) > target_days and item == valid[-1 - target_days]:
            older = item

    if len(valid) > target_days:
        older = valid[-1 - target_days]

    return latest_value, older[1], min(target_days, len(valid) - 1)


def latest_china_change(china: dict[str, Any]) -> tuple[float, int]:
    rows = china.get("series", [])
    if not isinstance(rows, list):
        return 0.0, 0

    def getter(row: dict[str, Any]) -> float | None:
        for key in (
            "import_volume_mbd",
            "estimated_import_volume_mbd",
            "volume_mbd",
            "value",
        ):
            value = to_float(row.get(key))
            if value is not None:
                return value
        return None

    latest, old, lookback = latest_and_lookback(rows, getter, 30)
    return pct_change(old, latest), lookback


def get_inventory_score(inventory: dict[str, Any]) -> float:
    for key in (
        "inventory_stress_score",
        "score",
        "stress_score",
    ):
        value = to_float(inventory.get(key))
        if value is not None:
            return clamp(value)
    return 50.0


def get_risk_score(
    interpretation: dict[str, Any],
    live_market: dict[str, Any],
) -> float:
    candidates = [
        interpretation.get("combined_risk_score"),
        interpretation.get("risk_score"),
        live_market.get("risk", {}).get("middle_east_conflict_impact"),
        live_market.get("risk", {}).get("global_trade_risk_index"),
    ]

    for candidate in candidates:
        value = to_float(candidate)
        if value is not None:
            return clamp(value)

    return 50.0


def latest_brent_change(
    market: dict[str, Any],
    live_market: dict[str, Any],
) -> tuple[float, int]:
    rows = market.get("rows", [])
    if isinstance(rows, list) and rows:
        latest, old, lookback = latest_and_lookback(rows, get_brent, 30)
        change = pct_change(old, latest)
        if latest is not None:
            return change, lookback

    current = to_float(
        live_market.get("prices", {}).get(
            "market_brent",
            live_market.get("prices", {}).get("live_brent"),
        )
    )
    return (0.0 if current is not None else 0.0), 0


def fundamental_pressure(balance_mbd: float) -> float:
    """
    A -2.0 ... +2.0 mb/d tartományt 100 ... 0 pontra fordítja.
    Negatív fizikai mérleg = magasabb felfelé mutató piaci nyomás.
    """
    return clamp(50.0 - balance_mbd * 25.0)


def momentum_score(change_pct: float, sensitivity: float) -> float:
    return clamp(50.0 + change_pct * sensitivity)


def classify_pressure(score: float) -> tuple[str, str, str]:
    if score < 35:
        return "loose", "Laza piac", "Loose market"
    if score < 55:
        return "balanced", "Kiegyensúlyozott", "Balanced"
    if score < 70:
        return "tight", "Feszes piac", "Tight market"
    return "high_pressure", "Erős felfelé mutató nyomás", "Strong upward pressure"


def direction_from_score(score: float) -> tuple[str, str, str]:
    if score >= 60:
        return "BULLISH", "Árfelhajtó", "Bullish"
    if score <= 40:
        return "BEARISH", "Árcsökkentő", "Bearish"
    return "NEUTRAL", "Semleges", "Neutral"


def level_from_score(score: float) -> tuple[str, str, str]:
    if score < 35:
        return "LOW", "Alacsony", "Low"
    if score < 65:
        return "MEDIUM", "Közepes", "Medium"
    return "HIGH", "Magas", "High"


def main() -> None:
    market = load_json(MARKET_FILE, {})
    live_market = load_json(LIVE_MARKET_FILE, {})
    china = load_json(CHINA_FILE, {})
    inventory = load_json(INVENTORY_FILE, {})
    interpretation = load_json(INTERPRETATION_FILE, {})
    global_balance = load_json(GLOBAL_BALANCE_FILE, {})

    latest_fundamental = global_balance.get("latest", {})
    balance_mbd = to_float(
        latest_fundamental.get(
            "balance_mbd",
            global_balance.get("balance_mbd"),
        )
    )

    if balance_mbd is None:
        raise RuntimeError(
            "Hiányzik a docs/data/global_oil_balance.json fizikai mérlege."
        )

    supply_mbd = to_float(
        latest_fundamental.get(
            "global_supply_mbd",
            global_balance.get("global_supply_mbd"),
        )
    )
    demand_mbd = to_float(
        latest_fundamental.get(
            "global_demand_mbd",
            global_balance.get("global_demand_mbd"),
        )
    )
    balance_period = (
        latest_fundamental.get("period")
        or global_balance.get("period")
    )

    china_change, china_lookback = latest_china_change(china)
    brent_change, brent_lookback = latest_brent_change(market, live_market)
    inventory_score = get_inventory_score(inventory)
    risk_score = get_risk_score(interpretation, live_market)

    components = {
        "fundamental_balance": round(fundamental_pressure(balance_mbd), 1),
        "inventory_tightness": round(inventory_score, 1),
        "china_import_momentum": round(momentum_score(china_change, 4.0), 1),
        "brent_momentum": round(momentum_score(brent_change, 2.0), 1),
        "geopolitical_supply_risk": round(risk_score, 1),
    }

    weights = {
        "fundamental_balance": 0.55,
        "inventory_tightness": 0.20,
        "china_import_momentum": 0.10,
        "brent_momentum": 0.05,
        "geopolitical_supply_risk": 0.10,
    }

    pressure_score = round(
        sum(components[key] * weights[key] for key in weights),
        1,
    )

    tightness_code, tightness_hu, tightness_en = classify_pressure(pressure_score)
    direction_code, direction_hu, direction_en = direction_from_score(pressure_score)

    # Kompatibilitási mezők a jelenlegi HTML-hez.
    # Nem önálló fizikai demand/supply becslések.
    demand_pressure = round(
        clamp(
            50.0
            + (components["china_import_momentum"] - 50.0) * 0.70
            + (components["brent_momentum"] - 50.0) * 0.30
        ),
        1,
    )
    supply_pressure = round(
        clamp(
            100.0
            - (
                components["fundamental_balance"] * 0.70
                + components["inventory_tightness"] * 0.20
                + components["geopolitical_supply_risk"] * 0.10
            )
        ),
        1,
    )
    legacy_balance_score = round((pressure_score - 50.0) * 2.0, 1)

    demand_level, demand_level_hu, demand_level_en = level_from_score(
        demand_pressure
    )
    supply_level, supply_level_hu, supply_level_en = level_from_score(
        supply_pressure
    )

    generated_at = datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")

    summary_hu = (
        f"Az EIA STEO fizikai mérlege {balance_period} időszakra "
        f"{balance_mbd:+.2f} millió hordó/nap. A fundamentális mérleg, "
        f"a készlethelyzet és a rövid távú piaci jelek alapján az "
        f"összesített nyomás {pressure_score:.1f}/100, ami "
        f"{tightness_hu.lower()} állapotot jelez."
    )

    summary_en = (
        f"The EIA STEO physical balance for {balance_period} is "
        f"{balance_mbd:+.2f} million barrels per day. Based on the "
        f"fundamental balance, inventories and short-term signals, the "
        f"combined pressure score is {pressure_score:.1f}/100, indicating "
        f"a {tightness_en.lower()} condition."
    )

    output = {
        "generated_at": generated_at,
        "meta": {
            "generated_at": generated_at,
            "generator": "scripts/generate_supply_demand_balance.py",
            "generator_version": "2.0.0",
            "indicator_type": "short_term_oil_market_pressure",
            "method_version": "pressure_v2_with_eia_physical_balance",
            "compatibility_fields_retained": True,
        },
        "indicator_type": "short_term_oil_market_pressure",
        "short_term_pressure_score": pressure_score,
        "market_tightness": tightness_code,
        "market_tightness_hu": tightness_hu,
        "market_tightness_en": tightness_en,
        "market_direction": direction_code,
        "market_direction_hu": direction_hu,
        "market_direction_en": direction_en,
        "fundamental_balance": {
            "period": balance_period,
            "global_supply_mbd": supply_mbd,
            "global_demand_mbd": demand_mbd,
            "balance_mbd": round(balance_mbd, 3),
            "balance_state": global_balance.get("balance_state"),
            "source": "EIA STEO",
        },
        "component_scores": components,
        "component_weights": weights,
        "drivers": {
            "physical_balance_mbd": round(balance_mbd, 3),
            "brent_change_pct": round(brent_change, 2),
            "china_import_volume_change_pct": round(china_change, 2),
            "inventory_stress_score": round(inventory_score, 1),
            "combined_risk_score": round(risk_score, 1),
            "brent_lookback_observations": brent_lookback,
            "china_lookback_observations": china_lookback,
            # Régi kulcs kompatibilitás miatt marad, de nincs használva.
            "usa_production_value_change_pct": None,
        },
        "data_freshness": {
            "fundamental_period": balance_period,
            "brent_lookback_observations": brent_lookback,
            "china_lookback_observations": china_lookback,
        },
        "method_hu": (
            "A rövid távú olajpiaci nyomásindikátor elsődleges alapja az "
            "EIA STEO globális fizikai kínálat–keresleti mérlege. Ezt az "
            "amerikai készlethelyzet, a kínai importmomentum, a Brent "
            "ármomentum és a geopolitikai ellátási kockázat egészíti ki. "
            "A mutató nem árfolyam-előrejelzés."
        ),
        "method_en": (
            "The short-term oil-market pressure indicator is anchored in "
            "the EIA STEO global physical supply-demand balance. It is "
            "supplemented by U.S. inventory conditions, China import "
            "momentum, Brent price momentum and geopolitical supply risk. "
            "The indicator is not a price forecast."
        ),
        "summary_hu": summary_hu,
        "summary_en": summary_en,
        # Régi mezők: a jelenlegi index.html kompatibilitása miatt.
        "lookback_days": max(brent_lookback, china_lookback),
        "supply_pressure": supply_pressure,
        "supply_level": supply_level,
        "supply_level_hu": supply_level_hu,
        "supply_level_en": supply_level_en,
        "demand_pressure": demand_pressure,
        "demand_level": demand_level,
        "demand_level_hu": demand_level_hu,
        "demand_level_en": demand_level_en,
        "balance_score": legacy_balance_score,
    }

    save_json_atomic(OUTPUT_FILE, output)

    print(f"{OUTPUT_FILE.relative_to(ROOT)} frissítve.")
    print(
        f"Physical balance: {balance_mbd:+.3f} mb/d | "
        f"Pressure: {pressure_score:.1f}/100 | "
        f"State: {tightness_code}"
    )


if __name__ == "__main__":
    main()

