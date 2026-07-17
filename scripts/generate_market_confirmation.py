#!/usr/bin/env python3

"""
Market Confirmation Layer v1

Cél:
- Az OMPI-tól elkülönülő piaci megerősítési réteg létrehozása.
- A modell kizárólag megfigyelt piaci ár- és momentumadatokat használ.
- Az OMPI pontszámát nem módosítja.

Bemeneti fájlok:
- docs/data/energy-market.json
  vagy
- docs/data/energy_dashboard.json
  vagy
- docs/data/market_data.json

- docs/data/ompi.json

Kimeneti fájlok:
- docs/data/market-confirmation.json
- docs/data/market-confirmation-history.json
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "docs" / "data"

OMPI_PATH = DATA_DIR / "ompi.json"

MARKET_CANDIDATE_PATHS = [
    DATA_DIR / "energy-market.json",
    DATA_DIR / "energy_dashboard.json",
    DATA_DIR / "market_data.json",
    DATA_DIR / "energy-data.json",
    ROOT / "energy-market.json",
]

OUTPUT_PATH = DATA_DIR / "market-confirmation.json"
HISTORY_PATH = DATA_DIR / "market-confirmation-history.json"

METHOD_VERSION = "market_confirmation_v1_2026_07"

WEIGHTS = {
    "short_term_momentum": 0.40,
    "medium_term_momentum": 0.30,
    "brent_wti_spread": 0.15,
    "market_stress": 0.15,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_timestamp() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def today_utc() -> str:
    return utc_now().strftime("%Y-%m-%d")


def clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def is_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def safe_float(value: Any, default: float | None = None) -> float | None:
    if is_number(value):
        return float(value)

    if isinstance(value, str):
        cleaned = (
            value.strip()
            .replace(" ", "")
            .replace(",", ".")
            .replace("%", "")
        )

        match = re.search(r"[-+]?\d+(?:\.\d+)?", cleaned)

        if match:
            try:
                result = float(match.group(0))
                if math.isfinite(result):
                    return result
            except ValueError:
                pass

    return default


def load_json(path: Path, required: bool = True) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Hiányzó fájl: {path}")
        return {}

    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Hibás JSON-fájl: {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise RuntimeError(f"A JSON gyökérelem nem objektum: {path}")

    return data


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def find_market_path() -> Path:
    for path in MARKET_CANDIDATE_PATHS:
        if path.exists():
            return path

    raise FileNotFoundError(
        "Nem található a piaci adatfájl. A támogatott fájlnevek: "
        + ", ".join(str(path.relative_to(ROOT)) for path in MARKET_CANDIDATE_PATHS)
    )


def validate_weights() -> None:
    total = sum(WEIGHTS.values())

    if abs(total - 1.0) > 0.000001:
        raise RuntimeError(f"A súlyok összege nem 1.0: {total}")


def get_nested_number(
    data: dict[str, Any],
    path: list[str],
) -> float | None:
    current: Any = data

    for key in path:
        if not isinstance(current, dict):
            return None

        current = current.get(key)

    return safe_float(current)


def extract_market_values(data: dict[str, Any]) -> dict[str, float | None]:
    market = data.get("market")
    market_stress = data.get("market_stress")

    if not isinstance(market, dict):
        market = {}

    if not isinstance(market_stress, dict):
        market_stress = {}

    observed_values = market_stress.get("observed_values")

    if not isinstance(observed_values, dict):
        observed_values = {}

    brent = (
        safe_float(observed_values.get("brent"))
        or safe_float(market.get("brent"))
        or get_nested_number(data, ["spot", "brent"])
    )

    wti = (
        safe_float(observed_values.get("wti"))
        or safe_float(market.get("wti"))
        or get_nested_number(data, ["spot", "wti"])
    )

    brent_1d = (
        safe_float(observed_values.get("brent_1d_change"))
        if observed_values.get("brent_1d_change") is not None
        else safe_float(market.get("brent_1d_change"))
    )

    brent_7d = (
        safe_float(observed_values.get("brent_7d_change"))
        if observed_values.get("brent_7d_change") is not None
        else safe_float(market.get("brent_7d_change"))
    )

    brent_30d = safe_float(market.get("brent_30d_trend"))

    spread = (
        safe_float(observed_values.get("brent_wti_spread"))
        if observed_values.get("brent_wti_spread") is not None
        else None
    )

    if spread is None and brent is not None and wti is not None:
        spread = brent - wti

    stress_score = safe_float(market_stress.get("score"))

    return {
        "brent": brent,
        "wti": wti,
        "brent_1d_change_pct": brent_1d,
        "brent_7d_change_pct": brent_7d,
        "brent_30d_change_pct": brent_30d,
        "brent_wti_spread_usd": spread,
        "market_stress_score": stress_score,
    }


def score_change(change_pct: float | None, multiplier: float) -> float:
    if change_pct is None:
        return 50.0

    return clamp(50.0 + change_pct * multiplier)


def build_short_term_component(values: dict[str, float | None]) -> dict[str, Any]:
    one_day = values["brent_1d_change_pct"]
    seven_day = values["brent_7d_change_pct"]

    one_day_score = score_change(one_day, 2.0)
    seven_day_score = score_change(seven_day, 1.5)

    score = one_day_score * 0.40 + seven_day_score * 0.60
    weight = WEIGHTS["short_term_momentum"]

    available = sum(value is not None for value in (one_day, seven_day))

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "brent_1d_change_pct": one_day,
        "brent_7d_change_pct": seven_day,
        "one_day_score": round(one_day_score, 1),
        "seven_day_score": round(seven_day_score, 1),
        "data_quality": "AVAILABLE" if available == 2 else "PARTIAL",
        "method": "40pct_1d_plus_60pct_7d",
    }


def build_medium_term_component(values: dict[str, float | None]) -> dict[str, Any]:
    thirty_day = values["brent_30d_change_pct"]

    score = score_change(thirty_day, 1.25)
    weight = WEIGHTS["medium_term_momentum"]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "brent_30d_change_pct": thirty_day,
        "data_quality": "AVAILABLE" if thirty_day is not None else "MISSING_FALLBACK",
        "method": "50_plus_30d_change_times_1_25",
    }


def build_spread_component(values: dict[str, float | None]) -> dict[str, Any]:
    spread = values["brent_wti_spread_usd"]

    if spread is None:
        score = 50.0
        quality = "MISSING_FALLBACK"
    else:
        score = clamp(50.0 + spread * 4.0)
        quality = "AVAILABLE"

    weight = WEIGHTS["brent_wti_spread"]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "brent_wti_spread_usd": (
            round(spread, 2) if spread is not None else None
        ),
        "data_quality": quality,
        "method": "50_plus_spread_times_4",
    }


def build_market_stress_component(values: dict[str, float | None]) -> dict[str, Any]:
    raw_score = values["market_stress_score"]

    if raw_score is None:
        score = 50.0
        quality = "MISSING_FALLBACK"
    else:
        score = clamp(raw_score)
        quality = "AVAILABLE"

    weight = WEIGHTS["market_stress"]

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": weight * 100,
        "contribution": round(score * weight, 2),
        "source_market_stress_score": raw_score,
        "data_quality": quality,
        "method": "direct_market_stress_score",
    }


def classify_market_score(score: float) -> dict[str, str]:
    if score >= 70:
        return {
            "trend": "STRONGLY_BULLISH",
            "trend_hu": "Erősen felfelé mutató",
            "trend_en": "Strongly bullish",
        }

    if score >= 57:
        return {
            "trend": "BULLISH",
            "trend_hu": "Felfelé mutató",
            "trend_en": "Bullish",
        }

    if score > 43:
        return {
            "trend": "NEUTRAL",
            "trend_hu": "Semleges",
            "trend_en": "Neutral",
        }

    if score > 30:
        return {
            "trend": "BEARISH",
            "trend_hu": "Lefelé mutató",
            "trend_en": "Bearish",
        }

    return {
        "trend": "STRONGLY_BEARISH",
        "trend_hu": "Erősen lefelé mutató",
        "trend_en": "Strongly bearish",
    }


def compare_with_ompi(
    market_score: float,
    ompi_score: float | None,
) -> dict[str, str]:
    if ompi_score is None:
        return {
            "relationship": "OMPI_UNAVAILABLE",
            "relationship_hu": "Az OMPI nem érhető el",
            "relationship_en": "OMPI unavailable",
        }

    ompi_bullish = ompi_score >= 57
    ompi_bearish = ompi_score <= 43
    market_bullish = market_score >= 57
    market_bearish = market_score <= 43

    if ompi_bullish and market_bullish:
        relationship = "CONFIRMED"
        hu = "A piac megerősíti a fundamentális felfelé mutató jelzést."
        en = "The market confirms the bullish fundamental signal."

    elif ompi_bearish and market_bearish:
        relationship = "CONFIRMED"
        hu = "A piac megerősíti a fundamentális lefelé mutató jelzést."
        en = "The market confirms the bearish fundamental signal."

    elif (
        (ompi_bullish and market_bearish)
        or (ompi_bearish and market_bullish)
    ):
        relationship = "DIVERGENCE"
        hu = "A piaci ármozgás és a fundamentális jelzés ellentétes irányú."
        en = "Market price action and the fundamental signal point in opposite directions."

    elif (
        (ompi_bullish and 43 < market_score < 57)
        or (ompi_bearish and 43 < market_score < 57)
    ):
        relationship = "PARTIALLY_CONFIRMED"
        hu = "A piac még nem igazolja teljesen a fundamentális jelzést."
        en = "The market does not yet fully confirm the fundamental signal."

    else:
        relationship = "NEUTRAL"
        hu = "A piac és a fundamentális jelzés közötti kapcsolat semleges."
        en = "The relationship between market action and fundamentals is neutral."

    return {
        "relationship": relationship,
        "relationship_hu": hu,
        "relationship_en": en,
    }


def calculate_data_quality(
    components: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    missing: list[str] = []
    partial: list[str] = []

    penalty = 0.0

    for name, component in components.items():
        quality = str(component.get("data_quality", "UNKNOWN")).upper()
        weight = safe_float(component.get("weight"), 0.0) or 0.0

        if "MISSING" in quality:
            missing.append(name)
            penalty += weight * 50.0
        elif "PARTIAL" in quality:
            partial.append(name)
            penalty += weight * 20.0

    confidence = clamp(100.0 - penalty)

    if missing:
        status = "PARTIAL"
    elif partial:
        status = "PARTIAL"
    else:
        status = "GOOD"

    return {
        "status": status,
        "confidence_score": round(confidence, 1),
        "missing_components": missing,
        "partial_components": partial,
    }


def load_history() -> list[dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []

    try:
        with HISTORY_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return []

    if isinstance(data, dict) and isinstance(data.get("history"), list):
        return [
            row
            for row in data["history"]
            if isinstance(row, dict)
        ]

    if isinstance(data, list):
        return [
            row
            for row in data
            if isinstance(row, dict)
        ]

    return []


def update_history(output: dict[str, Any]) -> dict[str, Any]:
    history = load_history()

    row = {
        "date": today_utc(),
        "generated_at": output["generated_at"],
        "score": output["score"],
        "trend": output["trend"],
        "relationship": output["ompi_comparison"]["relationship"],
        "ompi_score": output["ompi_comparison"].get("ompi_score"),
        "data_quality": output["data_quality"]["status"],
    }

    history = [
        item
        for item in history
        if item.get("date") != row["date"]
    ]

    history.append(row)
    history.sort(key=lambda item: str(item.get("date", "")))
    history = history[-1095:]

    return {
        "generated_at": output["generated_at"],
        "index": "MARKET_CONFIRMATION",
        "method_version": METHOD_VERSION,
        "history": history,
    }


def main() -> None:
    validate_weights()

    market_path = find_market_path()
    market_data = load_json(market_path)
    ompi_data = load_json(OMPI_PATH, required=False)

    values = extract_market_values(market_data)

    components = {
        "short_term_momentum": build_short_term_component(values),
        "medium_term_momentum": build_medium_term_component(values),
        "brent_wti_spread": build_spread_component(values),
        "market_stress": build_market_stress_component(values),
    }

    score = round(
        sum(
            safe_float(component.get("contribution"), 0.0) or 0.0
            for component in components.values()
        ),
        1,
    )

    trend_data = classify_market_score(score)

    ompi_score = safe_float(ompi_data.get("score"))
    comparison = compare_with_ompi(score, ompi_score)
    comparison["ompi_score"] = ompi_score
    comparison["market_confirmation_score"] = score

    data_quality = calculate_data_quality(components)

    output = {
        "generated_at": utc_timestamp(),
        "index": "MARKET_CONFIRMATION",
        "index_name": "Oil Market Confirmation Layer",
        "method_version": METHOD_VERSION,
        "score": score,
        **trend_data,
        "weights": WEIGHTS,
        "observed_market_values": {
            key: (
                round(value, 4)
                if isinstance(value, float)
                else value
            )
            for key, value in values.items()
        },
        "components": components,
        "ompi_comparison": comparison,
        "data_quality": data_quality,
        "source_file": str(market_path.relative_to(ROOT)),
        "model_notes": {
            "included_in_ompi": False,
            "fundamental_inputs_excluded": True,
            "price_data_only": False,
            "uses_existing_market_stress": True,
            "does_not_modify_ompi": True,
        },
        "summary_hu": (
            f"A piaci megerősítési mutató értéke {score:.1f}/100, "
            f"ami {trend_data['trend_hu'].lower()} piaci trendet jelez. "
            f"{comparison['relationship_hu']}"
        ),
        "summary_en": (
            f"The market confirmation score is {score:.1f}/100, "
            f"indicating a {trend_data['trend_en'].lower()} market trend. "
            f"{comparison['relationship_en']}"
        ),
    }

    history_output = update_history(output)

    write_json(OUTPUT_PATH, output)
    write_json(HISTORY_PATH, history_output)

    print("=" * 72)
    print("Market Confirmation Layer generálás sikeres")
    print("=" * 72)
    print(f"Forrás: {market_path.relative_to(ROOT)}")
    print(f"Pontszám: {score}/100")
    print(f"Trend: {output['trend']}")
    print(f"OMPI-kapcsolat: {comparison['relationship']}")
    print(f"Adatbizalom: {data_quality['status']} ({data_quality['confidence_score']}/100)")
    print(f"Létrehozva: {OUTPUT_PATH.relative_to(ROOT)}")
    print(f"Létrehozva: {HISTORY_PATH.relative_to(ROOT)}")
    print("=" * 72)


if __name__ == "__main__":
    main()
