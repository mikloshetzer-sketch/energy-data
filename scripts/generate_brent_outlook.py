#!/usr/bin/env python3
"""
Brent Outlook v2 generator.

Purpose
-------
Creates a 30-day Brent scenario range from:
- OMPI fundamental pressure
- Brent price momentum
- Brent trend structure
- realised Brent volatility

The script does NOT recalculate OMPI.

Default inputs
--------------
- docs/data/ompi.json
- live-market.json OR docs/data/live-market.json
- market-history.json OR docs/data/market-history.json

Outputs
-------
- docs/data/brent-outlook.json
- docs/data/brent-outlook-history.json

Only Python standard-library modules are required.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence


MODEL_VERSION = "2.0"
HORIZON_DAYS = 30

FUNDAMENTAL_MAX_SHIFT = 0.06
MOMENTUM_MAX_SHIFT = 0.04
TREND_MAX_SHIFT = 0.02
TOTAL_MAX_SHIFT = 0.12

MIN_VOLATILITY_BAND = 0.05
MAX_VOLATILITY_BAND = 0.18

MIN_HISTORY_FOR_VALID = 21
MIN_HISTORY_FOR_FULL_MODEL = 61


@dataclass(frozen=True)
class PricePoint:
    timestamp: datetime
    price: float


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def round_or_none(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Input file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Invalid JSON in {path}: line {exc.lineno}, column {exc.colno}"
        ) from exc


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")

    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    temporary.replace(path)


def resolve_existing_path(
    explicit_path: Optional[str],
    candidates: Sequence[Path],
    label: str,
) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if path.exists():
            return path
        raise RuntimeError(f"{label} file not found: {path}")

    for candidate in candidates:
        if candidate.exists():
            return candidate

    checked = ", ".join(str(path) for path in candidates)
    raise RuntimeError(f"{label} file not found. Checked: {checked}")


def normalise_key(value: Any) -> str:
    return (
        str(value)
        .strip()
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
        .replace("/", "_")
    )


def parse_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None

    if isinstance(value, str):
        cleaned = (
            value.strip()
            .replace("$", "")
            .replace("USD", "")
            .replace("usd", "")
            .replace(",", "")
            .replace("%", "")
        )
        if not cleaned:
            return None
        try:
            number = float(cleaned)
        except ValueError:
            return None
        return number if math.isfinite(number) else None

    return None


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000.0
        try:
            parsed = datetime.fromtimestamp(raw, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None

        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"

        parsed = None
        for parser in (
            lambda item: datetime.fromisoformat(item),
            lambda item: datetime.strptime(item, "%Y-%m-%d"),
            lambda item: datetime.strptime(item, "%d/%m/%Y"),
            lambda item: datetime.strptime(item, "%m/%d/%Y"),
        ):
            try:
                parsed = parser(raw)
                break
            except ValueError:
                continue

        if parsed is None:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def iter_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from iter_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_dicts(child)


def first_numeric_by_keys(
    payload: Any,
    preferred_keys: Sequence[str],
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> Optional[float]:
    keys = {normalise_key(key) for key in preferred_keys}

    for item in iter_dicts(payload):
        for key, raw_value in item.items():
            if normalise_key(key) not in keys:
                continue

            number = parse_float(raw_value)
            if number is None:
                continue
            if minimum is not None and number < minimum:
                continue
            if maximum is not None and number > maximum:
                continue
            return number

    return None


def extract_ompi_score(payload: Any) -> float:
    preferred_keys = (
        "score",
        "ompi_score",
        "index",
        "index_value",
        "value",
        "current_score",
        "latest_score",
    )

    # Prefer dictionaries explicitly associated with OMPI.
    for item in iter_dicts(payload):
        item_keys = {normalise_key(key) for key in item}
        item_text = " ".join(str(value).lower() for value in item.values() if isinstance(value, str))
        looks_like_ompi = (
            "ompi" in item_text
            or "ompi_score" in item_keys
            or normalise_key(item.get("dataset", "")) == "ompi"
        )
        if not looks_like_ompi:
            continue

        score = first_numeric_by_keys(item, preferred_keys, minimum=0, maximum=100)
        if score is not None:
            return score

    score = first_numeric_by_keys(payload, preferred_keys, minimum=0, maximum=100)
    if score is None:
        raise RuntimeError("Could not find a valid OMPI score between 0 and 100.")
    return score


DATE_KEYS = (
    "date",
    "datetime",
    "timestamp",
    "time",
    "as_of",
    "as_of_date",
    "observed_at",
    "generated_at",
    "period",
)

PRICE_KEYS = (
    "price",
    "value",
    "close",
    "last",
    "last_price",
    "current",
    "current_price",
    "price_usd",
    "usd",
    "brent",
    "brent_price",
    "brent_usd",
    "brent_close",
)


def dict_mentions_brent(item: dict[str, Any]) -> bool:
    for key, value in item.items():
        key_name = normalise_key(key)

        if "brent" in key_name:
            return True

        if isinstance(value, str) and "brent" in value.lower():
            return True

    return False


def extract_datetime_from_dict(item: dict[str, Any]) -> Optional[datetime]:
    normalised = {normalise_key(key): value for key, value in item.items()}

    for key in DATE_KEYS:
        if key in normalised:
            parsed = parse_datetime(normalised[key])
            if parsed is not None:
                return parsed

    return None


def extract_price_from_dict(
    item: dict[str, Any],
    require_brent_context: bool,
) -> Optional[float]:
    normalised = {normalise_key(key): value for key, value in item.items()}
    mentions_brent = dict_mentions_brent(item)

    # Highest priority: a key whose name explicitly contains "brent".
    for key, raw_value in normalised.items():
        if "brent" not in key:
            continue

        direct = parse_float(raw_value)
        if direct is not None and direct > 0:
            return direct

        if isinstance(raw_value, dict):
            nested = extract_price_from_dict(raw_value, require_brent_context=False)
            if nested is not None:
                return nested

    if require_brent_context and not mentions_brent:
        return None

    for key in PRICE_KEYS:
        if key not in normalised:
            continue

        number = parse_float(normalised[key])
        if number is not None and number > 0:
            return number

    return None


def collect_brent_points(payload: Any, require_brent_context: bool = True) -> list[PricePoint]:
    points: list[PricePoint] = []

    for item in iter_dicts(payload):
        timestamp = extract_datetime_from_dict(item)
        if timestamp is None:
            continue

        price = extract_price_from_dict(item, require_brent_context=require_brent_context)
        if price is None:
            continue

        points.append(PricePoint(timestamp=timestamp, price=price))

    return points


def deduplicate_points(points: Iterable[PricePoint]) -> list[PricePoint]:
    by_date: dict[str, PricePoint] = {}

    for point in points:
        day_key = point.timestamp.date().isoformat()
        existing = by_date.get(day_key)
        if existing is None or point.timestamp >= existing.timestamp:
            by_date[day_key] = point

    return sorted(by_date.values(), key=lambda point: point.timestamp)


def extract_live_brent(payload: Any) -> PricePoint:
    candidates: list[PricePoint] = []

    for item in iter_dicts(payload):
        if not dict_mentions_brent(item):
            continue

        price = extract_price_from_dict(item, require_brent_context=False)
        if price is None:
            continue

        timestamp = extract_datetime_from_dict(item) or utc_now()
        candidates.append(PricePoint(timestamp=timestamp, price=price))

    if candidates:
        return max(candidates, key=lambda point: point.timestamp)

    # Fallback for a file that contains only one commodity and uses generic keys.
    price = first_numeric_by_keys(
        payload,
        (
            "brent",
            "brent_price",
            "brent_usd",
            "current_price",
            "price_usd",
            "price",
            "close",
            "last",
            "value",
        ),
        minimum=1,
        maximum=1000,
    )

    if price is None:
        raise RuntimeError("Could not find a valid live Brent price.")

    timestamp: Optional[datetime] = None
    for item in iter_dicts(payload):
        timestamp = extract_datetime_from_dict(item)
        if timestamp is not None:
            break

    return PricePoint(timestamp=timestamp or utc_now(), price=price)


def extract_history_points(payload: Any) -> list[PricePoint]:
    points = collect_brent_points(payload, require_brent_context=True)

    if len(points) < 2:
        # Fallback for Brent-only history files with generic field names.
        points = collect_brent_points(payload, require_brent_context=False)

    return deduplicate_points(points)


def merge_live_with_history(
    history: Sequence[PricePoint],
    live: PricePoint,
) -> list[PricePoint]:
    return deduplicate_points([*history, live])


def percentage_change(points: Sequence[PricePoint], periods: int) -> Optional[float]:
    if len(points) <= periods:
        return None

    latest = points[-1].price
    previous = points[-1 - periods].price

    if previous <= 0:
        return None

    return latest / previous - 1.0


def moving_average(points: Sequence[PricePoint], periods: int) -> Optional[float]:
    if len(points) < periods:
        return None

    values = [point.price for point in points[-periods:]]
    return statistics.fmean(values)


def daily_returns(points: Sequence[PricePoint]) -> list[float]:
    returns: list[float] = []

    for previous, current in zip(points, points[1:]):
        if previous.price <= 0 or current.price <= 0:
            continue
        returns.append(current.price / previous.price - 1.0)

    return returns


def realised_volatility_band(
    points: Sequence[PricePoint],
    lookback_returns: int = 20,
    horizon_days: int = HORIZON_DAYS,
) -> tuple[float, Optional[float]]:
    returns = daily_returns(points)

    if len(returns) < 2:
        return MIN_VOLATILITY_BAND, None

    sample = returns[-lookback_returns:]
    daily_std = statistics.stdev(sample) if len(sample) >= 2 else 0.0
    raw_band = daily_std * math.sqrt(horizon_days)
    bounded_band = clamp(raw_band, MIN_VOLATILITY_BAND, MAX_VOLATILITY_BAND)

    return bounded_band, daily_std


def calculate_fundamental_shift(ompi_score: float) -> float:
    return clamp(
        ((ompi_score - 50.0) / 50.0) * FUNDAMENTAL_MAX_SHIFT,
        -FUNDAMENTAL_MAX_SHIFT,
        FUNDAMENTAL_MAX_SHIFT,
    )


def calculate_momentum_shift(
    change_5d: Optional[float],
    change_20d: Optional[float],
    change_60d: Optional[float],
) -> tuple[float, float, float]:
    components = (
        (change_5d, 0.25),
        (change_20d, 0.45),
        (change_60d, 0.30),
    )

    available = [(value, weight) for value, weight in components if value is not None]
    if not available:
        return 0.0, 0.0, 0.0

    available_weight = sum(weight for _, weight in available)
    weighted_return = sum(value * weight for value, weight in available) / available_weight

    # A weighted 10% price move maps to the maximum ±4% outlook adjustment.
    normalised_signal = clamp(weighted_return / 0.10, -1.0, 1.0)
    shift = normalised_signal * MOMENTUM_MAX_SHIFT

    return shift, weighted_return, available_weight


def calculate_trend_shift(
    current_price: float,
    ma20: Optional[float],
    ma60: Optional[float],
) -> tuple[float, str]:
    if ma20 is None:
        return 0.0, "INSUFFICIENT_DATA"

    if ma60 is None:
        if current_price > ma20:
            return TREND_MAX_SHIFT * 0.5, "SHORT_TERM_UPTREND"
        if current_price < ma20:
            return -TREND_MAX_SHIFT * 0.5, "SHORT_TERM_DOWNTREND"
        return 0.0, "FLAT"

    if current_price > ma20 > ma60:
        return TREND_MAX_SHIFT, "STRONG_UPTREND"

    if current_price < ma20 < ma60:
        return -TREND_MAX_SHIFT, "STRONG_DOWNTREND"

    if current_price > ma20 and current_price > ma60:
        return TREND_MAX_SHIFT * 0.5, "MIXED_BULLISH"

    if current_price < ma20 and current_price < ma60:
        return -TREND_MAX_SHIFT * 0.5, "MIXED_BEARISH"

    return 0.0, "SIDEWAYS_OR_MIXED"


def classify_direction(total_shift: float) -> str:
    if total_shift >= 0.08:
        return "STRONGLY_BULLISH"
    if total_shift >= 0.02:
        return "BULLISH"
    if total_shift <= -0.08:
        return "STRONGLY_BEARISH"
    if total_shift <= -0.02:
        return "BEARISH"
    return "NEUTRAL"


def classify_range_position(
    current_price: float,
    lower_price: float,
    center_price: float,
    upper_price: float,
) -> str:
    if current_price < lower_price:
        return "BELOW_RANGE"
    if current_price > upper_price:
        return "ABOVE_RANGE"

    lower_midpoint = (lower_price + center_price) / 2.0
    upper_midpoint = (center_price + upper_price) / 2.0

    if current_price < lower_midpoint:
        return "LOWER_HALF"
    if current_price > upper_midpoint:
        return "UPPER_HALF"
    return "NEAR_CENTER"


def sign(value: float, tolerance: float = 0.0025) -> int:
    if value > tolerance:
        return 1
    if value < -tolerance:
        return -1
    return 0


def calculate_confidence(
    fundamental_shift: float,
    momentum_shift: float,
    trend_shift: float,
    volatility_band: float,
    history_count: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    fundamental_sign = sign(fundamental_shift)
    market_sign = sign(momentum_shift + trend_shift)

    if history_count < MIN_HISTORY_FOR_VALID:
        reasons.append("Insufficient Brent history for a stable signal.")
        return "LOW", reasons

    if fundamental_sign != 0 and market_sign != 0 and fundamental_sign != market_sign:
        reasons.append("OMPI and Brent market behaviour point in opposite directions.")
        return "LOW", reasons

    if volatility_band >= 0.15:
        reasons.append("Realised volatility is high, widening the scenario range.")
        return "LOW", reasons

    if history_count < MIN_HISTORY_FOR_FULL_MODEL:
        reasons.append("The model has enough data for a basic outlook but not all 60-day metrics.")
        return "MEDIUM", reasons

    if fundamental_sign == market_sign and fundamental_sign != 0:
        reasons.append("OMPI, momentum and trend broadly confirm the same direction.")
    else:
        reasons.append("The signal is mixed or close to neutral.")

    # Until Market Confirmation is implemented, confidence is capped at MEDIUM.
    reasons.append("Confidence is capped at MEDIUM until Market Confirmation is available.")
    return "MEDIUM", reasons


def history_record_from_outlook(outlook: dict[str, Any]) -> dict[str, Any]:
    return {
        "as_of_date": outlook["current"]["date"],
        "generated_at": outlook["generated_at"],
        "current_price_usd": outlook["current"]["price_usd"],
        "center_price_usd": outlook["outlook"]["center_price_usd"],
        "lower_price_usd": outlook["outlook"]["lower_price_usd"],
        "upper_price_usd": outlook["outlook"]["upper_price_usd"],
        "direction": outlook["outlook"]["direction"],
        "confidence": outlook["outlook"]["confidence"],
        "ompi_score": outlook["signals"]["ompi_score"],
        "total_center_shift_pct": outlook["signals"]["total_center_shift_pct"],
        "volatility_band_pct": outlook["signals"]["volatility_band_pct"],
        "model_version": outlook["methodology"]["model_version"],
    }


def update_history(path: Path, outlook: dict[str, Any]) -> dict[str, Any]:
    if path.exists():
        existing = load_json(path)
    else:
        existing = []

    if isinstance(existing, dict):
        records = existing.get("history") or existing.get("records") or existing.get("data") or []
    elif isinstance(existing, list):
        records = existing
    else:
        records = []

    clean_records = [record for record in records if isinstance(record, dict)]
    new_record = history_record_from_outlook(outlook)
    as_of_date = new_record["as_of_date"]

    replaced = False
    updated: list[dict[str, Any]] = []

    for record in clean_records:
        if str(record.get("as_of_date", "")) == as_of_date:
            if not replaced:
                updated.append(new_record)
                replaced = True
            continue
        updated.append(record)

    if not replaced:
        updated.append(new_record)

    updated.sort(key=lambda record: str(record.get("as_of_date", "")))

    return {
        "dataset": "brent_outlook_history",
        "model_version": MODEL_VERSION,
        "updated_at": outlook["generated_at"],
        "history": updated,
    }


def build_outlook(
    ompi_payload: Any,
    live_market_payload: Any,
    market_history_payload: Any,
) -> dict[str, Any]:
    generated_at = utc_now()
    ompi_score = extract_ompi_score(ompi_payload)
    live_point = extract_live_brent(live_market_payload)
    history_points = extract_history_points(market_history_payload)
    points = merge_live_with_history(history_points, live_point)

    if len(points) < 2:
        raise RuntimeError(
            "At least two valid Brent observations are required in market history."
        )

    current_point = points[-1]
    current_price = current_point.price

    change_5d = percentage_change(points, 5)
    change_20d = percentage_change(points, 20)
    change_60d = percentage_change(points, 60)

    ma20 = moving_average(points, 20)
    ma60 = moving_average(points, 60)

    fundamental_shift = calculate_fundamental_shift(ompi_score)
    momentum_shift, weighted_momentum_return, momentum_coverage = calculate_momentum_shift(
        change_5d,
        change_20d,
        change_60d,
    )
    trend_shift, trend_state = calculate_trend_shift(current_price, ma20, ma60)

    raw_total_shift = fundamental_shift + momentum_shift + trend_shift
    total_shift = clamp(raw_total_shift, -TOTAL_MAX_SHIFT, TOTAL_MAX_SHIFT)

    center_price = current_price * (1.0 + total_shift)

    volatility_band, daily_volatility = realised_volatility_band(points)
    lower_price = center_price * (1.0 - volatility_band)
    upper_price = center_price * (1.0 + volatility_band)

    direction = classify_direction(total_shift)
    range_position = classify_range_position(
        current_price,
        lower_price,
        center_price,
        upper_price,
    )
    confidence, confidence_reasons = calculate_confidence(
        fundamental_shift,
        momentum_shift,
        trend_shift,
        volatility_band,
        len(points),
    )

    quality_status = "VALID" if len(points) >= MIN_HISTORY_FOR_VALID else "LIMITED"

    return {
        "generated_at": iso_z(generated_at),
        "dataset": "brent_outlook",
        "horizon_days": HORIZON_DAYS,
        "current": {
            "price_usd": round(current_price, 2),
            "date": current_point.timestamp.date().isoformat(),
            "timestamp": iso_z(current_point.timestamp),
        },
        "outlook": {
            "center_price_usd": round(center_price, 2),
            "lower_price_usd": round(lower_price, 2),
            "upper_price_usd": round(upper_price, 2),
            "direction": direction,
            "confidence": confidence,
            "current_range_position": range_position,
        },
        "signals": {
            "ompi_score": round(ompi_score, 2),
            "fundamental_shift_pct": round(fundamental_shift * 100.0, 2),
            "momentum_shift_pct": round(momentum_shift * 100.0, 2),
            "trend_shift_pct": round(trend_shift * 100.0, 2),
            "raw_total_center_shift_pct": round(raw_total_shift * 100.0, 2),
            "total_center_shift_pct": round(total_shift * 100.0, 2),
            "volatility_band_pct": round(volatility_band * 100.0, 2),
        },
        "market_metrics": {
            "change_5d_pct": round_or_none(
                None if change_5d is None else change_5d * 100.0
            ),
            "change_20d_pct": round_or_none(
                None if change_20d is None else change_20d * 100.0
            ),
            "change_60d_pct": round_or_none(
                None if change_60d is None else change_60d * 100.0
            ),
            "weighted_momentum_pct": round(weighted_momentum_return * 100.0, 2),
            "momentum_data_coverage": round(momentum_coverage, 2),
            "moving_average_20d": round_or_none(ma20),
            "moving_average_60d": round_or_none(ma60),
            "daily_realised_volatility_20d": round_or_none(daily_volatility, 6),
            "trend_state": trend_state,
        },
        "confidence_details": {
            "reasons": confidence_reasons,
            "market_confirmation_available": False,
            "confidence_cap": "MEDIUM",
        },
        "quality": {
            "status": quality_status,
            "history_observations": len(points),
            "minimum_required_observations": MIN_HISTORY_FOR_VALID,
            "full_model_observations": MIN_HISTORY_FOR_FULL_MODEL,
            "market_confirmation_available": False,
        },
        "methodology": {
            "model_version": MODEL_VERSION,
            "type": "scenario_range",
            "not_price_target": True,
            "components": {
                "ompi_fundamental_max_shift_pct": FUNDAMENTAL_MAX_SHIFT * 100.0,
                "brent_momentum_max_shift_pct": MOMENTUM_MAX_SHIFT * 100.0,
                "trend_max_shift_pct": TREND_MAX_SHIFT * 100.0,
                "total_center_shift_cap_pct": TOTAL_MAX_SHIFT * 100.0,
                "minimum_range_width_pct": MIN_VOLATILITY_BAND * 100.0,
                "maximum_range_width_pct": MAX_VOLATILITY_BAND * 100.0,
            },
            "notes": [
                "OMPI is read as an external fundamental input and is not recalculated.",
                "Brent momentum uses available 5-, 20- and 60-observation changes.",
                "The range width is based on realised daily Brent volatility scaled to 30 days.",
                "Confidence cannot exceed MEDIUM before Market Confirmation is implemented.",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Brent Outlook v2 JSON files."
    )
    parser.add_argument("--ompi", help="Path to ompi.json")
    parser.add_argument("--live-market", help="Path to live-market.json")
    parser.add_argument("--market-history", help="Path to market-history.json")
    parser.add_argument(
        "--output",
        default="docs/data/brent-outlook.json",
        help="Output path for brent-outlook.json",
    )
    parser.add_argument(
        "--history-output",
        default="docs/data/brent-outlook-history.json",
        help="Output path for brent-outlook-history.json",
    )
    parser.add_argument(
        "--project-root",
        default=".",
        help="Repository root. Defaults to the current working directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.project_root).resolve()

    try:
        ompi_path = resolve_existing_path(
            args.ompi,
            (
                root / "docs/data/ompi.json",
                root / "data/ompi.json",
                root / "ompi.json",
            ),
            "OMPI",
        )
        live_market_path = resolve_existing_path(
            args.live_market,
            (
                root / "live-market.json",
                root / "docs/data/live-market.json",
                root / "data/live-market.json",
            ),
            "Live market",
        )
        market_history_path = resolve_existing_path(
            args.market_history,
            (
                root / "market-history.json",
                root / "docs/data/market-history.json",
                root / "data/market-history.json",
            ),
            "Market history",
        )

        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = root / output_path

        history_output_path = Path(args.history_output)
        if not history_output_path.is_absolute():
            history_output_path = root / history_output_path

        outlook = build_outlook(
            load_json(ompi_path),
            load_json(live_market_path),
            load_json(market_history_path),
        )

        history_payload = update_history(history_output_path, outlook)

        write_json(output_path, outlook)
        write_json(history_output_path, history_payload)

        print(
            json.dumps(
                {
                    "status": "success",
                    "output": str(output_path),
                    "history_output": str(history_output_path),
                    "current_brent_usd": outlook["current"]["price_usd"],
                    "center_price_usd": outlook["outlook"]["center_price_usd"],
                    "range_usd": [
                        outlook["outlook"]["lower_price_usd"],
                        outlook["outlook"]["upper_price_usd"],
                    ],
                    "direction": outlook["outlook"]["direction"],
                    "confidence": outlook["outlook"]["confidence"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # Defensive final guard for GitHub Actions logs.
        print(f"UNEXPECTED ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
