#!/usr/bin/env python3
"""
One-time backfill for docs/data/brent-outlook-history.json.

Purpose
-------
Enrich legacy Brent Outlook history records with the detailed schema introduced
in schema_version 2.0, while preserving every existing flat forecast field.

The script reuses scripts/generate_brent_outlook.py for all model calculations.
It reconstructs only information supported by historical source files available
in the repository as of each forecast date. It does not use current snapshots
for past dates, preventing look-ahead bias.

Important
---------
- Existing flat values are never recalculated or overwritten.
- Already enriched records are skipped unless --force is used.
- Completed evaluation blocks are preserved.
- A backup is written before the history file is changed.
- Dry-run is the default. Use --write to save changes.
- Backfilled detail may be partial when historical inputs do not exist.
"""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import math
import shutil
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence


DEFAULT_HISTORY_PATH = Path("docs/data/brent-outlook-history.json")
DEFAULT_GENERATOR_PATH = Path("scripts/generate_brent_outlook.py")
BACKFILL_VERSION = "1.0"


def load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise RuntimeError(f"File not found: {path}") from exc
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


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    elif isinstance(value, (int, float)):
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


def normalise_key(value: Any) -> str:
    return (
        str(value)
        .strip()
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
        .replace("/", "_")
    )


DATE_KEYS = {
    "date",
    "datetime",
    "timestamp",
    "time",
    "as_of",
    "as_of_date",
    "observed_at",
    "generated_at",
    "updated_at",
    "period",
}


def iter_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from iter_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_dicts(child)


def extract_record_datetime(record: dict[str, Any]) -> Optional[datetime]:
    normalised = {normalise_key(key): value for key, value in record.items()}
    for key in DATE_KEYS:
        if key in normalised:
            parsed = parse_datetime(normalised[key])
            if parsed is not None:
                return parsed
    return None


def select_latest_dated_record(
    payload: Any,
    as_of_date: date,
) -> Optional[dict[str, Any]]:
    """
    Return the latest dated dictionary whose date is not after as_of_date.

    This generic selector supports list-based and nested history JSON files.
    """
    candidates: list[tuple[datetime, dict[str, Any]]] = []
    end_of_day = datetime.combine(
        as_of_date,
        time.max,
        tzinfo=timezone.utc,
    )

    for record in iter_dicts(payload):
        timestamp = extract_record_datetime(record)
        if timestamp is None or timestamp > end_of_day:
            continue
        candidates.append((timestamp, record))

    if not candidates:
        return None

    candidates.sort(key=lambda item: item[0])
    return copy.deepcopy(candidates[-1][1])


def resolve_first_existing(
    root: Path,
    candidates: Sequence[str],
) -> Optional[Path]:
    for candidate in candidates:
        path = root / candidate
        if path.exists():
            return path
    return None


def load_generator(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(
        "brent_outlook_generator",
        path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load generator module: {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def build_filtered_market_history(
    generator: Any,
    full_payload: Any,
    as_of_date: date,
) -> list[dict[str, Any]]:
    points = generator.extract_history_points(full_payload)
    filtered = [
        point
        for point in points
        if point.timestamp.date() <= as_of_date
    ]
    return [
        {
            "date": point.timestamp.date().isoformat(),
            "brent": point.price,
        }
        for point in filtered
    ]


def synthetic_live_payload(record: dict[str, Any]) -> dict[str, Any]:
    price = parse_float(record.get("current_price_usd"))
    if price is None or price <= 0:
        raise RuntimeError(
            f"Invalid current_price_usd for {record.get('as_of_date')}"
        )

    timestamp = parse_datetime(record.get("generated_at"))
    if timestamp is None:
        parsed_date = date.fromisoformat(str(record["as_of_date"]))
        timestamp = datetime.combine(
            parsed_date,
            time.max,
            tzinfo=timezone.utc,
        )

    return {
        "dataset": "backfill_live_market",
        "generated_at": timestamp.isoformat().replace("+00:00", "Z"),
        "brent": {
            "price_usd": price,
            "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        },
    }


def fallback_ompi_payload(record: dict[str, Any]) -> dict[str, Any]:
    score = parse_float(record.get("ompi_score"))
    if score is None:
        raise RuntimeError(
            f"No historical OMPI input for {record.get('as_of_date')}"
        )
    return {
        "dataset": "ompi",
        "as_of_date": record.get("as_of_date"),
        "ompi_score": score,
    }


def preserve_flat_fields(
    original: dict[str, Any],
    enriched: dict[str, Any],
) -> dict[str, Any]:
    """
    Keep all original keys and values as the source of truth.

    New nested details are added only where the original record has no value.
    """
    result = copy.deepcopy(enriched)

    for key, value in original.items():
        if key == "evaluation":
            existing_eval = value
            if (
                isinstance(existing_eval, dict)
                and existing_eval.get("status") == "COMPLETED"
            ):
                result["evaluation"] = copy.deepcopy(existing_eval)
            continue
        result[key] = copy.deepcopy(value)

    return result


def source_status(
    path: Optional[Path],
    selected_record: Optional[dict[str, Any]],
    fallback_used: bool = False,
) -> dict[str, Any]:
    return {
        "available": selected_record is not None,
        "path": str(path) if path is not None else None,
        "fallback_used": fallback_used,
    }


def enrich_record(
    generator: Any,
    record: dict[str, Any],
    market_history_payload: Any,
    historical_payloads: dict[str, tuple[Optional[Path], Any]],
) -> tuple[dict[str, Any], list[str]]:
    as_of_raw = str(record.get("as_of_date", ""))
    try:
        as_of_date = date.fromisoformat(as_of_raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid as_of_date: {as_of_raw}") from exc

    filtered_market = build_filtered_market_history(
        generator,
        market_history_payload,
        as_of_date,
    )
    if len(filtered_market) < 2:
        raise RuntimeError(
            f"Insufficient Brent history through {as_of_raw}"
        )

    selected: dict[str, Optional[dict[str, Any]]] = {}
    missing: list[str] = []

    for key, (_, payload) in historical_payloads.items():
        selected[key] = (
            select_latest_dated_record(payload, as_of_date)
            if payload is not None
            else None
        )

    ompi_fallback = False
    ompi_payload = selected.get("ompi")
    if ompi_payload is None:
        ompi_payload = fallback_ompi_payload(record)
        ompi_fallback = True
        missing.append("ompi_history")

    for key in (
        "inventory",
        "supply_demand_balance",
        "chokepoint",
        "market_confirmation",
    ):
        if selected.get(key) is None:
            missing.append(f"{key}_history")

    optional_status = {
        "inventory": source_status(
            historical_payloads["inventory"][0],
            selected.get("inventory"),
        ),
        "supply_demand_balance": source_status(
            historical_payloads["supply_demand_balance"][0],
            selected.get("supply_demand_balance"),
        ),
        "chokepoint": source_status(
            historical_payloads["chokepoint"][0],
            selected.get("chokepoint"),
        ),
        "market_confirmation": source_status(
            historical_payloads["market_confirmation"][0],
            selected.get("market_confirmation"),
        ),
    }

    reconstructed_outlook = generator.build_outlook(
        ompi_payload,
        synthetic_live_payload(record),
        filtered_market,
        inventory_payload=selected.get("inventory"),
        supply_demand_payload=selected.get("supply_demand_balance"),
        chokepoint_payload=selected.get("chokepoint"),
        market_confirmation_payload=selected.get("market_confirmation"),
        optional_input_status=optional_status,
    )

    enriched = generator.history_record_from_outlook(
        reconstructed_outlook
    )
    enriched = preserve_flat_fields(record, enriched)

    enriched["backfill"] = {
        "version": BACKFILL_VERSION,
        "status": "PARTIAL" if missing else "COMPLETE",
        "method": "historical_reconstruction",
        "original_flat_forecast_preserved": True,
        "look_ahead_bias_prevented": True,
        "missing_historical_inputs": sorted(set(missing)),
        "ompi_fallback_from_legacy_record": ompi_fallback,
        "reconstructed_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
    }

    return enriched, missing


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill detailed Brent Outlook history fields without "
            "changing legacy flat forecast values."
        )
    )
    parser.add_argument(
        "--project-root",
        default=".",
        help="Repository root. Defaults to current directory.",
    )
    parser.add_argument(
        "--history",
        default=str(DEFAULT_HISTORY_PATH),
        help="Path to brent-outlook-history.json.",
    )
    parser.add_argument(
        "--generator",
        default=str(DEFAULT_GENERATOR_PATH),
        help="Path to generate_brent_outlook.py.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the enriched history. Default is dry-run.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild records that already have schema_version 2.0.",
    )
    parser.add_argument(
        "--start-date",
        help="Optional first as_of_date to process (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        help="Optional last as_of_date to process (YYYY-MM-DD).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.project_root).resolve()

    history_path = Path(args.history)
    if not history_path.is_absolute():
        history_path = root / history_path

    generator_path = Path(args.generator)
    if not generator_path.is_absolute():
        generator_path = root / generator_path

    market_history_path = resolve_first_existing(
        root,
        (
            "market-history.json",
            "docs/data/market-history.json",
            "data/market-history.json",
        ),
    )
    if market_history_path is None:
        raise RuntimeError("No market-history.json file found.")

    historical_specs: dict[str, Sequence[str]] = {
        "ompi": (
            "docs/data/ompi-history.json",
            "ompi-history.json",
            "data/ompi-history.json",
        ),
        "inventory": (
            "docs/data/inventory_stress_history.json",
            "docs/data/inventory-stress-history.json",
            "inventory_stress_history.json",
            "inventory-stress-history.json",
        ),
        "supply_demand_balance": (
            "docs/data/global_oil_supply_demand_history.json",
            "docs/data/global-oil-supply-demand-history.json",
            "global_oil_supply_demand_history.json",
            "global-oil-supply-demand-history.json",
            "docs/data/global_oil_balance_history.json",
            "global_oil_balance_history.json",
        ),
        "chokepoint": (
            "chokepoint-impact-history.json",
            "docs/data/chokepoint-impact-history.json",
            "data/chokepoint-impact-history.json",
        ),
        "market_confirmation": (
            "docs/data/market-confirmation-history.json",
            "market-confirmation-history.json",
            "data/market-confirmation-history.json",
        ),
    }

    historical_payloads: dict[
        str,
        tuple[Optional[Path], Any],
    ] = {}
    for key, candidates in historical_specs.items():
        path = resolve_first_existing(root, candidates)
        payload = load_json(path) if path is not None else None
        historical_payloads[key] = (path, payload)

    generator = load_generator(generator_path)
    history_payload = load_json(history_path)
    market_history_payload = load_json(market_history_path)

    if isinstance(history_payload, dict):
        records = history_payload.get("history")
    elif isinstance(history_payload, list):
        records = history_payload
        history_payload = {
            "dataset": "brent_outlook_history",
            "model_version": getattr(generator, "MODEL_VERSION", None),
            "updated_at": None,
            "history": records,
        }
    else:
        records = None

    if not isinstance(records, list):
        raise RuntimeError("History JSON does not contain a valid history list.")

    start_date = (
        date.fromisoformat(args.start_date)
        if args.start_date
        else None
    )
    end_date = (
        date.fromisoformat(args.end_date)
        if args.end_date
        else None
    )

    updated_records: list[dict[str, Any]] = []
    enriched_count = 0
    skipped_count = 0
    failed: list[dict[str, str]] = []
    partial_dates: list[str] = []

    for raw_record in records:
        if not isinstance(raw_record, dict):
            skipped_count += 1
            continue

        record = copy.deepcopy(raw_record)
        record_date_raw = str(record.get("as_of_date", ""))

        try:
            record_date = date.fromisoformat(record_date_raw)
        except ValueError:
            updated_records.append(record)
            failed.append({
                "as_of_date": record_date_raw,
                "error": "Invalid as_of_date",
            })
            continue

        outside_range = (
            (start_date is not None and record_date < start_date)
            or (end_date is not None and record_date > end_date)
        )
        already_enriched = (
            str(record.get("schema_version", "")) == "2.0"
            and isinstance(record.get("signals"), dict)
            and isinstance(record.get("evaluation"), dict)
        )

        if outside_range or (already_enriched and not args.force):
            updated_records.append(record)
            skipped_count += 1
            continue

        try:
            enriched, missing = enrich_record(
                generator,
                record,
                market_history_payload,
                historical_payloads,
            )
            updated_records.append(enriched)
            enriched_count += 1
            if missing:
                partial_dates.append(record_date_raw)
        except Exception as exc:
            updated_records.append(record)
            failed.append({
                "as_of_date": record_date_raw,
                "error": f"{type(exc).__name__}: {exc}",
            })

    updated_records.sort(
        key=lambda item: str(item.get("as_of_date", ""))
    )

    output_payload = copy.deepcopy(history_payload)
    output_payload["history"] = updated_records
    output_payload["backfill_summary"] = {
        "backfill_version": BACKFILL_VERSION,
        "mode": "write" if args.write else "dry_run",
        "enriched_records": enriched_count,
        "skipped_records": skipped_count,
        "failed_records": len(failed),
        "partial_records": len(partial_dates),
        "partial_dates": partial_dates,
        "failures": failed,
        "completed_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
    }

    if args.write:
        backup_path = history_path.with_suffix(
            history_path.suffix + ".pre-backfill.bak"
        )
        shutil.copy2(history_path, backup_path)
        write_json(history_path, output_payload)

    print(json.dumps({
        "status": "success" if not failed else "completed_with_warnings",
        "mode": "write" if args.write else "dry_run",
        "history_path": str(history_path),
        "market_history_path": str(market_history_path),
        "enriched_records": enriched_count,
        "skipped_records": skipped_count,
        "failed_records": len(failed),
        "partial_records": len(partial_dates),
        "failures": failed,
        "historical_sources": {
            key: str(path) if path is not None else None
            for key, (path, _) in historical_payloads.items()
        },
    }, ensure_ascii=False, indent=2))

    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
