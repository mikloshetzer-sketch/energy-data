#!/usr/bin/env python3
"""
OMPI v2 – Oil Market Pressure Index

Cél:
- önálló, 0–100 közötti olajpiaci nyomásindex előállítása;
- a fizikai piac, készletek, OPEC+ puffer, geopolitikai kockázat,
  kínai importmomentum és szoroskockázat egyesítése;
- a Brent ár kizárása az OMPI pontszámából a visszacsatolás elkerülésére.

Bemenetek:
- docs/data/global_oil_balance.json
- docs/data/inventory_stress.json
- docs/data/chokepoint_status.json
- docs/data/market_interpretation.json
- china-oil-import.json

Kimenetek:
- docs/data/ompi.json
- docs/data/ompi-history.json

OMPI v2 súlyok:
- fizikai kínálat–keresleti mérleg: 35%
- készlethelyzet: 20%
- OPEC+ tényleges tartalékkapacitás: 15%
- közel-keleti konfliktushatás: 15%
- kínai importmomentum: 10%
- szoroskockázat: 5%

Megjegyzés:
A chokepoint komponens súlya ideiglenesen 5%, mert a jelenlegi
chokepoint modell részben átfed a geopolitikai kockázati modellel.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]

GLOBAL_BALANCE_FILE = ROOT / "docs" / "data" / "global_oil_balance.json"
INVENTORY_FILE = ROOT / "docs" / "data" / "inventory_stress.json"
CHOKEPOINT_FILE = ROOT / "docs" / "data" / "chokepoint_status.json"
MARKET_INTERPRETATION_FILE = ROOT / "docs" / "data" / "market_interpretation.json"
CHINA_FILE = ROOT / "china-oil-import.json"

OUTPUT_FILE = ROOT / "docs" / "data" / "ompi.json"
HISTORY_FILE = ROOT / "docs" / "data" / "ompi-history.json"

METHOD_VERSION = "ompi_v2_fundamental_2026_07"

# Ideiglenes paraméter. Később külön OPEC-adatmodul váltsa ki.
DEFAULT_OPEC_EFFECTIVE_SPARE_CAPACITY_MBD = float(
    os.getenv("OPEC_EFFECTIVE_SPARE_CAPACITY_MBD", "0.17")
)

WEIGHTS = {
    "physical_balance": 0.35,
    "inventory_stress": 0.20,
    "opec_buffer": 0.15,
    "geopolitical_risk": 0.15,
    "china_import_momentum": 0.10,
    "chokepoint_risk": 0.05,
}

CHOKEPOINT_WEIGHTS = {
    "hormuz": 0.40,
    "bab_el_mandeb": 0.25,
    "suez": 0.20,
    "malacca": 0.15,
}

FRESH_QUALITY_VALUES = {
    "OK",
    "FRESH",
    "CURRENT",
    "AVAILABLE",
    "VALID",
    "LIVE",
}

FALLBACK_QUALITY_VALUES = {
    "FALLBACK",
    "FALLBACK_PREVIOUS",
    "PREVIOUS",
    "STALE",
    "PARTIAL_FALLBACK",
}


def clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None

    if isinstance(value, str):
        cleaned = (
            value.strip()
            .replace("\u00a0", "")
            .replace(",", ".")
            .replace("%", "")
        )
        if not cleaned:
            return None
        try:
            number = float(cleaned)
            return number if math.isfinite(number) else None
        except ValueError:
            return None

    return None


def load_json(path: Path, default: Any = None, required: bool = False) -> Any:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Hiányzó kötelező fájl: {path}")
        return default

    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError) as exc:
        if required:
            raise RuntimeError(f"Nem olvasható JSON: {path}: {exc}") from exc
        return default


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        temporary_path = Path(handle.name)

    temporary_path.replace(path)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def normalize_quality(value: Any, fallback: str = "UNKNOWN") -> str:
    if value is None:
        return fallback
    text = str(value).strip().upper()
    return text or fallback


def classify_score(score: float) -> dict[str, str]:
    if score < 25:
        return {
            "level": "LOW",
            "label_hu": "Alacsony",
            "label_en": "Low",
            "direction": "BEARISH",
            "direction_hu": "Lefelé mutató",
            "direction_en": "Bearish",
            "description_hu": (
                "Laza vagy jól ellátott piac, alacsony felfelé irányuló árnyomással."
            ),
            "description_en": (
                "A loose or well-supplied market with low upward price pressure."
            ),
        }

    if score < 45:
        return {
            "level": "MODERATE",
            "label_hu": "Mérsékelt",
            "label_en": "Moderate",
            "direction": "BEARISH",
            "direction_hu": "Enyhén lefelé mutató",
            "direction_en": "Mildly bearish",
            "description_hu": (
                "Mérsékelt piaci nyomás, a kínálati oldal még kezelhető pufferekkel rendelkezik."
            ),
            "description_en": (
                "Moderate market pressure, with still-manageable supply-side buffers."
            ),
        }

    if score < 65:
        return {
            "level": "ELEVATED",
            "label_hu": "Emelkedett",
            "label_en": "Elevated",
            "direction": "NEUTRAL",
            "direction_hu": "Semleges",
            "direction_en": "Neutral",
            "description_hu": (
                "Emelkedett sérülékenység, de még nincs egyértelműen szélsőséges piaci feszültség."
            ),
            "description_en": (
                "Elevated vulnerability without clearly extreme market stress."
            ),
        }

    if score < 80:
        return {
            "level": "HIGH",
            "label_hu": "Magas",
            "label_en": "High",
            "direction": "BULLISH",
            "direction_hu": "Felfelé mutató",
            "direction_en": "Bullish",
            "description_hu": (
                "Erős felfelé irányuló árnyomás és korlátozott piaci alkalmazkodóképesség."
            ),
            "description_en": (
                "Strong upward price pressure and limited market adjustment capacity."
            ),
        }

    return {
        "level": "EXTREME",
        "label_hu": "Rendkívüli",
        "label_en": "Extreme",
        "direction": "BULLISH",
        "direction_hu": "Erősen felfelé mutató",
        "direction_en": "Strongly bullish",
        "description_hu": (
            "Rendkívül feszes vagy sérülékeny piac, jelentős ellátási és árkockázattal."
        ),
        "description_en": (
            "An extremely tight or vulnerable market with substantial supply and price risk."
        ),
    }


def get_current_balance(global_balance: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    current = (
        global_balance.get("current")
        or global_balance.get("latest")
        or global_balance
    )

    if not isinstance(current, dict):
        raise RuntimeError("A global_oil_balance.json jelenlegi blokkja érvénytelen.")

    balance = to_float(
        current.get(
            "balance_mbd",
            global_balance.get(
                "current_balance_mbd",
                global_balance.get("balance_mbd"),
            ),
        )
    )

    if balance is None:
        raise RuntimeError(
            "Hiányzik a global_oil_balance.json fizikai balance_mbd értéke."
        )

    metadata = {
        "period": (
            current.get("period")
            or global_balance.get("current_period")
            or global_balance.get("period")
        ),
        "global_supply_mbd": to_float(
            current.get(
                "global_supply_mbd",
                global_balance.get("global_supply_mbd"),
            )
        ),
        "global_demand_mbd": to_float(
            current.get(
                "global_demand_mbd",
                global_balance.get("global_demand_mbd"),
            )
        ),
        "source_generated_at": global_balance.get("generated_at"),
        "data_quality": normalize_quality(
            global_balance.get("data_quality"), "AVAILABLE"
        ),
    }

    return balance, metadata


def physical_balance_score(balance_mbd: float) -> float:
    # +2 mb/d többlet ≈ 0 pont; 0 mb/d = 50; -2 mb/d hiány ≈ 100.
    return clamp(50.0 - balance_mbd * 25.0)


def get_inventory_component(data: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    score = to_float(data.get("inventory_stress_score"))
    if score is None:
        score = 50.0
        quality = "MISSING_FALLBACK"
    else:
        quality = normalize_quality(data.get("data_quality"), "AVAILABLE")

    return clamp(score), {
        "data_quality": quality,
        "level": data.get("inventory_stress_level"),
        "level_hu": data.get("inventory_stress_level_hu"),
        "level_en": data.get("inventory_stress_level_en"),
        "source_generated_at": data.get("generated_at"),
        "summary_hu": data.get("summary_hu"),
        "summary_en": data.get("summary_en"),
    }


def opec_buffer_score(
    balance_mbd: float,
    effective_spare_capacity_mbd: float,
) -> tuple[float, dict[str, Any]]:
    """
    Az OPEC-puffer csak fizikai hiány esetén válik szűk keresztmetszetté.

    Ha nincs hiány:
      - a komponens alacsony nyomást ad;
      - minél nagyobb a többlet, annál alacsonyabb a pontszám.

    Ha van hiány:
      - coverage_ratio = spare capacity / deficit;
      - 100% vagy jobb lefedés -> alacsony pont;
      - 0% lefedés -> 100 pont.
    """
    spare_capacity = max(0.0, effective_spare_capacity_mbd)

    if balance_mbd >= 0:
        score = clamp(20.0 - min(balance_mbd, 2.0) * 5.0)
        return score, {
            "effective_spare_capacity_mbd": round(spare_capacity, 3),
            "physical_deficit_mbd": 0.0,
            "coverage_ratio": None,
            "coverage_pct": None,
            "method": "no_physical_deficit",
            "parameter_status": "TEMPORARY_STATIC_INPUT",
        }

    deficit = abs(balance_mbd)
    coverage_ratio = spare_capacity / max(deficit, 0.01)
    score = clamp(100.0 - min(coverage_ratio, 1.0) * 80.0)

    return score, {
        "effective_spare_capacity_mbd": round(spare_capacity, 3),
        "physical_deficit_mbd": round(deficit, 3),
        "coverage_ratio": round(coverage_ratio, 4),
        "coverage_pct": round(coverage_ratio * 100.0, 1),
        "method": "effective_spare_capacity_divided_by_deficit",
        "parameter_status": "TEMPORARY_STATIC_INPUT",
    }


def get_geopolitical_component(
    interpretation: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    risk_components = interpretation.get("risk_components", {})
    if not isinstance(risk_components, dict):
        risk_components = {}

    score = to_float(risk_components.get("middle_east_conflict_impact"))

    if score is None:
        score = 50.0
        quality = "MISSING_FALLBACK"
    else:
        quality = "AVAILABLE"

    return clamp(score), {
        "source_field": "risk_components.middle_east_conflict_impact",
        "data_quality": quality,
        "source_generated_at": interpretation.get("generated_at"),
        "excluded_fields": [
            "combined_risk_score",
            "risk_components.chokepoint_risk",
            "risk_components.brent_price_change_risk",
            "risk_components.inventory_stress",
            "risk_components.brent_volatility_risk",
        ],
    }


def get_chokepoint_component(
    data: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    rows = data.get("chokepoints", [])
    if not isinstance(rows, list):
        rows = []

    available: dict[str, float] = {}
    details: list[dict[str, Any]] = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        chokepoint_id = str(row.get("id", "")).strip()
        score = to_float(row.get("score"))

        if chokepoint_id and score is not None:
            available[chokepoint_id] = clamp(score)
            details.append(
                {
                    "id": chokepoint_id,
                    "score": round(clamp(score), 1),
                    "level": row.get("level"),
                }
            )

    weighted_sum = 0.0
    available_weight = 0.0

    for chokepoint_id, weight in CHOKEPOINT_WEIGHTS.items():
        if chokepoint_id in available:
            weighted_sum += available[chokepoint_id] * weight
            available_weight += weight

    if available_weight <= 0:
        score = 50.0
        quality = "MISSING_FALLBACK"
    else:
        score = clamp(weighted_sum / available_weight)
        quality = "AVAILABLE"

    return score, {
        "data_quality": quality,
        "source_generated_at": data.get("generated_at"),
        "method": "weighted_route_scores",
        "route_weights": CHOKEPOINT_WEIGHTS,
        "available_route_weight": round(available_weight, 3),
        "routes": details,
        "model_overlap": "PARTIAL",
        "temporary_reduced_weight": True,
    }


def extract_china_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if not isinstance(data, dict):
        return []

    candidate_keys = (
        "rows",
        "data",
        "series",
        "history",
        "observations",
        "monthly",
        "values",
    )

    for key in candidate_keys:
        value = data.get(key)
        if isinstance(value, list):
            rows = [item for item in value if isinstance(item, dict)]
            if rows:
                return rows

    nested = data.get("china")
    if isinstance(nested, dict):
        return extract_china_rows(nested)

    return []


def extract_china_value(row: dict[str, Any]) -> float | None:
    candidate_fields = (
        "value",
        "import_kbd",
        "imports_kbd",
        "crude_import_kbd",
        "crude_oil_import_kbd",
        "china_crude_import_kbd",
        "daily_import_kbd",
        "TOTIMPSB",
    )

    for field in candidate_fields:
        value = to_float(row.get(field))
        if value is not None:
            return value

    return None


def extract_china_period(row: dict[str, Any]) -> str | None:
    for field in ("period", "date", "month", "time", "TIME_PERIOD"):
        value = row.get(field)
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return None


def mean_or_none(values: Iterable[float]) -> float | None:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    return mean(clean) if clean else None


def pct_change(old: float | None, new: float | None) -> float | None:
    if old is None or new is None or old == 0:
        return None
    return ((new - old) / abs(old)) * 100.0


def get_china_component(data: Any) -> tuple[float, dict[str, Any]]:
    rows = extract_china_rows(data)

    observations: list[tuple[str | None, float]] = []
    for row in rows:
        value = extract_china_value(row)
        if value is not None:
            observations.append((extract_china_period(row), value))

    if len(observations) < 3:
        return 50.0, {
            "data_quality": "INSUFFICIENT_HISTORY_FALLBACK",
            "observation_count": len(observations),
            "method": "neutral_fallback",
        }

    values = [value for _, value in observations]
    latest_period = observations[-1][0]
    latest_value = values[-1]

    avg_3m = mean_or_none(values[-3:])
    avg_12m = mean_or_none(values[-12:])

    previous_3m = mean_or_none(values[-15:-12]) if len(values) >= 15 else None
    year_ago_value = values[-13] if len(values) >= 13 else None

    short_vs_long_pct = pct_change(avg_12m, avg_3m)
    three_month_yoy_pct = pct_change(previous_3m, avg_3m)
    latest_yoy_pct = pct_change(year_ago_value, latest_value)

    available_signals: list[tuple[float, float]] = []

    if short_vs_long_pct is not None:
        available_signals.append((short_vs_long_pct, 0.40))
    if three_month_yoy_pct is not None:
        available_signals.append((three_month_yoy_pct, 0.35))
    if latest_yoy_pct is not None:
        available_signals.append((latest_yoy_pct, 0.25))

    if not available_signals:
        return 50.0, {
            "data_quality": "INSUFFICIENT_HISTORY_FALLBACK",
            "observation_count": len(observations),
            "latest_period": latest_period,
            "latest_value_kbd": round(latest_value, 2),
            "method": "neutral_fallback",
        }

    total_weight = sum(weight for _, weight in available_signals)
    composite_change = sum(
        signal * weight for signal, weight in available_signals
    ) / total_weight

    score = clamp(50.0 + composite_change * 3.0)

    return score, {
        "data_quality": (
            "AVAILABLE"
            if len(observations) >= 13
            else "PARTIAL_HISTORY"
        ),
        "observation_count": len(observations),
        "latest_period": latest_period,
        "latest_value_kbd": round(latest_value, 2),
        "average_3m_kbd": round(avg_3m, 2) if avg_3m is not None else None,
        "average_12m_kbd": round(avg_12m, 2) if avg_12m is not None else None,
        "short_vs_long_pct": (
            round(short_vs_long_pct, 2)
            if short_vs_long_pct is not None
            else None
        ),
        "three_month_yoy_pct": (
            round(three_month_yoy_pct, 2)
            if three_month_yoy_pct is not None
            else None
        ),
        "latest_yoy_pct": (
            round(latest_yoy_pct, 2)
            if latest_yoy_pct is not None
            else None
        ),
        "composite_change_pct": round(composite_change, 2),
        "method": (
            "40%_3m_vs_12m_35%_3m_yoy_25%_latest_yoy_renormalized"
        ),
    }


def quality_weight(component_name: str, metadata: dict[str, Any]) -> float:
    quality = normalize_quality(metadata.get("data_quality"), "UNKNOWN")

    if quality in FRESH_QUALITY_VALUES:
        return 1.0

    if quality in FALLBACK_QUALITY_VALUES:
        return 0.75

    if "MISSING" in quality or "INSUFFICIENT" in quality:
        return 0.40

    if "PARTIAL" in quality:
        return 0.70

    if component_name == "opec_buffer":
        # Az OPEC-adat jelenleg statikus paraméter, ezért nem teljes értékű friss adat.
        return 0.65

    return 0.60


def build_component(
    name: str,
    score: float,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    weight = WEIGHTS[name]
    contribution = score * weight

    return {
        "score": round(score, 1),
        "weight": weight,
        "weight_pct": round(weight * 100.0, 1),
        "contribution": round(contribution, 2),
        **metadata,
    }


def build_quality_summary(
    components: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    weighted_quality = 0.0
    missing: list[str] = []
    fallback: list[str] = []
    partial: list[str] = []

    for name, component in components.items():
        q_weight = quality_weight(name, component)
        weighted_quality += WEIGHTS[name] * q_weight

        quality = normalize_quality(component.get("data_quality"), "UNKNOWN")

        if "MISSING" in quality or "INSUFFICIENT" in quality:
            missing.append(name)
        elif quality in FALLBACK_QUALITY_VALUES:
            fallback.append(name)
        elif "PARTIAL" in quality or quality == "UNKNOWN":
            partial.append(name)

        if component.get("parameter_status") == "TEMPORARY_STATIC_INPUT":
            partial.append(name)

    confidence_score = round(clamp(weighted_quality * 100.0), 1)

    if missing:
        status = "LIMITED"
    elif fallback or partial:
        status = "PARTIAL"
    else:
        status = "HIGH"

    return {
        "status": status,
        "confidence_score": confidence_score,
        "coverage_pct": round(
            100.0
            * sum(
                WEIGHTS[name]
                for name in components
                if name not in missing
            ),
            1,
        ),
        "missing_components": sorted(set(missing)),
        "fallback_components": sorted(set(fallback)),
        "partial_components": sorted(set(partial)),
    }


def build_summary(
    score: float,
    classification: dict[str, str],
    balance_mbd: float,
    components: dict[str, dict[str, Any]],
    quality: dict[str, Any],
) -> tuple[str, str]:
    strongest = max(
        components.items(),
        key=lambda item: item[1]["contribution"],
    )[0]

    labels_hu = {
        "physical_balance": "fizikai kínálat–keresleti mérleg",
        "inventory_stress": "készlethelyzet",
        "opec_buffer": "OPEC+ puffer",
        "geopolitical_risk": "geopolitikai kockázat",
        "china_import_momentum": "kínai importmomentum",
        "chokepoint_risk": "szoroskockázat",
    }

    labels_en = {
        "physical_balance": "physical supply-demand balance",
        "inventory_stress": "inventory position",
        "opec_buffer": "OPEC+ buffer",
        "geopolitical_risk": "geopolitical risk",
        "china_import_momentum": "Chinese import momentum",
        "chokepoint_risk": "chokepoint risk",
    }

    summary_hu = (
        f"Az OMPI értéke {score:.1f}/100, ami "
        f"{classification['label_hu'].lower()} olajpiaci nyomást jelez. "
        f"A fizikai mérleg {balance_mbd:+.2f} millió hordó/nap. "
        f"A legnagyobb súlyozott hozzájárulást jelenleg a "
        f"{labels_hu[strongest]} adja. "
        f"Az adatbizalmi státusz: {quality['status']} "
        f"({quality['confidence_score']:.1f}/100)."
    )

    summary_en = (
        f"The OMPI stands at {score:.1f}/100, indicating "
        f"{classification['label_en'].lower()} oil-market pressure. "
        f"The physical balance is {balance_mbd:+.2f} million barrels per day. "
        f"The largest weighted contribution currently comes from the "
        f"{labels_en[strongest]}. "
        f"Data-confidence status: {quality['status']} "
        f"({quality['confidence_score']:.1f}/100)."
    )

    return summary_hu, summary_en


def update_history(current_payload: dict[str, Any]) -> None:
    existing = load_json(HISTORY_FILE, default={})

    if isinstance(existing, list):
        history = existing
    elif isinstance(existing, dict):
        history = existing.get("history", [])
    else:
        history = []

    if not isinstance(history, list):
        history = []

    history_date = current_payload["generated_at"][:10]

    history_row = {
        "date": history_date,
        "generated_at": current_payload["generated_at"],
        "score": current_payload["score"],
        "level": current_payload["level"],
        "direction": current_payload["direction"],
        "confidence_score": current_payload["data_quality"]["confidence_score"],
        "components": {
            key: value["score"]
            for key, value in current_payload["components"].items()
        },
    }

    replaced = False
    for index, row in enumerate(history):
        if isinstance(row, dict) and row.get("date") == history_date:
            history[index] = history_row
            replaced = True
            break

    if not replaced:
        history.append(history_row)

    history = sorted(
        [row for row in history if isinstance(row, dict)],
        key=lambda row: str(row.get("date", "")),
    )[-1095:]

    history_payload = {
        "index": "OMPI",
        "method_version": METHOD_VERSION,
        "updated_at": current_payload["generated_at"],
        "history": history,
    }

    atomic_write_json(HISTORY_FILE, history_payload)


def validate_weights() -> None:
    total = sum(WEIGHTS.values())
    if not math.isclose(total, 1.0, abs_tol=1e-9):
        raise RuntimeError(f"Az OMPI-súlyok összege nem 1, hanem {total}.")


def main() -> None:
    validate_weights()

    global_balance = load_json(
        GLOBAL_BALANCE_FILE,
        default={},
        required=True,
    )
    inventory = load_json(INVENTORY_FILE, default={})
    chokepoints = load_json(CHOKEPOINT_FILE, default={})
    interpretation = load_json(MARKET_INTERPRETATION_FILE, default={})
    china = load_json(CHINA_FILE, default={})

    balance_mbd, balance_meta = get_current_balance(global_balance)
    physical_score = physical_balance_score(balance_mbd)

    inventory_score, inventory_meta = get_inventory_component(inventory)
    opec_score, opec_meta = opec_buffer_score(
        balance_mbd,
        DEFAULT_OPEC_EFFECTIVE_SPARE_CAPACITY_MBD,
    )
    geopolitical_score, geopolitical_meta = get_geopolitical_component(
        interpretation
    )
    china_score, china_meta = get_china_component(china)
    chokepoint_score, chokepoint_meta = get_chokepoint_component(chokepoints)

    components = {
        "physical_balance": build_component(
            "physical_balance",
            physical_score,
            {
                **balance_meta,
                "balance_mbd": round(balance_mbd, 3),
                "method": "clamp_50_minus_balance_times_25",
            },
        ),
        "inventory_stress": build_component(
            "inventory_stress",
            inventory_score,
            inventory_meta,
        ),
        "opec_buffer": build_component(
            "opec_buffer",
            opec_score,
            {
                **opec_meta,
                "data_quality": "PARTIAL_STATIC_PARAMETER",
            },
        ),
        "geopolitical_risk": build_component(
            "geopolitical_risk",
            geopolitical_score,
            geopolitical_meta,
        ),
        "china_import_momentum": build_component(
            "china_import_momentum",
            china_score,
            china_meta,
        ),
        "chokepoint_risk": build_component(
            "chokepoint_risk",
            chokepoint_score,
            chokepoint_meta,
        ),
    }

    score = round(
        sum(component["contribution"] for component in components.values()),
        1,
    )

    classification = classify_score(score)
    quality = build_quality_summary(components)
    summary_hu, summary_en = build_summary(
        score,
        classification,
        balance_mbd,
        components,
        quality,
    )

    generated_at = utc_now_iso()

    payload = {
        "generated_at": generated_at,
        "index": "OMPI",
        "index_name": "Oil Market Pressure Index",
        "method_version": METHOD_VERSION,
        "score": score,
        "level": classification["level"],
        "level_hu": classification["label_hu"],
        "level_en": classification["label_en"],
        "direction": classification["direction"],
        "direction_hu": classification["direction_hu"],
        "direction_en": classification["direction_en"],
        "description_hu": classification["description_hu"],
        "description_en": classification["description_en"],
        "weights": WEIGHTS,
        "components": components,
        "data_quality": quality,
        "market_confirmation": {
            "included_in_ompi": False,
            "brent_momentum": None,
            "status": "NOT_CALCULATED",
            "note_hu": (
                "A Brent-momentum nem része az OMPI pontszámának. "
                "Később külön piaci megerősítési rétegként használható."
            ),
            "note_en": (
                "Brent momentum is not part of the OMPI score. "
                "It may later be used as a separate market-confirmation layer."
            ),
        },
        "model_notes": {
            "chokepoint_weight_temporary": True,
            "chokepoint_overlap_warning": True,
            "opec_input_temporary_static": True,
            "brent_excluded_from_score": True,
        },
        "summary_hu": summary_hu,
        "summary_en": summary_en,
    }

    atomic_write_json(OUTPUT_FILE, payload)
    update_history(payload)

    print(
        f"OMPI elkészült: {score:.1f}/100 "
        f"({classification['level']}); "
        f"adatbizalom: {quality['confidence_score']:.1f}/100"
    )
    print(f"Kimenet: {OUTPUT_FILE}")
    print(f"Történet: {HISTORY_FILE}")


if __name__ == "__main__":
    main()
