#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Risk Model v2.1
======================

Purpose
-------
Build a Strait of Hormuz specific risk signal for the energy-data repository.

Target architecture:

    SECURITY RISK       40%
    FLOW DISRUPTION     35%
    LOGISTICS STRESS    25%
                        ↓
                 HORMUZ COMPOSITE

Version 2.1 improves the SECURITY layer.

Main methodological changes from v2.0:
- Geographic proximity alone cannot make an event Hormuz-security relevant.
- Aviation / airline incidents are explicitly excluded from maritime security.
- "War" or generic military context is not treated as a physical attack.
- Flow/recovery news is separated from security events.
- Direct Hormuz political threats/closure/blockade statements are treated
  separately from physical maritime attacks.
- Maritime + attack combinations remain strong security signals.
- Regional ME security remains contextual only.
- Stale strike history remains historical context only.
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# ============================================================
# CONFIG
# ============================================================

MODEL_VERSION = "2.1-security-filter"

OUTPUT_FILE = Path("hormuz-risk.json")

ME_BASE_URL = (
    "https://raw.githubusercontent.com/"
    "mikloshetzer-sketch/me-security-monitor/main"
)

EVENTS_URL = f"{ME_BASE_URL}/events.json"
SECURITY_SIGNAL_URL = f"{ME_BASE_URL}/security-signal.json"
STRIKE_HISTORY_URL = f"{ME_BASE_URL}/data/strike_history.json"

HTTP_TIMEOUT_SECONDS = 20

# Approximate centre point for Strait of Hormuz.
HORMUZ_LAT = 26.56
HORMUZ_LON = 56.25

# Geography is supporting context only.
GEO_MAX_DISTANCE_KM = 650.0

LIVE_WINDOW_DAYS = 7
MAX_EVENT_AGE_DAYS = 30


# ============================================================
# KEYWORDS
# ============================================================

DIRECT_HORMUZ_TERMS = [
    "strait of hormuz",
    "hormuz",
    "hormuzi",
    "hormuzi-szoros",
]

MARITIME_TERMS = [
    "tanker",
    "oil tanker",
    "crude tanker",
    "lng tanker",
    "vessel",
    "merchant vessel",
    "commercial vessel",
    "cargo ship",
    "oil carrier",
    "shipping",
    "maritime",
    "ship lane",
    "ship lanes",
    "shipping lane",
    "shipping lanes",
    "naval vessel",
    "warship",
    "merchant ship",
]

PHYSICAL_ATTACK_TERMS = [
    "tanker attacked",
    "tanker struck",
    "vessel attacked",
    "vessel struck",
    "ship attacked",
    "ship struck",
    "maritime attack",
    "naval attack",
    "hit by missile",
    "hit by projectile",
    "struck by missile",
    "struck by projectile",
    "anti-ship missile",
    "anti ship missile",
    "sea mine",
    "naval mine",
    "drone boat",
    "drone vessel",
    "explosion aboard",
    "explosion on board",
    "sabotage",
]

GENERIC_ATTACK_TERMS = [
    "attack",
    "attacked",
    "strike",
    "struck",
    "hit",
    "projectile",
    "missile",
    "anti-ship",
    "anti ship",
    "drone",
    "uav",
    "usv",
    "mine",
    "explosion",
    "sabotage",
]

MARITIME_SECURITY_TERMS = [
    "seizure",
    "seized",
    "boarding",
    "boarded",
    "detained vessel",
    "intercepted vessel",
    "interception of vessel",
    "hijack",
    "hijacked",
    "piracy",
    "harassment",
]

CLOSURE_THREAT_TERMS = [
    "hormuz closed",
    "hormuz closure",
    "close hormuz",
    "close the strait",
    "strait will remain closed",
    "strait remains closed",
    "will not reopen",
    "not reopen",
    "blockade",
    "blocked",
    "blocking the strait",
    "shipping blockade",
    "ship lanes blocked",
    "ship lanes choked",
    "chokes ship lanes",
    "closure of the strait",
]

ENERGY_TERMS = [
    "oil",
    "crude",
    "lng",
    "gas",
    "energy",
    "petroleum",
    "terminal",
    "refinery",
    "aramco",
    "pipeline",
    "export",
    "exports",
    "shipment",
    "shipments",
    "barrel",
    "barrels",
]

ENERGY_INFRASTRUCTURE_TERMS = [
    "oil terminal",
    "lng terminal",
    "gas terminal",
    "export terminal",
    "oil facility",
    "gas facility",
    "refinery",
    "pipeline",
    "loading terminal",
    "port facility",
]

REGIONAL_MARITIME_TERMS = [
    "persian gulf",
    "gulf of oman",
    "oman gulf",
    "fujairah",
    "bandar abbas",
    "kharg",
    "jebel ali",
    "ras tanura",
    "uae waters",
    "iranian waters",
    "omani waters",
]

REGIONAL_ACTOR_TERMS = [
    "iran",
    "iranian",
    "oman",
    "omani",
    "uae",
    "emirates",
    "united arab emirates",
]

# These terms identify stories that are primarily aviation related.
# They should not become Hormuz maritime-security events merely
# because Dubai/UAE is geographically close.
AVIATION_TERMS = [
    "flydubai",
    "airline",
    "aircraft",
    "airplane",
    "aeroplane",
    "flight",
    "co-pilot",
    "copilot",
    "pilot",
    "cockpit",
    "passenger plane",
    "aviation",
    "airport",
]

FLOW_POSITIVE_TERMS = [
    "exports exceed",
    "exports rise",
    "exports increase",
    "exports recover",
    "traffic recovers",
    "traffic recovery",
    "flows recover",
    "flows increase",
    "shipments rise",
    "shipments increase",
    "shipments through hormuz",
    "highest since",
    "pre-war levels",
    "prewar levels",
    "back to pre-war",
    "back to prewar",
    "transported",
    "million barrels",
]

FLOW_NEGATIVE_TERMS = [
    "exports fall",
    "exports decline",
    "traffic falls",
    "traffic drops",
    "flows fall",
    "flows decline",
    "shipments fall",
    "shipments decline",
    "disruption",
    "closure",
    "closed",
    "blocked",
    "blockade",
    "chokes ship lanes",
]


# ============================================================
# DATA CLASS
# ============================================================

@dataclass
class EventAssessment:
    event_id: str
    date: str
    title: str
    source: str
    source_url: str
    category: str
    location: str
    confidence: float
    age_days: int

    direct_hormuz: bool
    maritime: bool
    physical_attack: bool
    generic_attack: bool
    maritime_security: bool
    closure_threat: bool
    energy: bool
    energy_infrastructure: bool
    regional_maritime: bool
    regional_actor: bool
    aviation: bool

    distance_km: Optional[float]
    geographic_relevance: float
    textual_relevance: float
    temporal_weight: float

    security_event_type: str
    security_score: float
    relevance_level: str

    flow_positive_signal: bool
    flow_negative_signal: bool


# ============================================================
# BASIC HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def contains_any(text: str, terms: List[str]) -> bool:
    return any(term in text for term in terms)


def parse_date(value: Any) -> Optional[datetime]:
    if not value:
        return None

    text = str(value).strip()

    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M UTC",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            return dt.astimezone(timezone.utc)

        except ValueError:
            continue

    try:
        dt = datetime.fromisoformat(
            text.replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except ValueError:
        return None


def age_in_days(date_value: Any) -> Optional[int]:
    dt = parse_date(date_value)

    if dt is None:
        return None

    delta = utc_now() - dt

    return max(
        0,
        int(delta.total_seconds() // 86400),
    )


# ============================================================
# NETWORK
# ============================================================

def fetch_json(url: str) -> Tuple[Optional[Any], Dict[str, Any]]:
    metadata = {
        "url": url,
        "success": False,
        "error": None,
    }

    request = Request(
        url,
        headers={
            "User-Agent": "energy-data-hormuz-risk-model/2.1",
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(
            request,
            timeout=HTTP_TIMEOUT_SECONDS,
        ) as response:

            raw = response.read().decode("utf-8")
            data = json.loads(raw)

            metadata["success"] = True

            return data, metadata

    except HTTPError as exc:
        metadata["error"] = f"HTTP {exc.code}: {exc.reason}"

    except URLError as exc:
        metadata["error"] = f"URL error: {exc.reason}"

    except TimeoutError:
        metadata["error"] = "Request timeout"

    except json.JSONDecodeError as exc:
        metadata["error"] = f"Invalid JSON: {exc}"

    except Exception as exc:
        metadata["error"] = f"{type(exc).__name__}: {exc}"

    return None, metadata


# ============================================================
# GEOGRAPHY
# ============================================================

def haversine_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:

    radius = 6371.0

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)

    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1)
        * math.cos(phi2)
        * math.sin(delta_lambda / 2.0) ** 2
    )

    c = 2.0 * math.atan2(
        math.sqrt(a),
        math.sqrt(1.0 - a),
    )

    return radius * c


def geographic_relevance(
    location: Dict[str, Any],
) -> Tuple[Optional[float], float]:

    if not isinstance(location, dict):
        return None, 0.0

    lat = location.get("lat")
    lon = location.get("lng")

    try:
        lat = float(lat)
        lon = float(lon)

    except (TypeError, ValueError):
        return None, 0.0

    distance = haversine_km(
        HORMUZ_LAT,
        HORMUZ_LON,
        lat,
        lon,
    )

    if distance <= 100:
        score = 1.0

    elif distance <= 250:
        score = 0.85

    elif distance <= 400:
        score = 0.60

    elif distance <= GEO_MAX_DISTANCE_KM:
        score = 0.30

    else:
        score = 0.0

    return round(distance, 1), score


# ============================================================
# TEMPORAL DECAY
# ============================================================

def temporal_weight(age_days: int) -> float:

    if age_days <= 1:
        return 1.00

    if age_days <= 3:
        return 0.90

    if age_days <= 7:
        return 0.75

    if age_days <= 14:
        return 0.50

    if age_days <= 21:
        return 0.30

    if age_days <= 30:
        return 0.15

    return 0.0


# ============================================================
# EVENT TEXT
# ============================================================

def build_event_text(event: Dict[str, Any]) -> str:

    title = normalize_text(event.get("title"))
    summary = normalize_text(event.get("summary"))

    tags = event.get("tags", [])

    if isinstance(tags, list):
        tags_text = " ".join(
            normalize_text(tag)
            for tag in tags
        )
    else:
        tags_text = normalize_text(tags)

    location = event.get("location", {})

    if isinstance(location, dict):
        location_text = normalize_text(
            location.get("name")
        )
    else:
        location_text = normalize_text(location)

    return " ".join(
        [
            title,
            summary,
            tags_text,
            location_text,
        ]
    )


# ============================================================
# EVENT CLASSIFICATION
# ============================================================

def classify_security_event(
    *,
    direct_hormuz: bool,
    maritime: bool,
    physical_attack: bool,
    generic_attack: bool,
    maritime_security: bool,
    closure_threat: bool,
    energy_infrastructure: bool,
    regional_maritime: bool,
    aviation: bool,
) -> str:

    # Aviation incidents are excluded unless there is also an
    # explicit, independent Hormuz maritime-security component.
    if aviation and not (
        maritime
        or maritime_security
        or closure_threat
    ):
        return "NOT_SECURITY"

    if maritime and physical_attack:
        return "MARITIME_ATTACK"

    if maritime and generic_attack:
        return "MARITIME_ATTACK"

    if maritime and maritime_security:
        return "MARITIME_SECURITY_INCIDENT"

    if direct_hormuz and closure_threat:
        return "HORMUZ_CLOSURE_THREAT"

    if regional_maritime and closure_threat:
        return "MARITIME_DISRUPTION_THREAT"

    if energy_infrastructure and generic_attack:
        return "ENERGY_INFRASTRUCTURE_ATTACK"

    # Direct Hormuz mention + generic attack is accepted only if
    # maritime context is also present. This prevents generic war
    # reporting from becoming a Hormuz attack.
    if (
        direct_hormuz
        and maritime
        and generic_attack
    ):
        return "HORMUZ_MARITIME_THREAT"

    return "NOT_SECURITY"


def calculate_textual_relevance(
    *,
    direct_hormuz: bool,
    maritime: bool,
    physical_attack: bool,
    generic_attack: bool,
    maritime_security: bool,
    closure_threat: bool,
    energy: bool,
    energy_infrastructure: bool,
    regional_maritime: bool,
    regional_actor: bool,
    security_event_type: str,
) -> float:

    if security_event_type == "NOT_SECURITY":
        return 0.0

    score = 0.0

    if direct_hormuz:
        score += 0.35

    if regional_maritime:
        score += 0.25

    if maritime:
        score += 0.20

    if physical_attack:
        score += 0.35

    elif generic_attack:
        score += 0.20

    if maritime_security:
        score += 0.25

    if closure_threat:
        score += 0.30

    if energy_infrastructure:
        score += 0.15

    elif energy:
        score += 0.05

    if regional_actor:
        score += 0.05

    return clamp(score, 0.0, 1.0)


def calculate_event_security_score(
    *,
    textual: float,
    geographic: float,
    confidence: float,
    temporal: float,
    event_type: str,
) -> float:

    if event_type == "NOT_SECURITY":
        return 0.0

    # Geography is now only a small supporting modifier.
    relevance = (
        textual * 0.90
        + geographic * 0.10
    )

    severity_by_type = {
        "MARITIME_ATTACK": 1.00,
        "MARITIME_SECURITY_INCIDENT": 0.80,
        "HORMUZ_CLOSURE_THREAT": 0.75,
        "MARITIME_DISRUPTION_THREAT": 0.65,
        "ENERGY_INFRASTRUCTURE_ATTACK": 0.75,
        "HORMUZ_MARITIME_THREAT": 0.70,
    }

    severity = severity_by_type.get(
        event_type,
        0.0,
    )

    confidence_multiplier = (
        0.70
        + clamp(confidence, 0.0, 1.0) * 0.30
    )

    score = (
        100.0
        * relevance
        * severity
        * confidence_multiplier
        * temporal
    )

    return round(
        clamp(score, 0.0, 100.0),
        2,
    )


def relevance_level(score: float) -> str:

    if score >= 60:
        return "CRITICAL"

    if score >= 40:
        return "HIGH"

    if score >= 20:
        return "MEDIUM"

    if score >= 8:
        return "LOW"

    return "MINIMAL"


# ============================================================
# EVENT ASSESSMENT
# ============================================================

def assess_event(
    event: Dict[str, Any],
) -> Optional[EventAssessment]:

    date_value = event.get("date")
    age = age_in_days(date_value)

    if age is None:
        return None

    if age > MAX_EVENT_AGE_DAYS:
        return None

    text = build_event_text(event)

    direct_hormuz = contains_any(
        text,
        DIRECT_HORMUZ_TERMS,
    )

    maritime = contains_any(
        text,
        MARITIME_TERMS,
    )

    physical_attack = contains_any(
        text,
        PHYSICAL_ATTACK_TERMS,
    )

    generic_attack = contains_any(
        text,
        GENERIC_ATTACK_TERMS,
    )

    maritime_security = contains_any(
        text,
        MARITIME_SECURITY_TERMS,
    )

    closure_threat = contains_any(
        text,
        CLOSURE_THREAT_TERMS,
    )

    energy = contains_any(
        text,
        ENERGY_TERMS,
    )

    energy_infrastructure = contains_any(
        text,
        ENERGY_INFRASTRUCTURE_TERMS,
    )

    regional_maritime = contains_any(
        text,
        REGIONAL_MARITIME_TERMS,
    )

    regional_actor = contains_any(
        text,
        REGIONAL_ACTOR_TERMS,
    )

    aviation = contains_any(
        text,
        AVIATION_TERMS,
    )

    flow_positive = contains_any(
        text,
        FLOW_POSITIVE_TERMS,
    )

    flow_negative = contains_any(
        text,
        FLOW_NEGATIVE_TERMS,
    )

    location = event.get("location", {})

    distance_km, geo_score = geographic_relevance(
        location
    )

    security_event_type = classify_security_event(
        direct_hormuz=direct_hormuz,
        maritime=maritime,
        physical_attack=physical_attack,
        generic_attack=generic_attack,
        maritime_security=maritime_security,
        closure_threat=closure_threat,
        energy_infrastructure=energy_infrastructure,
        regional_maritime=regional_maritime,
        aviation=aviation,
    )

    # Keep an event if it has either:
    # 1. a real Hormuz security role, or
    # 2. a useful Hormuz flow signal.
    #
    # This allows recovery/throughput news to survive for the
    # future Flow layer without contaminating Security.
    useful_flow_event = (
        direct_hormuz
        and (
            flow_positive
            or flow_negative
            or energy
        )
    )

    useful_regional_flow_event = (
        maritime
        and energy
        and (
            flow_positive
            or flow_negative
        )
    )

    if (
        security_event_type == "NOT_SECURITY"
        and not useful_flow_event
        and not useful_regional_flow_event
    ):
        return None

    text_score = calculate_textual_relevance(
        direct_hormuz=direct_hormuz,
        maritime=maritime,
        physical_attack=physical_attack,
        generic_attack=generic_attack,
        maritime_security=maritime_security,
        closure_threat=closure_threat,
        energy=energy,
        energy_infrastructure=energy_infrastructure,
        regional_maritime=regional_maritime,
        regional_actor=regional_actor,
        security_event_type=security_event_type,
    )

    temporal = temporal_weight(age)

    confidence = clamp(
        safe_float(
            event.get("confidence"),
            0.5,
        ),
        0.0,
        1.0,
    )

    security_score = calculate_event_security_score(
        textual=text_score,
        geographic=geo_score,
        confidence=confidence,
        temporal=temporal,
        event_type=security_event_type,
    )

    source = event.get("source", {})

    if isinstance(source, dict):
        source_name = str(
            source.get("name", "")
        )
        source_url = str(
            source.get("url", "")
        )
    else:
        source_name = str(source)
        source_url = ""

    if isinstance(location, dict):
        location_name = str(
            location.get("name", "")
        )
    else:
        location_name = str(location)

    return EventAssessment(
        event_id=str(
            event.get("id", "")
        ),
        date=str(date_value),
        title=str(
            event.get("title", "")
        ),
        source=source_name,
        source_url=source_url,
        category=str(
            event.get("category", "")
        ),
        location=location_name,
        confidence=round(
            confidence,
            3,
        ),
        age_days=age,

        direct_hormuz=direct_hormuz,
        maritime=maritime,
        physical_attack=physical_attack,
        generic_attack=generic_attack,
        maritime_security=maritime_security,
        closure_threat=closure_threat,
        energy=energy,
        energy_infrastructure=energy_infrastructure,
        regional_maritime=regional_maritime,
        regional_actor=regional_actor,
        aviation=aviation,

        distance_km=distance_km,
        geographic_relevance=round(
            geo_score,
            3,
        ),
        textual_relevance=round(
            text_score,
            3,
        ),
        temporal_weight=round(
            temporal,
            3,
        ),

        security_event_type=security_event_type,
        security_score=security_score,
        relevance_level=relevance_level(
            security_score
        ),

        flow_positive_signal=flow_positive,
        flow_negative_signal=flow_negative,
    )


# ============================================================
# REGIONAL SECURITY CONTEXT
# ============================================================

def extract_regional_security(
    security_signal: Any,
) -> Dict[str, Any]:

    result = {
        "available": False,
        "score": None,
        "risk_level": None,
        "confidence": None,
        "updated": None,
        "age_days": None,
        "freshness": "UNAVAILABLE",
    }

    if not isinstance(
        security_signal,
        dict,
    ):
        return result

    meta = security_signal.get(
        "meta",
        {},
    )

    summary = security_signal.get(
        "summary",
        {},
    )

    if not isinstance(summary, dict):
        return result

    score = summary.get(
        "normalized_risk_score"
    )

    if score is None:
        return result

    updated = None

    if isinstance(meta, dict):
        updated = meta.get("updated")

    age = age_in_days(updated)

    if age is None:
        freshness = "UNKNOWN"

    elif age <= 1:
        freshness = "FRESH"

    elif age <= 3:
        freshness = "RECENT"

    elif age <= 7:
        freshness = "AGING"

    else:
        freshness = "STALE"

    result.update(
        {
            "available": True,
            "score": round(
                safe_float(score),
                2,
            ),
            "risk_level": summary.get(
                "risk_level"
            ),
            "confidence": summary.get(
                "confidence"
            ),
            "updated": updated,
            "age_days": age,
            "freshness": freshness,
        }
    )

    return result


# ============================================================
# SECURITY AGGREGATION
# ============================================================

def aggregate_security(
    events: List[EventAssessment],
    regional_security: Dict[str, Any],
) -> Dict[str, Any]:

    # Only genuine security events enter the security score.
    security_events = [
        event
        for event in events
        if (
            event.age_days <= LIVE_WINDOW_DAYS
            and event.security_event_type
            != "NOT_SECURITY"
        )
    ]

    security_events.sort(
        key=lambda item: item.security_score,
        reverse=True,
    )

    top_scores = [
        event.security_score
        for event in security_events[:8]
    ]

    if top_scores:

        weights = [
            1.00,
            0.85,
            0.70,
            0.55,
            0.45,
            0.35,
            0.25,
            0.20,
        ]

        weighted_sum = 0.0
        weight_sum = 0.0

        for score, weight in zip(
            top_scores,
            weights,
        ):
            weighted_sum += score * weight
            weight_sum += weight

        event_signal = (
            weighted_sum / weight_sum
            if weight_sum
            else 0.0
        )

    else:
        event_signal = None

    regional_score = (
        regional_security.get("score")
        if regional_security.get(
            "available"
        )
        else None
    )

    regional_freshness = (
        regional_security.get(
            "freshness"
        )
    )

    if regional_score is not None:

        regional_context = clamp(
            regional_score,
            0.0,
            100.0,
        )

        if regional_freshness == "STALE":
            regional_context *= 0.40

        elif regional_freshness == "AGING":
            regional_context *= 0.70

    else:
        regional_context = None

    if event_signal is not None:

        if regional_context is not None:
            final_score = (
                event_signal * 0.85
                + regional_context * 0.15
            )

        else:
            final_score = event_signal

    else:

        # No live Hormuz-specific security events.
        # Regional ME context may provide only a capped
        # low-confidence background estimate.
        if regional_context is not None:
            final_score = min(
                regional_context * 0.35,
                25.0,
            )
        else:
            final_score = None

    if final_score is not None:
        final_score = round(
            clamp(
                final_score,
                0.0,
                100.0,
            ),
            2,
        )

    maritime_attack_count = sum(
        1
        for event in security_events
        if event.security_event_type
        == "MARITIME_ATTACK"
    )

    closure_count = sum(
        1
        for event in security_events
        if event.security_event_type
        in {
            "HORMUZ_CLOSURE_THREAT",
            "MARITIME_DISRUPTION_THREAT",
        }
    )

    direct_hormuz_security_count = sum(
        1
        for event in security_events
        if event.direct_hormuz
    )

    if final_score is None:
        level = "UNKNOWN"

    elif final_score >= 80:
        level = "SEVERE"

    elif final_score >= 60:
        level = "HIGH"

    elif final_score >= 40:
        level = "ELEVATED"

    elif final_score >= 20:
        level = "GUARDED"

    else:
        level = "LOW"

    if maritime_attack_count >= 1:
        confidence = "HIGH"

    elif direct_hormuz_security_count >= 1:
        confidence = "MEDIUM"

    elif security_events:
        confidence = "MEDIUM"

    elif regional_score is not None:
        confidence = "LOW"

    else:
        confidence = "UNAVAILABLE"

    return {
        "score": final_score,
        "level": level,
        "confidence": confidence,
        "event_signal": (
            round(event_signal, 2)
            if event_signal is not None
            else None
        ),
        "regional_context_score": (
            round(regional_context, 2)
            if regional_context is not None
            else None
        ),
        "security_events_7d": len(
            security_events
        ),
        "direct_hormuz_security_events_7d": (
            direct_hormuz_security_count
        ),
        "maritime_attack_events_7d": (
            maritime_attack_count
        ),
        "closure_threat_events_7d": (
            closure_count
        ),
    }


# ============================================================
# HISTORICAL STRIKE DATA
# ============================================================

def inspect_strike_history(
    strike_history: Any,
) -> Dict[str, Any]:

    result = {
        "available": False,
        "generated_at": None,
        "age_days": None,
        "freshness": "UNAVAILABLE",
        "event_count": None,
        "role": "historical_context_only",
    }

    if not isinstance(
        strike_history,
        dict,
    ):
        return result

    generated_at = strike_history.get(
        "generated_at"
    )

    age = age_in_days(generated_at)

    if age is None:
        freshness = "UNKNOWN"

    elif age <= 3:
        freshness = "RECENT"

    elif age <= 7:
        freshness = "AGING"

    else:
        freshness = "STALE"

    summary = strike_history.get(
        "summary",
        {},
    )

    event_count = None

    if isinstance(summary, dict):
        event_count = summary.get(
            "event_count"
        )

    result.update(
        {
            "available": True,
            "generated_at": generated_at,
            "age_days": age,
            "freshness": freshness,
            "event_count": event_count,
        }
    )

    return result


# ============================================================
# DATA QUALITY
# ============================================================

def calculate_data_quality(
    events_meta: Dict[str, Any],
    security_meta: Dict[str, Any],
    strike_meta: Dict[str, Any],
    assessments: List[EventAssessment],
) -> Dict[str, Any]:

    score = 0
    issues: List[str] = []

    if events_meta.get("success"):
        score += 50
    else:
        issues.append(
            "ME events.json unavailable"
        )

    if security_meta.get("success"):
        score += 25
    else:
        issues.append(
            "ME security-signal.json unavailable"
        )

    # Historical strike data has deliberately low importance.
    if strike_meta.get("success"):
        score += 10
    else:
        issues.append(
            "Historical strike_history unavailable"
        )

    live_security = [
        event
        for event in assessments
        if (
            event.age_days <= LIVE_WINDOW_DAYS
            and event.security_event_type
            != "NOT_SECURITY"
        )
    ]

    if live_security:
        score += 15
    else:
        issues.append(
            "No live Hormuz-specific security events detected"
        )

    score = int(
        clamp(score, 0, 100)
    )

    if score >= 85:
        level = "GOOD"

    elif score >= 65:
        level = "MODERATE"

    elif score >= 40:
        level = "LIMITED"

    else:
        level = "POOR"

    return {
        "score": score,
        "level": level,
        "issues": issues,
    }


# ============================================================
# MAIN MODEL
# ============================================================

def build_model() -> Dict[str, Any]:

    generated_at = utc_now()

    print(
        "Fetching ME Security Monitor data..."
    )

    events_data, events_meta = fetch_json(
        EVENTS_URL
    )

    security_data, security_meta = fetch_json(
        SECURITY_SIGNAL_URL
    )

    strike_data, strike_meta = fetch_json(
        STRIKE_HISTORY_URL
    )

    assessments: List[EventAssessment] = []

    if isinstance(events_data, list):

        for event in events_data:

            if not isinstance(event, dict):
                continue

            assessment = assess_event(event)

            if assessment is not None:
                assessments.append(
                    assessment
                )

    assessments.sort(
        key=lambda item: (
            item.security_score,
            -item.age_days,
        ),
        reverse=True,
    )

    regional_security = (
        extract_regional_security(
            security_data
        )
    )

    security_layer = aggregate_security(
        assessments,
        regional_security,
    )

    historical_context = (
        inspect_strike_history(
            strike_data
        )
    )

    data_quality = calculate_data_quality(
        events_meta,
        security_meta,
        strike_meta,
        assessments,
    )

    flow_signals = [
        event
        for event in assessments
        if (
            event.age_days <= LIVE_WINDOW_DAYS
            and (
                event.flow_positive_signal
                or event.flow_negative_signal
            )
        )
    ]

    security_events = [
        event
        for event in assessments
        if (
            event.age_days <= LIVE_WINDOW_DAYS
            and event.security_event_type
            != "NOT_SECURITY"
        )
    ]

    layers = {
        "security": {
            "weight": 0.40,
            "status": "ACTIVE",
            **security_layer,
        },

        "flow": {
            "weight": 0.35,
            "status": "NOT_YET_IMPLEMENTED",
            "score": None,
            "note": (
                "Physical throughput requires a dedicated "
                "flow data source. News text is retained "
                "only as a supporting signal."
            ),
            "supporting_news_signals": len(
                flow_signals
            ),
        },

        "logistics": {
            "weight": 0.25,
            "status": "NOT_YET_IMPLEMENTED",
            "score": None,
            "note": (
                "Freight, insurance, dark-transit, STS and "
                "alternative-route indicators will be added "
                "in a later model stage."
            ),
        },
    }

    composite = {
        "score": None,
        "level": "PENDING",
        "status": (
            "WAITING_FOR_FLOW_AND_LOGISTICS"
        ),
        "method": (
            "security 40% + flow 35% + logistics 25%"
        ),
    }

    top_security_events = [
        asdict(event)
        for event in sorted(
            security_events,
            key=lambda item: item.security_score,
            reverse=True,
        )[:20]
    ]

    supporting_flow_news = [
        {
            "date": event.date,
            "title": event.title,
            "source": event.source,
            "source_url": event.source_url,
            "positive_flow_signal": (
                event.flow_positive_signal
            ),
            "negative_flow_signal": (
                event.flow_negative_signal
            ),
            "security_event_type": (
                event.security_event_type
            ),
        }
        for event in flow_signals[:15]
    ]

    # Diagnostic list:
    # useful for checking stories retained for Flow but excluded
    # from Security.
    non_security_flow_events = [
        {
            "date": event.date,
            "title": event.title,
            "source": event.source,
            "security_event_type": (
                event.security_event_type
            ),
            "positive_flow_signal": (
                event.flow_positive_signal
            ),
            "negative_flow_signal": (
                event.flow_negative_signal
            ),
        }
        for event in flow_signals
        if event.security_event_type
        == "NOT_SECURITY"
    ][:15]

    return {
        "meta": {
            "model": "Hormuz Risk Model",
            "version": MODEL_VERSION,
            "generated_at": (
                generated_at.isoformat()
            ),
            "repository": "energy-data",
            "automatic": True,
        },

        "methodology": {
            "security_weight": 0.40,
            "flow_weight": 0.35,
            "logistics_weight": 0.25,

            "principles": [
                "AIS is not physical throughput.",
                (
                    "Missing data does not automatically "
                    "equal zero risk."
                ),
                (
                    "Regional Middle East risk is context, "
                    "not direct Hormuz risk."
                ),
                (
                    "Geographic proximity alone cannot "
                    "create a Hormuz security event."
                ),
                (
                    "Aviation incidents are excluded from "
                    "Hormuz maritime security."
                ),
                (
                    "Flow/recovery reporting is separated "
                    "from physical security incidents."
                ),
                (
                    "Historical strike history is context "
                    "and calibration data, not the primary "
                    "live security feed."
                ),
            ],
        },

        "sources": {
            "events": events_meta,
            "regional_security": security_meta,
            "strike_history": strike_meta,
        },

        "regional_security": regional_security,

        "historical_context": historical_context,

        "data_quality": data_quality,

        "layers": layers,

        "composite": composite,

        "security_event_count_7d": len(
            security_events
        ),

        "flow_signal_count_7d": len(
            flow_signals
        ),

        "top_security_events": (
            top_security_events
        ),

        "supporting_flow_news": (
            supporting_flow_news
        ),

        "non_security_flow_events": (
            non_security_flow_events
        ),
    }


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    try:
        result = build_model()

        OUTPUT_FILE.write_text(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        security = (
            result.get("layers", {})
            .get("security", {})
        )

        quality = result.get(
            "data_quality",
            {},
        )

        print()
        print("Hormuz Risk Model completed")
        print("---------------------------")

        print(
            "Model version:",
            result.get("meta", {}).get(
                "version"
            ),
        )

        print(
            "Security score:",
            security.get("score"),
        )

        print(
            "Security level:",
            security.get("level"),
        )

        print(
            "Security confidence:",
            security.get("confidence"),
        )

        print(
            "Security events 7d:",
            result.get(
                "security_event_count_7d"
            ),
        )

        print(
            "Maritime attacks 7d:",
            security.get(
                "maritime_attack_events_7d"
            ),
        )

        print(
            "Closure threats 7d:",
            security.get(
                "closure_threat_events_7d"
            ),
        )

        print(
            "Flow signals 7d:",
            result.get(
                "flow_signal_count_7d"
            ),
        )

        print(
            "Data quality:",
            quality.get("level"),
            f"({quality.get('score')}/100)",
        )

        print(
            "Output:",
            OUTPUT_FILE,
        )

        return 0

    except Exception as exc:

        print(
            f"ERROR: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
