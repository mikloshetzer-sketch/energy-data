#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hormuz Risk Model v2
====================

Purpose
-------
Build a Strait of Hormuz specific risk signal for the energy-data repository.

Target architecture:

    SECURITY RISK       40%
    FLOW DISRUPTION     35%
    LOGISTICS STRESS    25%
                        ↓
                 HORMUZ COMPOSITE

Version 0.1 implements the SECURITY layer.

Primary live sources:
- me-security-monitor/events.json
- me-security-monitor/security-signal.json

Historical/context source:
- me-security-monitor/data/strike_history.json

Important methodological rules:
- AIS is NOT treated as physical throughput.
- Missing/stale data must NOT automatically become zero risk.
- Regional Middle East risk is context, not direct Hormuz risk.
- Hormuz relevance is determined from text + geography + context.
- Flow and logistics layers are intentionally not scored yet.
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

MODEL_VERSION = "2.0-security-alpha"

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

# Events inside this distance may receive geographic relevance.
GEO_MAX_DISTANCE_KM = 650.0

# Live-event scoring window.
LIVE_WINDOW_DAYS = 7

# Older events can still contribute, but with strong decay.
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
    "ship",
    "shipping",
    "maritime",
    "merchant vessel",
    "commercial vessel",
    "cargo ship",
    "oil carrier",
    "naval",
]

ATTACK_TERMS = [
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
    "intercept",
    "interception",
    "seizure",
    "seized",
    "detained",
    "boarding",
    "boarded",
    "sabotage",
    "threat",
    "threatened",
    "fire",
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

FLOW_POSITIVE_TERMS = [
    "exports exceed",
    "exports rise",
    "exports increase",
    "traffic recovers",
    "traffic recovery",
    "flows recover",
    "flows increase",
    "pre-war levels",
    "prewar levels",
]

FLOW_NEGATIVE_TERMS = [
    "exports fall",
    "exports decline",
    "traffic falls",
    "traffic drops",
    "flows fall",
    "flows decline",
    "disruption",
    "closure",
    "closed",
    "blocked",
    "blockade",
]


# ============================================================
# DATA CLASSES
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
    attack: bool
    energy: bool
    regional_maritime: bool
    regional_actor: bool

    distance_km: Optional[float]
    geographic_relevance: float
    textual_relevance: float
    temporal_weight: float

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
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))

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

    return max(0, int(delta.total_seconds() // 86400))


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
            "User-Agent": "energy-data-hormuz-risk-model/2.0",
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
# EVENT ANALYSIS
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


def calculate_textual_relevance(
    direct_hormuz: bool,
    maritime: bool,
    attack: bool,
    energy: bool,
    regional_maritime: bool,
    regional_actor: bool,
) -> float:

    score = 0.0

    # Direct Hormuz reference is strongest.
    if direct_hormuz:
        score += 0.65

    # Regional maritime geography is also strong.
    if regional_maritime:
        score += 0.35

    # Maritime context.
    if maritime:
        score += 0.25

    # Attack/security context.
    if attack:
        score += 0.20

    # Energy relevance.
    if energy:
        score += 0.15

    # Iran/UAE/Oman alone is intentionally weak.
    if regional_actor:
        score += 0.05

    # Context combinations.
    if maritime and attack:
        score += 0.20

    if maritime and energy:
        score += 0.10

    if regional_actor and maritime:
        score += 0.10

    if regional_actor and attack and maritime:
        score += 0.10

    return clamp(score, 0.0, 1.0)


def calculate_event_security_score(
    textual: float,
    geographic: float,
    confidence: float,
    temporal: float,
    direct_hormuz: bool,
    maritime: bool,
    attack: bool,
) -> float:

    # Text is more important than raw coordinates because the
    # ME monitor can assign broad regional locations to relevant
    # Hormuz articles.
    relevance = (
        textual * 0.75
        + geographic * 0.25
    )

    severity = 0.30

    if maritime:
        severity += 0.15

    if attack:
        severity += 0.30

    if maritime and attack:
        severity += 0.15

    if direct_hormuz:
        severity += 0.10

    severity = clamp(
        severity,
        0.0,
        1.0,
    )

    # ME events currently often use confidence around 0.55.
    # We do not want medium confidence to suppress the signal
    # excessively, therefore confidence is converted to a
    # multiplier between 0.70 and 1.00.
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

    attack = contains_any(
        text,
        ATTACK_TERMS,
    )

    energy = contains_any(
        text,
        ENERGY_TERMS,
    )

    regional_maritime = contains_any(
        text,
        REGIONAL_MARITIME_TERMS,
    )

    regional_actor = contains_any(
        text,
        REGIONAL_ACTOR_TERMS,
    )

    location = event.get("location", {})

    distance_km, geo_score = geographic_relevance(
        location
    )

    text_score = calculate_textual_relevance(
        direct_hormuz=direct_hormuz,
        maritime=maritime,
        attack=attack,
        energy=energy,
        regional_maritime=regional_maritime,
        regional_actor=regional_actor,
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
        direct_hormuz=direct_hormuz,
        maritime=maritime,
        attack=attack,
    )

    # Avoid broad regional noise.
    #
    # An event is considered Hormuz relevant if:
    # - it directly mentions Hormuz, OR
    # - it has regional maritime context, OR
    # - it combines maritime content with attack/energy/actor,
    # - or it is geographically close AND has useful context.
    relevant = (
        direct_hormuz
        or regional_maritime
        or (
            maritime
            and (
                attack
                or energy
                or regional_actor
            )
        )
        or (
            geo_score >= 0.60
            and (
                maritime
                or attack
                or energy
            )
        )
    )

    if not relevant:
        return None

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

    flow_positive = contains_any(
        text,
        FLOW_POSITIVE_TERMS,
    )

    flow_negative = contains_any(
        text,
        FLOW_NEGATIVE_TERMS,
    )

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
        attack=attack,
        energy=energy,
        regional_maritime=regional_maritime,
        regional_actor=regional_actor,
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

    recent = [
        event
        for event in events
        if event.age_days <= LIVE_WINDOW_DAYS
    ]

    recent_sorted = sorted(
        recent,
        key=lambda item: item.security_score,
        reverse=True,
    )

    # We use the strongest recent events rather than summing
    # everything. This limits duplicate-news amplification.
    top_scores = [
        event.security_score
        for event in recent_sorted[:8]
    ]

    if top_scores:

        # Weighted top-event signal.
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

    # Regional security is contextual only.
    #
    # It cannot create a high Hormuz score on its own.
    # Its maximum contribution is intentionally limited.
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

        # No current Hormuz-specific events:
        # do NOT return zero automatically.
        #
        # Regional context can provide a low-confidence
        # background estimate, but it is capped.
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

    direct_count = sum(
        1
        for event in recent
        if event.direct_hormuz
    )

    maritime_attack_count = sum(
        1
        for event in recent
        if event.maritime
        and event.attack
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

    if recent:

        if direct_count >= 1:
            confidence = "HIGH"

        elif maritime_attack_count >= 1:
            confidence = "MEDIUM"

        else:
            confidence = "LOW"

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
        "events_7d": len(recent),
        "direct_hormuz_events_7d": direct_count,
        "maritime_attack_events_7d": (
            maritime_attack_count
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
        "role": (
            "historical_context_only"
        ),
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
    relevant_events: List[EventAssessment],
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

    if strike_meta.get("success"):
        score += 10
    else:
        issues.append(
            "Historical strike_history unavailable"
        )

    recent_relevant = [
        event
        for event in relevant_events
        if event.age_days <= LIVE_WINDOW_DAYS
    ]

    if recent_relevant:
        score += 15
    else:
        issues.append(
            "No Hormuz-relevant live events detected in 7-day window"
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
        if event.age_days <= LIVE_WINDOW_DAYS
        and (
            event.flow_positive_signal
            or event.flow_negative_signal
        )
    ]

    # Security is the only operational layer in this version.
    #
    # Flow and Logistics remain explicitly unavailable rather
    # than receiving artificial zero values.
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
                "flow data source. News text is retained only "
                "as a supporting signal."
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

    # No composite is calculated until all three layers are
    # implemented. This prevents a misleading Hormuz score.
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

    top_events = [
        asdict(event)
        for event in assessments[:20]
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
        }
        for event in flow_signals[:10]
    ]

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
                (
                    "AIS is not physical throughput."
                ),
                (
                    "Missing data does not automatically "
                    "equal zero risk."
                ),
                (
                    "Regional Middle East risk is context, "
                    "not direct Hormuz risk."
                ),
                (
                    "Hormuz relevance uses text, geography "
                    "and contextual combinations."
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
        "relevant_event_count_30d": len(
            assessments
        ),
        "relevant_event_count_7d": sum(
            1
            for event in assessments
            if event.age_days
            <= LIVE_WINDOW_DAYS
        ),
        "top_relevant_events": top_events,
        "supporting_flow_news": (
            supporting_flow_news
        ),
    }


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
            "Relevant events 7d:",
            result.get(
                "relevant_event_count_7d"
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
