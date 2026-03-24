import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict

EVENTS_PATH = "events.json"
OUT_PATH = "security-signal.json"

CAT_W = {
    "military": 3.0,
    "security": 2.0,
    "political": 1.0,
    "other": 0.5,
}

ISW_MULT = 1.3


def norm_cat(category: str) -> str:
    category = (category or "other").strip().lower()
    return category if category in CAT_W else "other"


def source_type(event: dict) -> str:
    src_type = (((event.get("source") or {}).get("type")) or "news").strip().lower()
    return "isw" if src_type == "isw" else "news"


def recency_weight(age_days: int, window_days: int) -> float:
    if window_days <= 1:
        return 1.0
    ratio = age_days / (window_days - 1)
    return 1.0 - 0.6 * ratio  # newest=1.0, oldest=0.4


def risk_score(event: dict, age_days: int, window_days: int) -> float:
    category = norm_cat(event.get("category"))
    category_weight = CAT_W[category]
    source_weight = ISW_MULT if source_type(event) == "isw" else 1.0
    time_weight = recency_weight(age_days, window_days)
    return category_weight * source_weight * time_weight


def risk_level(total_risk: float) -> str:
    if total_risk >= 250:
        return "HIGH"
    if total_risk >= 120:
        return "MEDIUM"
    return "LOW"


def confidence_level(source_counts: dict, total_events: int) -> str:
    isw_count = source_counts.get("isw", 0)
    if total_events >= 150 and isw_count >= 3:
        return "HIGH"
    if total_events >= 60:
        return "MEDIUM"
    return "LOW"


def round2(value: float) -> float:
    return round(value, 2)


def round1(value: float) -> float:
    return round(value, 1)


def parse_event_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None


def top_events_payload(events, today, window_days, limit=10):
    scored_events = []

    for event in events:
        event_date = parse_event_date(event.get("date"))
        if not event_date:
            continue

        age_days = (today - event_date).days
        score = risk_score(event, age_days, window_days)

        scored_events.append({
            "score": round2(score),
            "date": event.get("date"),
            "category": norm_cat(event.get("category")),
            "source_type": source_type(event),
            "title": event.get("title"),
            "location": ((event.get("location") or {}).get("name")) or "Unknown",
            "url": ((event.get("source") or {}).get("url")) or None,
        })

    scored_events.sort(key=lambda item: item["score"], reverse=True)
    return scored_events[:limit]


def main():
    now_dt = datetime.now(timezone.utc)
    today = now_dt.date()
    window_days = 7
    period_start = today - timedelta(days=window_days - 1)

    with open(EVENTS_PATH, "r", encoding="utf-8") as f:
        events = json.load(f)

    window_events = []
    for event in events:
        event_date = parse_event_date(event.get("date"))
        if not event_date:
            continue
        if period_start <= event_date <= today:
            window_events.append(event)

    category_counts = defaultdict(int)
    source_counts = defaultdict(int)
    location_risk = defaultdict(float)
    total_risk = 0.0

    for event in window_events:
        category = norm_cat(event.get("category"))
        src = source_type(event)
        location = ((event.get("location") or {}).get("name")) or "Unknown"
        location = location.strip() or "Unknown"

        category_counts[category] += 1
        source_counts[src] += 1

        event_date = parse_event_date(event.get("date"))
        age_days = (today - event_date).days
        score = risk_score(event, age_days, window_days)

        total_risk += score
        location_risk[location] += score

    top_locations = [
        {
            "name": name,
            "risk": round2(score)
        }
        for name, score in sorted(location_risk.items(), key=lambda x: x[1], reverse=True)[:5]
    ]

    total_events = len(window_events)
    level = risk_level(total_risk)
    confidence = confidence_level(source_counts, total_events)

    normalized_risk_score = min(100.0, round1((total_risk / 250.0) * 100.0))

    payload = {
        "meta": {
            "updated": now_dt.strftime("%Y-%m-%d %H:%M UTC"),
            "window_days": window_days,
            "method": "events-based osint signal v1",
            "source_file": EVENTS_PATH,
        },
        "summary": {
            "period_start": period_start.isoformat(),
            "period_end": today.isoformat(),
            "total_events": total_events,
            "total_risk": round2(total_risk),
            "normalized_risk_score": normalized_risk_score,
            "risk_level": level,
            "confidence": confidence,
        },
        "category_counts": {
            "military": category_counts["military"],
            "security": category_counts["security"],
            "political": category_counts["political"],
            "other": category_counts["other"],
        },
        "source_counts": {
            "news": source_counts["news"],
            "isw": source_counts["isw"],
        },
        "top_locations": top_locations,
        "top_events": top_events_payload(window_events, today, window_days, limit=10),
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(
        f"Wrote {OUT_PATH} | "
        f"total_events={total_events} | "
        f"total_risk={round2(total_risk)} | "
        f"normalized={normalized_risk_score}"
    )


if __name__ == "__main__":
    main()
