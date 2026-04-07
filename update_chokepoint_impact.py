import json
import os
from datetime import datetime, timezone

import requests

TANKER_INPUT_FILE = "tanker-data.json"
OUTPUT_FILE = "chokepoint-impact.json"
HISTORY_FILE = "chokepoint-impact-history.json"

ME_SECURITY_SIGNAL_URL = (
    "https://raw.githubusercontent.com/"
    "mikloshetzer-sketch/me-security-monitor/main/security-signal.json"
)

CONFLICT_END_MATRIX_URL = (
    "https://raw.githubusercontent.com/"
    "mikloshetzer-sketch/conflict-end-matrix/main/dashboard_data.json"
)

CHOKEPOINTS = {
    "hormuz": {
        "name": "Hormuzi-szoros",
        "region": "middle_east",
        "trade_share": 0.16,
        "energy_share": 0.30,
        "substitution_penalty": 0.95,
        "disruption_level": 0.78,
        "notes": "Kiemelten fontos olaj- és energiatranzit pont.",
    },
    "bab_el_mandeb": {
        "name": "Bab el-Mandeb",
        "region": "middle_east",
        "trade_share": 0.10,
        "energy_share": 0.12,
        "substitution_penalty": 0.72,
        "disruption_level": 0.72,
        "notes": "Vörös-tengeri és Szuez felé irányuló forgalom kulcspontja.",
    },
    "suez": {
        "name": "Szuezi térség",
        "region": "middle_east",
        "trade_share": 0.12,
        "energy_share": 0.08,
        "substitution_penalty": 0.68,
        "disruption_level": 0.55,
        "notes": "A globális kereskedelem egyik fő tengelye, kerülhető, de jelentős költséggel.",
    },
    "bosporus": {
        "name": "Boszporusz",
        "region": "black_sea",
        "trade_share": 0.04,
        "energy_share": 0.05,
        "substitution_penalty": 0.70,
        "disruption_level": 0.22,
        "notes": "Erős regionális jelentőség, különösen a Fekete-tenger irányában.",
    },
    "malacca": {
        "name": "Malaka-szoros",
        "region": "asia",
        "trade_share": 0.24,
        "energy_share": 0.22,
        "substitution_penalty": 0.88,
        "disruption_level": 0.10,
        "notes": "Ázsiai tengeri kereskedelem és energiaáramlás egyik fő útvonala.",
    },
    "panama": {
        "name": "Panama-csatorna",
        "region": "americas",
        "trade_share": 0.06,
        "energy_share": 0.03,
        "substitution_penalty": 0.62,
        "disruption_level": 0.18,
        "notes": "Főként globális logisztikai és konténerforgalmi jelentőség.",
    },
    "gibraltar": {
        "name": "Gibraltári-szoros",
        "region": "europe",
        "trade_share": 0.08,
        "energy_share": 0.06,
        "substitution_penalty": 0.55,
        "disruption_level": 0.08,
        "notes": "Atlanti–mediterrán átjáró, magas általános tengeri jelentőséggel.",
    },
}

REGIONAL_ADJUSTMENT = {
    "middle_east": 1.10,
    "black_sea": 1.00,
    "asia": 1.00,
    "americas": 1.00,
    "europe": 1.00,
}

DYNAMIC_ZONE_MAP = {
    "hormuz": "hormuz",
    "bab_el_mandeb": "bab_el_mandeb",
    "suez": "suez",
    "bosporus": "bosporus",
}

AIS_SIGNAL_MULTIPLIER = {
    0: 0.95,
    1: 1.00,
    2: 1.03,
    3: 1.06,
    4: 1.08,
    5: 1.10,
}

# Frissességi küszöbök órában
ME_SIGNAL_MAX_AGE_HOURS = 48
CONFLICT_SIGNAL_MAX_AGE_HOURS = 48


def safe_load_json(path, default):
    if not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            content = f.read().strip()
            if not content:
                return default
            return json.loads(content)
    except Exception as e:
        print(
            f"Figyelmeztetés: hibás vagy üres JSON fájl ({path}), "
            f"alapérték használata. Hiba: {e}"
        )
        return default


def save_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def clamp(value, min_value, max_value):
    return max(min_value, min(value, max_value))


def round4(value):
    return round(value, 4)


def round2(value):
    return round(value, 2)


def parse_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", ".").strip())
        except Exception:
            return None
    return None


def parse_datetime_utc(value):
    if not value or not isinstance(value, str):
        return None

    candidates = [
        "%Y-%m-%d %H:%M UTC",
        "%Y-%m-%d %H:%M:%S UTC",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d",
    ]

    for fmt in candidates:
        try:
            dt = datetime.strptime(value.strip(), fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

    return None


def hours_since(timestamp_str, now):
    dt = parse_datetime_utc(timestamp_str)
    if not dt:
        return None
    return (now - dt).total_seconds() / 3600.0


def is_stale(timestamp_str, now, max_age_hours):
    age = hours_since(timestamp_str, now)
    if age is None:
        return True
    return age > max_age_hours


def combined_weight(trade_share, energy_share):
    return round4((0.4 * trade_share) + (0.6 * energy_share))


def get_ais_zone_counts(tanker_data):
    if not tanker_data:
        return {
            "hormuz": 0,
            "suez": 0,
            "bab_el_mandeb": 0,
            "bosporus": 0,
        }

    summary = tanker_data.get("summary", {}) or {}

    return {
        "hormuz": int(summary.get("in_hormuz", 0) or 0),
        "suez": int(summary.get("in_suez", 0) or 0),
        "bab_el_mandeb": int(summary.get("in_bab_el_mandeb", 0) or 0),
        "bosporus": int(summary.get("in_bosporus", 0) or 0),
    }


def ais_signal_multiplier(count):
    if count >= 5:
        return AIS_SIGNAL_MULTIPLIER[5]
    return AIS_SIGNAL_MULTIPLIER.get(count, 1.0)


def estimated_impact_score(cfg, ais_count):
    cw = combined_weight(cfg["trade_share"], cfg["energy_share"])
    disruption = cfg["disruption_level"]
    substitution = cfg["substitution_penalty"]
    regional = REGIONAL_ADJUSTMENT.get(cfg["region"], 1.0)
    ais_mult = ais_signal_multiplier(ais_count)

    score = cw * disruption * substitution * regional * ais_mult
    return round4(score), cw, ais_mult


def status_from_score(score):
    if score >= 0.18:
        return "severe"
    if score >= 0.11:
        return "high"
    if score >= 0.06:
        return "medium"
    return "low"


def build_items(tanker_data):
    zone_counts = get_ais_zone_counts(tanker_data)
    items = []

    for key, cfg in CHOKEPOINTS.items():
        ais_zone = DYNAMIC_ZONE_MAP.get(key)
        ais_count = zone_counts.get(ais_zone, 0) if ais_zone else 0

        score, cw, ais_mult = estimated_impact_score(cfg, ais_count)

        items.append({
            "key": key,
            "name": cfg["name"],
            "region": cfg["region"],
            "trade_share": cfg["trade_share"],
            "energy_share": cfg["energy_share"],
            "combined_weight": cw,
            "substitution_penalty": cfg["substitution_penalty"],
            "disruption_level": cfg["disruption_level"],
            "regional_adjustment": REGIONAL_ADJUSTMENT.get(cfg["region"], 1.0),
            "ais_count_signal": ais_count,
            "ais_signal_multiplier": ais_mult,
            "estimated_impact": score,
            "status": status_from_score(score),
            "notes": cfg["notes"],
        })

    items.sort(key=lambda x: x["estimated_impact"], reverse=True)
    return items, zone_counts


def global_trade_risk_index(items):
    if not items:
        return 0

    total = sum(item["estimated_impact"] for item in items)
    scaled = total / 1.2 * 100
    return round(clamp(scaled, 0, 100), 1)


def middle_east_conflict_impact(items):
    me_items = [x for x in items if x["region"] == "middle_east"]
    if not me_items:
        return {"label": "unknown", "score": 0}

    score = sum(x["estimated_impact"] for x in me_items)
    scaled = round(clamp((score / 0.6) * 100, 0, 100), 1)

    if scaled >= 75:
        label = "severe"
    elif scaled >= 55:
        label = "high"
    elif scaled >= 30:
        label = "medium"
    else:
        label = "low"

    return {
        "label": label,
        "score": scaled,
    }


def top_risk_summary(items, limit=5):
    top_items = items[:limit]
    return [
        {
            "name": item["name"],
            "estimated_impact": item["estimated_impact"],
            "status": item["status"],
        }
        for item in top_items
    ]


def fetch_me_security_signal(now):
    try:
        response = requests.get(ME_SECURITY_SIGNAL_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Figyelmeztetés: me-security signal letöltése sikertelen: {e}")
        return {
            "normalized_risk_score": 0.0,
            "raw_total_risk": 0.0,
            "risk_level": "UNKNOWN",
            "confidence": "LOW",
            "total_events": 0,
            "top_locations": [],
            "available": False,
            "updated": None,
            "stale": True,
            "age_hours": None,
            "used_in_blend": False,
        }

    summary = data.get("summary", {}) or {}
    meta = data.get("meta", {}) or {}

    updated = meta.get("updated")
    stale = is_stale(updated, now, ME_SIGNAL_MAX_AGE_HOURS)
    age_hours = hours_since(updated, now)

    return {
        "normalized_risk_score": parse_number(summary.get("normalized_risk_score")) or 0.0,
        "raw_total_risk": parse_number(summary.get("total_risk")) or 0.0,
        "risk_level": summary.get("risk_level") or "UNKNOWN",
        "confidence": summary.get("confidence") or "LOW",
        "total_events": int(summary.get("total_events", 0) or 0),
        "top_locations": data.get("top_locations", []) or [],
        "available": True,
        "updated": updated,
        "stale": stale,
        "age_hours": round2(age_hours) if age_hours is not None else None,
        "used_in_blend": not stale,
    }


def normalize_conflict_index(value):
    """
    A conflict-end-matrix numerikus indexét 0-100 skálára húzza.
    Konzervatív skála:
      -100 vagy alacsonyabb -> 100
      0 vagy magasabb       -> 0
    """
    num = parse_number(value)
    if num is None:
        return 0.0

    normalized = abs(num)
    normalized = clamp(normalized, 0, 100)
    return round(normalized, 1)


def fetch_conflict_end_matrix_signal(now):
    try:
        response = requests.get(CONFLICT_END_MATRIX_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Figyelmeztetés: conflict-end-matrix letöltése sikertelen: {e}")
        return {
            "available": False,
            "updated": None,
            "age_hours": None,
            "stale": True,
            "used_in_blend": False,
            "conflict_index_raw": None,
            "conflict_index_normalized": 0.0,
            "assessment": "unknown",
            "trajectory": "unknown",
            "outlook": "unknown",
            "article_count": 0,
        }

    updated = (
        data.get("updated")
        or data.get("meta", {}).get("updated")
        or data.get("generated_at")
    )

    conflict_index_raw = (
        data.get("conflict_index")
        if "conflict_index" in data
        else data.get("summary", {}).get("conflict_index")
    )

    article_count = (
        data.get("article_count")
        if "article_count" in data
        else data.get("summary", {}).get("article_count", 0)
    )

    assessment = (
        data.get("assessment")
        or data.get("summary", {}).get("assessment")
        or "unknown"
    )
    trajectory = (
        data.get("trajectory")
        or data.get("summary", {}).get("trajectory")
        or "unknown"
    )
    outlook = (
        data.get("outlook")
        or data.get("summary", {}).get("outlook")
        or "unknown"
    )

    stale = is_stale(updated, now, CONFLICT_SIGNAL_MAX_AGE_HOURS)
    age_hours = hours_since(updated, now)

    return {
        "available": True,
        "updated": updated,
        "age_hours": round2(age_hours) if age_hours is not None else None,
        "stale": stale,
        "used_in_blend": not stale,
        "conflict_index_raw": parse_number(conflict_index_raw),
        "conflict_index_normalized": normalize_conflict_index(conflict_index_raw),
        "assessment": assessment,
        "trajectory": trajectory,
        "outlook": outlook,
        "article_count": int(article_count or 0),
    }


def effective_signal_score(signal_dict, key_name):
    """
    Ha a jel nem elérhető vagy stale, akkor 0-val vesszük figyelembe.
    """
    if not signal_dict.get("available"):
        return 0.0
    if signal_dict.get("stale"):
        return 0.0
    return parse_number(signal_dict.get(key_name)) or 0.0


def blend_middle_east_score(structural_score, me_signal_score, conflict_score):
    return round(
        clamp(
            (0.50 * structural_score) +
            (0.30 * me_signal_score) +
            (0.20 * conflict_score),
            0,
            100,
        ),
        1,
    )


def blend_global_trade_score(structural_global_score, me_signal_score, conflict_score):
    return round(
        clamp(
            (0.75 * structural_global_score) +
            (0.15 * me_signal_score) +
            (0.10 * conflict_score),
            0,
            100,
        ),
        1,
    )


def impact_label_from_score(score):
    if score >= 75:
        return "severe"
    if score >= 55:
        return "high"
    if score >= 30:
        return "medium"
    return "low"


def append_history(history, timestamp, global_index_value, me_impact, items):
    if history is None or not isinstance(history, dict):
        history = {"snapshots": []}

    snapshots = history.get("snapshots", [])
    if not isinstance(snapshots, list):
        snapshots = []

    today = timestamp[:10]

    snapshots.append({
        "timestamp": timestamp,
        "date": today,
        "global_trade_risk_index": global_index_value,
        "middle_east_conflict_impact_score": me_impact["score"],
        "top_risks": top_risk_summary(items, limit=5),
    })

    history["snapshots"] = snapshots[-120:]
    return history


def find_previous_day_snapshot(history, today):
    if not history or not isinstance(history, dict):
        return None

    snapshots = history.get("snapshots", [])
    if not isinstance(snapshots, list):
        return None

    candidates = [s for s in snapshots if s.get("date") != today]
    if not candidates:
        return None

    return candidates[-1]


def compute_daily_change(current_global_index, current_me_score, previous_snapshot):
    if not previous_snapshot:
        return {
            "global_trade_risk_index_change": None,
            "middle_east_conflict_impact_change": None,
            "direction_global": "n/a",
            "direction_middle_east": "n/a",
        }

    prev_global = previous_snapshot.get("global_trade_risk_index", 0)
    prev_me = previous_snapshot.get("middle_east_conflict_impact_score", 0)

    global_change = round2(current_global_index - prev_global)
    me_change = round2(current_me_score - prev_me)

    return {
        "global_trade_risk_index_change": global_change,
        "middle_east_conflict_impact_change": me_change,
        "direction_global": "up" if global_change > 0 else "down" if global_change < 0 else "flat",
        "direction_middle_east": "up" if me_change > 0 else "down" if me_change < 0 else "flat",
    }


def all_zone_counts_zero(zone_counts):
    if not isinstance(zone_counts, dict):
        return True
    return all(int(v or 0) == 0 for v in zone_counts.values())


def main():
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%d %H:%M UTC")
    today = now.strftime("%Y-%m-%d")

    tanker_data = safe_load_json(TANKER_INPUT_FILE, default=None)
    history = safe_load_json(HISTORY_FILE, default={"snapshots": []})

    items, zone_counts = build_items(tanker_data)

    structural_global_index = global_trade_risk_index(items)
    structural_me_impact = middle_east_conflict_impact(items)

    me_signal = fetch_me_security_signal(now)
    conflict_signal = fetch_conflict_end_matrix_signal(now)

    me_signal_score = effective_signal_score(me_signal, "normalized_risk_score")
    conflict_signal_score = effective_signal_score(conflict_signal, "conflict_index_normalized")

    global_index_value = blend_global_trade_score(
        structural_global_index,
        me_signal_score,
        conflict_signal_score,
    )

    blended_me_score = blend_middle_east_score(
        structural_me_impact["score"],
        me_signal_score,
        conflict_signal_score,
    )

    me_impact = {
        "label": impact_label_from_score(blended_me_score),
        "score": blended_me_score,
        "components": {
            "structural_score": structural_me_impact["score"],

            "osint_signal_score": me_signal["normalized_risk_score"],
            "osint_raw_total_risk": me_signal["raw_total_risk"],
            "osint_risk_level": me_signal["risk_level"],
            "osint_confidence": me_signal["confidence"],
            "osint_total_events": me_signal["total_events"],
            "osint_top_locations": me_signal["top_locations"],
            "osint_updated": me_signal["updated"],
            "osint_available": me_signal["available"],
            "osint_stale": me_signal["stale"],
            "osint_age_hours": me_signal["age_hours"],
            "osint_used_in_blend": me_signal["used_in_blend"],

            "conflict_signal_score": conflict_signal["conflict_index_normalized"],
            "conflict_signal_raw_index": conflict_signal["conflict_index_raw"],
            "conflict_signal_assessment": conflict_signal["assessment"],
            "conflict_signal_trajectory": conflict_signal["trajectory"],
            "conflict_signal_outlook": conflict_signal["outlook"],
            "conflict_signal_article_count": conflict_signal["article_count"],
            "conflict_signal_updated": conflict_signal["updated"],
            "conflict_signal_available": conflict_signal["available"],
            "conflict_signal_stale": conflict_signal["stale"],
            "conflict_signal_age_hours": conflict_signal["age_hours"],
            "conflict_signal_used_in_blend": conflict_signal["used_in_blend"],
        }
    }

    previous_snapshot = find_previous_day_snapshot(history, today)
    daily_change = compute_daily_change(
        global_index_value,
        me_impact["score"],
        previous_snapshot
    )

    tanker_zero_flag = all_zone_counts_zero(zone_counts)

    payload = {
        "meta": {
            "updated": timestamp,
            "method": (
                "chokepoint structural impact model v3 + "
                "me osint signal v1 + conflict end matrix signal v1"
            ),
            "uses_tanker_signal": True,
            "uses_me_security_signal": True,
            "uses_conflict_end_matrix_signal": True,
            "tanker_input_source": TANKER_INPUT_FILE,
            "me_security_input_source": ME_SECURITY_SIGNAL_URL,
            "conflict_end_matrix_input_source": CONFLICT_END_MATRIX_URL,
            "stale_input_flags": {
                "all_tracked_zone_counts_zero": tanker_zero_flag,
                "me_security_signal_stale": me_signal["stale"],
                "conflict_end_matrix_signal_stale": conflict_signal["stale"],
            },
            "blend_weights": {
                "middle_east": {
                    "structural_score": 0.50,
                    "me_security_signal": 0.30,
                    "conflict_end_matrix_signal": 0.20,
                },
                "global": {
                    "structural_global_score": 0.75,
                    "me_security_signal": 0.15,
                    "conflict_end_matrix_signal": 0.10,
                },
            },
        },
        "global_trade_risk_index": global_index_value,
        "middle_east_conflict_impact": me_impact,
        "daily_change": daily_change,
        "tracked_zone_counts": zone_counts,
        "top_risks": top_risk_summary(items, limit=5),
        "chokepoints": items,
    }

    history = append_history(
        history,
        timestamp=timestamp,
        global_index_value=global_index_value,
        me_impact=me_impact,
        items=items
    )

    save_json(OUTPUT_FILE, payload)
    save_json(HISTORY_FILE, history)

    print(f"{OUTPUT_FILE} frissítve.")
    print(f"{HISTORY_FILE} frissítve.")
    print(
        "ME security signal | "
        f"available={me_signal['available']} | "
        f"stale={me_signal['stale']} | "
        f"score={me_signal['normalized_risk_score']} | "
        f"events={me_signal['total_events']}"
    )
    print(
        "Conflict end matrix signal | "
        f"available={conflict_signal['available']} | "
        f"stale={conflict_signal['stale']} | "
        f"normalized_score={conflict_signal['conflict_index_normalized']} | "
        f"articles={conflict_signal['article_count']}"
    )
    print(
        "Tracked zones | "
        f"all_zero={tanker_zero_flag} | counts={zone_counts}"
    )


if __name__ == "__main__":
    main()
