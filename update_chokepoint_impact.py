import json
import os
from datetime import datetime, timezone

TANKER_INPUT_FILE = "tanker-data.json"
OUTPUT_FILE = "chokepoint-impact.json"

# --------------------------------------------------
# V2 MODEL CONFIG
# --------------------------------------------------
# Ezek V1/V2 elemzői baseline értékek.
# Nem "hard truth", hanem tudatos modellparaméterek,
# amelyeket később finomíthatsz saját módszertan szerint.
#
# trade_share: az adott chokepoint szerepe a tengeri kereskedelemben
# energy_share: az adott chokepoint szerepe az energiakereskedelemben
# substitution_penalty: mennyire nehéz helyettesíteni / megkerülni
# disruption_level: aktuális kockázati szint 0-1 skálán
#
# combined_weight = 0.4 * trade_share + 0.6 * energy_share
# estimated_impact = combined_weight * disruption_level * substitution_penalty
# --------------------------------------------------

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

# Opcionális regionális korrekció
REGIONAL_ADJUSTMENT = {
    "middle_east": 1.10,
    "black_sea": 1.00,
    "asia": 1.00,
    "americas": 1.00,
    "europe": 1.00,
}

# AIS csak kiegészítő jelzésként szerepel.
DYNAMIC_ZONE_MAP = {
    "hormuz": "hormuz",
    "bab_el_mandeb": "bab_el_mandeb",
    "suez": "suez",
    "bosporus": "bosporus",
}

# Ezek csak soft jelzésként módosítják az impactet, nem dominálják.
AIS_SIGNAL_MULTIPLIER = {
    0: 0.95,
    1: 1.00,
    2: 1.03,
    3: 1.06,
    4: 1.08,
    5: 1.10,
}


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def clamp(value, min_value, max_value):
    return max(min_value, min(value, max_value))


def round4(value):
    return round(value, 4)


def combined_weight(trade_share, energy_share):
    # Energia-orientált súlyozás
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

    # Skálázott 0-100 index
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


def top_risk_summary(items, limit=3):
    top_items = items[:limit]
    return [
        {
            "name": item["name"],
            "estimated_impact": item["estimated_impact"],
            "status": item["status"],
        }
        for item in top_items
    ]


def main():
    tanker_data = load_json(TANKER_INPUT_FILE)
    items, zone_counts = build_items(tanker_data)

    payload = {
        "meta": {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "method": "chokepoint structural impact model v2",
            "uses_tanker_signal": True,
            "tanker_input_source": TANKER_INPUT_FILE,
        },
        "global_trade_risk_index": global_trade_risk_index(items),
        "middle_east_conflict_impact": middle_east_conflict_impact(items),
        "tracked_zone_counts": zone_counts,
        "top_risks": top_risk_summary(items, limit=5),
        "chokepoints": items,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"{OUTPUT_FILE} frissítve.")


if __name__ == "__main__":
    main()
