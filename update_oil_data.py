import json
import os
import re
from datetime import datetime, timezone

import requests

API_KEY = os.environ.get("EIA_API_KEY")

if not API_KEY:
    raise RuntimeError("Hiányzik az EIA_API_KEY secret.")

SPOT_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
INVENTORY_URL = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
STEO_URL = "https://api.eia.gov/v2/steo/data/"
BRIEF_URL = "https://raw.githubusercontent.com/mikloshetzer-sketch/me-security-monitor/main/brief.md"

# Piaci / futures jellegű árforrás
YAHOO_BRENT_URL = "https://query1.finance.yahoo.com/v8/finance/chart/BZ=F?interval=1d&range=5d"
YAHOO_WTI_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CL=F?interval=1d&range=5d"

MONTHS = ["Jan", "Feb", "Már", "Ápr", "Máj", "Jún", "Júl", "Aug", "Szept", "Okt", "Nov", "Dec"]


def request_with_xparams(url: str, x_params: dict):
    params = {"api_key": API_KEY}
    headers = {
        "X-Params": json.dumps(x_params),
        "User-Agent": "energy-dashboard-bot"
    }

    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_rows(data: dict):
    return data.get("response", {}).get("data", [])


def extract_latest_value(data: dict):
    rows = extract_rows(data)
    if not rows:
        return None
    return rows[0].get("value")


def fetch_eia_price(product_code: str, length: int = 1):
    x_params = {
        "frequency": "daily",
        "data": ["value"],
        "facets": {
            "product": [product_code]
        },
        "sort": [
            {
                "column": "period",
                "direction": "desc"
            }
        ],
        "offset": 0,
        "length": length
    }

    data = request_with_xparams(SPOT_URL, x_params)
    return extract_rows(data)


def fetch_inventory():
    x_params = {
        "frequency": "weekly",
        "data": ["value"],
        "facets": {
            "product": ["EPC0"]
        },
        "sort": [
            {
                "column": "period",
                "direction": "desc"
            }
        ],
        "offset": 0,
        "length": 1
    }

    data = request_with_xparams(INVENTORY_URL, x_params)
    return extract_latest_value(data)


def fetch_global_supply():
    x_params = {
        "frequency": "monthly",
        "data": ["value"],
        "facets": {
            "seriesId": ["PAPR_WORLD"]
        },
        "sort": [
            {
                "column": "period",
                "direction": "desc"
            }
        ],
        "offset": 0,
        "length": 1
    }

    data = request_with_xparams(STEO_URL, x_params)
    return extract_latest_value(data)


def fetch_global_supply_series(length: int = 24):
    x_params = {
        "frequency": "monthly",
        "data": ["value"],
        "facets": {
            "seriesId": ["PAPR_WORLD"]
        },
        "sort": [
            {
                "column": "period",
                "direction": "desc"
            }
        ],
        "offset": 0,
        "length": length
    }

    data = request_with_xparams(STEO_URL, x_params)
    return extract_rows(data)


def fetch_geo_risk():
    response = requests.get(BRIEF_URL, timeout=30)
    response.raise_for_status()

    text = response.text
    cleaned = text.replace("**", "")

    patterns = [
        r"Total\s+window\s+risk:\s*([0-9]+(?:\.[0-9]+)?)",
        r"cumulative\s+risk\s+index\s+of\s+([0-9]+(?:\.[0-9]+)?)",
        r"risk\s+index\s+of\s+([0-9]+(?:\.[0-9]+)?)"
    ]

    for pattern in patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            return float(match.group(1))

    return None


def fetch_yahoo_last_price(url: str):
    """
    Piaci / futures jellegű utolsó ár Yahoo chart JSON-ból.
    """
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*"
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    result = data.get("chart", {}).get("result", [])
    if not result:
        return None

    meta = result[0].get("meta", {}) or {}
    regular_market = meta.get("regularMarketPrice")
    if regular_market is not None:
        return float(regular_market)

    closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
    closes = [x for x in closes if x is not None]
    if closes:
        return float(closes[-1])

    return None


def classify_risk(score):
    # Hiányzó forrásadatból nem képzünk mesterséges kockázati szintet.
    if score is None:
        return {"level": "unknown", "label": "Nincs adat"}

    if score < 100:
        return {"level": "low", "label": "Alacsony"}

    if score < 180:
        return {"level": "moderate", "label": "Mérsékelt"}

    if score < 250:
        return {"level": "elevated", "label": "Emelkedett"}

    if score < 350:
        return {"level": "high", "label": "Magas"}

    return {"level": "extreme", "label": "Extrém"}


def market_stress_label(level):
    labels = {
        "normal": "Normál",
        "watch": "Mérsékelten feszült",
        "tense": "Feszült",
        "severe": "Erősen feszült",
        "shock": "Sokkhelyzet",
        "unknown": "Nincs elegendő adat",
    }
    return labels.get(level, "Nincs elegendő adat")


def classify_market_stress(
    risk_level,
    brent_value,
    wti_value,
    brent_1d_change,
    brent_7d_change,
    wti_1d_change,
    wti_7d_change,
    inventory_value,
    supply_value,
):
    """Átlátható, 0–100-as piaci feszültségi index.

    Kizárólag a program által ténylegesen lekért adatokból számol:
    - Brent és WTI piaci árszint,
    - Brent és WTI 1 és 7 napos abszolút ármozgás,
    - Brent–WTI árkülönbség,
    - ME Security Monitor geopolitikai kategória,
    - amerikai kereskedelmi kőolajkészlet,
    - EIA globális kínálati adat.

    A modell nem állít semmit tankerforgalomról, biztosítási díjakról,
    szoroslezárásról vagy tényleges termeléskiesésről, mert ezekhez nincs
    strukturált adatforrás ebben a rendszerben.
    """
    brent = to_float(brent_value)
    wti = to_float(wti_value)
    b1 = to_float(brent_1d_change)
    b7 = to_float(brent_7d_change)
    w1 = to_float(wti_1d_change)
    w7 = to_float(wti_7d_change)
    inventory = to_float(inventory_value)
    supply = to_float(supply_value)

    components = {
        "volatility": 0.0,       # max. 30
        "price_level": 0.0,      # max. 20
        "geopolitical": 0.0,     # max. 20
        "supply_buffer": 0.0,    # max. 15
        "brent_wti_spread": 0.0, # max. 15
    }

    available = {
        "brent_price": brent is not None,
        "wti_price": wti is not None,
        "brent_1d": b1 is not None,
        "brent_7d": b7 is not None,
        "wti_1d": w1 is not None,
        "wti_7d": w7 is not None,
        "geopolitical": risk_level not in [None, "", "unknown"],
        "inventory": inventory is not None,
        "global_supply": supply is not None,
    }

    # 1. Volatilitás: mindkét irányú, abszolút elmozdulás számít.
    daily_moves = [abs(x) for x in [b1, w1] if x is not None]
    weekly_moves = [abs(x) for x in [b7, w7] if x is not None]
    daily_move = max(daily_moves, default=None)
    weekly_move = max(weekly_moves, default=None)

    if daily_move is not None:
        if daily_move >= 12:
            daily_points = 18.0
        elif daily_move >= 8:
            daily_points = 15.0
        elif daily_move >= 5:
            daily_points = 11.0
        elif daily_move >= 3:
            daily_points = 7.0
        elif daily_move >= 1.5:
            daily_points = 3.0
        else:
            daily_points = 0.0
    else:
        daily_points = 0.0

    if weekly_move is not None:
        if weekly_move >= 18:
            weekly_points = 12.0
        elif weekly_move >= 12:
            weekly_points = 10.0
        elif weekly_move >= 8:
            weekly_points = 7.0
        elif weekly_move >= 4:
            weekly_points = 4.0
        else:
            weekly_points = 0.0
    else:
        weekly_points = 0.0

    components["volatility"] = min(daily_points + weekly_points, 30.0)

    # 2. Abszolút árszint. Elsődlegesen Brent, hiányában WTI.
    reference_price = brent if brent is not None else wti
    if reference_price is not None:
        if reference_price >= 110:
            components["price_level"] = 20.0
        elif reference_price >= 100:
            components["price_level"] = 18.0
        elif reference_price >= 95:
            components["price_level"] = 16.0
        elif reference_price >= 90:
            components["price_level"] = 14.0
        elif reference_price >= 85:
            components["price_level"] = 12.0
        elif reference_price >= 80:
            components["price_level"] = 8.0
        elif reference_price >= 75:
            components["price_level"] = 4.0

    # 3. Geopolitikai háttér. Ez háttértényező, önmagában nem okoz sokkhelyzetet.
    geo_points = {
        "low": 0.0,
        "moderate": 5.0,
        "elevated": 10.0,
        "high": 15.0,
        "extreme": 20.0,
    }
    components["geopolitical"] = geo_points.get(risk_level, 0.0)

    # 4. Készlet- és kínálati puffer. Csak tényleges EIA-adatokból.
    buffer_points = 0.0
    if inventory is not None:
        if inventory < 390:
            buffer_points += 9.0
        elif inventory < 410:
            buffer_points += 7.0
        elif inventory < 430:
            buffer_points += 4.0
        elif inventory < 450:
            buffer_points += 2.0

    if supply is not None:
        if supply < 100:
            buffer_points += 6.0
        elif supply < 101:
            buffer_points += 4.0
        elif supply < 102:
            buffer_points += 2.0

    components["supply_buffer"] = min(buffer_points, 15.0)

    # 5. Brent–WTI spread. Csak akkor számolható, ha mindkét ár elérhető.
    spread = None
    if brent is not None and wti is not None:
        spread = brent - wti
        abs_spread = abs(spread)
        if abs_spread >= 12:
            components["brent_wti_spread"] = 15.0
        elif abs_spread >= 9:
            components["brent_wti_spread"] = 12.0
        elif abs_spread >= 6:
            components["brent_wti_spread"] = 8.0
        elif abs_spread >= 3:
            components["brent_wti_spread"] = 4.0

    score = round(min(sum(components.values()), 100.0), 1)

    core_available = sum([
        reference_price is not None,
        daily_move is not None or weekly_move is not None,
        risk_level not in [None, "", "unknown"],
    ])

    if core_available < 2:
        level = "unknown"
    elif score < 20:
        level = "normal"
    elif score < 40:
        level = "watch"
    elif score < 60:
        level = "tense"
    elif score < 80:
        level = "severe"
    else:
        level = "shock"

    triggers = []

    # Minimumszintek kizárólag mérhető piaci adatokból.
    if level != "unknown":
        if daily_move is not None and daily_move >= 12:
            level = "shock"
            triggers.append("legalább 12%-os napi ármozgás")
        elif daily_move is not None and daily_move >= 8 and level in ["normal", "watch", "tense"]:
            level = "severe"
            triggers.append("legalább 8%-os napi ármozgás")

        if reference_price is not None and reference_price >= 85 and risk_level in ["elevated", "high", "extreme"]:
            if level in ["normal", "watch"]:
                level = "tense"
            triggers.append("85 USD feletti olajár emelkedett geopolitikai háttér mellett")

        if reference_price is not None and reference_price >= 95 and risk_level in ["high", "extreme"]:
            if level in ["normal", "watch", "tense"]:
                level = "severe"
            triggers.append("95 USD feletti olajár magas geopolitikai háttér mellett")

    missing = [name for name, is_available in available.items() if not is_available]
    completeness = round((sum(available.values()) / len(available)) * 100)

    if level == "unknown":
        note = "A piaci feszültség nem értékelhető megbízhatóan, mert több alapvető adat nem érhető el."
    elif level == "shock":
        note = "A rendelkezésre álló ár-, volatilitási és háttéradatok szélsőséges piaci reakciót jeleznek."
    elif level == "severe":
        note = "Az árszint, a rövid távú ármozgás és a kockázati háttér erősen feszült piaci környezetet jelez."
    elif level == "tense":
        note = "A rendelkezésre álló mutatók fokozott piaci érzékenységet és érdemi feszültséget jeleznek."
    elif level == "watch":
        note = "A piac érzékenyebb a szokásosnál, de a mért adatok még nem mutatnak szélsőséges reakciót."
    else:
        note = "A rendelkezésre álló piaci adatok jelenleg nem jeleznek rendkívüli rövid távú feszültséget."

    return {
        "score": score if level != "unknown" else None,
        "level": level,
        "label": market_stress_label(level),
        "note": note,
        "components": {key: round(value, 1) for key, value in components.items()},
        "triggers": triggers,
        "methodology": (
            "A 0–100-as mutató a Brent és WTI napi és heti abszolút ármozgását "
            "(30%), az aktuális olajárszintet (20%), a ME Security Monitor "
            "geopolitikai kategóriáját (20%), az EIA amerikai készlet- és globális "
            "kínálati adatait (15%), valamint a Brent–WTI árkülönbséget (15%) értékeli."
        ),
        "limitations": (
            "A mutató nem mér közvetlenül tankerforgalmat, biztosítási díjakat, "
            "szoroslezárást vagy tényleges fizikai termeléskiesést."
        ),
        "data_quality": {
            "completeness_percent": completeness,
            "missing_fields": missing,
            "available_fields": [name for name, is_available in available.items() if is_available],
        },
        "observed_values": {
            "brent": brent,
            "wti": wti,
            "brent_1d_change": b1,
            "brent_7d_change": b7,
            "wti_1d_change": w1,
            "wti_7d_change": w7,
            "brent_wti_spread": round(spread, 2) if spread is not None else None,
            "inventory_million_barrels": round(inventory / 1000, 1) if inventory is not None else None,
            "global_supply_mbd": supply,
            "geopolitical_level": risk_level,
        },
    }


RISK_LEVEL_ORDER = {
    "unknown": -1,
    "low": 0,
    "moderate": 1,
    "elevated": 2,
    "high": 3,
    "extreme": 4,
}


def level_at_least(current_level, minimum_level):
    """A két kockázati szint közül a magasabbat adja vissza."""
    current_rank = RISK_LEVEL_ORDER.get(current_level, 0)
    minimum_rank = RISK_LEVEL_ORDER.get(minimum_level, 0)
    return minimum_level if minimum_rank > current_rank else current_level


def energy_risk_label(level):
    labels = {
        "low": "Alacsony",
        "moderate": "Mérsékelt",
        "elevated": "Emelkedett",
        "high": "Magas",
        "extreme": "Extrém",
    }
    return labels.get(level, "Nincs adat")


def classify_energy_risk(
    geo_risk_score,
    geo_risk_level,
    market_stress_level,
    brent_value,
    wti_value,
    brent_1d_change,
    brent_7d_change,
    wti_1d_change,
    wti_7d_change,
    inventory_value,
    supply_value,
):
    """
    Összetett energiapiaci kockázati index.

    A végső pontszám 0–100 közötti érték. A modell figyelembe veszi:
    - geopolitikai kockázatot,
    - Brent és WTI árszintet,
    - rövid távú ármozgást,
    - piaci stresszt,
    - amerikai készleteket,
    - globális kínálatot.

    A pontszám mellett minimumszint-szabályok is működnek. Ezek megakadályozzák,
    hogy magas olajár és gyors drágulás mellett a rendszer mérsékelt szintet mutasson.
    """
    brent = to_float(brent_value)
    wti = to_float(wti_value)
    b1 = to_float(brent_1d_change) or 0.0
    b7 = to_float(brent_7d_change) or 0.0
    w1 = to_float(wti_1d_change) or 0.0
    w7 = to_float(wti_7d_change) or 0.0
    inventory = to_float(inventory_value)
    supply = to_float(supply_value)

    components = {
        "geopolitical": 0.0,
        "price_level": 0.0,
        "price_momentum": 0.0,
        "market_stress": 0.0,
        "supply_pressure": 0.0,
    }

    # 1. Geopolitikai komponens: maximum 40 pont.
    if geo_risk_score is not None:
        components["geopolitical"] = min(max(float(geo_risk_score) / 350.0 * 40.0, 0.0), 40.0)
    else:
        # Hiányzó geopolitikai pontszám esetén nem képzünk feltételezett pontot.
        components["geopolitical"] = 0.0

    # 2. Abszolút olajárszint: maximum 20 pont.
    reference_price = max([x for x in [brent, wti] if x is not None], default=None)
    if reference_price is not None:
        if reference_price >= 110:
            components["price_level"] = 20.0
        elif reference_price >= 100:
            components["price_level"] = 17.0
        elif reference_price >= 90:
            components["price_level"] = 14.0
        elif reference_price >= 85:
            components["price_level"] = 11.0
        elif reference_price >= 80:
            components["price_level"] = 8.0
        elif reference_price >= 75:
            components["price_level"] = 4.0

    # 3. Rövid távú ármozgás: maximum 20 pont.
    daily_move = max(abs(b1), abs(w1), 0.0)
    weekly_move = max(abs(b7), abs(w7), 0.0)

    if daily_move >= 10:
        daily_points = 12.0
    elif daily_move >= 7:
        daily_points = 10.0
    elif daily_move >= 5:
        daily_points = 8.0
    elif daily_move >= 3:
        daily_points = 6.0
    elif daily_move >= 1.5:
        daily_points = 3.0
    else:
        daily_points = 0.0

    if weekly_move >= 15:
        weekly_points = 8.0
    elif weekly_move >= 10:
        weekly_points = 7.0
    elif weekly_move >= 7:
        weekly_points = 5.0
    elif weekly_move >= 3:
        weekly_points = 3.0
    else:
        weekly_points = 0.0

    components["price_momentum"] = min(daily_points + weekly_points, 20.0)

    # 4. Piaci stressz: maximum 10 pont.
    stress_points = {
        "normal": 0.0,
        "watch": 3.0,
        "tense": 6.0,
        "severe": 8.0,
        "shock": 10.0,
    }
    components["market_stress"] = stress_points.get(market_stress_level, 0.0)

    # 5. Készlet- és kínálati nyomás: maximum 10 pont.
    supply_points = 0.0
    if inventory is not None:
        if inventory < 400:
            supply_points += 6.0
        elif inventory < 420:
            supply_points += 4.0
        elif inventory < 440:
            supply_points += 2.0

    if supply is not None:
        if supply < 100:
            supply_points += 4.0
        elif supply < 101:
            supply_points += 3.0
        elif supply < 102:
            supply_points += 1.0

    components["supply_pressure"] = min(supply_points, 10.0)

    score = round(min(sum(components.values()), 100.0), 1)

    if score < 25:
        level = "low"
    elif score < 40:
        level = "moderate"
    elif score < 55:
        level = "elevated"
    elif score < 75:
        level = "high"
    else:
        level = "extreme"

    triggers = []

    # Minimumszint-szabályok gyors katonai/piaci eszkalációra.
    if geo_risk_level == "extreme" or market_stress_level == "shock":
        level = level_at_least(level, "extreme")
        triggers.append("extrém geopolitikai vagy piaci sokk")

    if geo_risk_level == "high":
        level = level_at_least(level, "high")
        triggers.append("magas geopolitikai kockázat")

    if reference_price is not None and reference_price >= 85 and geo_risk_level in ["moderate", "elevated", "high", "extreme"]:
        level = level_at_least(level, "high")
        triggers.append("85 USD feletti olajár geopolitikai feszültség mellett")

    if daily_move >= 5 and geo_risk_level in ["elevated", "high", "extreme"]:
        level = level_at_least(level, "high")
        triggers.append("legalább 5%-os napi árugrás")

    if weekly_move >= 10 and geo_risk_level in ["moderate", "elevated", "high", "extreme"]:
        level = level_at_least(level, "high")
        triggers.append("legalább 10%-os heti Brent-emelkedés")

    if reference_price is not None and reference_price >= 100 and daily_move >= 5:
        level = level_at_least(level, "extreme")
        triggers.append("100 USD feletti ár és gyors napi emelkedés")

    return {
        "score": score,
        "level": level,
        "label": energy_risk_label(level),
        "components": {key: round(value, 1) for key, value in components.items()},
        "triggers": triggers,
        "method": "Összetett index: geopolitika 40%, árszint 20%, ármozgás 20%, piaci stressz 10%, készlet és kínálat 10%.",
    }


def regional_dependency_model():
    return {
        "europe": {
            "factor": 1.00,
            "label": "Közepes kitettség",
            "icon": "🇪🇺",
            "note": "Európa importfüggő, ezért az ár- és ellátási sokkok gyorsan átgyűrűzhetnek az inflációba és az üzemanyagköltségekbe."
        },
        "america": {
            "factor": 0.60,
            "label": "Alacsonyabb közvetlen kitettség",
            "icon": "🇺🇸",
            "note": "Az USA termelői háttere erősebb, ezért közvetlenül kevésbé sérülékeny, bár a globális árak a hazai üzemanyagpiacon is megjelennek."
        },
        "asia": {
            "factor": 1.55,
            "label": "Nagyon magas kitettség",
            "icon": "🌏",
            "note": "Ázsia erősen függ a közel-keleti importtól, ezért a Hormuzi-szoroshoz és a tankerforgalomhoz kapcsolódó zavarok itt csapódnak le legerősebben."
        }
    }


def classify_level_from_score(score):
    if score >= 12:
        return "extreme", "Nagyon magas hatás"
    if score >= 8:
        return "high", "Magas hatás"
    if score >= 5:
        return "moderate", "Mérsékelt hatás"
    return "low", "Korlátozott hatás"


def build_regional_text(region_key, level):
    texts = {
        "asia": {
            "extreme": "Ázsia a közel-keleti importfüggőség miatt jelenleg a legsérülékenyebb régió, ezért a szállítási zavarok, biztosítási költségek és importárak gyorsan felerősödhetnek.",
            "high": "Az ázsiai piacokon a közel-keleti kitettség miatt az ár- és ellátási sokk erősen érvényesülhet, különösen a finomított termékek és a tengeri szállítás terén.",
            "moderate": "Ázsia továbbra is érzékeny a közel-keleti fejleményekre, de a jelenlegi mozgás még nem utal maximális régiós nyomásra.",
            "low": "Ázsiában is megjelenik a kockázat, de a jelenlegi indikátorok még nem utalnak szélsőséges piaci átgyűrűzésre."
        },
        "europe": {
            "extreme": "Az európai piacokon az importköltségek, finomított termékárak és inflációs kockázatok rendkívül erősen emelkedhetnek.",
            "high": "Európában a jelenlegi helyzet magas árnyomást és költségoldali sérülékenységet jelent, különösen az energiaintenzív ágazatok számára.",
            "moderate": "Az európai piacok érzékelhetően reagálhatnak az olajár-emelkedésre, főként az importköltségek és az üzemanyagárak oldalán.",
            "low": "Európában a hatás jelenleg inkább költségoldali figyelmeztetésként jelenik meg, nem szélsőséges sokkként."
        },
        "america": {
            "extreme": "Az amerikai piacokon is erősen megjelenhet a globális olajsokk, főként az üzemanyagárakon és a finomítói marzsokon keresztül.",
            "high": "Az USA közvetlenül kevésbé sérülékeny, de a globális ármozgás már erőteljesen átszivároghat a hazai üzemanyag- és finomítói piacra.",
            "moderate": "Az amerikai piacra a globális olajár-sokk mérsékelt, de jól érzékelhető üzemanyag- és piaci hangulati hatással járhat.",
            "low": "Az amerikai piac termelői háttere miatt közvetlenül kevésbé sérülékeny, bár a globális ártrendek itt is megjelennek."
        }
    }
    return texts[region_key][level]


def classify_regional_impact(brent_value, wti_value, brent_1d_change, brent_7d_change, risk_score, risk_level):
    dependency = regional_dependency_model()

    brent = brent_value if brent_value is not None else 0
    wti = wti_value if wti_value is not None else 0
    b1 = brent_1d_change if brent_1d_change is not None else 0
    b7 = brent_7d_change if brent_7d_change is not None else 0
    spread = (brent - wti) if (brent_value is not None and wti_value is not None) else 0
    risk = risk_score if risk_score is not None else 0

    base_pressure = (
        (risk / 70.0) +
        max(b1, 0) * 0.80 +
        max(b7, 0) * 0.35 +
        max(spread, 0) * 0.20
    )

    if risk_level == "high":
        base_pressure += 1.2
    elif risk_level == "extreme":
        base_pressure += 2.5

    result = {}

    for region_key, dep in dependency.items():
        score = base_pressure * dep["factor"]
        level, label = classify_level_from_score(score)

        result[region_key] = {
            "level": level,
            "label": label,
            "score": round(score, 1),
            "factor": dep["factor"],
            "icon": dep["icon"],
            "dependency_label": dep["label"],
            "dependency_note": dep["note"],
            "text": build_regional_text(region_key, level)
        }

    return result


def assess_production_impact(risk_level, market_stress_level, current_supply, production_2026, production_2027):
    supply = to_float(current_supply)

    avg_2026 = average_year_values(production_2026)
    avg_2027 = average_year_values(production_2027)

    delta = None
    if avg_2026 is not None and avg_2027 is not None:
        delta = avg_2027 - avg_2026

    if risk_level == "extreme" or market_stress_level == "shock":
        return {
            "level": "critical",
            "label": "Súlyos lefelé mutató kockázat",
            "direction": "negatív",
            "text": "A jelenlegi helyzet tartós fennmaradása már érdemben zavarhatja a termelési és exportpályát, különösen a Közel-Kelethez kapcsolódó ellátási csomópontokon."
        }

    if risk_level == "high" and market_stress_level in ["severe", "shock"]:
        return {
            "level": "high",
            "label": "Jelentős kockázat",
            "direction": "negatív",
            "text": "A magas geopolitikai kockázat és a feszült piaci reakciók növelik annak esélyét, hogy a termelési pálya lefelé módosuljon vagy az exportfolyamatok sérüljenek."
        }

    if risk_level == "high":
        return {
            "level": "elevated",
            "label": "Érdemi lefelé mutató kockázat",
            "direction": "negatív",
            "text": "A jelenlegi geopolitikai környezet önmagában is emeli a termelési kilátások sérülékenységét, még akkor is, ha a globális kínálati pálya egyelőre stabil marad."
        }

    if delta is not None and delta > 0.3 and market_stress_level in ["normal", "watch"]:
        return {
            "level": "stable",
            "label": "Korlátozott hatás",
            "direction": "semleges",
            "text": "A havi termelési pálya alapján a globális kínálat jelenleg viszonylag stabil, így a mostani helyzet inkább kockázatként, mint azonnali termelési kiesésként jelenik meg."
        }

    if supply is not None and supply >= 103:
        return {
            "level": "stable",
            "label": "Korlátozott hatás",
            "direction": "semleges",
            "text": "A jelenlegi globális termelési szint alapján a piac egyelőre rendelkezik bizonyos kínálati pufferekkel, ezért a helyzet rövid távon inkább sérülékenységet, mint közvetlen visszaesést jelez."
        }

    return {
        "level": "watch",
        "label": "Mérsékelt kockázat",
        "direction": "vegyes",
        "text": "A jelenlegi helyzet a termelési pályára még nem gyakorol egyértelmű azonnali törést, de a kockázati környezet romlása gyorsan lefelé mutató hatássá alakulhat."
    }


def fmt_price(value):
    try:
        return f"{float(value):.2f} USD/hordó"
    except Exception:
        return "nincs adat"


def fmt_inventory(value):
    try:
        num = float(value)
        million_barrels = num / 1000
        return f"{million_barrels:.1f} millió hordó"
    except Exception:
        return "nincs adat"


def fmt_supply(value):
    try:
        return f"{float(value):.1f} millió hordó/nap"
    except Exception:
        return "102 millió hordó/nap"


def fmt_percent(value):
    try:
        if value is None:
            return "nincs adat"
        sign = "+" if value > 0 else ""
        return f"{sign}{value:.2f}%"
    except Exception:
        return "nincs adat"


def calculate_trend_percent(rows):
    try:
        if not rows or len(rows) < 2:
            return None

        latest = float(rows[0]["value"])
        oldest = float(rows[-1]["value"])

        if oldest == 0:
            return None

        return ((latest - oldest) / oldest) * 100
    except Exception:
        return None


def calculate_change_from_days(rows, days_back):
    try:
        if not rows or len(rows) < 2:
            return None

        latest = float(rows[0]["value"])
        compare_index = min(days_back, len(rows) - 1)
        previous = float(rows[compare_index]["value"])

        if previous == 0:
            return None

        return ((latest - previous) / previous) * 100
    except Exception:
        return None


def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def format_production_series(rows):
    series = []

    try:
        ordered = list(reversed(rows))

        for row in ordered:
            period = row.get("period")
            value = row.get("value")

            if period is None or value is None:
                continue

            try:
                num_value = round(float(value), 1)
            except Exception:
                continue

            series.append({
                "date": period,
                "value": num_value
            })

    except Exception:
        return []

    return series


def build_year_series(series, year):
    result = []

    for month_index in range(1, 13):
        date_key = f"{year}-{month_index:02d}"
        found = next((item for item in series if item["date"] == date_key), None)

        result.append({
            "month": MONTHS[month_index - 1],
            "value": found["value"] if found else None
        })

    return result


def average_year_values(year_series):
    values = [item["value"] for item in year_series if item.get("value") is not None]
    if not values:
        return None
    return sum(values) / len(values)


def generate_status_text(brent_trend, risk_score):
    if brent_trend is None and risk_score is None:
        return "A globális olajpiacon jelenleg vegyes jelzések láthatók, ezért az irányadó folyamatok csak korlátozottan értékelhetők."

    if risk_score is not None and risk_score >= 350:
        return "A globális olajpiacot extrém geopolitikai kockázati környezet terheli, ami az árak gyors és érzékeny reakcióját vetíti előre."

    if risk_score is not None and risk_score >= 250 and brent_trend is not None and brent_trend > 3:
        return "A globális olajpiacon emelkedő árnyomás és magas geopolitikai kockázat figyelhető meg, ami feszesebb piaci környezetre utal."

    if risk_score is not None and risk_score >= 250:
        return "A globális olajpiacot magas geopolitikai kockázat terheli, miközben a befektetői és ellátásbiztonsági érzékenység is erős maradt."

    if brent_trend is not None and brent_trend >= 5:
        return "Az olajpiacon rövid távon erősebb emelkedő árnyomás érzékelhető, amit a piaci szereplők fokozott óvatossága is kísérhet."

    if brent_trend is not None and brent_trend <= -5:
        return "Az olajpiac rövid távon enyhülő árnyomást mutat, ami kiegyensúlyozottabb piaci környezetre utalhat."

    return "A globális olajpiac jelenleg kiegyensúlyozottabb képet mutat, de a geopolitikai és kínálati tényezők továbbra is érdemben befolyásolják a kilátásokat."


def generate_supply_note(inventory_value, supply_value):
    inventory = to_float(inventory_value)
    supply = to_float(supply_value)

    if inventory is not None and inventory < 420:
        return "Az amerikai készletszint viszonylag alacsonyabb, ami érzékenyebbé teheti a piacot az ellátási zavarokra."

    if inventory is not None and inventory > 460:
        return "Az amerikai készletszint viszonylag magasabb, ami rövid távon részben tompíthatja az árakra nehezedő felfelé mutató nyomást."

    if supply is not None and supply >= 103:
        return "A globális kínálat jelenleg viszonylag stabil, ami részben ellensúlyozhatja a geopolitikai bizonytalanságok árnövelő hatását."

    if supply is not None and supply < 101:
        return "A globális kínálat mérsékeltebb szintje feszesebb piaci egyensúlyt vetíthet előre, különösen geopolitikai zavarok esetén."

    return "A kínálati oldalt egyszerre befolyásolja a globális termelés szintje, az amerikai készlethelyzet és a fő exportőrök piaci magatartása."


def generate_risk_note(risk_score):
    if risk_score is None:
        return "A geopolitikai kockázatok jelenleg is érdemi tényezőt jelentenek, de a pontos kockázati szint nem áll rendelkezésre."

    if risk_score >= 350:
        return "A közel-keleti kockázati környezet extrém szintre emelkedett, ami már közvetlen ellátásbiztonsági és szállítási aggodalmakat is felvethet."

    if risk_score >= 250:
        return "A Közel-Kelethez kapcsolódó geopolitikai kockázatok magas szinten maradtak, ami érzékenyen érintheti a fő energiaszállítási útvonalakat."

    if risk_score >= 180:
        return "A térségben emelkedett kockázati környezet figyelhető meg, ami továbbra is támogatja a geopolitikai kockázati prémium fennmaradását."

    if risk_score >= 100:
        return "A térségben mérsékelt, de figyelmet igénylő geopolitikai kockázatok maradtak fenn."

    return "A geopolitikai kockázatok jelenleg alacsonyabb szinten állnak, bár a stratégiai szűk keresztmetszetek továbbra is érzékeny pontok maradnak."


def generate_drivers_text(brent_trend, risk_score, inventory_value):
    inventory = to_float(inventory_value)

    parts = []

    if risk_score is not None and risk_score >= 250:
        parts.append("A jelenlegi ármozgásokat erősen befolyásolja a közel-keleti geopolitikai kockázatok magas szintje")
    elif risk_score is not None and risk_score >= 180:
        parts.append("A jelenlegi ármozgásokat érezhetően befolyásolja a közel-keleti geopolitikai feszültség")
    else:
        parts.append("A jelenlegi ármozgásokat elsősorban a keresleti és kínálati fundamentumok alakítják")

    if brent_trend is not None and brent_trend >= 5:
        parts.append("miközben a Brent ár elmúlt 30 napos emelkedése is felfelé mutató piaci nyomást jelez")
    elif brent_trend is not None and brent_trend <= -5:
        parts.append("miközben a Brent ár 30 napos csökkenése mérsékeltebb piaci hangulatra utal")
    else:
        parts.append("miközben a Brent ár 30 napos mozgása inkább mérsékelt irányt mutat")

    if inventory is not None and inventory < 420:
        parts.append("és az amerikai készletszint sem nyújt erős puffert az esetleges ellátási zavarokkal szemben")
    elif inventory is not None and inventory > 460:
        parts.append("miközben a magasabb amerikai készletszint részben csökkentheti a rövid távú árnyomást")
    else:
        parts.append("miközben az amerikai készletszint önmagában nem utal szélsőséges piaci helyzetre")

    text = ", ".join(parts)
    text = text[0].upper() + text[1:] + "."
    return text


try:
    brent_rows = fetch_eia_price("EPCBRENT", length=35)
    brent_value = brent_rows[0].get("value") if brent_rows else None
    brent_trend = calculate_trend_percent(brent_rows)
    brent_1d_change = calculate_change_from_days(brent_rows, 1)
    brent_7d_change = calculate_change_from_days(brent_rows, 7)
except Exception:
    brent_rows = []
    brent_value = None
    brent_trend = None
    brent_1d_change = None
    brent_7d_change = None

try:
    wti_rows = fetch_eia_price("EPCWTI", length=15)
    wti_value = wti_rows[0].get("value") if wti_rows else None
    wti_1d_change = calculate_change_from_days(wti_rows, 1)
    wti_7d_change = calculate_change_from_days(wti_rows, 7)
except Exception:
    wti_rows = []
    wti_value = None
    wti_1d_change = None
    wti_7d_change = None

# ÚJ: piaci / futures jellegű árak
try:
    market_brent_value = fetch_yahoo_last_price(YAHOO_BRENT_URL)
except Exception:
    market_brent_value = None

try:
    market_wti_value = fetch_yahoo_last_price(YAHOO_WTI_URL)
except Exception:
    market_wti_value = None

# Ha a market ár nem jön meg, fallback a spot árra
if market_brent_value is None:
    market_brent_value = to_float(brent_value)

if market_wti_value is None:
    market_wti_value = to_float(wti_value)

try:
    inventory_value = fetch_inventory()
except Exception:
    inventory_value = None

try:
    supply_value = fetch_global_supply()
except Exception:
    supply_value = None

try:
    production_rows = fetch_global_supply_series(length=24)
    production_series = format_production_series(production_rows)
except Exception:
    production_series = []

try:
    geo_risk_score = fetch_geo_risk()
except Exception:
    geo_risk_score = None

risk_info = classify_risk(geo_risk_score)

market_stress = classify_market_stress(
    risk_level=risk_info["level"],
    brent_value=market_brent_value,
    wti_value=market_wti_value,
    brent_1d_change=brent_1d_change,
    brent_7d_change=brent_7d_change,
    wti_1d_change=wti_1d_change,
    wti_7d_change=wti_7d_change,
    inventory_value=inventory_value,
    supply_value=supply_value,
)

energy_risk = classify_energy_risk(
    geo_risk_score=geo_risk_score,
    geo_risk_level=risk_info["level"],
    market_stress_level=market_stress["level"],
    brent_value=market_brent_value,
    wti_value=market_wti_value,
    brent_1d_change=brent_1d_change,
    brent_7d_change=brent_7d_change,
    wti_1d_change=wti_1d_change,
    wti_7d_change=wti_7d_change,
    inventory_value=inventory_value,
    supply_value=supply_value,
)

summary_status = generate_status_text(brent_trend, geo_risk_score)
summary_supply = generate_supply_note(inventory_value, supply_value)
summary_risk = generate_risk_note(geo_risk_score)
drivers_text = generate_drivers_text(brent_trend, geo_risk_score, inventory_value)

production_2026 = build_year_series(production_series, 2026)
production_2027 = build_year_series(production_series, 2027)

production_impact = assess_production_impact(
    risk_info["level"],
    market_stress["level"],
    supply_value,
    production_2026,
    production_2027
)

regional_impact = classify_regional_impact(
    brent_value=market_brent_value,
    wti_value=market_wti_value,
    brent_1d_change=brent_1d_change,
    brent_7d_change=brent_7d_change,
    risk_score=geo_risk_score,
    risk_level=risk_info["level"]
)

oil_data = {
    "market": {
        "brent": fmt_price(market_brent_value),
        "wti": fmt_price(market_wti_value),
        "inventory": fmt_inventory(inventory_value),
        "supply": fmt_supply(supply_value),
        "brent_30d_trend": fmt_percent(brent_trend),
        "brent_1d_change": fmt_percent(brent_1d_change),
        "brent_7d_change": fmt_percent(brent_7d_change),
        "wti_1d_change": fmt_percent(wti_1d_change),
        "wti_7d_change": fmt_percent(wti_7d_change)
    },
    "spot": {
        "brent": fmt_price(brent_value),
        "wti": fmt_price(wti_value)
    },
    "production": {
        "current": fmt_supply(supply_value),
        "series": production_series,
        "by_year": {
            "2026": production_2026,
            "2027": production_2027
        }
    },
    "production_impact": {
        "level": production_impact["level"],
        "label": production_impact["label"],
        "direction": production_impact["direction"],
        "text": production_impact["text"]
    },
    "regional_dependency": regional_dependency_model(),
    "regional_impact": regional_impact,
    "meta": {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "source": "EIA + Yahoo Finance + ME Security Monitor"
    },
    "risk": {
        "score": geo_risk_score,
        "level": risk_info["level"],
        "label": risk_info["label"],
        "type": "geopolitical"
    },
    "energy_risk": {
        "score": energy_risk["score"],
        "level": energy_risk["level"],
        "label": energy_risk["label"],
        "components": energy_risk["components"],
        "triggers": energy_risk["triggers"],
        "method": energy_risk["method"]
    },
    "market_stress": {
        "score": market_stress["score"],
        "level": market_stress["level"],
        "label": market_stress["label"],
        "note": market_stress["note"],
        "components": market_stress["components"],
        "triggers": market_stress["triggers"],
        "methodology": market_stress["methodology"],
        "limitations": market_stress["limitations"],
        "data_quality": market_stress["data_quality"],
        "observed_values": market_stress["observed_values"]
    },
    "forecast": {
        "one_month": "nincs automatikus előrejelzés",
        "three_months": "nincs automatikus előrejelzés",
        "twelve_months": "nincs automatikus előrejelzés",
        "note": "A rendszer jelenleg megfigyelt adatokat értékel, önálló árfolyam-előrejelzést nem készít."
    },
    "summary": {
        "status": summary_status,
        "supply_note": summary_supply,
        "risk_note": summary_risk
    },
    "drivers": {
        "text": drivers_text
    },
    "notes": {
        "price_basis": "A market blokk piaci (futures) árakat használ, a spot blokk EIA napi spot adatokat tartalmaz.",
        "chart_basis": "A diagram piaci napi jegyzést követ, ezért eltérhet a spot adatoktól.",
        "production_basis": "A termelési görbe havi EIA STEO adatsor.",
        "regional_basis": "A modell a geopolitikai kockázatot és ármozgást együtt értékeli.",
        "energy_risk_basis": "A fő energiapiaci barométer kizárólag a lekért geopolitikai, ár-, készlet- és kínálati adatokból képez összetett indexet.",
        "market_stress_basis": "A piaci feszültség a Brent és WTI árszintjét, napi és heti abszolút változását, a Brent–WTI spreadet, a geopolitikai kategóriát, valamint az EIA készlet- és kínálati adatokat használja.",
        "model_limitations": "A rendszer nem mér közvetlenül tankerforgalmat, biztosítási díjakat, szoroslezárást vagy tényleges fizikai termeléskiesést."
    }
}

with open("oil-data.json", "w", encoding="utf-8") as f:
    json.dump(oil_data, f, ensure_ascii=False, indent=2)

print("oil-data.json frissítve (átlátható energy_risk + market_stress modellel)")
       
