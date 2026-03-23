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
    if score is None:
        return {"level": "elevated", "label": "Emelkedett"}

    if score < 100:
        return {"level": "low", "label": "Alacsony"}

    if score < 180:
        return {"level": "moderate", "label": "Mérsékelt"}

    if score < 250:
        return {"level": "elevated", "label": "Emelkedett"}

    if score < 350:
        return {"level": "high", "label": "Magas"}

    return {"level": "extreme", "label": "Extrém"}


def classify_market_stress(risk_level, brent_1d_change, brent_7d_change, wti_1d_change):
    b1 = brent_1d_change if brent_1d_change is not None else 0
    b7 = brent_7d_change if brent_7d_change is not None else 0
    w1 = wti_1d_change if wti_1d_change is not None else 0

    if risk_level == "extreme":
        return {
            "level": "shock",
            "label": "Sokkhelyzet",
            "note": "Az ármozgások és a geopolitikai háttér rendkívül feszült piaci környezetre utalnak."
        }

    if risk_level == "high" and (b1 >= 10 or w1 >= 10):
        return {
            "level": "shock",
            "label": "Sokkhelyzet",
            "note": "A magas geopolitikai kockázat és a hirtelen napi árugrások sokkszerű piaci reakciót jeleznek."
        }

    if risk_level == "high" and (b1 >= 5 or b7 >= 10):
        return {
            "level": "severe",
            "label": "Erősen feszült",
            "note": "A piac intenzíven reagál a geopolitikai kockázatokra, az árak rövid távon erősen volatilisek."
        }

    if risk_level == "high" or b7 >= 7 or w1 >= 5:
        return {
            "level": "tense",
            "label": "Feszült",
            "note": "A piaci környezet feszült, az árak emelkedése és a kockázati háttér együtt növeli a bizonytalanságot."
        }

    if risk_level == "elevated" or b1 >= 3 or b7 >= 3:
        return {
            "level": "watch",
            "label": "Mérsékelten feszült",
            "note": "A piacon emelkedő érzékenység látható, de még nem alakult ki szélsőséges sokkhelyzet."
        }

    return {
        "level": "normal",
        "label": "Normál",
        "note": "A piaci reakciók jelenleg nem utalnak szélsőséges rövid távú feszültségre."
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
    risk = risk_score if risk_score is not None else 180

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
    risk_info["level"],
    brent_1d_change,
    brent_7d_change,
    wti_1d_change
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
       
