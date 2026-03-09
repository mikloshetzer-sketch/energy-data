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

oil_data = {
    "market": {
        "brent": fmt_price(brent_value),
        "wti": fmt_price(wti_value),
        "inventory": fmt_inventory(inventory_value),
        "supply": fmt_supply(supply_value),
        "brent_30d_trend": fmt_percent(brent_trend),
        "brent_1d_change": fmt_percent(brent_1d_change),
        "brent_7d_change": fmt_percent(brent_7d_change),
        "wti_1d_change": fmt_percent(wti_1d_change),
        "wti_7d_change": fmt_percent(wti_7d_change)
    },
    "production": {
        "current": fmt_supply(supply_value),
        "series": production_series,
        "by_year": {
            "2026": production_2026,
            "2027": production_2027
        }
    },
    "meta": {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "source": "EIA + GitHub Actions + ME Security Monitor"
    },
    "risk": {
        "score": geo_risk_score,
        "level": risk_info["level"],
        "label": risk_info["label"]
    },
    "market_stress": {
        "level": market_stress["level"],
        "label": market_stress["label"],
        "note": market_stress["note"]
    },
    "forecast": {
        "one_month": "80–85 USD/hordó",
        "three_months": "78–90 USD/hordó",
        "twelve_months": "75–95 USD/hordó"
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
        "price_basis": "Az árak EIA napi spot adatok, nem valós idejű futures jegyzések.",
        "chart_basis": "A diagram valós idejű vagy közel valós idejű piaci jegyzést mutathat, ezért eltérhet a napi spot adatoktól.",
        "production_basis": "A termelési görbe havi, részben előretekintő EIA STEO adatsor, ezért jövőbeli hónapokat is tartalmazhat."
    }
}

with open("oil-data.json", "w", encoding="utf-8") as f:
    json.dump(oil_data, f, ensure_ascii=False, indent=2)

print("oil-data.json frissítve (összes meglévő funkció + piaci feszültség + 2026/2027 termelési görbék).")
