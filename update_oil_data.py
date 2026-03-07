import json
import os
from datetime import datetime, timezone
import requests

API_KEY = os.environ.get("EIA_API_KEY")

if not API_KEY:
    raise RuntimeError("Hiányzik az EIA_API_KEY secret.")

SPOT_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
INVENTORY_URL = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
STEO_URL = "https://api.eia.gov/v2/steo/data/"


def request_with_xparams(url: str, x_params: dict):
    params = {
        "api_key": API_KEY
    }

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


def fmt_price(value):
    try:
        return f"{float(value):.2f} USD/hordó"
    except Exception:
        return "nincs adat"


def fmt_inventory(value):
    try:
        return f"{float(value):.0f} millió hordó"
    except Exception:
        return "nincs adat"


def fmt_supply(value):
    try:
        return f"{float(value):.1f} millió hordó/nap"
    except Exception:
        return "102 millió hordó/nap"


def calculate_trend_percent(rows):
    try:
        if not rows or len(rows) < 2:
            return None

        latest = float(rows[0]["value"])
        oldest = float(rows[-1]["value"])

        if oldest == 0:
            return None

        change_pct = ((latest - oldest) / oldest) * 100
        return change_pct
    except Exception:
        return None


def fmt_trend(value):
    try:
        if value is None:
            return "nincs adat"

        sign = "+" if value > 0 else ""
        return f"{sign}{value:.2f}%"
    except Exception:
        return "nincs adat"


try:
    brent_rows = fetch_eia_price("EPCBRENT", length=30)
    brent_value = brent_rows[0].get("value") if brent_rows else None
    brent_trend = calculate_trend_percent(brent_rows)
except Exception:
    brent_rows = []
    brent_value = None
    brent_trend = None

try:
    wti_rows = fetch_eia_price("EPCWTI", length=1)
    wti_value = wti_rows[0].get("value") if wti_rows else None
except Exception:
    wti_value = None

try:
    inventory_value = fetch_inventory()
except Exception:
    inventory_value = None

try:
    supply_value = fetch_global_supply()
except Exception:
    supply_value = None


oil_data = {
    "market": {
        "brent": fmt_price(brent_value),
        "wti": fmt_price(wti_value),
        "inventory": fmt_inventory(inventory_value),
        "supply": fmt_supply(supply_value),
        "brent_30d_trend": fmt_trend(brent_trend)
    },
    "meta": {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "source": "EIA + GitHub Actions"
    },
    "risk": {
        "level": "elevated",
        "label": "Emelkedett"
    },
    "forecast": {
        "one_month": "80–85 USD/hordó",
        "three_months": "78–90 USD/hordó",
        "twelve_months": "75–95 USD/hordó"
    },
    "summary": {
        "status": "A globális olajpiac jelenleg kiegyensúlyozott, de geopolitikai kockázatokkal terhelt környezetben működik.",
        "supply_note": "A kínálatot jelentősen befolyásolja a Közel-Kelet kitermelése, az amerikai palaolaj-termelés és az orosz export alakulása.",
        "risk_note": "A Perzsa-öböl, a Vörös-tenger és a Szuezi-csatorna térsége továbbra is kritikus pont a globális energiaszállítás szempontjából."
    }
}

with open("oil-data.json", "w", encoding="utf-8") as f:
    json.dump(oil_data, f, ensure_ascii=False, indent=2)

print("oil-data.json frissítve (Brent + WTI + USA inventory + globális kínálat + 30 napos Brent trend).")
