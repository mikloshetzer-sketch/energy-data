import json
import os
from datetime import datetime, timezone
import requests

API_KEY = os.environ.get("EIA_API_KEY")

if not API_KEY:
    raise RuntimeError("Hiányzik az EIA_API_KEY secret.")

BASE_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

def fetch_eia_value(product_code: str):

    params = {
        "api_key": API_KEY
    }

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
        "length": 1
    }

    headers = {
        "X-Params": json.dumps(x_params),
        "User-Agent": "energy-dashboard-bot"
    }

    response = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    rows = data.get("response", {}).get("data", [])

    if not rows:
        return None

    return rows[0].get("value")


def fmt_price(value):
    try:
        return f"{float(value):.2f} USD/hordó"
    except Exception:
        return "nincs adat"


try:
    brent_value = fetch_eia_value("EPCBRENT")
except Exception:
    brent_value = None

try:
    wti_value = fetch_eia_value("EPCWTI")
except Exception:
    wti_value = None


oil_data = {
    "market": {
        "brent": fmt_price(brent_value),
        "wti": fmt_price(wti_value),
        "inventory": "445 millió hordó",
        "supply": "102 millió hordó/nap"
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

print("oil-data.json frissítve.")
