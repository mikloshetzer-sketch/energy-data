import json
import os
from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.parse import urlencode

API_KEY = os.environ.get("EIA_API_KEY")

if not API_KEY:
    raise RuntimeError("Hiányzik az EIA_API_KEY secret.")

def fetch_json(url: str) -> dict:
    with urlopen(url) as response:
        return json.load(response)

def latest_value_from_response(data: dict, fallback="nincs adat"):
    try:
        rows = data["response"]["data"]
        if not rows:
            return fallback
        return rows[0].get("value", fallback)
    except Exception:
        return fallback

def fmt_price(value):
    try:
        num = float(value)
        return f"{num:.2f} USD/hordó"
    except Exception:
        return "nincs adat"

# EIA spot prices – Brent
brent_url = (
    "https://api.eia.gov/v2/petroleum/pri/spt/data/?"
    + urlencode({
        "api_key": API_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 1,
        "facets[product][]": "EPCBRENT"
    })
)

# EIA spot prices – WTI
wti_url = (
    "https://api.eia.gov/v2/petroleum/pri/spt/data/?"
    + urlencode({
        "api_key": API_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 1,
        "facets[product][]": "EPCWTI"
    })
)

brent_data = fetch_json(brent_url)
wti_data = fetch_json(wti_url)

brent_value = latest_value_from_response(brent_data)
wti_value = latest_value_from_response(wti_data)

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

print("oil-data.json frissítve valódi Brent és WTI adatokkal.")
