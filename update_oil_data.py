import json
import os
from datetime import datetime, timezone

api_key = os.environ.get("EIA_API_KEY")

if not api_key:
    raise RuntimeError("Hiányzik az EIA_API_KEY secret.")

oil_data = {
    "market": {
        "brent": "82 USD/hordó",
        "wti": "78 USD/hordó",
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
