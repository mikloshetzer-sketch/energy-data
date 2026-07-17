import json
import os
import re
from datetime import datetime, timezone
from typing import Any

import requests


OIL_FILE = "oil-data.json"
CHOKEPOINT_FILE = "chokepoint-impact.json"
OUTPUT_FILE = "live-market.json"

YAHOO_BRENT_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/"
    "BZ=F?interval=5m&range=1d"
)
YAHOO_WTI_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/"
    "CL=F?interval=5m&range=1d"
)

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; energy-data-dashboard/1.0; "
        "+https://github.com/mikloshetzer-sketch/energy-data)"
    ),
    "Accept": "application/json,text/plain,*/*",
}


def safe_load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8-sig") as file:
            content = file.read().strip()
            return json.loads(content) if content else default
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Figyelmeztetés: {path} nem olvasható: {exc}")
        return default


def save_json(path: str, payload: dict[str, Any]) -> None:
    temporary_path = f"{path}.tmp"

    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
        file.write("\n")

    os.replace(temporary_path, path)


def parse_number(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        cleaned = (
            value.strip()
            .replace(",", ".")
            .replace("$", "")
            .replace("USD", "")
            .replace("usd", "")
        )
        match = re.search(r"-?\d+(?:\.\d+)?", cleaned)

        if match:
            try:
                return float(match.group(0))
            except ValueError:
                return None

    return None


def iso_utc_from_unix(timestamp: int | float | None) -> str | None:
    if timestamp is None:
        return None

    try:
        return datetime.fromtimestamp(
            float(timestamp),
            tz=timezone.utc,
        ).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        return None


def extract_spot_prices(oil_data: dict[str, Any]) -> dict[str, Any]:
    candidates: list[tuple[Any, Any]] = []

    # Az oil-data.json valódi EIA spot blokkja az elsődleges.
    spot = oil_data.get("spot")
    if isinstance(spot, dict):
        candidates.append((spot.get("brent"), spot.get("wti")))

    prices = oil_data.get("prices")
    if isinstance(prices, dict):
        candidates.append(
            (
                prices.get("spot_brent", prices.get("brent")),
                prices.get("spot_wti", prices.get("wti")),
            )
        )

    market = oil_data.get("market")
    if isinstance(market, dict):
        candidates.append((market.get("brent"), market.get("wti")))

    candidates.append((oil_data.get("brent"), oil_data.get("wti")))

    for brent_raw, wti_raw in candidates:
        brent = parse_number(brent_raw)
        wti = parse_number(wti_raw)

        if brent is not None or wti is not None:
            return {
                "spot_brent": brent,
                "spot_wti": wti,
            }

    return {
        "spot_brent": None,
        "spot_wti": None,
    }


def fetch_yahoo_quote(
    url: str,
    symbol: str,
) -> dict[str, Any]:
    response = requests.get(
        url,
        headers=REQUEST_HEADERS,
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    chart = payload.get("chart", {})
    error = chart.get("error")

    if error:
        raise RuntimeError(f"Yahoo Finance hiba ({symbol}): {error}")

    results = chart.get("result") or []
    if not results:
        raise RuntimeError(f"Nincs Yahoo Finance eredmény: {symbol}")

    result = results[0]
    meta = result.get("meta") or {}
    timestamps = result.get("timestamp") or []

    quote_blocks = (
        result.get("indicators", {})
        .get("quote", [])
    )
    quote = quote_blocks[0] if quote_blocks else {}

    closes = quote.get("close") or []
    valid_points: list[tuple[int | float | None, float]] = []

    for index, close_raw in enumerate(closes):
        close = parse_number(close_raw)

        if close is None:
            continue

        timestamp = (
            timestamps[index]
            if index < len(timestamps)
            else None
        )
        valid_points.append((timestamp, close))

    if valid_points:
        timestamp, price = valid_points[-1]
    else:
        price = parse_number(
            meta.get("regularMarketPrice")
        )
        timestamp = meta.get("regularMarketTime")

    if price is None:
        raise RuntimeError(
            f"Nincs használható Yahoo Finance ár: {symbol}"
        )

    return {
        "price": round(price, 4),
        "timestamp": iso_utc_from_unix(timestamp),
        "currency": meta.get("currency") or "USD",
        "exchange": meta.get("exchangeName"),
        "instrument_type": meta.get("instrumentType"),
        "symbol": symbol,
        "source": "Yahoo Finance chart",
    }


def fetch_market_prices() -> dict[str, Any]:
    results: dict[str, Any] = {}
    errors: dict[str, str] = {}

    sources = {
        "brent": (YAHOO_BRENT_URL, "BZ=F"),
        "wti": (YAHOO_WTI_URL, "CL=F"),
    }

    for name, (url, symbol) in sources.items():
        try:
            results[name] = fetch_yahoo_quote(url, symbol)
        except (
            requests.RequestException,
            ValueError,
            KeyError,
            RuntimeError,
        ) as exc:
            errors[name] = str(exc)
            print(f"Figyelmeztetés: {name} lekérdezési hiba: {exc}")

    return {
        "quotes": results,
        "errors": errors,
    }


def extract_chokepoint_values(
    cp_data: dict[str, Any],
) -> dict[str, Any]:
    me = cp_data.get("middle_east_conflict_impact", {}) or {}
    meta = cp_data.get("meta", {}) or {}

    return {
        "global_trade_risk_index": parse_number(
            cp_data.get("global_trade_risk_index")
        ),
        "middle_east_conflict_impact": parse_number(
            me.get("score")
        ),
        "middle_east_conflict_label": me.get("label"),
        "daily_change": cp_data.get("daily_change", {}) or {},
        "top_risks": cp_data.get("top_risks", []) or [],
        "me_components": me.get("components", {}) or {},
        "risk_meta": {
            "updated": meta.get("updated"),
            "method": meta.get("method"),
            "uses_tanker_signal": meta.get("uses_tanker_signal"),
            "uses_me_security_signal": meta.get(
                "uses_me_security_signal"
            ),
            "tanker_input_source": meta.get(
                "tanker_input_source"
            ),
            "me_security_input_source": meta.get(
                "me_security_input_source"
            ),
        },
    }


def main() -> None:
    oil_data = safe_load_json(OIL_FILE, {})
    cp_data = safe_load_json(CHOKEPOINT_FILE, {})

    now = datetime.now(timezone.utc)
    generated_at = now.isoformat().replace("+00:00", "Z")
    legacy_updated = now.strftime("%Y-%m-%d %H:%M UTC")

    spot_prices = extract_spot_prices(oil_data)
    market_result = fetch_market_prices()
    quotes = market_result["quotes"]
    errors = market_result["errors"]

    brent_quote = quotes.get("brent")
    wti_quote = quotes.get("wti")

    market_brent = (
        brent_quote["price"]
        if brent_quote
        else spot_prices["spot_brent"]
    )
    market_wti = (
        wti_quote["price"]
        if wti_quote
        else spot_prices["spot_wti"]
    )

    brent_fallback = brent_quote is None
    wti_fallback = wti_quote is None
    fallback_used = brent_fallback or wti_fallback

    if market_brent is None and market_wti is None:
        raise RuntimeError(
            "Sem Yahoo futures, sem EIA spot ár nem érhető el."
        )

    cp_values = extract_chokepoint_values(cp_data)

    payload = {
        "meta": {
            # Régi mezők változatlanul megmaradnak.
            "updated": legacy_updated,
            "source_mode": "live",
            # Új, pontosabb metaadatok.
            "generated_at": generated_at,
            "update_interval_minutes": 30,
            "data_mode": "periodic_market_snapshot",
            "is_streaming": False,
            "fallback_used": fallback_used,
        },
        "prices": {
            # Új, pontos mezőnevek
            "market_brent": market_brent,
            "market_wti": market_wti,
            "spot_brent": spot_prices["spot_brent"],
            "spot_wti": spot_prices["spot_wti"],
            # Visszafelé kompatibilis mezők a jelenlegi dashboardhoz
            "live_brent": market_brent,
            "live_wti": market_wti,
            "live_source": (
                "yahoo_futures"
                if not fallback_used
                else "mixed_or_spot_fallback"
            ),
            "market_source": "Yahoo Finance chart",
            "spot_source": "EIA via oil-data.json",
            "market_symbol_brent": "BZ=F",
            "market_symbol_wti": "CL=F",
            "market_timestamp_brent": (
                brent_quote.get("timestamp")
                if brent_quote
                else None
            ),
            "market_timestamp_wti": (
                wti_quote.get("timestamp")
                if wti_quote
                else None
            ),
            "brent_fallback_used": brent_fallback,
            "wti_fallback_used": wti_fallback,
            "currency": "USD",
            "unit": "USD/barrel",
        },
        "source_status": {
            "brent": {
                "status": (
                    "market_futures"
                    if not brent_fallback
                    else "eia_spot_fallback"
                ),
                "error": errors.get("brent"),
            },
            "wti": {
                "status": (
                    "market_futures"
                    if not wti_fallback
                    else "eia_spot_fallback"
                ),
                "error": errors.get("wti"),
            },
        },
        "risk": {
            "global_trade_risk_index": cp_values[
                "global_trade_risk_index"
            ],
            "middle_east_conflict_impact": cp_values[
                "middle_east_conflict_impact"
            ],
            "middle_east_conflict_label": cp_values[
                "middle_east_conflict_label"
            ],
            "daily_change": cp_values["daily_change"],
            "top_risks": cp_values["top_risks"],
            "me_components": cp_values["me_components"],
            "risk_meta": cp_values["risk_meta"],
        },
    }

    save_json(OUTPUT_FILE, payload)

    print(f"{OUTPUT_FILE} frissítve.")
    print(
        f"Brent: {market_brent} "
        f"({'fallback' if brent_fallback else 'Yahoo futures'})"
    )
    print(
        f"WTI: {market_wti} "
        f"({'fallback' if wti_fallback else 'Yahoo futures'})"
    )
    print(
        "Szoroskockázati index: "
        f"{cp_values['global_trade_risk_index']}"
    )


if __name__ == "__main__":
    main()

