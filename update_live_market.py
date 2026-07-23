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
UKOILWATCH_BRENT_URL = "https://ukoilwatch.com/api/v1/brent"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; energy-data-dashboard/1.0; "
        "+https://github.com/mikloshetzer-sketch/energy-data)"
    ),
    "Accept": "application/json,text/plain,*/*",
}

# Brent-validáció. A cél nem a valódi piaci sokkok blokkolása, hanem az
# egyetlen forrásból érkező, nem megerősített kilengések kiszűrése.
NORMAL_MOVE_PCT = 5.0
SOURCE_AGREEMENT_PCT = 5.0
SECONDARY_CONFIRM_PCT = 7.0
MAX_YAHOO_AGE_MINUTES = 120


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


def parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    try:
        text = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def percent_difference(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return abs((a - b) / b) * 100.0


def signed_change_pct(new: float | None, old: float | None) -> float | None:
    if new is None or old is None or old == 0:
        return None
    return ((new - old) / old) * 100.0


def extract_spot_prices(oil_data: dict[str, Any]) -> dict[str, Any]:
    candidates: list[tuple[Any, Any]] = []

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
            return {"spot_brent": brent, "spot_wti": wti}

    return {"spot_brent": None, "spot_wti": None}


def fetch_yahoo_quote(url: str, symbol: str) -> dict[str, Any]:
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
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
    quote_blocks = result.get("indicators", {}).get("quote", [])
    quote = quote_blocks[0] if quote_blocks else {}
    closes = quote.get("close") or []
    valid_points: list[tuple[int | float | None, float]] = []

    for index, close_raw in enumerate(closes):
        close = parse_number(close_raw)
        if close is None:
            continue
        timestamp = timestamps[index] if index < len(timestamps) else None
        valid_points.append((timestamp, close))

    if valid_points:
        timestamp, price = valid_points[-1]
    else:
        price = parse_number(meta.get("regularMarketPrice"))
        timestamp = meta.get("regularMarketTime")

    if price is None:
        raise RuntimeError(f"Nincs használható Yahoo Finance ár: {symbol}")

    return {
        "price": round(price, 4),
        "timestamp": iso_utc_from_unix(timestamp),
        "currency": meta.get("currency") or "USD",
        "exchange": meta.get("exchangeName"),
        "instrument_type": meta.get("instrumentType"),
        "symbol": symbol,
        "source": "Yahoo Finance chart",
    }


def _get_path(payload: Any, path: str) -> Any:
    current = payload
    for key in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _walk_price_candidates(value: Any, path: str = "") -> list[tuple[str, float]]:
    """Recursively collect plausible Brent price values from unknown JSON shapes."""
    found: list[tuple[str, float]] = []

    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            key_lower = str(key).lower()

            # Prefer fields whose names actually look like price/Brent values.
            if any(token in key_lower for token in (
                "price", "brent", "close", "last", "value", "usd"
            )):
                number = parse_number(child)
                if number is not None and 20.0 <= number <= 300.0:
                    found.append((child_path, number))

            found.extend(_walk_price_candidates(child, child_path))

    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_walk_price_candidates(child, f"{path}[{index}]"))

    return found


def fetch_ukoilwatch_brent() -> dict[str, Any]:
    response = requests.get(
        UKOILWATCH_BRENT_URL,
        headers=REQUEST_HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    # First try known/expected paths.
    price_paths = [
        "price",
        "brent.price",
        "data.price",
        "current.price",
        "latest.price",
        "current_price",
        "price_usd",
        "brent_usd",
        "usd_per_barrel",
        "data.brent",
        "current.brent",
        "data.current.price",
        "data.latest.price",
        "quote.price",
        "quote.last",
        "data.quote.price",
        "data.close",
        "close",
        "last",
    ]

    price = None
    selected_path = None

    for candidate_path in price_paths:
        candidate = parse_number(_get_path(payload, candidate_path))
        if candidate is not None and 20.0 <= candidate <= 300.0:
            price = candidate
            selected_path = candidate_path
            break

    # If the provider changes its JSON wrapper, search nested structures
    # rather than dropping the secondary source completely.
    if price is None:
        candidates = _walk_price_candidates(payload)

        # Prefer paths explicitly mentioning Brent/price/last/close.
        priority_tokens = ("brent", "price", "last", "close")
        candidates.sort(
            key=lambda item: (
                -sum(token in item[0].lower() for token in priority_tokens),
                len(item[0]),
            )
        )

        if candidates:
            selected_path, price = candidates[0]

    if price is None:
        raise RuntimeError("UKOilWatch válaszban nem található megbízható Brent ár.")

    timestamp = None
    for candidate_path in [
        "timestamp",
        "as_of",
        "asOf",
        "updated",
        "updated_at",
        "data.timestamp",
        "data.updated",
        "current.timestamp",
        "data.current.timestamp",
        "quote.timestamp",
    ]:
        value = _get_path(payload, candidate_path)
        if value:
            timestamp = str(value)
            break

    return {
        "price": round(price, 4),
        "timestamp": timestamp,
        "currency": "USD",
        "symbol": "Stooq cb.f via UKOilWatch",
        "source": "UKOilWatch / Stooq",
        "_parsed_path": selected_path,
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

    try:
        results["brent_secondary"] = fetch_ukoilwatch_brent()
    except (
        requests.RequestException,
        ValueError,
        KeyError,
        RuntimeError,
    ) as exc:
        errors["brent_secondary"] = str(exc)
        print(f"Figyelmeztetés: UKOilWatch Brent hiba: {exc}")

    return {"quotes": results, "errors": errors}


def yahoo_quote_is_fresh(quote: dict[str, Any] | None, now: datetime) -> bool:
    if not quote:
        return False
    dt = parse_iso_datetime(quote.get("timestamp"))
    if dt is None:
        return True
    age_minutes = (now - dt).total_seconds() / 60.0
    return -5 <= age_minutes <= MAX_YAHOO_AGE_MINUTES


def previous_brent_value(previous_output: dict[str, Any]) -> float | None:
    prices = previous_output.get("prices")
    if not isinstance(prices, dict):
        return None
    return parse_number(
        prices.get("market_brent", prices.get("live_brent"))
    )


def choose_brent_price(
    yahoo_quote: dict[str, Any] | None,
    secondary_quote: dict[str, Any] | None,
    previous_value: float | None,
    spot_value: float | None,
    now: datetime,
) -> dict[str, Any]:
    yahoo = (
        parse_number(yahoo_quote.get("price"))
        if yahoo_quote and yahoo_quote_is_fresh(yahoo_quote, now)
        else None
    )
    secondary = (
        parse_number(secondary_quote.get("price"))
        if secondary_quote
        else None
    )

    # Két aktuális forrás rendelkezésre áll.
    if yahoo is not None and secondary is not None:
        source_gap = percent_difference(yahoo, secondary) or 0.0
        yahoo_move = signed_change_pct(yahoo, previous_value)
        secondary_move = signed_change_pct(secondary, previous_value)

        if source_gap <= SOURCE_AGREEMENT_PCT:
            return {
                "price": yahoo,
                "source": "yahoo_confirmed",
                "fallback": False,
                "note": f"Yahoo és UKOilWatch eltérés: {source_gap:.2f}%",
            }

        # Nagy forráseltérésnél azt nézzük, megerősítik-e ugyanazt az irányt.
        if yahoo_move is not None and secondary_move is not None:
            same_direction = (
                (yahoo_move >= 0 and secondary_move >= 0)
                or (yahoo_move <= 0 and secondary_move <= 0)
            )
            if same_direction and source_gap <= SECONDARY_CONFIRM_PCT:
                return {
                    "price": yahoo,
                    "source": "yahoo_direction_confirmed",
                    "fallback": False,
                    "note": (
                        "Nagyobb mozgás, de a másodlagos forrás "
                        "azonos irányt erősít meg."
                    ),
                }

        # Ha a két aktuális forrás nagyon eltér, az önálló Yahoo-kilengést
        # nem engedjük át. A független Stooq-alapú kontrollt használjuk.
        return {
            "price": secondary,
            "source": "secondary_market_control",
            "fallback": True,
            "note": (
                f"Yahoo/UKOilWatch eltérés {source_gap:.2f}%; "
                "független kontrollár használva."
            ),
        }

    # Csak Yahoo érhető el.
    if yahoo is not None:
        move = signed_change_pct(yahoo, previous_value)
        if move is None or abs(move) <= NORMAL_MOVE_PCT:
            return {
                "price": yahoo,
                "source": "yahoo_unconfirmed_normal_move",
                "fallback": False,
                "note": "Normál elmozdulás; Yahoo elfogadva.",
            }

        # Nagy, nem megerősített elmozdulásnál egy ciklusra megtartjuk az
        # előző jó értéket. Így egy forráshiba nem írja felül azonnal az árat.
        if previous_value is not None:
            return {
                "price": previous_value,
                "source": "previous_value_guard",
                "fallback": True,
                "note": (
                    f"Nem megerősített Yahoo elmozdulás: {move:+.2f}%; "
                    "előző érték megtartva."
                ),
            }

        return {
            "price": yahoo,
            "source": "yahoo_bootstrap",
            "fallback": False,
            "note": "Nincs előző érték; Yahoo induló értékként elfogadva.",
        }

    # Yahoo nincs, de a másodlagos aktuális forrás igen.
    if secondary is not None:
        secondary_move = signed_change_pct(secondary, previous_value)

        # A másodlagos forrás önmagában is használható normál mozgásnál.
        if (
            secondary_move is None
            or abs(secondary_move) <= NORMAL_MOVE_PCT
            or previous_value is None
        ):
            return {
                "price": secondary,
                "source": "secondary_market_fallback",
                "fallback": True,
                "note": "Yahoo nem érhető el; UKOilWatch/Stooq használva.",
            }

        # Nagy, egyetlen forrásból jövő ugrásnál nem írjuk felül vakon
        # az előző értéket. A következő futás újraértékeli a helyzetet.
        return {
            "price": previous_value,
            "source": "previous_value_guard",
            "fallback": True,
            "note": (
                f"Nem megerősített másodlagos elmozdulás: "
                f"{secondary_move:+.2f}%; előző érték megtartva."
            ),
        }

    # Mindkét piaci forrás hibás: előző jó érték, majd EIA spot.
    if previous_value is not None:
        return {
            "price": previous_value,
            "source": "previous_value_fallback",
            "fallback": True,
            "note": "Piaci források nem érhetők el; előző érték megtartva.",
        }

    if spot_value is not None:
        return {
            "price": spot_value,
            "source": "eia_spot_fallback",
            "fallback": True,
            "note": "Piaci források nem érhetők el; EIA spot használva.",
        }

    return {
        "price": None,
        "source": "unavailable",
        "fallback": True,
        "note": "Nincs használható Brent-adat.",
    }


def extract_chokepoint_values(cp_data: dict[str, Any]) -> dict[str, Any]:
    me = cp_data.get("middle_east_conflict_impact", {}) or {}
    meta = cp_data.get("meta", {}) or {}

    return {
        "global_trade_risk_index": parse_number(
            cp_data.get("global_trade_risk_index")
        ),
        "middle_east_conflict_impact": parse_number(me.get("score")),
        "middle_east_conflict_label": me.get("label"),
        "daily_change": cp_data.get("daily_change", {}) or {},
        "top_risks": cp_data.get("top_risks", []) or [],
        "me_components": me.get("components", {}) or {},
        "risk_meta": {
            "updated": meta.get("updated"),
            "method": meta.get("method"),
            "uses_tanker_signal": meta.get("uses_tanker_signal"),
            "uses_me_security_signal": meta.get("uses_me_security_signal"),
            "tanker_input_source": meta.get("tanker_input_source"),
            "me_security_input_source": meta.get("me_security_input_source"),
        },
    }


def main() -> None:
    oil_data = safe_load_json(OIL_FILE, {})
    cp_data = safe_load_json(CHOKEPOINT_FILE, {})
    previous_output = safe_load_json(OUTPUT_FILE, {})

    now = datetime.now(timezone.utc)
    generated_at = now.isoformat().replace("+00:00", "Z")
    legacy_updated = now.strftime("%Y-%m-%d %H:%M UTC")

    spot_prices = extract_spot_prices(oil_data)
    market_result = fetch_market_prices()
    quotes = market_result["quotes"]
    errors = market_result["errors"]

    brent_quote = quotes.get("brent")
    brent_secondary = quotes.get("brent_secondary")
    wti_quote = quotes.get("wti")

    previous_brent = previous_brent_value(previous_output)
    brent_selection = choose_brent_price(
        yahoo_quote=brent_quote,
        secondary_quote=brent_secondary,
        previous_value=previous_brent,
        spot_value=spot_prices["spot_brent"],
        now=now,
    )

    market_brent = brent_selection["price"]
    market_wti = (
        wti_quote["price"] if wti_quote else spot_prices["spot_wti"]
    )

    brent_fallback = bool(brent_selection["fallback"])
    wti_fallback = wti_quote is None
    fallback_used = brent_fallback or wti_fallback

    if market_brent is None and market_wti is None:
        raise RuntimeError(
            "Sem piaci, sem tartalék Brent/WTI ár nem érhető el."
        )

    cp_values = extract_chokepoint_values(cp_data)

    # A JSON séma és a meglévő mezőnevek változatlanok maradnak.
    payload = {
        "meta": {
            "updated": legacy_updated,
            "source_mode": "live",
            "generated_at": generated_at,
            "update_interval_minutes": 30,
            "data_mode": "periodic_market_snapshot",
            "is_streaming": False,
            "fallback_used": fallback_used,
        },
        "prices": {
            "market_brent": market_brent,
            "market_wti": market_wti,
            "spot_brent": spot_prices["spot_brent"],
            "spot_wti": spot_prices["spot_wti"],
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
                brent_quote.get("timestamp") if brent_quote else None
            ),
            "market_timestamp_wti": (
                wti_quote.get("timestamp") if wti_quote else None
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
        f"Brent: {market_brent} | "
        f"választás={brent_selection['source']} | "
        f"{brent_selection['note']}"
    )
    if brent_secondary:
        parsed_path = brent_secondary.get("_parsed_path")
        path_info = f" | mező={parsed_path}" if parsed_path else ""
        print(
            f"UKOilWatch kontroll: {brent_secondary.get('price')}"
            f"{path_info}"
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
