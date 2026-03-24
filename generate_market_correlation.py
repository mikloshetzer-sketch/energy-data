import json
import numpy as np
from datetime import datetime

INPUT_FILE = "market-history.json"
OUTPUT_FILE = "market-correlation.json"

MAX_LAG = 3


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def pct_change(series):
    result = []
    for i in range(1, len(series)):
        prev = series[i - 1]
        curr = series[i]

        if prev is None or curr is None or prev == 0:
            result.append(None)
        else:
            result.append((curr - prev) / prev)
    return result


def clean_pair(a, b):
    x, y = [], []
    for i in range(len(a)):
        if a[i] is not None and b[i] is not None:
            x.append(a[i])
            y.append(b[i])
    return np.array(x), np.array(y)


def corr(a, b):
    if len(a) < 5:
        return 0
    return float(np.corrcoef(a, b)[0, 1])


def lag_corr(base, target, lag):
    if lag > 0:
        base = base[lag:]
        target = target[:-lag]
    elif lag < 0:
        base = base[:lag]
        target = target[-lag:]

    x, y = clean_pair(base, target)
    return corr(x, y)


def find_best_lag(base, target):
    best_lag = 0
    best_corr = -999

    for lag in range(-MAX_LAG, MAX_LAG + 1):
        c = lag_corr(base, target, lag)
        if abs(c) > abs(best_corr):
            best_corr = c
            best_lag = lag

    return best_lag, best_corr


def main():
    data = load_json(INPUT_FILE)
    rows = data.get("rows", [])

    # szűrés
    rows = [r for r in rows if r.get("market_brent")]

    brent = [r.get("market_brent") for r in rows]
    hormuz = [r.get("hormuz_impact") for r in rows]
    suez = [r.get("suez_impact") for r in rows]
    bab = [r.get("bab_el_mandeb_impact") for r in rows]

    brent_chg = pct_change(brent)
    hormuz_chg = pct_change(hormuz)
    suez_chg = pct_change(suez)
    bab_chg = pct_change(bab)

    # sima korreláció
    correlations = {}
    for name, series in [
        ("hormuz", hormuz_chg),
        ("suez", suez_chg),
        ("bab_el_mandeb", bab_chg),
    ]:
        x, y = clean_pair(brent_chg, series)
        correlations[name] = round(corr(x, y), 3)

    # lead-lag
    leader_lag = {}
    for name, series in [
        ("hormuz", hormuz_chg),
        ("suez", suez_chg),
        ("bab_el_mandeb", bab_chg),
    ]:
        lag, c = find_best_lag(brent_chg, series)
        leader_lag[name] = lag

    output = {
        "updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "correlations": correlations,
        "leader_lag_days": leader_lag,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print("market-correlation.json frissítve")


if __name__ == "__main__":
    main()
