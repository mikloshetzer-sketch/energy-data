#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hormuz Flow Validation v1.0: independent, descriptive source comparison.

Reads existing collector outputs, does not change the risk index. All oil
volumes are AIS-derived estimates, NOT measured physical oil throughput.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

VERSION = '1.0-observation-comparison'
ROOT = Path(__file__).resolve().parent
PORTWATCH = ROOT / 'hormuz-flow.json'
LIVE = ROOT / 'hormuz-live-ais.json'
VESSELS = ROOT / 'hormuz-vessels.json'
OUTPUT = ROOT / 'hormuz-flow-validation.json'


def load(path):
    if not path.exists():
        return None, 'FILE_MISSING'
    try:
        with path.open(encoding='utf-8-sig') as f:
            obj = json.load(f)
        return (obj, None) if isinstance(obj, dict) else (None, 'INVALID_JSON_ROOT')
    except (OSError, ValueError) as exc:
        return None, f'UNREADABLE: {type(exc).__name__}'


def day(value):
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value[:10], '%Y-%m-%d').date()
    except ValueError:
        return None


def finite_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if value != value or value in (float('inf'), -float('inf')):
        return None
    return value


def avg(values):
    values = [x for x in values if finite_number(x) is not None]
    return round(sum(values) / len(values), 2) if values else None


def main():
    now = datetime.now(timezone.utc)
    today = now.date()
    inputs = {}
    objects = {}
    for key, path in [('portwatch', PORTWATCH), ('live_ais', LIVE), ('vessels', VESSELS)]:
        obj, err = load(path)
        objects[key] = obj
        inputs[key] = {'file': path.name, 'available': obj is not None, 'error': err,
                       'generated_at': (obj or {}).get('meta', {}).get('generated_at')}
    port = objects['portwatch'] or {}
    live = objects['live_ais'] or {}
    port_by_day = {}
    for row in port.get('history', []):
        if not isinstance(row, dict):
            continue
        dt = day(row.get('date'))
        if dt and dt <= today:
            port_by_day[dt.isoformat()] = row
    crossing_by_day = {}
    for row in live.get('daily_crossings', []):
        if not isinstance(row, dict):
            continue
        dt = day(row.get('day'))
        direction = row.get('direction')
        count = finite_number(row.get('count'))
        if dt and dt < today and direction in ('inbound', 'outbound', 'in_strait') and count is not None:
            crossing_by_day.setdefault(dt.isoformat(), {})[direction] = count
    oil_by_day = {}
    for row in live.get('daily_oil_export', {}).get('days', []):
        if not isinstance(row, dict):
            continue
        dt = day(row.get('day'))
        if dt and dt < today:
            oil_by_day[dt.isoformat()] = row

    windows = {}
    for n in (7, 30, 90):
        # Completed UTC calendar days only; current day excluded consistently.
        dates = [(today - timedelta(days=i)).isoformat() for i in range(n, 0, -1)]
        p_days = [x for x in dates if x in port_by_day]
        a_days = [x for x in dates if x in crossing_by_day and
                  all(k in crossing_by_day[x] for k in ('inbound', 'outbound'))]
        o_days = [x for x in dates if x in oil_by_day]
        overlap = sorted(set(p_days) & set(a_days))
        comparable = []
        for x in overlap:
            row = port_by_day[x]
            crossings = crossing_by_day[x]
            comparable.append({'day': x, 'portwatch_all_vessels': row.get('n_total'),
                               'portwatch_tankers': row.get('n_tanker'),
                               'live_ais_inbound': crossings['inbound'],
                               'live_ais_outbound': crossings['outbound'],
                               'live_ais_crossings_both_directions': crossings['inbound'] + crossings['outbound'],
                               'live_ais_oil_proxy_barrels': oil_by_day.get(x, {}).get('total_barrels'),
                               'comparison_validated': False})
        windows[f'{n}d'] = {
            'start_date': dates[0], 'end_date': dates[-1], 'expected_days': n,
            'portwatch': {'observed_days': len(p_days), 'coverage_pct': round(100 * len(p_days)/n, 2),
                          'missing_days': [x for x in dates if x not in port_by_day],
                          'average_tankers_per_observed_day': avg([port_by_day[x].get('n_tanker') for x in p_days])},
            'live_ais_crossings': {'observed_days': len(a_days), 'coverage_pct': round(100 * len(a_days)/n, 2),
                                   'missing_days': [x for x in dates if x not in a_days],
                                   'average_both_directions_per_observed_day': avg([
                                       crossing_by_day[x]['inbound'] + crossing_by_day[x]['outbound'] for x in a_days])},
            'live_ais_oil_proxy': {'observed_days': len(o_days), 'coverage_pct': round(100 * len(o_days)/n, 2),
                                   'missing_days': [x for x in dates if x not in o_days],
                                   'average_barrels_per_observed_day': avg([
                                       oil_by_day[x].get('total_barrels') for x in o_days]),
                                   'physical_flow_measurement': False},
            'overlap_days': len(overlap), 'overlap_coverage_pct': round(100 * len(overlap)/n, 2),
            'daily_side_by_side': comparable,
            'direct_difference_or_ratio_calculated': False,
            'comparison_warning': ('PortWatch vessel counts, Live AIS crossings and AIS-derived barrels '
                                   'use different populations/definitions. Side-by-side observations are '
                                   'not a validated numerical comparison.')}
    latest_port = max(port_by_day) if port_by_day else None
    latest_ais = max(crossing_by_day) if crossing_by_day else None
    latest_oil = max(oil_by_day) if oil_by_day else None
    result = {
        'meta': {'model': 'Hormuz Flow Validation', 'version': VERSION,
                 'generated_at': now.isoformat(), 'mode': 'DESCRIPTIVE_ONLY'},
        'inputs': inputs,
        'latest_observations': {'portwatch': latest_port, 'live_ais_crossings': latest_ais,
                                'live_ais_oil_proxy': latest_oil},
        'source_lags_calendar_days': {
            'portwatch': (today - day(latest_port)).days if latest_port else None,
            'live_ais_crossings': (today - day(latest_ais)).days if latest_ais else None,
            'live_ais_oil_proxy': (today - day(latest_oil)).days if latest_oil else None},
        'windows': windows,
        'vessel_monitor': {'available': objects['vessels'] is not None,
                           'status': 'AVAILABLE_NOT_JOINED' if objects['vessels'] else 'UNAVAILABLE',
                           'reason': ('Vessel-level reconciliation requires a verified common identity, '
                                      'direction and observation-time schema. No DWT-to-cargo conversion '
                                      'is inferred from this dataset.')},
        'assessment': {'status': 'OBSERVATION_COMPARISON_ONLY',
                       'physical_flow_validated': False, 'validated_physical_flow_mbd': None,
                       'flow_disruption_score': None, 'ready_for_composite_risk': False,
                       'warnings': [
                           'AIS-derived oil barrels are estimates, not verified loaded cargo or throughput.',
                           'PortWatch tanker counts are not the same metric as all-vessel AIS crossings.',
                           'Different source definitions and coverage prevent direct ratios or calibration.',
                           'Missing days are unknown, never interpreted as zero traffic.',
                           'No independent physical oil-flow source has been validated.'
                       ]}}
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(f'[OK] {OUTPUT.name} | PortWatch={latest_port} AIS={latest_ais} oil-proxy={latest_oil}')
    for name, win in windows.items():
        print(f"{name}: overlap {win['overlap_days']}/{win['expected_days']} days; "
              f"PortWatch {win['portwatch']['observed_days']}; "
              f"AIS {win['live_ais_crossings']['observed_days']}; "
              f"oil-proxy {win['live_ais_oil_proxy']['observed_days']}")
    return 0 if port and live else 2


if __name__ == '__main__':
    sys.exit(main())
