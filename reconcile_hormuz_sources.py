#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hormuz Source Reconciliation 1.0 — source definitions and overlap audit.

Descriptive diagnostics only. Does not equate PortWatch tanker counts with
all-vessel AIS crossings, infer physical throughput, or modify risk scores.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FILES = {
    'validation': 'hormuz-flow-validation.json',
    'portwatch': 'hormuz-flow.json',
    'live_ais': 'hormuz-live-ais.json',
    'vessels': 'hormuz-vessels.json',
}
OUTPUT = ROOT / 'hormuz-source-reconciliation.json'
VERSION = '1.0-source-definitions'


def read(name):
    path = ROOT / FILES[name]
    try:
        value = json.loads(path.read_text(encoding='utf-8-sig'))
        if not isinstance(value, dict):
            return None, 'INVALID_JSON_ROOT'
        return value, None
    except FileNotFoundError:
        return None, 'FILE_MISSING'
    except (ValueError, OSError) as exc:
        return None, f'UNREADABLE_{type(exc).__name__}'


def iso_day(value):
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value[:10], '%Y-%m-%d').date().isoformat()
    except ValueError:
        return None


def valid_count(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and 0 <= value < float('inf')


def main():
    now = datetime.now(timezone.utc)
    sources = {}
    for key in FILES:
        data, error = read(key)
        sources[key] = {'data': data, 'error': error}

    port = sources['portwatch']['data'] or {}
    live = sources['live_ais']['data'] or {}
    validation = sources['validation']['data'] or {}
    vessels = sources['vessels']['data'] or {}

    port_rows = {}
    for row in port.get('history', []):
        if not isinstance(row, dict):
            continue
        dt = iso_day(row.get('date'))
        if dt:
            port_rows[dt] = row

    live_rows = {}
    for row in live.get('daily_crossings', []):
        if not isinstance(row, dict):
            continue
        dt = iso_day(row.get('day'))
        direction = row.get('direction')
        if dt and direction in ('inbound', 'outbound') and valid_count(row.get('count')):
            live_rows.setdefault(dt, {})[direction] = row['count']

    days = sorted(set(port_rows) & set(live_rows))
    comparable_days = []
    for dt in days:
        a = live_rows[dt]
        if not all(k in a for k in ('inbound', 'outbound')):
            continue
        p = port_rows[dt]
        p_all, p_tankers = p.get('n_total'), p.get('n_tanker')
        both = a['inbound'] + a['outbound']
        flags = []
        if valid_count(p_all) and p_all == 0 and both > 0:
            flags.append('PORTWATCH_ZERO_LIVE_AIS_POSITIVE')
        if valid_count(p_tankers) and p_tankers == 0 and both > 0:
            flags.append('PORTWATCH_ZERO_TANKERS_LIVE_ALL_VESSELS_POSITIVE_NOT_COMPARABLE')
        if not valid_count(p_all) or not valid_count(p_tankers):
            flags.append('PORTWATCH_COUNT_INVALID_OR_MISSING')
        comparable_days.append({
            'date': dt,
            'portwatch_all_vessels': p_all if valid_count(p_all) else None,
            'portwatch_tankers': p_tankers if valid_count(p_tankers) else None,
            'live_ais_inbound_all_vessels': a['inbound'],
            'live_ais_outbound_all_vessels': a['outbound'],
            'live_ais_both_directions_all_vessels': both,
            'diagnostic_flags': flags,
            'numerically_comparable': False,
        })

    zero_conflicts = [r['date'] for r in comparable_days
                      if 'PORTWATCH_ZERO_LIVE_AIS_POSITIVE' in r['diagnostic_flags']]
    port_latest = iso_day((port.get('latest_observation') or {}).get('date'))
    source_meta = live.get('source') or {}
    result = {
        'meta': {'name': 'Hormuz Source Reconciliation', 'version': VERSION,
                 'generated_at': now.isoformat(), 'mode': 'METHODOLOGY_AUDIT_ONLY'},
        'input_status': {
            key: {'file': FILES[key], 'available': val['data'] is not None,
                  'error': val['error'],
                  'generated_at': ((val['data'] or {}).get('meta') or {}).get('generated_at')}
            for key, val in sources.items()
        },
        'source_definitions': {
            'portwatch': {'provider': 'IMF PortWatch',
                          'dataset': 'Daily_Chokepoints_Data', 'portid': 'chokepoint6',
                          'metric': 'AIS-observed daily chokepoint vessel and tanker counts',
                          'latest_observation': port_latest,
                          'source_status': (port.get('source_diagnostics') or {}).get('status'),
                          'geofence_equivalence_verified': False},
            'live_ais': {'provider': source_meta.get('name', 'Hormuz API'),
                         'metric': 'API-defined daily inbound/outbound all-vessel crossings',
                         'last_poll': (live.get('summary') or {}).get('last_poll'),
                         'geofence_equivalence_verified': False,
                         'same_vessel_population_as_portwatch_verified': False},
            'vessels': {'metric': 'Ship-level zone and micro-geofence tracking',
                        'micro_geofence_bounds': {'lat_min': 25.75, 'lat_max': 27.35,
                                                  'lon_min': 55.65, 'lon_max': 57.25},
                        'crossing_confirmation': 'multi-observation zone sequence',
                        'identities_reconciled_with_other_sources': False,
                        'source_file_parseable': sources['vessels']['data'] is not None},
        },
        'overlap': {'common_days_with_both_live_directions': len(comparable_days),
                    'portwatch_zero_live_positive_days': zero_conflicts,
                    'daily_diagnostics': comparable_days[-90:],
                    'direct_difference_or_ratio_permitted': False},
        'validation_reference': {
            'version': (validation.get('meta') or {}).get('version'),
            'status': (validation.get('assessment') or {}).get('status'),
            'physical_flow_validated': False,
        },
        'assessment': {
            'status': 'DEFINITIONS_NOT_YET_RECONCILED',
            'verified_physical_oil_flow_mbd': None,
            'flow_risk_score': None,
            'composite_integration_allowed': False,
            'open_questions': [
                'Verify PortWatch chokepoint6 geometry against Hormuz API crossing geometry.',
                'Verify daily timezone boundaries and whether events represent unique ships or crossings.',
                'Check PortWatch zero-count days against source quality and historical revisions.',
                'Validate tanker-only matching using common IMO/MMSI and timestamp evidence, if exposed.',
                'Obtain independent cargo or loading evidence before claiming physical oil throughput.',
            ],
        },
    }
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print('Hormuz Source Reconciliation:', result['assessment']['status'])
    print('Common days:', len(comparable_days), 'zero-vs-positive days:', len(zero_conflicts))
    print('Saved:', OUTPUT.name)
    return 0 if sources['portwatch']['data'] and sources['live_ais']['data'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
