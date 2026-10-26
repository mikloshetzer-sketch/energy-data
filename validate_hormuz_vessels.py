#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hormuz Vessel Cross-Validation 1.0.

Independent, descriptive vessel evidence audit. Never converts DWT to oil
throughput, and never equates current AIS presence with a transit event.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INPUT = ROOT / 'hormuz-vessels.json'
RECONCILIATION = ROOT / 'hormuz-source-reconciliation.json'
OUTPUT = ROOT / 'hormuz-vessel-cross-validation.json'
VERSION = '1.0-vessel-evidence-audit'
EVENT_NAMES = (
    'confirmed_hormuz_transits', 'confirmed_micro_hormuz_transits',
    'possible_direct_transits', 'possible_micro_skipped_core_transits',
)


def load(path):
    try:
        value = json.loads(path.read_text(encoding='utf-8-sig'))
        return value if isinstance(value, dict) else None
    except (OSError, ValueError):
        return None


def valid_id(value, length):
    value = str(value or '').strip()
    return value if value.isdigit() and len(value) == length and any(c != '0' for c in value) else None


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and 0 <= value < float('inf')


def parse_time(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError, TypeError, OverflowError):
        return None


def main():
    data = load(INPUT)
    if data is None:
        raise SystemExit('ERROR: Missing or invalid hormuz-vessels.json')
    recon = load(RECONCILIATION)
    now = datetime.now(timezone.utc)
    vessels = data.get('vessels') if isinstance(data.get('vessels'), list) else []
    vessels = [v for v in vessels if isinstance(v, dict)]
    tracking = data.get('persistent_tracking') if isinstance(data.get('persistent_tracking'), dict) else {}
    events = tracking.get('events') if isinstance(tracking.get('events'), dict) else {}
    state = data.get('persistent_vessel_state')
    state = state if isinstance(state, dict) else {}
    source_at = parse_time((data.get('meta') or {}).get('generated_at'))
    source_age_hours = round((now-source_at).total_seconds()/3600, 2) if source_at else None

    identities = Counter()
    by_category = Counter()
    observed = {'total': len(vessels), 'oil_related': 0, 'crude_related': 0,
                'product_related': 0, 'oil_with_known_dwt': 0,
                'oil_known_dwt_sum': 0, 'valid_imo': 0, 'valid_mmsi': 0,
                'oil_with_reliable_identifier': 0}
    conflicts = []
    for v in vessels:
        imo = valid_id(v.get('imo'), 7)
        mmsi = valid_id(v.get('mmsi'), 9)
        if imo: observed['valid_imo'] += 1
        if mmsi: observed['valid_mmsi'] += 1
        for key in (('IMO', imo), ('MMSI', mmsi)):
            if key[1]:
                identities[key] += 1
        if v.get('oil_related') is True:
            observed['oil_related'] += 1
            if imo or mmsi: observed['oil_with_reliable_identifier'] += 1
            if numeric(v.get('dwt')):
                observed['oil_with_known_dwt'] += 1
                observed['oil_known_dwt_sum'] += v['dwt']
        if v.get('crude_related') is True: observed['crude_related'] += 1
        if v.get('product_related') is True: observed['product_related'] += 1
        by_category[str(v.get('ship_category') or 'UNKNOWN')] += 1
    for (kind, identifier), count in identities.items():
        if count > 1:
            conflicts.append({'identifier_type': kind, 'identifier': identifier, 'record_count': count})

    event_summary = {}
    for name in EVENT_NAMES:
        rows = events.get(name)
        if isinstance(rows, list):
            oil = [e for e in rows if isinstance(e, dict) and e.get('oil_related') is True]
            event_summary[name] = {
                'available': True, 'event_count_this_snapshot': len(rows),
                'oil_related_event_count': len(oil),
                'outbound_oil_events': sum(str(e.get('direction')).upper() == 'OUTBOUND' for e in oil),
                'inbound_oil_events': sum(str(e.get('direction')).upper() == 'INBOUND' for e in oil),
                'note': 'Events may overlap between broad and micro geofences; do not sum as unique transits.',
            }
        else:
            event_summary[name] = {'available': False, 'event_count_this_snapshot': None,
                                   'note': 'Missing event history is unknown, not zero.'}
    generated = (data.get('meta') or {}).get('generated_at')
    result = {
        'meta': {'model': 'Hormuz Vessel Cross-Validation', 'version': VERSION,
                 'generated_at': now.isoformat(), 'mode': 'DESCRIPTIVE_ONLY'},
        'inputs': {'vessels': INPUT.name, 'source_generated_at': generated,
                   'source_age_hours': source_age_hours,
                   'reconciliation_available': recon is not None,
                   'reconciliation_generated_at': ((recon or {}).get('meta') or {}).get('generated_at')},
        'identity_quality': {'records_with_valid_imo': observed['valid_imo'],
                             'records_with_valid_mmsi': observed['valid_mmsi'],
                             'duplicate_identifiers_in_snapshot': conflicts[:100],
                             'duplicate_identifier_count': len(conflicts),
                             'cross_source_imo_mmsi_match_possible': False,
                             'reason': 'Aggregate PortWatch and daily Live AIS outputs do not expose vessel IDs.'},
        'snapshot_observations': observed,
        'ship_categories': dict(by_category.most_common(30)),
        'persistent_tracking': {'available': bool(tracking), 'state_records': len(state),
                                'event_schema_available': bool(events), 'events': event_summary,
                                'historical_unique_transit_count_validated': False},
        'source_quality': data.get('data_quality'),
        'assessment': {'status': 'VESSEL_EVIDENCE_ONLY', 'physical_oil_flow_validated': False,
                       'validated_oil_barrels_per_day': None, 'risk_score': None,
                       'ready_for_composite': False,
                       'warnings': [
                           'Vessel snapshot counts measure AIS presence, not daily passages.',
                           'Confirmed broad and micro transit events may refer to the same ship.',
                           'Possible transit events are not confirmed passages.',
                           'DWT is maximum deadweight capacity, not observed oil cargo.',
                           'AIS source timestamps and coverage must be checked before inference.',
                           'PortWatch and Live AIS aggregate outputs cannot be matched by IMO/MMSI.',
                       ]},
    }
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(f'Wrote {OUTPUT.name}: {len(vessels)} vessel records, {observed["oil_related"]} oil-related')
    print('Persistent tracking:', bool(tracking), 'event history:', bool(events))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
