#!/usr/bin/env python3
"""Hormuz API schema and source-specific identity stability diagnostics.

No raw vessel IDs, names, coordinates or individual positions are published.
The private comparison state is retained only within the GitHub Actions run
unless explicitly persisted by the operator. Do not commit private state.
"""
from __future__ import annotations
import hashlib
import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

URL = 'https://hormuz.data-tracking.net/api/ships'
OUT = Path('hormuz-api-identity-diagnostic.json')
STATE = Path(os.environ.get('HORMUZ_IDENTITY_STATE', '/tmp/hormuz-identity-state.json'))
VERSION = '1.1-identity-stability'


def fetch():
    req = Request(URL, headers={'User-Agent': 'energy-data-schema-audit/1.1', 'Accept': 'application/json'})
    with urlopen(req, timeout=45) as response:
        data = json.load(response)
    if isinstance(data, list):
        return data, 'list'
    if isinstance(data, dict):
        for key in ('ships', 'vessels', 'data', 'results'):
            if isinstance(data.get(key), list):
                return data[key], 'dict'
    raise ValueError('No vessel list in API response')


def nonempty(value):
    return value is not None and str(value).strip() != ''


def numeric(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def timestamp(value):
    if not nonempty(value):
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            return None
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def km_distance(a, b):
    if not all(numeric(x) is not None for x in (a.get('lat'), a.get('lon'), b.get('lat'), b.get('lon'))):
        return None
    lat1, lon1, lat2, lon2 = map(math.radians, (float(a['lat']), float(a['lon']), float(b['lat']), float(b['lon'])))
    h = math.sin((lat2-lat1)/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin((lon2-lon1)/2)**2
    return 6371.0088 * 2 * math.asin(min(1, math.sqrt(h)))


def private_key(raw_id, salt):
    return hashlib.sha256((salt + '\0' + raw_id).encode()).hexdigest()


def main():
    rows, wrapper = fetch()
    if not rows or any(not isinstance(row, dict) for row in rows):
        raise ValueError('Empty or malformed vessel list')
    now = datetime.now(timezone.utc)
    presence, filled, types, lengths = Counter(), Counter(), defaultdict(Counter), defaultdict(Counter)
    by_id = defaultdict(list)
    for row in rows:
        for key, value in row.items():
            presence[key] += 1
            types[key][type(value).__name__] += 1
            if nonempty(value):
                filled[key] += 1
                if any(token in key.lower() for token in ('mmsi', 'imo', 'vessel_id', 'ship_id', 'identity', 'identifier')):
                    lengths[key][len(str(value).strip())] += 1
        raw_id = str(row.get('mmsi') or '').strip()
        if raw_id:
            by_id[raw_id].append(row)
    duplicated = {key: group for key, group in by_id.items() if len(group) > 1}
    conflicting_categories = sum(len({str(x.get('ship_category') or '').strip().lower() for x in group}) > 1 for group in duplicated.values())
    conflicting_names = sum(len({str(x.get('name') or '').strip().casefold() for x in group}) > 1 for group in duplicated.values())

    # Cross-run matching requires a persistent private state with a stable salt.
    # No identity hashes, vessel names or coordinates appear in public output.
    prior = None
    try:
        candidate = json.loads(STATE.read_text(encoding='utf-8'))
        if candidate.get('version') == 1 and candidate.get('salt') and isinstance(candidate.get('records'), dict):
            prior = candidate
    except (OSError, ValueError, AttributeError):
        pass
    salt = prior['salt'] if prior else os.urandom(16).hex()
    current = {}
    for raw_id, group in by_id.items():
        if len(group) != 1:
            continue  # Ambiguous within the same snapshot.
        row = group[0]
        current[private_key(raw_id, salt)] = {
            'name_hash': private_key(str(row.get('name') or '').strip().casefold(), salt),
            'category': str(row.get('ship_category') or '').strip().lower(),
            'lat': numeric(row.get('latitude')),
            'lon': numeric(row.get('longitude')),
            'observed_at': str(row.get('timestamp') or ''),
        }
    prior_records = prior['records'] if prior else {}
    shared = set(current) & set(prior_records)
    name_changes = category_changes = impossible_motion = comparable_motion = stale_or_repeated = 0
    for key in shared:
        old, new = prior_records[key], current[key]
        if old.get('name_hash') != new.get('name_hash'):
            name_changes += 1
        if old.get('category') != new.get('category'):
            category_changes += 1
        t1, t2 = timestamp(old.get('observed_at')), timestamp(new.get('observed_at'))
        if t1 and t2 and t2 <= t1:
            stale_or_repeated += 1
        if t1 and t2 and t2 > t1:
            distance = km_distance(old, new)
            if distance is not None:
                comparable_motion += 1
                hours = (t2-t1).total_seconds()/3600
                if distance/hours > 100:  # diagnostic threshold, not proof of ID collision
                    impossible_motion += 1
    identity_fields = {k: {'present_records': presence[k], 'nonempty_records': filled[k],
                           'value_lengths': dict(sorted(lengths[k].items()))}
                       for k in sorted(presence) if any(t in k.lower() for t in ('mmsi', 'imo', 'vessel_id', 'ship_id', 'identity', 'identifier'))}
    output = {
        'meta': {'model': 'Hormuz API Identity Diagnostic', 'version': VERSION,
                 'generated_at': now.isoformat(), 'mode': 'AGGREGATES_ONLY'},
        'source': {'endpoint': URL, 'response_wrapper': wrapper, 'vessel_records': len(rows)},
        'fields': {k: {'present_records': presence[k], 'nonempty_records': filled[k], 'types': dict(types[k])}
                   for k in sorted(presence)},
        'identity_field_candidates': identity_fields,
        'identity_uniqueness': {'nonempty_mmsi_records': sum(len(v) for v in by_id.values()),
                                'distinct_source_mmsi_values': len(by_id),
                                'duplicate_mmsi_values': len(duplicated),
                                'records_with_duplicate_mmsi': sum(len(v) for v in duplicated.values()),
                                'duplicate_ids_with_name_conflicts': conflicting_names,
                                'duplicate_ids_with_category_conflicts': conflicting_categories,
                                'standard_9_digit_mmsi_records': sum(len(v) for k, v in by_id.items() if len(k) == 9 and k.isdigit())},
        'cross_run_stability': {'prior_private_state_available': prior is not None,
                                'matched_unambiguous_source_ids': len(shared),
                                'name_changed': name_changes, 'category_changed': category_changes,
                                'repeated_or_nonincreasing_observation_timestamp': stale_or_repeated,
                                'comparable_motion_pairs': comparable_motion,
                                'motion_above_100_kmh': impossible_motion,
                                'interpretation': 'Identity stability signals, not proof of real-world vessel identity.'},
        'privacy': {'raw_vessel_records_exported': False, 'identifiers_exported': False,
                    'coordinates_exported': False, 'private_state_exported': False},
        'assessment': {'api_identity_availability_verified': True,
                       'identifier_values_validated': False,
                       'source_ids_proven_stable': False,
                       'note': 'Cross-run stability requires a privately persisted state; absent state means first-run baseline only.'},
    }
    OUT.write_text(json.dumps(output, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps({'version': 1, 'salt': salt, 'records': current}, ensure_ascii=False), encoding='utf-8')
    try:
        os.chmod(STATE, 0o600)
    except OSError:
        pass
    print('Records:', len(rows), 'Duplicate source IDs:', len(duplicated))
    print('Cross-run private state:', prior is not None, 'Matched IDs:', len(shared))
    print('Diagnostic saved:', OUT)


if __name__ == '__main__':
    main()
