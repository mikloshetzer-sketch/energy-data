#!/usr/bin/env python3
"""Read-only schema/identity diagnostics for Hormuz /api/ships. No vessel identifiers are published."""
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

URL = 'https://hormuz.data-tracking.net/api/ships'
OUT = Path('hormuz-api-identity-diagnostic.json')


def main():
    request = Request(URL, headers={'User-Agent': 'energy-data-schema-audit/1.0', 'Accept': 'application/json'})
    with urlopen(request, timeout=45) as response:
        data = json.load(response)
    if isinstance(data, list):
        rows = data
        wrapper = 'list'
    elif isinstance(data, dict):
        wrapper = 'dict'
        rows = next((data[k] for k in ('ships', 'vessels', 'data', 'results') if isinstance(data.get(k), list)), None)
    else:
        raise ValueError('Unexpected API response type')
    if rows is None or not isinstance(rows, list):
        raise ValueError('No vessel list in API response')
    if not rows:
        raise ValueError('Empty vessel list: no schema conclusions possible')
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError('Vessel list contains non-object entries')
    field_presence = Counter()
    field_nonempty = Counter()
    field_types = {}
    id_fields = Counter()
    id_lengths = {}
    for row in rows:
        for key, value in row.items():
            field_presence[key] += 1
            field_types.setdefault(key, Counter())[type(value).__name__] += 1
            if value is not None and str(value).strip() != '':
                field_nonempty[key] += 1
            if any(token in key.lower() for token in ('mmsi', 'imo', 'vessel_id', 'ship_id', 'identity', 'identifier')):
                id_fields[key] += 1
                if value is not None:
                    value_str = str(value).strip()
                    if value_str:
                        id_lengths.setdefault(key, Counter())[len(value_str)] += 1
    output = {
        'meta': {'model': 'Hormuz API Identity Diagnostic', 'version': '1.0', 'generated_at': datetime.now(timezone.utc).isoformat(), 'mode': 'SCHEMA_ONLY_NO_RAW_RECORDS'},
        'source': {'endpoint': URL, 'response_wrapper': wrapper, 'vessel_records': len(rows)},
        'fields': {key: {'present_records': field_presence[key], 'nonempty_records': field_nonempty[key], 'types': dict(field_types[key])} for key in sorted(field_presence)},
        'identity_field_candidates': {key: {'present_records': count, 'nonempty_records': field_nonempty[key], 'value_lengths': dict(id_lengths.get(key, {}))} for key, count in sorted(id_fields.items())},
        'privacy': {'raw_vessel_records_exported': False, 'identifiers_exported': False, 'coordinates_exported': False},
        'assessment': {'api_identity_availability_verified': True, 'identifier_values_validated': False, 'note': 'Schema and nonempty-field coverage only; no individual vessel identifiers or locations retained.'}
    }
    OUT.write_text(json.dumps(output, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print('Records:', len(rows))
    print('Identity candidate fields:', list(id_fields))
    print('Diagnostic saved:', OUT)

if __name__ == '__main__':
    main()
