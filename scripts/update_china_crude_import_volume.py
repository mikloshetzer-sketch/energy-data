#!/usr/bin/env python3
"""Update China's monthly crude-oil import volume from the JODI Oil dataset.

The script downloads the official JODI Oil primary CSV ZIP, locates the CSV
inside the archive, validates its SDMX-style columns, filters China's crude-oil
imports in thousand barrels per day, and writes a frontend-ready JSON file.

Required JODI filters:
    REF_AREA       = CN
    ENERGY_PRODUCT = CRUDEOIL
    FLOW_BREAKDOWN = TOTIMPSB
    UNIT_MEASURE   = KBD

No third-party Python packages are required.
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

OUTPUT_FILE = Path("docs/data/china_crude_import_volume.json")

DEFAULT_JODI_URL = (
    "https://www.jodidata.org/_resources/files/downloads/oil-data/"
    "world_primary_csv.zip?iid=24"
)
JODI_URL = os.environ.get("JODI_OIL_CSV_URL", DEFAULT_JODI_URL)

REQUEST_TIMEOUT_SECONDS = 180
DOWNLOAD_RETRIES = 4
RETRY_BASE_DELAY_SECONDS = 10

REQUIRED_COLUMNS = {
    "REF_AREA",
    "TIME_PERIOD",
    "ENERGY_PRODUCT",
    "FLOW_BREAKDOWN",
    "UNIT_MEASURE",
    "OBS_VALUE",
    "ASSESSMENT_CODE",
}

FILTERS = {
    "REF_AREA": "CN",
    "ENERGY_PRODUCT": "CRUDEOIL",
    "FLOW_BREAKDOWN": "TOTIMPSB",
    "UNIT_MEASURE": "KBD",
}

ASSESSMENT_LABELS = {
    "1": "reasonable comparability",
    "2": "use with caution; consult metadata",
    "3": "not assessed",
}


@dataclass(frozen=True)
class Observation:
    period: str
    value_kbd: float
    assessment_code: str

    @property
    def value_mbd(self) -> float:
        return self.value_kbd / 1000.0


def log(message: str) -> None:
    print(message, flush=True)


def download_bytes(url: str) -> bytes:
    """Download a resource with retries and GitHub Actions-friendly headers."""
    last_error: Exception | None = None

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            log(f"Downloading JODI dataset (attempt {attempt}/{DOWNLOAD_RETRIES})")
            request = Request(
                url,
                headers={
                    "User-Agent": "energy-data-github-actions/1.0",
                    "Accept": "application/zip,text/csv,application/octet-stream,*/*",
                    "Cache-Control": "no-cache",
                    "Connection": "close",
                },
            )
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload = response.read()
                if not payload:
                    raise RuntimeError("JODI download returned an empty response")
                log(f"Downloaded {len(payload):,} bytes")
                return payload
        except (HTTPError, URLError, TimeoutError, RuntimeError) as error:
            last_error = error
            log(f"Download attempt {attempt} failed: {error}")
            if attempt < DOWNLOAD_RETRIES:
                time.sleep(RETRY_BASE_DELAY_SECONDS * attempt)

    raise RuntimeError(f"Unable to download JODI data. Last error: {last_error}")


def decode_csv_bytes(payload: bytes) -> str:
    """Decode a CSV payload using common JODI encodings."""
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise RuntimeError("Unable to decode JODI CSV payload")


def normalize_header(name: str) -> str:
    return name.strip().lstrip("\ufeff").upper()


def csv_has_required_columns(text: str) -> bool:
    try:
        reader = csv.reader(io.StringIO(text))
        header = next(reader)
    except (StopIteration, csv.Error):
        return False
    normalized = {normalize_header(column) for column in header}
    return REQUIRED_COLUMNS.issubset(normalized)


def extract_csv_text(payload: bytes) -> tuple[str, str]:
    """Return CSV text and source member name from ZIP or plain CSV input."""
    if zipfile.is_zipfile(io.BytesIO(payload)):
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            candidates = [
                info
                for info in archive.infolist()
                if not info.is_dir() and info.filename.lower().endswith(".csv")
            ]
            if not candidates:
                raise RuntimeError("The JODI ZIP archive contains no CSV file")

            # Prefer files whose headers match the SDMX-style JODI schema.
            for info in sorted(candidates, key=lambda item: item.file_size, reverse=True):
                text = decode_csv_bytes(archive.read(info))
                if csv_has_required_columns(text):
                    log(f"Using CSV member: {info.filename}")
                    return text, info.filename

            names = ", ".join(info.filename for info in candidates)
            raise RuntimeError(
                "No CSV member contains the required JODI columns. "
                f"CSV members found: {names}"
            )

    text = decode_csv_bytes(payload)
    if not csv_has_required_columns(text):
        raise RuntimeError("Downloaded CSV does not contain the required JODI columns")
    return text, "direct-download.csv"


def parse_number(raw_value: str) -> float | None:
    value = raw_value.strip().replace("\u00a0", "").replace(" ", "")
    if not value or value in {"..", ".", "NA", "N/A", "NULL", "-"}:
        return None

    # JODI values are normally dot-decimal. This also tolerates thousands commas.
    if value.count(",") > 0 and value.count(".") == 0:
        value = value.replace(",", ".") if value.count(",") == 1 else value.replace(",", "")
    else:
        value = value.replace(",", "")

    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def normalize_period(raw_period: str) -> str | None:
    period = raw_period.strip()
    if len(period) >= 7 and period[4] == "-":
        candidate = period[:7]
    elif len(period) == 6 and period.isdigit():
        candidate = f"{period[:4]}-{period[4:]}"
    else:
        return None

    try:
        datetime.strptime(candidate, "%Y-%m")
    except ValueError:
        return None
    return candidate


def parse_observations(csv_text: str) -> tuple[list[Observation], int]:
    reader = csv.DictReader(io.StringIO(csv_text))
    if reader.fieldnames is None:
        raise RuntimeError("JODI CSV has no header row")

    field_map = {normalize_header(name): name for name in reader.fieldnames}
    missing = REQUIRED_COLUMNS.difference(field_map)
    if missing:
        raise RuntimeError(f"Missing required JODI columns: {sorted(missing)}")

    observations_by_period: dict[str, Observation] = {}
    matched_rows = 0

    for row in reader:
        normalized = {
            canonical: (row.get(original) or "").strip()
            for canonical, original in field_map.items()
        }

        if any(normalized.get(key, "").upper() != expected for key, expected in FILTERS.items()):
            continue

        matched_rows += 1
        period = normalize_period(normalized.get("TIME_PERIOD", ""))
        value = parse_number(normalized.get("OBS_VALUE", ""))
        assessment_code = normalized.get("ASSESSMENT_CODE", "").strip()

        if period is None or value is None or value < 0:
            continue

        candidate = Observation(period, value, assessment_code)
        previous = observations_by_period.get(period)

        # If duplicate periods exist, prefer the observation with the strongest
        # JODI assessment code (1 before 2 before 3, unknown codes last).
        if previous is None:
            observations_by_period[period] = candidate
        else:
            previous_rank = {"1": 0, "2": 1, "3": 2}.get(previous.assessment_code, 9)
            candidate_rank = {"1": 0, "2": 1, "3": 2}.get(candidate.assessment_code, 9)
            if candidate_rank < previous_rank:
                observations_by_period[period] = candidate

    observations = sorted(observations_by_period.values(), key=lambda item: item.period)
    if not observations:
        raise RuntimeError(
            "No valid China crude-import observations remained after filtering. "
            f"Matched raw rows: {matched_rows}; filters: {FILTERS}"
        )

    return observations, matched_rows


def year_ago_value(observations: Iterable[Observation], latest_period: str) -> float | None:
    latest_date = datetime.strptime(latest_period, "%Y-%m")
    target = f"{latest_date.year - 1:04d}-{latest_date.month:02d}"
    lookup = {item.period: item.value_kbd for item in observations}
    return lookup.get(target)


def previous_value(observations: list[Observation]) -> float | None:
    return observations[-2].value_kbd if len(observations) >= 2 else None


def percent_change(current: float, previous: float | None) -> float | None:
    if previous is None or previous == 0:
        return None
    return round((current / previous - 1.0) * 100.0, 2)


def rolling_average(values: list[float], window: int) -> float | None:
    if not values:
        return None
    subset = values[-window:]
    return round(sum(subset) / len(subset), 1)


def build_output(
    observations: list[Observation], matched_rows: int, source_member: str
) -> dict[str, object]:
    latest = observations[-1]
    values = [item.value_kbd for item in observations]
    previous = previous_value(observations)
    year_ago = year_ago_value(observations, latest.period)

    series = [
        {
            "period": item.period,
            "date": f"{item.period}-01",
            "import_volume_kbd": round(item.value_kbd, 1),
            "import_volume_mbd": round(item.value_mbd, 3),
            "assessment_code": item.assessment_code or None,
            "assessment_label": ASSESSMENT_LABELS.get(item.assessment_code, "unknown"),
        }
        for item in observations
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataset": "china_crude_import_volume",
        "title_hu": "Kína havi nyersolajimport-volumene",
        "title_en": "China Monthly Crude Oil Import Volume",
        "frequency": "monthly",
        "unit": "thousand_barrels_per_day",
        "unit_short": "kbd",
        "source": {
            "name": "JODI Oil World Database",
            "provider": "Joint Organisations Data Initiative",
            "download_url": JODI_URL,
            "archive_member": source_member,
            "filters": FILTERS,
        },
        "methodology_hu": (
            "A modul a JODI Oil World Database havi adataiból Kína (CN) "
            "nyersolajimportját (CRUDEOIL, TOTIMPSB) választja ki KBD egységben. "
            "A közölt érték közvetlen JODI-megfigyelés; nincs napi interpoláció, "
            "tonna-hordó átváltás vagy Brent-árral végzett becslés."
        ),
        "methodology_en": (
            "The module selects China's (CN) monthly crude-oil imports "
            "(CRUDEOIL, TOTIMPSB) from the JODI Oil World Database in KBD. "
            "Values are direct JODI observations; no daily interpolation, "
            "tonne-to-barrel conversion, or Brent-price estimate is applied."
        ),
        "coverage": {
            "start_period": observations[0].period,
            "end_period": latest.period,
            "observation_count": len(observations),
            "matched_source_rows": matched_rows,
        },
        "latest": {
            "period": latest.period,
            "import_volume_kbd": round(latest.value_kbd, 1),
            "import_volume_mbd": round(latest.value_mbd, 3),
            "assessment_code": latest.assessment_code or None,
            "assessment_label": ASSESSMENT_LABELS.get(latest.assessment_code, "unknown"),
            "month_on_month_change_pct": percent_change(latest.value_kbd, previous),
            "year_on_year_change_pct": percent_change(latest.value_kbd, year_ago),
        },
        "summary": {
            "average_3m_kbd": rolling_average(values, 3),
            "average_6m_kbd": rolling_average(values, 6),
            "average_12m_kbd": rolling_average(values, 12),
            "historical_min_kbd": round(min(values), 1),
            "historical_max_kbd": round(max(values), 1),
        },
        "series": series,
    }


def write_json_atomic(data: dict[str, object], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_file.with_suffix(output_file.suffix + ".tmp")
    temporary.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output_file)


def main() -> int:
    try:
        payload = download_bytes(JODI_URL)
        csv_text, source_member = extract_csv_text(payload)
        observations, matched_rows = parse_observations(csv_text)
        output = build_output(observations, matched_rows, source_member)
        write_json_atomic(output, OUTPUT_FILE)

        latest = output["latest"]
        coverage = output["coverage"]
        log(f"Created {OUTPUT_FILE}")
        log(f"Source rows matching filters: {matched_rows}")
        log(f"Unique observations: {coverage['observation_count']}")
        log(
            "Latest observation: "
            f"{latest['period']} = {latest['import_volume_kbd']} kbd "
            f"({latest['import_volume_mbd']} mb/d)"
        )
        return 0
    except Exception as error:  # noqa: BLE001 - fail workflow with useful context
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
