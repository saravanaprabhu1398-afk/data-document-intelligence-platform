"""
Download a curated oncology slice of FDA drug labels from DailyMed.

Usage (local → directory):
    python download_dailymed.py --output-dir ./data/sample --limit 50

Usage (upload directly to S3):
    python download_dailymed.py \
        --output-dir s3://YOUR-BUCKET/clinical-docs/raw/pdfs \
        --limit 5000 \
        --drug-class "Antineoplastic Agents"

DailyMed API docs: https://dailymed.nlm.nih.gov/dailymed/app-support-web-services.cfm
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator
from urllib.parse import urljoin, urlencode

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DAILYMED_API   = "https://dailymed.nlm.nih.gov/dailymed/services/v2/"
DOWNLOAD_BASE  = "https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/"
PAGE_SIZE      = 100
REQUEST_DELAY  = 0.25   # seconds between API calls — be polite to the public server


@dataclass
class LabelRecord:
    set_id:       str
    title:        str
    published:    str
    pdf_url:      str = field(default="")
    source_url:   str = field(default="")


# ── API helpers ───────────────────────────────────────────────────────────────

def _get_json(url: str, params: dict | None = None, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            log.warning("Request failed (%s), retrying in %ds…", exc, wait)
            time.sleep(wait)
    return {}


def search_labels(drug_class: str, limit: int) -> Iterator[LabelRecord]:
    """
    Paginate DailyMed SPL search results for a given pharmacological drug class.
    Yields LabelRecord objects up to `limit`.
    """
    fetched = 0
    page = 1

    while fetched < limit:
        params = {
            "drug_class_moa": drug_class,
            "pagesize": min(PAGE_SIZE, limit - fetched),
            "page":     page,
        }
        log.info("Fetching page %d  (fetched=%d / limit=%d)", page, fetched, limit)
        data = _get_json(urljoin(DAILYMED_API, "spls.json"), params=params)

        records = data.get("data", [])
        if not records:
            log.info("No more results at page %d.", page)
            break

        for rec in records:
            set_id = rec.get("setid", "")
            if not set_id:
                continue
            yield LabelRecord(
                set_id     = set_id,
                title      = rec.get("title", ""),
                published  = rec.get("published_date", ""),
                source_url = DOWNLOAD_BASE + set_id + ".zip",
            )
            fetched += 1
            if fetched >= limit:
                break

        page += 1
        time.sleep(REQUEST_DELAY)


# ── Download + extract ────────────────────────────────────────────────────────

def download_and_extract(record: LabelRecord, dest_dir: Path) -> list[Path]:
    """
    Downloads the ZIP bundle for a label, extracts the PDF (and XML companion),
    and saves them under dest_dir/<set_id>/.

    Returns list of extracted file paths.
    """
    label_dir = dest_dir / record.set_id
    label_dir.mkdir(parents=True, exist_ok=True)

    existing_pdfs = list(label_dir.glob("*.pdf"))
    if existing_pdfs:
        log.debug("Already downloaded %s, skipping.", record.set_id)
        return existing_pdfs

    try:
        resp = requests.get(record.source_url, timeout=60, stream=True)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Failed to download %s: %s", record.set_id, exc)
        return []

    extracted: list[Path] = []
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                if name.endswith((".pdf", ".xml")):
                    out_path = label_dir / Path(name).name
                    out_path.write_bytes(zf.read(name))
                    extracted.append(out_path)
    except zipfile.BadZipFile as exc:
        log.error("Bad ZIP for %s: %s", record.set_id, exc)

    return extracted


def upload_to_s3(local_path: Path, s3_prefix: str, set_id: str) -> str:
    """
    Uploads a local file to S3 under s3_prefix/<set_id>/<filename>.
    Requires boto3 and AWS credentials in the environment.
    Returns the S3 URI of the uploaded object.
    """
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 is required for S3 uploads: pip install boto3")

    bucket, *key_parts = s3_prefix.removeprefix("s3://").split("/")
    key = "/".join(key_parts + [set_id, local_path.name])

    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), bucket, key)
    uri = f"s3://{bucket}/{key}"
    log.debug("Uploaded %s → %s", local_path.name, uri)
    return uri


# ── Main ──────────────────────────────────────────────────────────────────────

def run(drug_class: str, output_dir: str, limit: int) -> None:
    is_s3 = output_dir.startswith("s3://")
    local_tmp = Path("/tmp/dailymed_download") if is_s3 else Path(output_dir)
    local_tmp.mkdir(parents=True, exist_ok=True)

    downloaded = skipped = failed = 0

    for record in search_labels(drug_class, limit):
        files = download_and_extract(record, local_tmp)

        if not files:
            failed += 1
            continue

        pdfs = [f for f in files if f.suffix == ".pdf"]
        if not pdfs:
            failed += 1
            log.warning("No PDF found in bundle for %s", record.set_id)
            continue

        if is_s3:
            for f in files:
                upload_to_s3(f, output_dir, record.set_id)
            skipped += 1 if not pdfs else 0
        else:
            skipped += 0

        downloaded += 1
        time.sleep(REQUEST_DELAY)

    log.info(
        "Done. downloaded=%d  failed=%d  total_attempted=%d",
        downloaded, failed, downloaded + failed,
    )
    log.info("Files saved to: %s", output_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--drug-class",
        default="Antineoplastic Agents",
        help="DailyMed pharmacological drug class to filter on (default: Antineoplastic Agents)",
    )
    parser.add_argument(
        "--output-dir",
        default="./data/sample",
        help="Local directory or S3 prefix (s3://bucket/prefix) to save files",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of labels to download (default: 50 for local dev, 5000 for full run)",
    )
    args = parser.parse_args()
    run(drug_class=args.drug_class, output_dir=args.output_dir, limit=args.limit)


if __name__ == "__main__":
    main()
