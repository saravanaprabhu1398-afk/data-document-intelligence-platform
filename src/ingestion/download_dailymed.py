"""
Download a curated oncology slice of FDA drug labels from DailyMed
and land the XML (SPL) files in a Unity Catalog Volume.

The DailyMed API v2 serves the SPL XML document directly — no ZIP needed.
Files are saved preserving the set_id directory structure:

    /Volumes/<catalog>/<schema>/<volume>/<set_id>/<set_id>.xml

Usage — land locally (for inspection):
    python download_dailymed.py --output-dir ./data/sample --limit 50

Usage — upload directly to a UC Volume via Databricks Files API:
    python download_dailymed.py \\
        --output-dir /Volumes/clinical-lab/default/raw_clinical_pdf \\
        --limit 5000 \\
        --databricks-host https://your-workspace.azuredatabricks.net \\
        --databricks-token dapi...

DailyMed API docs: https://dailymed.nlm.nih.gov/dailymed/app-support-web-services.cfm
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DAILYMED_API  = "https://dailymed.nlm.nih.gov/dailymed/services/v2/"
PAGE_SIZE     = 100
REQUEST_DELAY = 0.25  # seconds between API calls — be polite to the public server


@dataclass
class LabelRecord:
    set_id:    str
    title:     str
    published: str


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
    Paginate DailyMed SPL search results filtered by Established Pharmacologic
    Class (EPC). Use drug_class_epc — not drug_class_moa — for broad therapeutic
    classes like 'Antineoplastic Agent'.
    """
    fetched = 0
    page = 1

    while fetched < limit:
        params = {
            "drug_class_epc": drug_class,
            "pagesize":       min(PAGE_SIZE, limit - fetched),
            "page":           page,
        }
        log.info("Fetching page %d  (fetched=%d / limit=%d)", page, fetched, limit)
        data = _get_json(DAILYMED_API + "spls.json", params=params)

        records = data.get("data", [])
        if not records:
            log.info("No more results at page %d.", page)
            break

        for rec in records:
            set_id = rec.get("setid", "")
            if not set_id:
                continue
            yield LabelRecord(
                set_id    = set_id,
                title     = rec.get("title", ""),
                published = rec.get("published_date", ""),
            )
            fetched += 1
            if fetched >= limit:
                break

        page += 1
        time.sleep(REQUEST_DELAY)


# ── Download XML directly from API ───────────────────────────────────────────

def download_xml(record: LabelRecord, dest_dir: Path) -> Path | None:
    """
    Fetches the SPL XML document directly from the DailyMed API v2 and saves
    it to dest_dir/<set_id>/<set_id>.xml.

    The API endpoint GET /spls/{set_id}.xml returns the full SPL document —
    no ZIP extraction needed.
    """
    label_dir = dest_dir / record.set_id
    label_dir.mkdir(parents=True, exist_ok=True)

    out_path = label_dir / f"{record.set_id}.xml"
    if out_path.exists():
        log.debug("Already present: %s", record.set_id)
        return out_path

    url = f"{DAILYMED_API}spls/{record.set_id}.xml"
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Download failed for %s: %s", record.set_id, exc)
        return None

    # Confirm we got XML, not an HTML error page
    content_type = resp.headers.get("Content-Type", "")
    if "xml" not in content_type and not resp.content.lstrip()[:5] == b"<?xml":
        log.warning(
            "Skipping %s — unexpected Content-Type: %s. Preview: %s",
            record.set_id, content_type, resp.text[:150].replace("\n", " "),
        )
        return None

    out_path.write_bytes(resp.content)
    log.debug("Saved %s  (%d KB)", record.set_id, len(resp.content) // 1024)
    return out_path


# ── Unity Catalog Volume upload ───────────────────────────────────────────────

def upload_to_volume(local_path: Path, volume_path: str, set_id: str, host: str, token: str) -> None:
    """
    Uploads a local file to a Unity Catalog Volume using the Databricks Files API.
    Placed at: <volume_path>/<set_id>/<filename>
    """
    dest = f"{volume_path.rstrip('/')}/{set_id}/{local_path.name}"
    url  = f"{host.rstrip('/')}/api/2.0/fs/files{dest}"

    with open(local_path, "rb") as f:
        resp = requests.put(
            url,
            headers={"Authorization": f"Bearer {token}"},
            data=f,
            timeout=120,
        )
    resp.raise_for_status()
    log.debug("Uploaded %s → %s", local_path.name, dest)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(
    drug_class:       str,
    output_dir:       str,
    limit:            int,
    databricks_host:  str | None,
    databricks_token: str | None,
) -> None:
    is_volume = output_dir.startswith("/Volumes/")
    upload    = is_volume and bool(databricks_host) and bool(databricks_token)

    local_tmp = Path("/tmp/dailymed_xml") if is_volume else Path(output_dir)
    local_tmp.mkdir(parents=True, exist_ok=True)

    if is_volume and not upload:
        log.warning(
            "output-dir is a Volume path but --databricks-host / --databricks-token "
            "were not provided. Files will be saved locally to /tmp/dailymed_xml instead. "
            "Then run: databricks fs cp --recursive /tmp/dailymed_xml %s",
            output_dir,
        )

    downloaded = failed = 0

    for record in search_labels(drug_class, limit):
        xml_path = download_xml(record, local_tmp)

        if xml_path is None:
            failed += 1
            continue

        if upload:
            try:
                upload_to_volume(xml_path, output_dir, record.set_id, databricks_host, databricks_token)
            except requests.RequestException as exc:
                log.error("Volume upload failed for %s: %s", record.set_id, exc)
                failed += 1
                continue

        downloaded += 1
        time.sleep(REQUEST_DELAY)

    log.info(
        "Done.  downloaded=%d  failed=%d  total_attempted=%d",
        downloaded, failed, downloaded + failed,
    )
    dest = output_dir if upload else str(local_tmp)
    log.info("Files at: %s", dest)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--drug-class",
        default="Antineoplastic Agent",
        help="DailyMed Established Pharmacologic Class (EPC) to filter on (default: Antineoplastic Agent)",
    )
    parser.add_argument(
        "--output-dir",
        default="./data/sample",
        help="Local directory or /Volumes/... path to write XML files",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max labels to download (default: 50 for dev, 5000 for full oncology slice)",
    )
    parser.add_argument(
        "--databricks-host",
        default=os.getenv("DATABRICKS_HOST"),
        help="Databricks workspace URL (or set DATABRICKS_HOST env var)",
    )
    parser.add_argument(
        "--databricks-token",
        default=os.getenv("DATABRICKS_TOKEN"),
        help="Databricks PAT (or set DATABRICKS_TOKEN env var)",
    )
    args = parser.parse_args()
    run(
        drug_class       = args.drug_class,
        output_dir       = args.output_dir,
        limit            = args.limit,
        databricks_host  = args.databricks_host,
        databricks_token = args.databricks_token,
    )


if __name__ == "__main__":
    main()
