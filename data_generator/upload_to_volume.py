#!/usr/bin/env python3
"""
Upload generated data to Unity Catalog Volume via Databricks Files API.
Usage: python upload_to_volume.py
"""
import os
import sys
import requests
from pathlib import Path

HOST = os.environ["DATABRICKS_HOST"].split()[0].strip()
TOKEN = os.environ["DATABRICKS_TOKEN"].strip()
VOLUME_BASE = "/Volumes/credit_card_dev/raw/landing"
LOCAL_OUTPUT = Path(__file__).parent / "output"

MAPPING = {
    "crm/customer_master.csv": "crm/customer_master/customer_master.csv",
    "crm/customer_address.csv": "crm/customer_address/customer_address.csv",
    "card/card_details.parquet": "card/card_details/card_details.parquet",
    "card/card_status.parquet": "card/card_status/card_status.parquet",
    "txn/transactions.json": "txn/transactions/transactions.json",
    "billing/billing_statements.csv": "billing/billing_statements/billing_statements.csv",
    "billing/billing_payments.csv": "billing/billing_payments/billing_payments.csv",
    "collections/collections_defaults.xlsx": "collections/collections_defaults/collections_defaults.xlsx",
    "collections/collections_recovery.xlsx": "collections/collections_recovery/collections_recovery.xlsx",
    "ref/ref_country.csv": "ref/ref_country/ref_country.csv",
    "ref/ref_state.csv": "ref/ref_state/ref_state.csv",
    "ref/ref_currency.csv": "ref/ref_currency/ref_currency.csv",
    "ref/dim_calendar.csv": "ref/dim_calendar/dim_calendar.csv",
}

HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def ensure_remote_dir(remote_dir: str):
    url = f"{HOST}/api/2.0/fs/directories{remote_dir}"
    requests.put(url, headers=HEADERS)
    return url


def upload_file(local_path: Path, remote_rel: str):
    remote_path = f"{VOLUME_BASE}/{remote_rel}"
    ensure_remote_dir(str(Path(remote_path).parent))
    url = f"{HOST}/api/2.0/fs/files{remote_path}"
    with open(local_path, "rb") as f:
        data = f.read()
    resp = requests.put(url, headers=HEADERS, data=data)
    if resp.status_code in (200, 201, 204):
        print(f"  OK: {local_path.name}")
    else:
        print(f"  ERR: {local_path.name} [{resp.status_code}]: {resp.text[:200]}")


if __name__ == "__main__":
    if not LOCAL_OUTPUT.exists():
        print(f"ERROR: {LOCAL_OUTPUT} not found.")
        sys.exit(1)
    print(f"Uploading to {HOST}{VOLUME_BASE}")
    print("=" * 60)
    for local_rel, remote_rel in MAPPING.items():
        local_path = LOCAL_OUTPUT / local_rel
        if not local_path.exists():
            print(f"  SKIP: {local_path.name} not found")
            continue
        upload_file(local_path, remote_rel)
    print("=" * 60)
    print("Upload complete.")
