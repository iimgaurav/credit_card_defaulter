#!/usr/bin/env python3
"""
Collections Data Generator — Defaults + Recovery
Output: Excel files to output/collections/
"""
import os
import random
from datetime import date, timedelta
import pandas as pd
from gen_utils import (
    gen_default_id, gen_recovery_id, gen_customer_id, gen_card_id, random_date,
)

OUTPUT_DIR = "output/collections"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_DEFAULTS = 5_000
NUM_CUSTOMERS = 10_000
NUM_CARDS = 15_000
STAGES = ["EARLY", "MID", "LATE", "LEGAL"]
RECOVERY_METHODS = ["SETTLEMENT", "GARNISHMENT", "CHARGEOFF"]
RECOVERY_STATUSES = ["PENDING", "PARTIAL", "FULL"]


def generate_defaults(n: int) -> list[dict]:
    records = []
    start = date(2023, 1, 1)
    end = date(2024, 12, 31)
    for i in range(n):
        cid = gen_customer_id(random.randint(1, NUM_CUSTOMERS))
        card_id = gen_card_id(random.randint(1, NUM_CARDS))
        default_date = random_date(start, end)
        dpd = random.randint(30, 180)
        outstanding = round(random.uniform(1000, 75000), 2)
        records.append({
            "default_id": gen_default_id(i + 1),
            "customer_id": cid,
            "card_id": card_id,
            "default_date": default_date.isoformat(),
            "days_past_due": dpd if random.random() > 0.03 else -1,
            "outstanding_amount": outstanding,
            "collection_stage": random.choice(STAGES) if random.random() > 0.02 else None,
            "last_contact_date": random_date(default_date, min(default_date + timedelta(days=60), end)).isoformat(),
        })
    return records


def generate_recovery(defaults: list[dict]) -> list[dict]:
    records = []
    for dflt in defaults:
        did = dflt["default_id"]
        num_recoveries = random.choices([0, 1, 2, 3], weights=[30, 40, 20, 10])[0]
        outstanding = dflt["outstanding_amount"]
        recovered_so_far = 0.0
        for j in range(num_recoveries):
            max_recoverable = outstanding - recovered_so_far
            if max_recoverable <= 0:
                break
            recv_amount = round(random.uniform(100, max_recoverable * 0.8), 2)
            recovered_so_far += recv_amount
            recv_date = random_date(
                date.fromisoformat(dflt["default_date"]),
                min(date.fromisoformat(dflt["default_date"]) + timedelta(days=180), date(2024, 12, 31))
            )
            records.append({
                "recovery_id": gen_recovery_id(len(records) + 1),
                "default_id": did,
                "recovery_date": recv_date.isoformat(),
                "recovery_amount": recv_amount,
                "recovery_method": random.choice(RECOVERY_METHODS),
                "recovery_status": random.choice(RECOVERY_STATUSES),
            })
    return records


if __name__ == "__main__":
    defaults = generate_defaults(NUM_DEFAULTS)
    recoveries = generate_recovery(defaults)

    pdf_defaults = pd.DataFrame(defaults)
    pdf_recoveries = pd.DataFrame(recoveries)

    with pd.ExcelWriter(f"{OUTPUT_DIR}/collections_defaults.xlsx", engine="openpyxl") as writer:
        pdf_defaults.to_excel(writer, sheet_name="defaults", index=False)
    print(f"Wrote {len(defaults)} default records")

    with pd.ExcelWriter(f"{OUTPUT_DIR}/collections_recovery.xlsx", engine="openpyxl") as writer:
        pdf_recoveries.to_excel(writer, sheet_name="recovery", index=False)
    print(f"Wrote {len(recoveries)} recovery records")
