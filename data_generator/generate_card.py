#!/usr/bin/env python3
"""
Card Data Generator — Card Details + Card Status
Output: Parquet files to output/card/
"""
import os
import random
from datetime import date, timedelta
import pandas as pd
from gen_utils import (
    gen_card_id, gen_customer_id, random_date,
)

OUTPUT_DIR = "output/card"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_CARDS = 15_000
NUM_CUSTOMERS = 10_000
CARD_TYPES = ["CREDIT", "DEBIT", "PREPAID"]
CARD_NETWORKS = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]
STATUS_CODES = ["ACTIVE", "BLOCKED", "CLOSED", "SUSPENDED"]
STATUS_REASONS = ["", "FRAUD", "LOST", "STOLEN", "VOLUNTARY_CLOSE", "DELINQUENT", "UPGRADE", "EXPIRED"]


def generate_card_details(n: int, customer_ids: list) -> list[dict]:
    records = []
    for i in range(n):
        cid = random.choice(customer_ids)
        issued = random_date(date(2018, 1, 1), date(2024, 6, 30))
        card_type = random.choice(CARD_TYPES)
        credit_limit = round(random.uniform(1000, 50000), 2) if card_type == "CREDIT" else 0.0
        cash_limit = round(random.uniform(0, credit_limit * 0.5), 2) if card_type == "CREDIT" else 0.0
        records.append({
            "card_id": gen_card_id(i + 1),
            "customer_id": cid,
            "card_type": card_type,
            "card_network": random.choice(CARD_NETWORKS),
            "issued_date": issued.isoformat(),
            "expiry_date": date(issued.year + 5, issued.month, 1).isoformat(),
            "credit_limit": credit_limit,
            "cash_limit": cash_limit,
            "interest_rate": round(random.uniform(8.99, 29.99), 2) if card_type == "CREDIT" else 0.0,
        })
    return records


def generate_card_status(cards: list[dict]) -> list[dict]:
    records = []
    for card in cards:
        cid = card["card_id"]
        issued = card["issued_date"]
        status_count = random.randint(1, 4)
        current = date.fromisoformat(issued)
        for j in range(status_count):
            current = random_date(current, current + timedelta(days=random.randint(30, 365)))
            rec = {
                "card_id": cid,
                "status_code": random.choice(STATUS_CODES),
                "status_date": current.isoformat(),
                "reason_code": random.choice(STATUS_REASONS),
            }
            records.append(rec)
    return records


if __name__ == "__main__":
    customer_ids = [gen_customer_id(i + 1) for i in range(NUM_CUSTOMERS)]
    cards = generate_card_details(NUM_CARDS, customer_ids)
    statuses = generate_card_status(cards)

    pdf_cards = pd.DataFrame(cards)
    pdf_status = pd.DataFrame(statuses)

    pdf_cards.to_parquet(f"{OUTPUT_DIR}/card_details.parquet", index=False)
    print(f"Wrote {len(cards)} card detail records")

    pdf_status.to_parquet(f"{OUTPUT_DIR}/card_status.parquet", index=False)
    print(f"Wrote {len(statuses)} card status records")
