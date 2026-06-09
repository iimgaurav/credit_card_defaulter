#!/usr/bin/env python3
"""
Transaction Data Generator — credit card transactions
Output: JSON lines file to output/txn/
"""
import os
import random
import json
from datetime import date, timedelta
from gen_utils import (
    gen_transaction_id, gen_card_id, MERCHANT_NAMES, MCC_CATEGORIES,
    COUNTRIES, CURRENCIES, random_date,
)

OUTPUT_DIR = "output/txn"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_TXNS = 500_000
NUM_CARDS = 15_000
TXN_TYPES = ["PURCHASE", "WITHDRAWAL", "REFUND"]
POS_ENTRY = ["CHIP", "SWIPE", "CONTACTLESS", "ONLINE", "MANUAL"]


def generate_transactions(n: int, card_ids: list) -> list[dict]:
    records = []
    start = date(2023, 1, 1)
    end = date(2024, 12, 31)
    for i in range(n):
        cid = random.choice(card_ids)
        txn_date = random_date(start, end)
        txn_type = random.choices(TXN_TYPES, weights=[85, 10, 5])[0]
        merchant = random.choice(MERCHANT_NAMES)
        mcc = random.choice(MCC_CATEGORIES)
        country = random.choice(COUNTRIES)
        amount = round(random.uniform(1.0, 2000.0), 2)
        if txn_type == "REFUND":
            amount = round(random.uniform(5.0, 500.0), 2)
        records.append({
            "transaction_id": gen_transaction_id(i + 1),
            "card_id": cid,
            "transaction_date": txn_date.isoformat(),
            "transaction_time": f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
            "merchant_name": merchant if random.random() > 0.02 else None,
            "merchant_category": mcc,
            "merchant_country": country[0],
            "amount": amount,
            "currency_code": random.choice(CURRENCIES),
            "transaction_type": txn_type if random.random() > 0.01 else None,
            "pos_entry_mode": random.choice(POS_ENTRY),
        })
    return records


if __name__ == "__main__":
    card_ids = [gen_card_id(i + 1) for i in range(NUM_CARDS)]
    txns = generate_transactions(NUM_TXNS, card_ids)

    with open(f"{OUTPUT_DIR}/transactions.json", "w") as f:
        for txn in txns:
            f.write(json.dumps(txn) + "\n")
    print(f"Wrote {len(txns)} transaction records")
