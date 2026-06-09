#!/usr/bin/env python3
"""
Billing Data Generator — Statements + Payments
Output: CSV files to output/billing/
"""
import csv
import os
import random
from datetime import date, timedelta
from gen_utils import gen_statement_id, gen_payment_id, gen_card_id, random_date

OUTPUT_DIR = "output/billing"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_STATEMENTS = 100_000
NUM_CARDS = 15_000
PAYMENT_METHODS = ["ACH", "WIRE", "CHEQUE", "CASH"]


def generate_statements(n: int, card_ids: list) -> list[dict]:
    records = []
    start = date(2023, 1, 1)
    end = date(2024, 12, 31)
    for i in range(n):
        cid = random.choice(card_ids)
        stmt_date = random_date(start, end)
        due_date = stmt_date + timedelta(days=random.randint(15, 25))
        opening = round(random.uniform(0, 15000), 2)
        purchases = round(random.uniform(0, 5000), 2)
        payments = round(random.uniform(0, opening + purchases), 2)
        credits = round(random.uniform(0, 500), 2)
        interest = round(random.uniform(0, 200), 2)
        fees = round(random.uniform(0, 50), 2)
        closing = round(opening + purchases - payments + credits + interest + fees, 2)
        if closing < 0:
            closing = 0.0
        min_due = round(closing * random.uniform(0.02, 0.10), 2) if closing > 0 else 0.0
        records.append({
            "statement_id": gen_statement_id(i + 1),
            "card_id": cid,
            "statement_date": stmt_date.isoformat(),
            "due_date": due_date.isoformat(),
            "opening_balance": opening,
            "total_purchases": purchases,
            "total_payments": payments,
            "total_credits": credits,
            "interest_charged": interest,
            "fees_charged": fees,
            "closing_balance": closing,
            "minimum_due": min_due,
        })
    return records


def generate_payments(statements: list[dict]) -> list[dict]:
    records = []
    for stmt in statements:
        sid = stmt["statement_id"]
        num_payments = random.choices([0, 1, 2], weights=[10, 80, 10])[0]
        for _ in range(num_payments):
            pmt_date = random_date(
                date.fromisoformat(stmt["statement_date"]),
                min(date.fromisoformat(stmt["due_date"]) + timedelta(days=30), date(2024, 12, 31))
            )
            records.append({
                "payment_id": gen_payment_id(len(records) + 1),
                "statement_id": sid,
                "payment_date": pmt_date.isoformat(),
                "payment_amount": round(stmt["closing_balance"] * random.uniform(0.1, 1.0), 2),
                "payment_method": random.choice(PAYMENT_METHODS),
            })
    return records


if __name__ == "__main__":
    card_ids = [gen_card_id(i + 1) for i in range(NUM_CARDS)]
    statements = generate_statements(NUM_STATEMENTS, card_ids)
    payments = generate_payments(statements)

    with open(f"{OUTPUT_DIR}/billing_statements.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(statements[0].keys()))
        w.writeheader()
        w.writerows(statements)
    print(f"Wrote {len(statements)} statement records")

    with open(f"{OUTPUT_DIR}/billing_payments.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(payments[0].keys()))
        w.writeheader()
        w.writerows(payments)
    print(f"Wrote {len(payments)} payment records")
