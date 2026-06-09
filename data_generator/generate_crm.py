#!/usr/bin/env python3
"""
CRM Data Generator — Customer Master + Customer Address
Output: CSV files to output/crm/
"""
import csv
import os
import random
from datetime import date, timedelta
from gen_utils import (
    gen_customer_id, FIRST_NAMES, LAST_NAMES, CITIES, STATES, STREET_NAMES,
    random_birth_date, random_income, random_credit_score, random_phone, random_email,
)

OUTPUT_DIR = "output/crm"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_CUSTOMERS = 10_000

GENDERS = ["M", "F", "O"]
MARITAL_STATUSES = ["SINGLE", "MARRIED", "DIVORCED", "WIDOWED", None]
EMPLOYMENT_STATUSES = ["EMPLOYED", "SELF_EMPLOYED", "UNEMPLOYED", "RETIRED", "STUDENT"]
ADDRESS_TYPES = ["HOME", "WORK", "BILLING"]


def generate_customer_master(n: int) -> list[dict]:
    records = []
    for i in range(n):
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        dob = random_birth_date()
        records.append({
            "customer_id": gen_customer_id(i + 1),
            "first_name": fname,
            "last_name": lname,
            "date_of_birth": dob.isoformat(),
            "gender": random.choice(GENDERS),
            "marital_status": random.choice(MARITAL_STATUSES),
            "email": random_email(fname, lname),
            "phone_number": random_phone(),
            "employment_status": random.choice(EMPLOYMENT_STATUSES),
            "annual_income": random_income(),
            "credit_score": random_credit_score(),
        })
    return records


def generate_customer_address(customers: list[dict]) -> list[dict]:
    records = []
    for cust in customers:
        cid = cust["customer_id"]
        addrs = {}
        for atype in ADDRESS_TYPES:
            state_code, country_code = random.choice(STATES)
            rec = {
                "customer_id": cid,
                "address_line1": f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
                "city": random.choice(CITIES),
                "state_code": state_code,
                "country_code": country_code,
                "zip_code": f"{random.randint(10000, 99999)}",
                "address_type": atype,
            }
            addrs[atype] = rec
        records.extend(addrs.values())
    return records


if __name__ == "__main__":
    customers = generate_customer_master(NUM_CUSTOMERS)
    addresses = generate_customer_address(customers)

    # Write customer master
    with open(f"{OUTPUT_DIR}/customer_master.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(customers[0].keys()))
        w.writeheader()
        w.writerows(customers)
    print(f"Wrote {len(customers)} customer records")

    # Write customer address
    with open(f"{OUTPUT_DIR}/customer_address.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(addresses[0].keys()))
        w.writeheader()
        w.writerows(addresses)
    print(f"Wrote {len(addresses)} address records")
