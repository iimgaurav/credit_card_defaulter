#!/usr/bin/env python3
"""
Validate generated data — row counts, schemas, samples.
"""
import csv
import json
import os
import pandas as pd

OUTPUT_DIR = "output"

checks = []


def check(name, path, expected_min, expected_max=None):
    try:
        ext = os.path.splitext(path)[1].lower()
        if ext == ".csv":
            with open(path) as f:
                n = sum(1 for _ in f) - 1  # minus header
        elif ext == ".json":
            with open(path) as f:
                n = sum(1 for _ in f)
        elif ext == ".parquet":
            n = len(pd.read_parquet(path))
        elif ext == ".xlsx":
            n = len(pd.read_excel(path))
        else:
            n = -1

        status = "PASS" if (expected_max is None and n >= expected_min) or (expected_min <= n <= expected_max) else "FAIL"
        print(f"  [{status}] {name}: {n} rows (target: {expected_min}–{expected_max or '∞'})")
        return status
    except Exception as e:
        print(f"  [ERR]  {name}: {e}")
        return "ERR"


if __name__ == "__main__":
    print("=" * 60)
    print("VALIDATING GENERATED DATA")
    print("=" * 60)

    results = []

    print("\n--- CRM ---")
    results.append(check("customer_master", f"{OUTPUT_DIR}/crm/customer_master.csv", 10000, 11000))
    results.append(check("customer_address", f"{OUTPUT_DIR}/crm/customer_address.csv", 29000, 31000))

    print("\n--- Card ---")
    results.append(check("card_details", f"{OUTPUT_DIR}/card/card_details.parquet", 15000, 16000))
    results.append(check("card_status", f"{OUTPUT_DIR}/card/card_status.parquet", 15000, 60000))

    print("\n--- Transactions ---")
    results.append(check("transactions", f"{OUTPUT_DIR}/txn/transactions.json", 500000, 510000))

    print("\n--- Billing ---")
    results.append(check("billing_statements", f"{OUTPUT_DIR}/billing/billing_statements.csv", 100000, 105000))
    results.append(check("billing_payments", f"{OUTPUT_DIR}/billing/billing_payments.csv", 80000, 110000))

    print("\n--- Collections ---")
    results.append(check("collections_defaults", f"{OUTPUT_DIR}/collections/collections_defaults.xlsx", 5000, 6000))
    results.append(check("collections_recovery", f"{OUTPUT_DIR}/collections/collections_recovery.xlsx", 2000, 20000))

    print("\n--- Reference ---")
    results.append(check("ref_country", f"{OUTPUT_DIR}/ref/ref_country.csv", 15, 15))
    results.append(check("ref_state", f"{OUTPUT_DIR}/ref/ref_state.csv", 50, 52))
    results.append(check("ref_currency", f"{OUTPUT_DIR}/ref/ref_currency.csv", 10, 10))
    results.append(check("dim_calendar", f"{OUTPUT_DIR}/ref/dim_calendar.csv", 3650, 4018))

    passed = results.count("PASS")
    failed = results.count("FAIL")
    errors = results.count("ERR")
    total = len(results)

    print("\n" + "=" * 60)
    print(f"SUMMARY: {passed}/{total} passed, {failed} failed, {errors} errors")
    if failed > 0:
        print("WARNING: Some checks failed — investigate above.")
    else:
        print("All generated data validated successfully!")
    print("=" * 60)
