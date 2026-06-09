#!/usr/bin/env python3
"""
Data Quality Issue Injector
Injects realistic quality issues into generated data files per schema_definitions.json.
"""
import csv
import json
import os
import random
import pandas as pd
from datetime import date, timedelta

OUTPUT_DIR = "output"

random.seed(123)


def inject_crm_issues():
    path = f"{OUTPUT_DIR}/crm/customer_master.csv"
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    n = len(rows)
    # Null email (5%)
    for i in random.sample(range(n), int(n * 0.05)):
        rows[i]["email"] = ""
    # Null phone (3%)
    for i in random.sample(range(n), int(n * 0.03)):
        rows[i]["phone_number"] = ""
    # Duplicate customer_id (2%)
    dupes = [rows[i] for i in random.sample(range(n), int(n * 0.02))]
    rows.extend(dupes)
    # Future DOB (1%)
    for i in random.sample(range(n), int(n * 0.01)):
        rows[i]["date_of_birth"] = date(2099, 6, 15).isoformat()
    # Too old DOB (>120, 0.5%)
    for i in random.sample(range(n), max(1, int(n * 0.005))):
        rows[i]["date_of_birth"] = date(1850, 1, 1).isoformat()
    # Negative income (2%)
    for i in random.sample(range(n), int(n * 0.02)):
        rows[i]["annual_income"] = str(-abs(float(rows[i]["annual_income"])))
    # Invalid gender (1%)
    for i in random.sample(range(n), int(n * 0.01)):
        rows[i]["gender"] = "X"

    random.shuffle(rows)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Injected issues into {path}: {len(rows)} rows")


def inject_address_issues():
    path = f"{OUTPUT_DIR}/crm/customer_address.csv"
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    n = len(rows)
    # Missing state (3%)
    for i in random.sample(range(n), int(n * 0.03)):
        rows[i]["state_code"] = ""
    # Invalid country code (2%)
    for i in random.sample(range(n), int(n * 0.02)):
        rows[i]["country_code"] = "ZZ"
    # Null zip (4%)
    for i in random.sample(range(n), int(n * 0.04)):
        rows[i]["zip_code"] = ""
    # Orphan customer_id (1%)
    for i in random.sample(range(n), int(n * 0.01)):
        rows[i]["customer_id"] = "CUST_ORPHAN_" + str(random.randint(1, 999))

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Injected issues into {path}: {len(rows)} rows")


def inject_card_issues():
    path = f"{OUTPUT_DIR}/card/card_details.parquet"
    df = pd.read_parquet(path)
    n = len(df)

    # Null credit_limit (3%)
    idxs = random.sample(range(n), int(n * 0.03))
    df.loc[idxs, "credit_limit"] = None

    # Negative interest_rate (2%)
    idxs = random.sample(range(n), int(n * 0.02))
    df.loc[idxs, "interest_rate"] = df.loc[idxs, "interest_rate"].abs() * -1

    # Duplicate card_id (1%)
    dupes = df.iloc[random.sample(range(n), int(n * 0.01))].copy()
    df = pd.concat([df, dupes], ignore_index=True)

    # Cash_limit > credit_limit (3%)
    idxs = random.sample(range(n), int(n * 0.03))
    for idx in idxs:
        cl = df.loc[idx, "credit_limit"]
        if pd.notna(cl) and cl > 0:
            df.loc[idx, "cash_limit"] = round(cl * random.uniform(1.1, 2.0), 2)

    df.to_parquet(path, index=False)
    print(f"Injected issues into {path}: {len(df)} rows")


def inject_card_status_issues():
    path = f"{OUTPUT_DIR}/card/card_status.parquet"
    df = pd.read_parquet(path)
    n = len(df)

    # Orphan card_id (2%)
    idxs = random.sample(range(n), int(n * 0.02))
    df.loc[idxs, "card_id"] = "CARD_ORPHAN_" + pd.Series([str(random.randint(1, 999)) for _ in idxs]).values

    # Invalid status code (1%)
    idxs = random.sample(range(n), int(n * 0.01))
    df.loc[idxs, "status_code"] = "INVALID"

    # Future status_date (1%)
    idxs = random.sample(range(n), int(n * 0.01))
    df.loc[idxs, "status_date"] = date(2030, 6, 15).isoformat()

    df.to_parquet(path, index=False)
    print(f"Injected issues into {path}: {len(df)} rows")


def inject_txn_issues():
    path = f"{OUTPUT_DIR}/txn/transactions.json"
    rows = []
    with open(path) as f:
        for line in f:
            rows.append(json.loads(line))

    n = len(rows)
    # Negative amounts (2%)
    for i in random.sample(range(n), int(n * 0.02)):
        rows[i]["amount"] = -abs(rows[i]["amount"])
    # Missing merchant (already 2% null in generator)
    # Invalid currency (1%)
    for i in random.sample(range(n), int(n * 0.01)):
        rows[i]["currency_code"] = "XYZ"
    # Duplicate transaction_id (0.5%)
    dupes = [rows[i] for i in random.sample(range(n), int(n * 0.005))]
    rows.extend(dupes)
    # Future date (0.5%)
    for i in random.sample(range(n), int(n * 0.005)):
        rows[i]["transaction_date"] = date(2030, 1, 15).isoformat()
    # Outlier amounts (0.5% very high)
    for i in random.sample(range(n), int(n * 0.005)):
        rows[i]["amount"] = round(random.uniform(50000, 200000), 2)

    random.shuffle(rows)
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    print(f"Injected issues into {path}: {len(rows)} rows")


def inject_billing_issues():
    # Statements
    path = f"{OUTPUT_DIR}/billing/billing_statements.csv"
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    n = len(rows)
    # Negative opening_balance (2%)
    for i in random.sample(range(n), int(n * 0.02)):
        rows[i]["opening_balance"] = str(-abs(float(rows[i]["opening_balance"])))
    # Due_date before statement_date (2%)
    for i in random.sample(range(n), int(n * 0.02)):
        stmt_d = date.fromisoformat(rows[i]["statement_date"])
        early = stmt_d - timedelta(days=random.randint(1, 10))
        rows[i]["due_date"] = early.isoformat()
    # Missing minimum_due (3%)
    for i in random.sample(range(n), int(n * 0.03)):
        rows[i]["minimum_due"] = ""

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Injected issues into {path}: {len(rows)} rows")

    # Payments
    path = f"{OUTPUT_DIR}/billing/billing_payments.csv"
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    n = len(rows)
    # Negative payment_amount (2%)
    for i in random.sample(range(n), int(n * 0.02)):
        rows[i]["payment_amount"] = str(-abs(float(rows[i]["payment_amount"])))
    # Payment_date before statement_date — need to cross-reference
    # Orphan statement_id (2%)
    for i in random.sample(range(n), int(n * 0.02)):
        rows[i]["statement_id"] = "STM_ORPHAN_" + str(random.randint(1, 999))

    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Injected issues into {path}: {len(rows)} rows")


def inject_collections_issues():
    path = f"{OUTPUT_DIR}/collections/collections_defaults.xlsx"
    df = pd.read_excel(path, sheet_name="defaults")
    n = len(df)

    # Duplicate default_id (2%)
    dupes = df.iloc[random.sample(range(n), int(n * 0.02))].copy()
    df = pd.concat([df, dupes], ignore_index=True)
    n = len(df)

    # Negative days_past_due (3%)
    idxs = random.sample(range(n), int(n * 0.03))
    df.loc[idxs, "days_past_due"] = df.loc[idxs, "days_past_due"].abs() * -1

    # Missing collection_stage (2%)
    idxs = random.sample(range(n), int(n * 0.02))
    df.loc[idxs, "collection_stage"] = None

    # Orphan customer_id (1%)
    idxs = random.sample(range(n), int(n * 0.01))
    df.loc[idxs, "customer_id"] = "CUST_ORPHAN_COL_" + pd.Series([str(random.randint(1, 999)) for _ in idxs]).values

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="defaults", index=False)
    print(f"Injected issues into {path}: {len(df)} rows")

    # Recovery
    path = f"{OUTPUT_DIR}/collections/collections_recovery.xlsx"
    df = pd.read_excel(path, sheet_name="recovery")
    n = len(df)

    # Orphan default_id (2%)
    idxs = random.sample(range(n), int(n * 0.02))
    df.loc[idxs, "default_id"] = "DFLT_ORPHAN_" + pd.Series([str(random.randint(1, 999)) for _ in idxs]).values

    # Recovery_amount > outstanding (we don't have outstanding here, simulate)
    # Negative recovery_amount (2%)
    idxs = random.sample(range(n), int(n * 0.02))
    df.loc[idxs, "recovery_amount"] = df.loc[idxs, "recovery_amount"].abs() * -1

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="recovery", index=False)
    print(f"Injected issues into {path}: {len(df)} rows")


if __name__ == "__main__":
    inject_crm_issues()
    inject_address_issues()
    inject_card_issues()
    inject_card_status_issues()
    inject_txn_issues()
    inject_billing_issues()
    inject_collections_issues()
    print("\nAll data quality issues injected.")
