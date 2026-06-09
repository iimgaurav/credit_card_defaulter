#!/usr/bin/env python3
"""
Reference & Calendar Data Generator — Country, State, Currency, Calendar
Output: CSV files to output/ref/, output/calendar/
"""
import csv
import os
import random
from datetime import date, timedelta
import pandas as pd
from gen_utils import COUNTRIES, STATES, CURRENCIES

OUTPUT_DIR_REF = "output/ref"
OUTPUT_DIR_CAL = "output/calendar"
os.makedirs(OUTPUT_DIR_REF, exist_ok=True)
os.makedirs(OUTPUT_DIR_CAL, exist_ok=True)


def generate_countries() -> list[dict]:
    records = []
    for code, name in COUNTRIES:
        records.append({
            "country_code": code,
            "country_name": name,
            "region": random.choice(["North America", "Europe", "Asia Pacific", "Middle East", "South America"]),
            "currency_code": random.choice(CURRENCIES),
        })
    return records


def generate_states() -> list[dict]:
    seen = set()
    records = []
    for code, ccode in STATES:
        key = (code, ccode)
        if key in seen:
            continue
        seen.add(key)
        records.append({
            "state_code": code,
            "state_name": f"State_{code}",
            "country_code": ccode,
            "region": "US Region",
        })
    return records


def generate_currencies() -> list[dict]:
    currency_data = [
        ("USD", "US Dollar", "$", "US"), ("EUR", "Euro", "€", "EU"),
        ("GBP", "British Pound", "£", "UK"), ("CAD", "Canadian Dollar", "C$", "CA"),
        ("AUD", "Australian Dollar", "A$", "AU"), ("JPY", "Japanese Yen", "¥", "JP"),
        ("INR", "Indian Rupee", "₹", "IN"), ("CHF", "Swiss Franc", "Fr", "CH"),
        ("SGD", "Singapore Dollar", "S$", "SG"), ("HKD", "Hong Kong Dollar", "HK$", "HK"),
    ]
    records = []
    for code, name, symbol, country in currency_data:
        records.append({
            "currency_code": code,
            "currency_name": name,
            "currency_symbol": symbol,
            "country_code": country,
        })
    return records


def generate_calendar(start_year: int = 2018, end_year: int = 2028) -> list[dict]:
    records = []
    current = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    while current <= end:
        is_weekend = current.weekday() >= 5
        is_holiday = (current.month == 1 and current.day == 1) or \
                     (current.month == 12 and current.day == 25)
        records.append({
            "date_key": current.isoformat(),
            "full_date": current.isoformat(),
            "day": current.day,
            "month": current.month,
            "year": current.year,
            "quarter": (current.month - 1) // 3 + 1,
            "day_of_week": current.weekday(),
            "day_name": current.strftime("%A"),
            "month_name": current.strftime("%B"),
            "week_number": current.isocalendar()[1],
            "is_weekend": is_weekend,
            "is_holiday": is_holiday,
            "fiscal_year": current.year if current.month >= 4 else current.year - 1,
            "fiscal_quarter": (current.month - 4) % 12 // 3 + 1,
        })
        current += timedelta(days=1)
    return records


def write_csv(records: list[dict], path: str):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        w.writeheader()
        w.writerows(records)


if __name__ == "__main__":
    countries = generate_countries()
    write_csv(countries, f"{OUTPUT_DIR_REF}/ref_country.csv")
    print(f"Wrote {len(countries)} country records")

    states = generate_states()
    write_csv(states, f"{OUTPUT_DIR_REF}/ref_state.csv")
    print(f"Wrote {len(states)} state records")

    currencies = generate_currencies()
    write_csv(currencies, f"{OUTPUT_DIR_REF}/ref_currency.csv")
    print(f"Wrote {len(currencies)} currency records")

    calendar = generate_calendar()
    write_csv(calendar, f"{OUTPUT_DIR_REF}/dim_calendar.csv")
    print(f"Wrote {len(calendar)} calendar records")
