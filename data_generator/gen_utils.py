"""Shared utilities for synthetic data generation."""
import random
import uuid
import hashlib
from datetime import datetime, timedelta, date
from typing import Optional

random.seed(42)

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
    "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
    "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
    "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson", "Watson",
]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "Charlotte", "Indianapolis", "San Francisco", "Seattle",
    "Denver", "Nashville", "Oklahoma City", "El Paso", "Washington", "Boston",
    "Las Vegas", "Portland", "Memphis", "Louisville", "Baltimore", "Milwaukee",
]

STATES = [
    ("AL", "US"), ("AK", "US"), ("AZ", "US"), ("AR", "US"), ("CA", "US"),
    ("CO", "US"), ("CT", "US"), ("DE", "US"), ("FL", "US"), ("GA", "US"),
    ("HI", "US"), ("ID", "US"), ("IL", "US"), ("IN", "US"), ("IA", "US"),
    ("KS", "US"), ("KY", "US"), ("LA", "US"), ("ME", "US"), ("MD", "US"),
    ("MA", "US"), ("MI", "US"), ("MN", "US"), ("MS", "US"), ("MO", "US"),
    ("MT", "US"), ("NE", "US"), ("NV", "US"), ("NH", "US"), ("NJ", "US"),
    ("NM", "US"), ("NY", "US"), ("NC", "US"), ("ND", "US"), ("OH", "US"),
    ("OK", "US"), ("OR", "US"), ("PA", "US"), ("RI", "US"), ("SC", "US"),
    ("SD", "US"), ("TN", "US"), ("TX", "US"), ("UT", "US"), ("VT", "US"),
    ("VA", "US"), ("WA", "US"), ("WV", "US"), ("WI", "US"), ("WY", "US"),
    ("DC", "US"),
]

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD", "JPY", "INR", "CHF", "SGD", "HKD"]

MERCHANT_NAMES = [
    "Amazon", "Walmart", "Target", "Best Buy", "Home Depot", "Costco", "Starbucks",
    "McDonalds", "Shell", "Exxon", "Uber", "Lyft", "Netflix", "Spotify", "Apple Store",
    "Google Play", "Macy's", "Nordstrom", "Sephora", "Nike", "Adidas", "Delta Air",
    "United Airlines", "Marriott", "Hilton", "Airbnb", "DoorDash", "Grubhub",
    "Kroger", "Safeway", "CVS", "Walgreens", "7-Eleven", "Wendy's", "Burger King",
]

MCC_CATEGORIES = [
    "Grocery", "Restaurant", "Gas", "Retail", "Travel", "Entertainment", "Health",
    "Electronics", "Clothing", "Home", "Transportation", "Utilities", "Insurance",
    "Education", "Other",
]

COUNTRIES = [
    ("US", "United States"), ("GB", "United Kingdom"), ("DE", "Germany"),
    ("FR", "France"), ("CA", "Canada"), ("AU", "Australia"), ("JP", "Japan"),
    ("IN", "India"), ("SG", "Singapore"), ("CH", "Switzerland"), ("HK", "Hong Kong"),
    ("AE", "UAE"), ("BR", "Brazil"), ("MX", "Mexico"), ("NL", "Netherlands"),
]

STREET_NAMES = [
    "Main St", "Oak Ave", "Elm St", "Park Ave", "Broadway", "Market St",
    "Washington Ave", "Lake Dr", "Hill Rd", "River Rd", "Maple Ave", "Cedar Ln",
    "Pine St", "Walnut Ave", "Cherry Blvd", "Forest Dr", "Sunset Blvd", "Highland Ave",
]


def gen_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12].upper()}"


def gen_customer_id(seed: int = None) -> str:
    if seed is not None:
        return f"CUST_{seed:06d}"
    return gen_id("CUST")


def gen_card_id(seed: int = None) -> str:
    if seed is not None:
        return f"CARD_{seed:06d}"
    return gen_id("CARD")


def gen_transaction_id(seed: int = None) -> str:
    if seed is not None:
        return f"TXN_{seed:08d}"
    return gen_id("TXN")


def gen_statement_id(seed: int = None) -> str:
    if seed is not None:
        return f"STM_{seed:07d}"
    return gen_id("STM")


def gen_payment_id(seed: int = None) -> str:
    if seed is not None:
        return f"PAY_{seed:07d}"
    return gen_id("PAY")


def gen_default_id(seed: int = None) -> str:
    if seed is not None:
        return f"DFLT_{seed:06d}"
    return gen_id("DFLT")


def gen_recovery_id(seed: int = None) -> str:
    if seed is not None:
        return f"RECV_{seed:06d}"
    return gen_id("RECV")


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def random_birth_date() -> date:
    start = date(1960, 1, 1)
    end = date(2002, 12, 31)
    return random_date(start, end)


def random_income() -> float:
    return round(random.uniform(20000, 250000), 2)


def random_credit_score() -> int:
    dist = random.choices(
        [random.randint(300, 580), random.randint(581, 669), random.randint(670, 739),
         random.randint(740, 799), random.randint(800, 850)],
        weights=[15, 20, 30, 25, 10]
    )[0]
    return dist


def random_phone() -> str:
    return f"{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


def random_email(fname: str, lname: str) -> str:
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "proton.me",
               "icloud.com", "aol.com", "mail.com"]
    domain = random.choice(domains)
    pattern = random.choice([
        f"{fname.lower()}.{lname.lower()}",
        f"{fname.lower()}{lname.lower()}",
        f"{fname[0].lower()}{lname.lower()}",
        f"{lname.lower()}.{fname.lower()}",
        f"{fname.lower()}{random.randint(1, 999)}",
    ])
    return f"{pattern}@{domain}"


def expand_template(template: dict, n: int) -> list[dict]:
    """Expand a template dict with _type markers into n records."""
    records = []
    for i in range(n):
        rec = {}
        for k, v in template.items():
            if callable(v):
                rec[k] = v(i)
            else:
                rec[k] = v
        records.append(rec)
    return records
