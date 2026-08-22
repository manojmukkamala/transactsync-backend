#!/usr/bin/env python3
"""Seed the TransactSync API with dummy data for dashboard development."""

import json
import random
import sys
from datetime import datetime, timedelta

import requests

BASE_URL = "https://dbx-transactsync-api-7474651502210516.aws.databricksapps.com"
LOAD_BY = "seed-script"


def get_token() -> str:
    import subprocess
    result = subprocess.run(
        ["databricks", "auth", "token"],
        capture_output=True, text=True
    )
    return json.loads(result.stdout)["access_token"]


def session_with_auth() -> requests.Session:
    s = requests.Session()
    token = get_token()
    s.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    return s


def post(s: requests.Session, endpoint: str, payload: dict) -> dict:
    r = s.post(f"{BASE_URL}{endpoint}", json=payload)
    r.raise_for_status()
    return r.json()


def get(s: requests.Session, endpoint: str, params: dict | None = None) -> list[dict] | dict:
    r = s.get(f"{BASE_URL}{endpoint}", params=params)
    r.raise_for_status()
    return r.json()


# --- Data definitions ---

CATEGORIES = [
    ("Groceries", "Supermarket and grocery purchases"),
    ("Dining & Takeout", "Restaurants, cafes, food delivery"),
    ("Transportation", "Gas, ride-sharing, public transit, parking"),
    ("Utilities", "Electricity, water, gas, internet, phone"),
    ("Rent & Mortgage", "Housing payments"),
    ("Insurance", "Health, auto, life insurance premiums"),
    ("Healthcare", "Medical bills, pharmacy, dental"),
    ("Subscriptions", "Streaming, software, membership subscriptions"),
    ("Shopping", "Online and retail purchases"),
    ("Travel", "Flights, hotels, vacation expenses"),
    ("Entertainment", "Movies, concerts, events, hobbies"),
    ("Home Improvement", "Repairs, maintenance, furnishings"),
    ("Education", "Courses, books, school supplies"),
    ("Office Supplies", "Work equipment and supplies"),
    ("Professional Services", "Legal, consulting, freelance services"),
    ("Charitable Donations", "Donations to nonprofits"),
    ("Taxes & Fees", "Government taxes and administrative fees"),
    ("Transfer", "Account transfers and inter-account movements"),
    ("Cash Withdrawal", "ATM and cash withdrawals"),
    ("Income - Salary", "Salary deposits"),
    ("Income - Refunds", "Refunds and tax returns"),
   ]

MERCHANTS = [
    ("Whole Foods Market", "Grocery store"),
    ("Trader Joe's", "Grocery store"),
    ("Costco Wholesale", "Warehouse club"),
    ("Amazon.com", "Online marketplace"),
    ("Target", "Retail store"),
    ("Walmart", "Retail store"),
    ("Starbucks", "Coffee chain"),
    ("Chipotle Mexican Grill", "Fast casual restaurant"),
    ("DoorDash", "Food delivery"),
    ("Uber Eats", "Food delivery"),
    ("Shell Gas Station", "Fuel station"),
    ("Chevron", "Fuel station"),
    ("Lyft", "Ride-sharing"),
    ("Uber Technologies", "Ride-sharing"),
    ("Comcast Xfinity", "Internet and cable provider"),
    ("PG&E", "Electricity and gas utility"),
    ("Sprint Wireless", "Phone carrier"),
    ("T-Mobile", "Phone carrier"),
    ("Blue Shield Insurance", "Health insurance"),
    ("GEICO Auto Insurance", "Auto insurance"),
    ("CVS Pharmacy", "Pharmacy"),
    ("Netflix", "Streaming subscription"),
    ("Spotify", "Music streaming"),
    ("Adobe Inc.", "Software subscription"),
    ("Delta Airlines", "Airline"),
    ("Marriott Hotels", "Hotel chain"),
    ("Hilton Hotels", "Hotel chain"),
    ("Apple Store", "Electronics retail"),
    ("Best Buy", "Electronics retail"),
    ("Home Depot", "Home improvement store"),
    ("Lowes", "Home improvement store"),
    ("Office Depot", "Office supplies"),
    ("Staples", "Office supplies"),
    ("McDonald's", "Fast food"),
    ("Panera Bread", "Bakery and cafe"),
    ("In-N-Out Burger", "Fast food burger chain"),
    ("Cheesecake Factory", "Restaurant chain"),
    ("Walgreens", "Pharmacy and convenience"),
    ("REI Co-op", "Outdoor gear retail"),
    ("Zara", "Clothing retail"),
    ("Nike Store", "Athletic apparel"),
    ("Circuit City Parking", "Parking garage"),
    ("Metropolitan Transit Authority", "Public transit"),
    ("Linklighting Electric", "Electric utility"),
    ("Waste Management", "Waste disposal service"),
    ("IRS", "Government tax agency"),
    ("PayPal Transfer", "Digital payment transfer"),
    ("Zelle Transfer", "Peer-to-peer payment"),
]

ACCOUNTS = [
    {
        "account_number": "BA-7821-4490",
        "financial_institution": "Bank of America",
        "account_name": "Main Checking",
        "account_owner": "Manoj Kamala",
        "account_type": "checking",
        "active": True,
        "comments": "Primary checking account for everyday expenses",
    },
    {
        "account_number": "BA-9012-3356",
        "financial_institution": "Bank of America",
        "account_name": "Savings Account",
        "account_owner": "Manoj Kamala",
        "account_type": "savings",
        "active": True,
        "comments": "Long-term savings account",
    },
    {
        "account_number": "CC-4916-XXXX",
        "financial_institution": "Chase Bank",
        "account_name": "Chase Sapphire Preferred",
        "account_owner": "Manoj Kamala",
        "account_type": "credit_card",
        "active": True,
        "comments": "Primary credit card for rewards",
    },
    {
        "account_number": "CC-5234-XXXX",
        "financial_institution": "Capital One",
        "account_name": "Capital One Quicksilver",
        "account_owner": "Manoj Kamala",
        "account_type": "credit_card",
        "active": True,
        "comments": "Everyday spending card",
    },
    {
        "account_number": "BA-1100-8821",
        "financial_institution": "Bank of America",
        "account_name": "Joint Checking",
        "account_owner": "Manoj and Family",
        "account_type": "checking",
        "active": True,
        "comments": "Shared household expenses",
    },
    {
        "account_number": "WF-3390-XXXX",
        "financial_institution": "Wells Fargo",
        "account_name": "Money Market",
        "account_owner": "Manoj Kamala",
        "account_type": "money_market",
        "active": True,
        "comments": "High-yield money market account",
    },
]

# Monthly billing cycles for the past 12 months
def generate_cycles() -> list[dict]:
    cycles = []
    now = datetime(2026, 6, 1)
    for i in range(14):
        start = now - timedelta(days=30 * i)
        end = start + timedelta(days=29)
        cycles.append({
            "cycle_start": start.strftime("%Y-%m-%dT00:00:00"),
            "cycle_end": end.strftime("%Y-%m-%dT23:59:59"),
            "cycle_description": f"Billing cycle {start.strftime('%B %Y')}",
        })
    return cycles


# Transaction templates grouped by category
TRANSACTION_TEMPLATES = {
    "Groceries": {
        "merchants": ["Whole Foods Market", "Trader Joe's", "Costco Wholesale"],
        "amount_range": (25, 350),
        "types": ["debit", "credit"],
    },
    "Dining & Takeout": {
        "merchants": ["Starbucks", "Chipotle Mexican Grill", "DoorDash", "Uber Eats", "McDonald's", "Panera Bread", "In-N-Out Burger", "Cheesecake Factory"],
        "amount_range": (3, 85),
        "types": ["credit", "debit"],
    },
    "Transportation": {
        "merchants": ["Shell Gas Station", "Chevron", "Lyft", "Uber Technologies", "Circuit City Parking", "Metropolitan Transit Authority"],
        "amount_range": (2.50, 80),
        "types": ["debit", "credit"],
    },
    "Utilities": {
        "merchants": ["Comcast Xfinity", "PG&E", "Linklighting Electric", "Waste Management"],
        "amount_range": (30, 250),
        "types": ["debit"],
    },
    "Rent & Mortgage": {
        "merchants": [],
        "amount_range": (1800, 2400),
        "types": ["debit"],
        "merchant_override": None,
    },
    "Insurance": {
        "merchants": ["Blue Shield Insurance", "GEICO Auto Insurance"],
        "amount_range": (50, 600),
        "types": ["debit"],
    },
    "Healthcare": {
        "merchants": ["CVS Pharmacy", "Walgreens"],
        "amount_range": (5, 200),
        "types": ["credit", "debit"],
    },
    "Subscriptions": {
        "merchants": ["Netflix", "Spotify", "Adobe Inc."],
        "amount_range": (5, 80),
        "types": ["credit"],
    },
    "Shopping": {
        "merchants": ["Amazon.com", "Target", "Walmart", "Apple Store", "Best Buy", "Zara", "Nike Store", "REI Co-op"],
        "amount_range": (10, 800),
        "types": ["credit", "debit"],
    },
    "Travel": {
        "merchants": ["Delta Airlines", "Marriott Hotels", "Hilton Hotels"],
        "amount_range": (50, 2500),
        "types": ["credit"],
    },
    "Home Improvement": {
        "merchants": ["Home Depot", "Lowes"],
        "amount_range": (15, 600),
        "types": ["debit", "credit"],
    },
    "Education": {
        "merchants": ["Amazon.com"],
        "amount_range": (10, 150),
        "types": ["credit", "debit"],
    },
    "Office Supplies": {
        "merchants": ["Office Depot", "Staples"],
        "amount_range": (5, 200),
        "types": ["debit", "credit"],
    },
    "Income - Salary": {
        "merchants": [],
        "amount_range": (4500, 5500),
        "types": ["deposit"],
        "merchant_override": "Employer Direct Deposit",
    },
    "Income - Refunds": {
        "merchants": [],
        "amount_range": (20, 400),
        "types": ["deposit"],
        "merchant_override": None,
    },
    "Taxes & Fees": {
        "merchants": ["IRS"],
        "amount_range": (50, 3000),
        "types": ["debit"],
    },
    "Transfer": {
        "merchants": ["PayPal Transfer", "Zelle Transfer"],
        "amount_range": (100, 2000),
        "types": ["transfer"],
    },
    "Cash Withdrawal": {
        "merchants": [],
        "amount_range": (40, 500),
        "types": ["withdrawal"],
        "merchant_override": "ATM Withdrawal",
    },
    # Skip: Professional Services, Charitable Donations - generate fewer
    "Professional Services": {
        "merchants": [],
        "amount_range": (100, 2000),
        "types": ["debit"],
        "merchant_override": None,
    },
    "Charitable Donations": {
        "merchants": [],
        "amount_range": (10, 500),
        "types": ["credit", "debit"],
        "merchant_override": None,
    },
}

# Weighted distribution for category frequency
CATEGORY_WEIGHTS = {
    "Groceries": 12,
    "Dining & Takeout": 18,
    "Transportation": 14,
    "Utilities": 4,
    "Rent & Mortgage": 1,
    "Insurance": 3,
    "Healthcare": 5,
    "Subscriptions": 3,
    "Shopping": 12,
    "Travel": 2,
    "Home Improvement": 4,
    "Education": 2,
    "Office Supplies": 2,
    "Income - Salary": 1,
    "Income - Refunds": 2,
    "Taxes & Fees": 1,
    "Transfer": 5,
    "Cash Withdrawal": 6,
    "Professional Services": 1,
    "Charitable Donations": 1,
}


def pick_account_for_type(txn_type: str) -> int:
    """Pick an account index based on transaction type."""
    if txn_type == "credit":
        # Credit card transactions
        return random.choice([3, 4])  # accounts 4-5 (index)
    elif txn_type == "debit":
        # Checking account debits
        return random.choice([1, 2, 5])  # accounts 2,3,6
    elif txn_type == "deposit":
        # Deposits go to checking or savings
        return random.choice([1, 2])  # accounts 2-3
    else:
        return random.randint(1, 6)


def generate_transactions(cycle_ids: list[int]) -> list[dict]:
    """Generate a large set of realistic transactions spanning all cycles."""
    transactions = []
    now = datetime(2026, 6, 1)

    # Build weighted category list
    weighted_cats = []
    for cat, weight in CATEGORY_WEIGHTS.items():
        if cat in TRANSACTION_TEMPLATES:
            weighted_cats.extend([cat] * weight)

    categories_by_name = {c[0]: c[1] for c in CATEGORIES}

    for _ in range(450):
        cat_name = random.choice(weighted_cats)
        template = TRANSACTION_TEMPLATES[cat_name]

        # Pick a random cycle and generate a date within it
        cycle_id = random.choice(cycle_ids)
        cycle_month_idx = cycle_ids.index(cycle_id)
        month_start = now - timedelta(days=30 * cycle_month_idx)
        day_offset = random.randint(0, 28)
        tx_date = month_start + timedelta(days=day_offset, hours=random.randint(6, 22), minutes=random.randint(0, 59))

        amount = round(random.uniform(*template["amount_range"]), 2)
        txn_type = random.choice(template["types"])

        if template.get("merchant_override"):
            merchant = template["merchant_override"]
        elif template["merchants"]:
            merchant = random.choice(template["merchants"])
        else:
            merchant_map = {
                "Rent & Mortgage": "Monthly Rent Payment",
                "Income - Salary": "Employer Direct Deposit",
                "Income - Refunds": random.choice(["Tax Refund", "Merchant Refund", "Insurance Rebate"]),
                "Professional Services": random.choice(["Legal Consulting LLC", "Tax Advisory Group", "Financial Planner Inc."]),
                "Charitable Donations": random.choice(["Red Cross", "Habitat for Humanity", "Local Food Bank", "United Way"]),
                "Transfer": random.choice(["PayPal Transfer", "Zelle Transfer"]),
                "Cash Withdrawal": "ATM Withdrawal",
            }
            merchant = merchant_map.get(cat_name, cat_name)

        account_id = pick_account_for_type(txn_type)

        expense_owners = ["Manoj Kamala", "Family Household", "Business Expense"]
        expense_owner = random.choice(expense_owners) if random.random() > 0.5 else None

        is_budgeted = random.random() < 0.4

        tx = {
            "transaction_date": tx_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "transaction_type": txn_type,
            "transaction_amount": amount,
            "merchant": merchant,
            "category": cat_name,
            "account_id": account_id,
            "cycle_id": cycle_id,
            "expense_owner": expense_owner,
            "llm_reasoning": f"Auto-categorized to {cat_name}",
            "comment": None,
            "is_budgeted": is_budgeted,
            "is_deleted": False,
        }

        # Add comments for some transactions
        if random.random() < 0.15:
            comments = [
                f"Monthly {cat_name.lower()} charge",
                f"Bulk purchase at {merchant}",
                "Split with colleague",
                f"Reimbursement pending",
                "Quarterly payment",
                "Annual renewal",
                "Family expense",
            ]
            tx["comment"] = random.choice(comments)

        transactions.append(tx)

    # Add salary transactions for each cycle (guarantee income)
    for cycle_id in cycle_ids:
        month_idx = cycle_ids.index(cycle_id)
        month_start = now - timedelta(days=30 * month_idx)
        payday1 = month_start + timedelta(days=7, hours=8, minutes=15)
        payday2 = month_start + timedelta(days=21, hours=8, minutes=15)
        for payday in [payday1, payday2]:
            if payday <= datetime(2026, 6, 15):
                transactions.append({
                    "transaction_date": payday.strftime("%Y-%m-%dT%H:%M:%S"),
                    "transaction_type": "deposit",
                    "transaction_amount": round(random.uniform(4800, 5200), 2),
                    "merchant": "Employer Direct Deposit",
                    "category": "Income - Salary",
                    "account_id": 1,
                    "cycle_id": cycle_id,
                    "is_budgeted": True,
                    "is_deleted": False,
                })

    # Add rent for each cycle
    for cycle_id in cycle_ids:
        month_idx = cycle_ids.index(cycle_id)
        month_start = now - timedelta(days=30 * month_idx)
        rent_date = month_start + timedelta(days=1, hours=10)
        if rent_date <= datetime(2026, 6, 15):
            transactions.append({
                "transaction_date": rent_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "transaction_type": "debit",
                "transaction_amount": round(random.uniform(2100, 2300), 2),
                "merchant": "Monthly Rent Payment",
                "category": "Rent & Mortgage",
                "account_id": 1,
                "cycle_id": cycle_id,
                "is_budgeted": True,
                "is_deleted": False,
            })

    # Sort by date for realism
    transactions.sort(key=lambda t: t["transaction_date"])
    return transactions


def generate_emails(account_ids: list[int]) -> list[dict]:
    """Generate dummy email records."""
    folders = ["INBOX", "Alerts"]
    from_addresses = [
        "alerts@bankofamerica.com",
        "noreply@chase.com",
        "notifications@capitalone.com",
        "service@wellsfargo.com",
        "receipts@amazon.com",
        "notifications@uber.com",
        "support@doordash.com",
    ]
    to_addresses = ["manoj.kamala@email.com", "family.kamala@email.com"]

    emails = []
    now = datetime(2026, 6, 1)
    for i in range(80):
        uid = 10000 + i
        folder = random.choice(folders)
        day_offset = random.randint(0, 400)
        email_date = now - timedelta(days=day_offset, hours=random.randint(6, 22))
        emails.append({
            "email_uid": uid,
            "folder": folder,
            "from_address": random.choice(from_addresses),
            "to_address": random.choice(to_addresses),
            "email_date": email_date.strftime("%Y-%m-%dT%H:%M:%S"),
        })
    return emails


def generate_files() -> list[dict]:
    """Generate dummy file records."""
    paths = ["/data/statements/bankofamerica", "/data/statements/chase", "/data/statements/capitalone"]
    files = []
    now = datetime(2026, 6, 1)
    for path in paths:
        for month_offset in range(12):
            created = now - timedelta(days=30 * month_offset)
            files.append({
                "file_name": f"statement_{created.strftime('%Y-%m')}.pdf",
                "file_path": path,
                "file_created_at": created.strftime("%Y-%m-%dT00:00:00"),
            })
    return files


def seed(s: requests.Session):
    """Main seeding function."""
    errors = []

    # 1. Create categories
    print("Creating categories...")
    cat_ids = {}
    for name, desc in CATEGORIES:
        try:
            resp = post(s, "/categories", {"category_name": name, "category_description": desc})
            cat_ids[name] = resp["category_id"]
        except Exception as e:
            errors.append(f"Category {name}: {e}")
    print(f"  Created {len(cat_ids)} categories")

    # 2. Create merchants
    print("Creating merchants...")
    merchant_ids = {}
    for name, desc in MERCHANTS:
        try:
            resp = post(s, "/merchants", {"merchant_name": name, "merchant_description": desc})
            merchant_ids[name] = resp["merchant_id"]
        except Exception as e:
            errors.append(f"Merchant {name}: {e}")
    print(f"  Created {len(merchant_ids)} merchants")

    # 3. Create accounts
    print("Creating accounts...")
    account_ids = []
    for acct in ACCOUNTS:
        try:
            payload = {"load_by": LOAD_BY, **acct}
            resp = post(s, "/accounts", payload)
            account_ids.append(resp["account_id"])
        except Exception as e:
            errors.append(f"Account {acct['account_name']}: {e}")
    print(f"  Created {len(account_ids)} accounts")

    # 4. Create cycles
    print("Creating cycles...")
    cycle_data = generate_cycles()
    cycle_ids = []
    for cycle in cycle_data:
        try:
            payload = {"load_by": LOAD_BY, **cycle}
            resp = post(s, "/cycles", payload)
            cycle_ids.append(resp["cycle_id"])
        except Exception as e:
            errors.append(f"Cycle {cycle['cycle_description']}: {e}")
    print(f"  Created {len(cycle_ids)} cycles")

    # 5. Create emails
    print("Creating emails...")
    email_count = 0
    emails = generate_emails(account_ids)
    for email in emails:
        try:
            payload = {"load_by": LOAD_BY, **email}
            post(s, "/emails", payload)
            email_count += 1
        except Exception as e:
            errors.append(f"Email uid={email['email_uid']}: {e}")
    print(f"  Created {email_count} emails")

    # 6. Create files
    print("Creating files...")
    file_count = 0
    files = generate_files()
    for f in files:
        try:
            payload = {"load_by": LOAD_BY, **f}
            post(s, "/files", payload)
            file_count += 1
        except Exception as e:
            errors.append(f"File {f['file_name']}: {e}")
    print(f"  Created {file_count} files")

    # 7. Create transactions (batched for readability)
    print("Creating transactions...")
    txns = generate_transactions(cycle_ids)
    txn_count = 0
    batch_size = 50
    for i in range(0, len(txns), batch_size):
        batch = txns[i:i + batch_size]
        for tx in batch:
            try:
                payload = {"load_by": LOAD_BY, **tx}
                post(s, "/transactions", payload)
                txn_count += 1
            except Exception as e:
                errors.append(f"Transaction {i+txn_count}: {e}")
        print(f"  Batch {i // batch_size + 1}: {min(batch_size, len(txns) - i)} transactions")
    print(f"  Created {txn_count} transactions total")

    # 8. Create email checkpoints
    print("Creating email checkpoints...")
    for folder in ["INBOX", "Alerts"]:
        try:
            post(s, "/email_checkpoints", {"folder": folder, "last_seen_uid": 10079})
        except Exception as e:
            errors.append(f"EmailCheckpoint {folder}: {e}")

    # 9. Create file checkpoints
    print("Creating file checkpoints...")
    for path in ["/data/statements/bankofamerica", "/data/statements/chase", "/data/statements/capitalone"]:
        try:
            post(s, "/checkpoints", {"identifier": path, "checkpoint": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        except Exception as e:
            errors.append(f"Checkpoint {path}: {e}")

    # Report
    print("\n--- Seeding Complete ---")
    print(f"Categories: {len(cat_ids)}")
    print(f"Merchants: {len(merchant_ids)}")
    print(f"Accounts: {len(account_ids)}")
    print(f"Cycles: {len(cycle_ids)}")
    print(f"Emails: {email_count}")
    print(f"Files: {file_count}")
    print(f"Transactions: {txn_count}")
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for err in errors[:20]:
            print(f"  - {err}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more")
    else:
        print("\nNo errors!")

    return errors


def validate(s: requests.Session):
    """Validate all data was written correctly."""
    print("\n--- Validation ---")
    issues = []

    # Check categories
    cats = get(s, "/categories")
    if len(cats) != len(CATEGORIES):
        issues.append(f"Categories: expected {len(CATEGORIES)}, got {len(cats)}")
    else:
        print(f"  Categories: {len(cats)} OK")

    # Check merchants
    merch = get(s, "/merchants")
    if len(merch) != len(MERCHANTS):
        issues.append(f"Merchants: expected {len(MERCHANTS)}, got {len(merch)}")
    else:
        print(f"  Merchants: {len(merch)} OK")

    # Check accounts
    accts = get(s, "/accounts")
    if len(accts) != len(ACCOUNTS):
        issues.append(f"Accounts: expected {len(ACCOUNTS)}, got {len(accts)}")
    else:
        print(f"  Accounts: {len(accts)} OK")

    # Check cycles
    cycs = get(s, "/cycles")
    if len(cycs) < 12:
        issues.append(f"Cycles: expected >= 12, got {len(cycs)}")
    else:
        print(f"  Cycles: {len(cycs)} OK")

    # Check transactions
    txns = get(s, "/transactions")
    if len(txns) < 400:
        issues.append(f"Transactions: expected >= 400, got {len(txns)}")
    else:
        print(f"  Transactions: {len(txns)} OK")

    # Check emails
    emls = get(s, "/emails")
    if len(emls) < 50:
        issues.append(f"Emails: expected >= 50, got {len(emls)}")
    else:
        print(f"  Emails: {len(emls)} OK")

    # Check files
    fss = get(s, "/files")
    if len(fss) < 20:
        issues.append(f"Files: expected >= 20, got {len(fss)}")
    else:
        print(f"  Files: {len(fss)} OK")

    # Check transaction date spread
    dates = [t["transaction_date"] for t in txns]
    min_date = min(dates)
    max_date = max(dates)
    print(f"  Transaction date range: {min_date[:10]} to {max_date[:10]}")

    # Check amount distribution
    amounts = [t["transaction_amount"] for t in txns]
    total = sum(amounts)
    print(f"  Total transaction volume: ${total:,.2f}")

    # Check account distribution
    acct_dist = {}
    for t in txns:
        aid = t["account_id"]
        acct_dist[aid] = acct_dist.get(aid, 0) + 1
    print(f"  Transactions per account: {acct_dist}")

    # Check category distribution
    cat_dist = {}
    for t in txns:
        cat = t.get("category", "Uncategorized")
        cat_dist[cat] = cat_dist.get(cat, 0) + 1
    print(f"  Category distribution:")
    for cat, count in sorted(cat_dist.items(), key=lambda x: -x[1]):
        print(f"    {cat}: {count}")

    if issues:
        print(f"\nValidation issues ({len(issues)}):")
        for issue in issues:
            print(f"  - {issue}")
        return False
    else:
        print("\nAll validations passed!")
        return True


if __name__ == "__main__":
    print("=== TransactSync Data Seeding Script ===\n")

    s = session_with_auth()

    # Verify connectivity
    health = get(s, "/health")
    print(f"API Health: {health}")

    errors = seed(s)
    valid = validate(s)

    if errors or not valid:
        sys.exit(1)
