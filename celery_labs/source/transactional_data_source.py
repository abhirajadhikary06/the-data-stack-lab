from faker import Faker
import random
import uuid
import json
import os
import libsql

from dotenv import load_dotenv

# -----------------------------------
# LOAD ENV VARIABLES
# -----------------------------------

load_dotenv()

# -----------------------------------
# CONNECT TO TURSO USING LIBSQL
# -----------------------------------

conn = libsql.connect(
    database=os.environ["TURSO_DATABASE_URL"],
    auth_token=os.environ["TURSO_AUTH_TOKEN"],
)

fake = Faker()

# -----------------------------------
# CREATE TABLES
# -----------------------------------

conn.execute("""
CREATE TABLE IF NOT EXISTS checkout_transactions (
    checkout_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    customer_id INTEGER,
    amount REAL,
    currency TEXT,
    payment_method TEXT,
    payment_status TEXT,
    gateway_transaction_id TEXT UNIQUE,
    created_at TEXT,
    updated_at TEXT
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS checkout_audit (
    audit_id INTEGER PRIMARY KEY,
    checkout_id INTEGER,
    old_status TEXT,
    new_status TEXT,
    failure_reason TEXT,
    refund_status TEXT,
    risk_score REAL,
    event_time TEXT,
    metadata TEXT
)
""")

conn.commit()

print("Tables created successfully")


# -----------------------------------
# GET NEXT AVAILABLE ID
# -----------------------------------

def get_next_id(table_name, id_column):

    result = conn.execute(
        f"SELECT MAX({id_column}) FROM {table_name}"
    ).fetchone()

    max_id = result[0]

    if max_id is None:
        return 1

    return max_id + 1


# -----------------------------------
# CHECK IF TRANSACTION EXISTS
# -----------------------------------

def transaction_exists(gateway_transaction_id):

    result = conn.execute("""
    SELECT 1
    FROM checkout_transactions
    WHERE gateway_transaction_id = ?
    LIMIT 1
    """, (gateway_transaction_id,)).fetchone()

    return result is not None


# -----------------------------------
# TABLE 1: checkout_transactions
# -----------------------------------

def generate_checkout_transactions(n=10):

    payment_methods = [
        "UPI",
        "Credit Card",
        "Debit Card",
        "Net Banking",
        "Wallet"
    ]

    payment_statuses = [
        "SUCCESS",
        "FAILED",
        "PENDING",
        "REFUNDED"
    ]

    currencies = ["INR", "USD", "EUR"]

    next_checkout_id = get_next_id(
        "checkout_transactions",
        "checkout_id"
    )

    inserted = 0

    while inserted < n:

        created_time = fake.date_time_between(
            start_date="-30d",
            end_date="now"
        )

        gateway_transaction_id = str(uuid.uuid4())

        # Skip duplicate transaction IDs
        if transaction_exists(gateway_transaction_id):
            print("Duplicate transaction found. Skipping...")
            continue

        transaction = {
            "checkout_id": next_checkout_id,
            "order_id": random.randint(1000, 9999),
            "customer_id": random.randint(1, 500),
            "amount": round(random.uniform(100, 5000), 2),
            "currency": random.choice(currencies),
            "payment_method": random.choice(payment_methods),
            "payment_status": random.choice(payment_statuses),
            "gateway_transaction_id": gateway_transaction_id,
            "created_at": str(created_time),
            "updated_at": str(created_time)
        }

        conn.execute("""
        INSERT INTO checkout_transactions (
            checkout_id,
            order_id,
            customer_id,
            amount,
            currency,
            payment_method,
            payment_status,
            gateway_transaction_id,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            transaction["checkout_id"],
            transaction["order_id"],
            transaction["customer_id"],
            transaction["amount"],
            transaction["currency"],
            transaction["payment_method"],
            transaction["payment_status"],
            transaction["gateway_transaction_id"],
            transaction["created_at"],
            transaction["updated_at"]
        ))

        print(
            f"Inserted Transaction: {transaction['checkout_id']}"
        )

        inserted += 1
        next_checkout_id += 1

    conn.commit()


# -----------------------------------
# TABLE 2: checkout_audit
# -----------------------------------

def generate_checkout_audit(n=10):

    statuses = [
        "PENDING",
        "SUCCESS",
        "FAILED",
        "REFUNDED"
    ]

    failure_reasons = [
        None,
        "Insufficient Balance",
        "Gateway Timeout",
        "Card Declined",
        "Fraud Suspected",
        "Network Error"
    ]

    refund_statuses = [
        "NOT_APPLICABLE",
        "INITIATED",
        "COMPLETED",
        "FAILED"
    ]

    next_audit_id = get_next_id(
        "checkout_audit",
        "audit_id"
    )

    max_checkout_id = conn.execute("""
    SELECT MAX(checkout_id)
    FROM checkout_transactions
    """).fetchone()[0]

    if max_checkout_id is None:
        print("No checkout transactions found")
        return

    inserted = 0

    while inserted < n:

        metadata = {
            "ip_address": fake.ipv4(),
            "device": fake.user_agent(),
            "location": fake.city()
        }

        audit = {
            "audit_id": next_audit_id,
            "checkout_id": random.randint(
                1,
                max_checkout_id
            ),
            "old_status": random.choice(statuses),
            "new_status": random.choice(statuses),
            "failure_reason": random.choice(
                failure_reasons
            ),
            "refund_status": random.choice(
                refund_statuses
            ),
            "risk_score": round(
                random.uniform(0, 100),
                2
            ),
            "event_time": str(
                fake.date_time_between(
                    start_date="-30d",
                    end_date="now"
                )
            ),
            "metadata": json.dumps(metadata)
        }

        conn.execute("""
        INSERT INTO checkout_audit (
            audit_id,
            checkout_id,
            old_status,
            new_status,
            failure_reason,
            refund_status,
            risk_score,
            event_time,
            metadata
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            audit["audit_id"],
            audit["checkout_id"],
            audit["old_status"],
            audit["new_status"],
            audit["failure_reason"],
            audit["refund_status"],
            audit["risk_score"],
            audit["event_time"],
            audit["metadata"]
        ))

        print(
            f"Inserted Audit: {audit['audit_id']}"
        )

        inserted += 1
        next_audit_id += 1

    conn.commit()


# -----------------------------------
# MAIN
# -----------------------------------

if __name__ == "__main__":

    print("\nGenerating checkout transactions...\n")
    generate_checkout_transactions(20)

    print("\nGenerating checkout audit logs...\n")
    generate_checkout_audit(20)

    print("\nFake data inserted successfully")

    # -----------------------------------
    # VERIFY DATA
    # -----------------------------------

    print("\n=== CHECKOUT TRANSACTIONS ===\n")

    rows = conn.execute("""
    SELECT *
    FROM checkout_transactions
    ORDER BY checkout_id DESC
    LIMIT 5
    """).fetchall()

    for row in rows:
        print(row)

    print("\n=== CHECKOUT AUDIT ===\n")

    rows = conn.execute("""
    SELECT *
    FROM checkout_audit
    ORDER BY audit_id DESC
    LIMIT 5
    """).fetchall()

    for row in rows:
        print(row)

    # CLOSE CONNECTION
    conn.close()