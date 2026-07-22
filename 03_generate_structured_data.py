# Databricks notebook source
CATALOG = "workspace"
SCHEMA = "insurance"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

import random
from datetime import date, timedelta

random.seed(42)

POLICY_NUMBERS = [
    "SL-IND-1001", "SL-FAM-2002", "SL-SEN-3003", "SL-CI-4004", "SL-MAT-5005",
    "SL-TOP-6006", "SL-DIA-7007", "SL-GRP-8008", "SL-ACC-9009", "SL-INT-1010",
]

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Ayaan",
    "Krishna", "Ishaan", "Ananya", "Diya", "Saanvi", "Aadhya", "Kavya", "Myra",
    "Anika", "Riya", "Priya", "Sneha", "Rahul", "Amit", "Suresh", "Neha", "Pooja",
]
LAST_NAMES = [
    "Sharma", "Verma", "Iyer", "Reddy", "Nair", "Gupta", "Rao", "Menon",
    "Kulkarni", "Patel", "Singh", "Mehta", "Joshi", "Chopra", "Bose",
]
CITIES = [
    "Mumbai", "Bengaluru", "Delhi", "Hyderabad", "Chennai", "Pune", "Kolkata",
    "Ahmedabad", "Jaipur", "Kochi",
]

NUM_CUSTOMERS = 80

customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    customer_id = f"C{i:04d}"
    policy_number = random.choice(POLICY_NUMBERS)
    dob = date(1955, 1, 1) + timedelta(days=random.randint(0, 60 * 365))
    signup_date = date(2023, 1, 1) + timedelta(days=random.randint(0, 900))
    customers.append({
        "customer_id": customer_id,
        "name": f"{first} {last}",
        "dob": dob.isoformat(),
        "policy_number": policy_number,
        "city": random.choice(CITIES),
        "phone": f"9{random.randint(100000000, 999999999)}",
        "email": f"{first.lower()}.{last.lower()}{i}@example.com",
        "signup_date": signup_date.isoformat(),
    })

print(f"Generated {len(customers)} customers")

# COMMAND ----------

TRANSACTION_TYPES = ["premium_payment", "claim_payout", "refund"]
STATUSES = ["completed", "pending", "failed"]

NUM_TRANSACTIONS = 400

transactions = []
for i in range(1, NUM_TRANSACTIONS + 1):
    cust = random.choice(customers)
    txn_type = random.choices(TRANSACTION_TYPES, weights=[0.6, 0.3, 0.1])[0]

    if txn_type == "premium_payment":
        amount = round(random.uniform(2000, 25000), 2)
    elif txn_type == "claim_payout":
        amount = round(random.uniform(5000, 500000), 2)
    else:
        amount = round(random.uniform(500, 15000), 2)

    txn_date = date(2023, 1, 1) + timedelta(days=random.randint(0, 900))
    status = random.choices(STATUSES, weights=[0.85, 0.10, 0.05])[0]

    transactions.append({
        "transaction_id": f"T{i:05d}",
        "customer_id": cust["customer_id"],
        "policy_number": cust["policy_number"],
        "transaction_type": txn_type,
        "amount": amount,
        "transaction_date": txn_date.isoformat(),
        "status": status,
    })

print(f"Generated {len(transactions)} transactions")

# COMMAND ----------

customers_df = spark.createDataFrame(customers)
transactions_df = spark.createDataFrame(transactions)

(customers_df.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.customers"))

(transactions_df.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.transactions"))

print(f"Wrote {customers_df.count()} rows to {CATALOG}.{SCHEMA}.customers")
print(f"Wrote {transactions_df.count()} rows to {CATALOG}.{SCHEMA}.transactions")

# COMMAND ----------

# Column comments help the LLM's SQL-generation step understand the schema
spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA}.customers IS
    'Health insurance customers, one row per policyholder, linked to a policy_number.'
""")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.customers ALTER COLUMN customer_id COMMENT 'Unique customer identifier, e.g. C0001'")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.customers ALTER COLUMN policy_number COMMENT 'FK to the insurance policy the customer holds, e.g. SL-IND-1001'")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.customers ALTER COLUMN signup_date COMMENT 'Date the customer first purchased a policy'")

spark.sql(f"""
    COMMENT ON TABLE {CATALOG}.{SCHEMA}.transactions IS
    'Financial transactions per customer: premium payments, claim payouts, and refunds.'
""")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.transactions ALTER COLUMN transaction_type COMMENT 'One of: premium_payment, claim_payout, refund'")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.transactions ALTER COLUMN status COMMENT 'One of: completed, pending, failed'")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.transactions ALTER COLUMN policy_number COMMENT 'FK to the insurance policy tied to this transaction'")

print("Table and column comments added.")

# COMMAND ----------

display(spark.sql(f"""
    SELECT c.policy_number, COUNT(DISTINCT c.customer_id) AS num_customers, COUNT(t.transaction_id) AS num_transactions
    FROM {CATALOG}.{SCHEMA}.customers c
    LEFT JOIN {CATALOG}.{SCHEMA}.transactions t ON c.customer_id = t.customer_id
    GROUP BY c.policy_number
    ORDER BY c.policy_number
"""))

print("\nPhase 3 complete: customers and transactions Delta tables created with FK-consistent policy_numbers.")
