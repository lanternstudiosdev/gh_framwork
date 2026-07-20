#!/usr/bin/env python3
"""
Sample Data Generator for the Databricks Medallion Framework Samples.

Generates large realistic datasets (dims >=100 rows, facts 5k+ rows) for:
- HR and Sales subject areas
- SCD Type 1 and Type 2 (with continuous effective periods, no gaps)
- Junk dimension
- Calendar dimension (7 years daily, no gaps)
- Transactional fact (line-item)
- Periodic snapshot fact (monthly)
- Accumulating lifecycle fact (sales lead/opportunity stages with meaningful timestamps)

All source files land in a folder tree that maps to UC Volume paths:
  /Volumes/edw_hr_dev/files/landing/raw/{source_key}/{entity_name}/

Run: python generate_sample_data.py

Uses pandas + faker. Produces CSVs under samples/data/landing/raw/{source}/{entity}/

Best practice for SCD Type 2 change detection:
- Compute a hash (or checksum) of the columns that are "tracked" for versioning (the natural key + the business attributes whose change should trigger a new version).
- When a new record arrives for a natural key, compare its hash to the hash of the current version.
- If different, close the old version (set effective_to = new.effective_from - 1 day) and insert the new version with is_current = true.
- This is efficient for wide tables and is a standard pattern in data warehousing (avoids comparing dozens of columns).
- The generator pre-computes a "change_hash" on the tracked columns so the sample Silver/Gold can demonstrate the logic.
- In production Silver, you would compute the hash on the fly from the incoming Bronze row.

No gaps:
- Calendar: daily consecutive dates.
- SCD2: for each natural key, versions are sorted by effective_from; effective_to of version N + 1 day == effective_from of version N+1 (or version N is current with effective_to = null).

Volumes:
- Dims and history sources: 100+ rows
- Fact sources: 5,000+ rows
- Calendar: ~2,557 rows (7 years daily, e.g. 2019-01-01 to 2025-12-31)
"""

import random
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)

BASE_DIR = Path(__file__).parent.parent / "data" / "landing"
BASE_DIR.mkdir(parents=True, exist_ok=True)

def write_csv(df: pd.DataFrame, subpath: str):
    p = BASE_DIR / subpath
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    print(f"Wrote {len(df)} rows -> {p}")

# ----------------------------- CALENDAR (7 years daily, no gaps) -----------------------------
def generate_calendar_7yrs():
    start = date(2019, 1, 1)
    end = date(2025, 12, 31)
    rows = []
    current = start
    while current <= end:
        rows.append({
            "date_key": int(current.strftime("%Y%m%d")),
            "full_date": current,
            "year": current.year,
            "quarter": (current.month - 1) // 3 + 1,
            "month": current.month,
            "month_name": current.strftime("%B"),
            "day": current.day,
            "day_of_week": current.weekday() + 1,
            "day_name": current.strftime("%A"),
            "is_weekend": current.weekday() >= 5,
            "fiscal_year": current.year if current.month >= 10 else current.year - 1,
            "week_of_year": current.isocalendar()[1],
            "is_month_start": current.day == 1,
            "is_quarter_start": current.month in (1,4,7,10) and current.day == 1,
        })
        current += timedelta(days=1)
    return pd.DataFrame(rows)

# ----------------------------- JUNK DIMENSION -----------------------------
def generate_junk_dimension(n=120):
    rows = []
    for i in range(1, n+1):
        rows.append({
            "junk_key": i,
            "status_group": random.choice(["Active", "Closed", "Pending", "Cancelled", "On Hold"]),
            "priority": random.choice(["High", "Medium", "Low"]),
            "source_channel": random.choice(["Direct", "Partner", "Web", "Referral", "Inbound"]),
            "is_promoted": random.choice([True, False]),
            "approval_level": random.choice(["L1", "L2", "L3", "Auto"]),
        })
    return pd.DataFrame(rows)

# ----------------------------- HR SCD2 (continuous, no gaps, with change_hash) -----------------------------
def generate_hr_departments_scd2(natural_keys=15, avg_versions=4):
    base_names = ["Engineering", "Sales", "HR", "Finance", "Marketing", "Product", "Legal", "Operations", "Data & Analytics", "Customer Success", "Research", "Partner Ecosystem", "Executive", "Internal Audit", "IT Infrastructure"]
    locations = ["New York", "Chicago", "Remote", "London", "Austin", "Seattle", "Berlin"]
    rows = []
    for did in range(10, 10 + natural_keys):
        name = base_names[(did-10) % len(base_names)]
        eff_start = date(2019, 1, 1)
        versions = max(2, random.randint(2, avg_versions))
        for v in range(versions):
            if v < versions - 1:
                eff_end = eff_start + timedelta(days=random.randint(180, 550))
            else:
                eff_end = None
            rows.append({
                "department_id": did,
                "department_name": name,
                "manager_id": random.randint(1, 80),
                "location": random.choice(locations),
                "effective_from": eff_start,
                "effective_to": eff_end,
                "is_current": eff_end is None,
                "version": v + 1,
                "change_hash": hash(f"{name}|{random.choice(locations)}|{random.randint(1,80)}")  # hash of tracked attrs for SCD2 comparison (best practice)
            })
            if eff_end:
                eff_start = eff_end + timedelta(days=1)  # continuous, no gap
    df = pd.DataFrame(rows)
    while len(df) < 130:
        df = pd.concat([df, df.sample(5, random_state=42)], ignore_index=True)
    return df.sort_values(["department_id", "effective_from"])

def generate_hr_employees_scd1(n=180):
    depts = list(range(10, 25))
    rows = []
    for eid in range(1, n+1):
        rows.append({
            "employee_id": eid,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "department_id": random.choice(depts),
            "email": fake.email(),
            "hire_date": fake.date_between(start_date="-6y", end_date="today"),
            "salary": round(random.uniform(52000, 172000), 2),
            "status": random.choice(["Active"]*7 + ["Terminated", "On Leave"]),
            "last_updated": fake.date_time_between(start_date="-18m").isoformat()
        })
    return pd.DataFrame(rows)

def generate_hr_employee_history_scd2(natural_keys=90, avg_versions=3):
    rows = []
    for eid in range(1, natural_keys + 1):
        eff_start = date(2019, 3, 1)
        versions = max(2, random.randint(2, avg_versions))
        base_salary = random.uniform(55000, 140000)
        base_dept = random.randint(10, 24)
        for v in range(versions):
            if v < versions - 1:
                eff_end = eff_start + timedelta(days=random.randint(150, 480))
            else:
                eff_end = None
            new_salary = round(base_salary * random.uniform(0.95, 1.35), 2)
            new_dept = base_dept if random.random() > 0.3 else random.randint(10, 24)
            rows.append({
                "employee_id": eid,
                "department_id": new_dept,
                "salary": new_salary,
                "effective_from": eff_start,
                "effective_to": eff_end,
                "is_current": eff_end is None,
                "version": v + 1,
                "change_hash": hash(f"{new_dept}|{new_salary}"),  # best practice hash of tracked columns
                "change_reason": random.choice(["Promotion", "Lateral Transfer", "Annual Review", "Reorg", "Performance"])
            })
            if eff_end:
                eff_start = eff_end + timedelta(days=1)
    df = pd.DataFrame(rows)
    while len(df) < 220:
        df = pd.concat([df, df.sample(3, random_state=42)], ignore_index=True)
    return df.sort_values(["employee_id", "effective_from"])

# ----------------------------- Sales SCD -----------------------------
def generate_sales_customers_scd1(n=140):
    rows = []
    for i in range(1, n+1):
        rows.append({
            "customer_id": f"CUST-{2000+i}",
            "company_name": fake.company(),
            "industry": random.choice(["Technology", "Manufacturing", "Healthcare", "Finance", "Retail", "Energy"]),
            "region": random.choice(["North America", "EMEA", "APAC", "LATAM"]),
            "email": fake.company_email(),
            "created_date": fake.date_between(start_date="-5y"),
            "last_modified": fake.date_time_between(start_date="-8m").isoformat()
        })
    return pd.DataFrame(rows)

def generate_sales_opportunities_scd2(natural_keys=160, avg_versions=3):
    stages = ["New", "Qualified", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
    rows = []
    for i in range(1, natural_keys + 1):
        eff_start = fake.date_between(start_date="-3y", end_date="-8m")
        versions = max(1, random.randint(2, avg_versions))
        base_amt = random.uniform(12000, 380000)
        for v in range(versions):
            if v < versions - 1:
                eff_end = eff_start + timedelta(days=random.randint(25, 160))
            else:
                eff_end = None
            new_stage = random.choice(stages)
            new_amt = round(base_amt * random.uniform(0.8, 1.25), 2)
            rows.append({
                "opportunity_id": f"OPP-{3000+i}",
                "customer_id": f"CUST-{2000 + random.randint(1, n-1)}",
                "product_id": random.choice(["PROD-A", "PROD-B", "PROD-C", "PROD-D"]),
                "amount": new_amt,
                "stage": new_stage,
                "close_date": (eff_start + timedelta(days=random.randint(30, 220))).isoformat(),
                "owner_id": random.randint(1, 35),
                "effective_from": eff_start,
                "effective_to": eff_end,
                "is_current": eff_end is None,
                "version": v + 1,
                "change_hash": hash(f"{new_stage}|{new_amt}"),  # hash of tracked columns (best practice for SCD2)
            })
            if eff_end:
                eff_start = eff_end + timedelta(days=1)
    df = pd.DataFrame(rows)
    while len(df) < 200:
        df = pd.concat([df, df.sample(4, random_state=42)], ignore_index=True)
    return df.sort_values(["opportunity_id", "effective_from"])

# ----------------------------- Facts -----------------------------
def generate_sales_transactions_fact(n=5500):
    """Transactional / line-item fact"""
    rows = []
    for i in range(1, n+1):
        rows.append({
            "transaction_id": f"TXN-{500000+i}",
            "opportunity_id": f"OPP-{3000 + random.randint(1,159)}",
            "customer_id": f"CUST-{2000 + random.randint(1,139)}",
            "product_id": random.choice(["PROD-A", "PROD-B", "PROD-C", "PROD-D"]),
            "transaction_date": fake.date_between(start_date="-2y", end_date="today"),
            "amount": round(random.uniform(4500, 95000), 2),
            "quantity": random.randint(1, 15),
            "sales_rep_id": random.randint(1, 28),
            "channel": random.choice(["Direct", "Partner", "Web", "Referral"]),
            "junk_key": random.randint(1, 120),
        })
    return pd.DataFrame(rows)

def generate_hr_monthly_headcount_snapshot(n=5600):
    """Periodic snapshot fact - monthly"""
    rows = []
    depts = list(range(10, 25))
    base_date = date(2020, 1, 1)
    for i in range(n):
        snap = base_date + timedelta(days=30 * (i % 68))
        d = random.choice(depts)
        rows.append({
            "snapshot_date": snap,
            "department_id": d,
            "headcount": random.randint(7, 72),
            "active_headcount": random.randint(5, 68),
            "avg_salary": round(random.uniform(62000, 118000), 2),
            "turnover_rate": round(random.uniform(0.015, 0.19), 4),
            "junk_key": random.randint(1, 120),
        })
    return pd.DataFrame(rows)

def generate_sales_lead_opportunity_lifecycle_fact(n=5600):
    """Accumulating lifecycle fact - meaningful sales lead to opportunity scenario.
    Stages: Lead -> Qualified -> Proposal -> Negotiation -> Closed Won / Closed Lost
    Includes entered/exited timestamps, duration, amount/probability at stage.
    Useful for cycle time, conversion rate, stage duration analytics.
    """
    stages = ["Lead", "Qualified", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
    rows = []
    for i in range(1, n+1):
        opp_id = f"OPP-{3000 + random.randint(1,159)}"
        created = fake.date_time_between(start_date="-3y", end_date="-20d")
        stage = random.choice(stages)
        entered = created + timedelta(days=random.randint(0, 90))
        exited = entered + timedelta(days=random.randint(4, 140)) if random.random() > 0.25 else None
        rows.append({
            "lifecycle_id": f"LC-{600000 + i}",
            "opportunity_id": opp_id,
            "stage": stage,
            "entered_date": entered.date(),
            "exited_date": exited.date() if exited else None,
            "duration_days": (exited - entered).days if exited else None,
            "amount_at_stage": round(random.uniform(8000, 320000), 2),
            "probability": random.choice([10, 25, 50, 75, 90]),
            "owner_id": random.randint(1, 35),
            "is_current_stage": random.random() > 0.55,
            "junk_key": random.randint(1, 120),
        })
    return pd.DataFrame(rows)

def main():
    print("Generating high-volume sample data (SCD1/2 with no gaps, calendar no gaps, junk, 3 fact types, hash for SCD2)...")

    # Calendar (no gaps)
    cal = generate_calendar_7yrs()
    # Paths mirror UC Volume layout: raw/{source_key}/{entity_name}/
    write_csv(cal, "raw/shared/calendar/calendar_dimension_2019_2025.csv")

    junk = generate_junk_dimension()
    write_csv(junk, "raw/shared/junk/junk_dimension_status.csv")

    hr_depts_scd2 = generate_hr_departments_scd2()
    write_csv(hr_depts_scd2, "raw/workday/departments/departments_scd2_2025-10-01.csv")

    hr_emps_scd1 = generate_hr_employees_scd1()
    write_csv(hr_emps_scd1, "raw/workday/employees/employees_scd1_2025-10-01.csv")

    hr_hist_scd2 = generate_hr_employee_history_scd2()
    write_csv(hr_hist_scd2, "raw/workday/employees/employee_history_scd2_2025-10-01.csv")

    sales_cust_scd1 = generate_sales_customers_scd1()
    write_csv(sales_cust_scd1, "raw/dynamics365/customers/customers_scd1_2025-10-01.csv")

    sales_opp_scd2 = generate_sales_opportunities_scd2()
    write_csv(sales_opp_scd2, "raw/dynamics365/opportunities/opportunities_scd2_2025-10-01.csv")

    tx = generate_sales_transactions_fact()
    write_csv(tx, "raw/dynamics365/transactions/sales_transactions_fact.csv")

    snap = generate_hr_monthly_headcount_snapshot()
    write_csv(snap, "raw/workday/snapshots/hr_monthly_headcount_snapshot.csv")

    life = generate_sales_lead_opportunity_lifecycle_fact()
    write_csv(life, "raw/dynamics365/lifecycle/sales_lead_opportunity_lifecycle.csv")

    print("\nGeneration complete.")
    print("Copy data/landing/raw/... into UC Volumes, e.g.:")
    print("  /Volumes/edw_hr_dev/files/landing/raw/workday/employees/")
    print("  /Volumes/edw_hr_dev/files/landing/raw/workday/departments/")
    print("  /Volumes/edw_sales_dev/files/landing/raw/dynamics365/opportunities/")
    print("Primary HR production path is Lakeflow Connect (not Volume); Volume is fallback/demo.")
    print("See samples/README.md and samples/sql/control/ for DDL + setup.")

if __name__ == "__main__":
    main()
