#!/usr/bin/env python3
"""
Dynamics 365 / Dataverse API Simulation Script

Simulates pulling data from published Dynamics 365 schemas (Opportunity, Account, Contact, etc.)
and lands results as CSV/Parquet for Auto Loader Bronze.

Pattern: Custom Extract → UC Volume (or local folder) → Auto Loader Bronze.

Preferred write target in Databricks:
  /Volumes/edw_sales_dev/files/landing/raw/dynamics365/{entity}/

Local simulation writes under ./landing/ which you copy into the volume.

Auth: Access Connector / workspace MI via External Location (no storage secrets in code).

Published Dynamics schemas referenced (common Dataverse / Dynamics 365):
- Opportunity (logical name: opportunity)
- Account (account)
- Contact (contact)

Run:
  python dynamics_api_simulator.py --output ./landing  (local)
  or in Databricks write under /Volumes/.../files/landing/raw/dynamics365/

Output folders (map to volume subpaths raw/dynamics365/...):
  sales/opportunities/  → raw/dynamics365/opportunities/
  sales/accounts/
  sales/contacts/
"""

import argparse
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(123)

def simulate_dynamics_opportunity(num_records=1200):
    """Simulates Dynamics Opportunity entity (published schema fields)."""
    stages = ["New", "Qualified", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
    rows = []
    for i in range(1, num_records + 1):
        created = fake.date_time_between(start_date="-3y", end_date="-5d")
        close = created + timedelta(days=random.randint(15, 240))
        rows.append({
            "opportunityid": f"OPP-{300000 + i}",           # Dynamics GUID-like in real
            "name": fake.bs().title() + " Project",
            "customerid": f"ACC-{200000 + random.randint(1, 800)}",  # lookup to Account
            "estimatedvalue": round(random.uniform(8000, 380000), 2),
            "actualvalue": round(random.uniform(5000, 350000), 2) if random.random() > 0.4 else None,
            "statecode": random.choice([0,1,2]),  # 0=Open,1=Won,2=Lost (Dynamics codes)
            "statuscode": random.choice([1,2,3,4,5,6]),
            "stage": random.choice(stages),
            "ownerid": f"USR-{random.randint(1,45)}",
            "createdon": created.isoformat(),
            "modifiedon": (created + timedelta(days=random.randint(1, 30))).isoformat(),
            "close_date": close.date().isoformat(),
            "probability": random.choice([10,25,50,75,90,100]),
            "msdyn_forecastcategory": random.choice(["Pipeline", "BestCase", "Committed", "Closed"]),
        })
    return pd.DataFrame(rows)

def simulate_dynamics_account(num_records=450):
    """Simulates Dynamics Account entity."""
    industries = ["Technology", "Manufacturing", "Healthcare", "Finance", "Retail", "Energy", "Consulting"]
    rows = []
    for i in range(1, num_records + 1):
        rows.append({
            "accountid": f"ACC-{200000 + i}",
            "name": fake.company(),
            "industrycode": random.choice(industries),
            "address1_city": fake.city(),
            "address1_country": random.choice(["United States", "United Kingdom", "Germany", "Canada", "Australia"]),
            "telephone1": fake.phone_number(),
            "websiteurl": fake.url(),
            "numberofemployees": random.randint(10, 25000),
            "revenue": round(random.uniform(1_000_000, 2_500_000_000), 2),
            "createdon": fake.date_time_between(start_date="-6y").isoformat(),
            "modifiedon": fake.date_time_between(start_date="-90d").isoformat(),
            "ownerid": f"USR-{random.randint(1,45)}",
        })
    return pd.DataFrame(rows)

def simulate_dynamics_contact(num_records=800):
    """Simulates Dynamics Contact entity (often linked to opportunities/accounts)."""
    rows = []
    for i in range(1, num_records + 1):
        rows.append({
            "contactid": f"CON-{500000 + i}",
            "fullname": fake.name(),
            "emailaddress1": fake.email(),
            "telephone1": fake.phone_number(),
            "jobtitle": fake.job(),
            "parentcustomerid": f"ACC-{200000 + random.randint(1,449)}",  # Account lookup
            "createdon": fake.date_time_between(start_date="-5y").isoformat(),
            "modifiedon": fake.date_time_between(start_date="-60d").isoformat(),
        })
    return pd.DataFrame(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default="./landing",
        help="Output base path (local ./landing or /Volumes/edw_sales_dev/files/landing/raw/dynamics365)",
    )
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv")
    args = parser.parse_args()

    base = Path(args.output)
    (base / "sales" / "opportunities").mkdir(parents=True, exist_ok=True)
    (base / "sales" / "accounts").mkdir(parents=True, exist_ok=True)
    (base / "sales" / "contacts").mkdir(parents=True, exist_ok=True)

    print("Simulating Dynamics 365 data using published schemas...")

    opps = simulate_dynamics_opportunity()
    accts = simulate_dynamics_account()
    conts = simulate_dynamics_contact()

    ts = datetime.now().strftime("%Y-%m-%d")

    if args.format == "csv":
        opps.to_csv(base / "sales" / "opportunities" / f"dynamics_opportunities_{ts}.csv", index=False)
        accts.to_csv(base / "sales" / "accounts" / f"dynamics_accounts_{ts}.csv", index=False)
        conts.to_csv(base / "sales" / "contacts" / f"dynamics_contacts_{ts}.csv", index=False)
    else:
        opps.to_parquet(base / "sales" / "opportunities" / f"dynamics_opportunities_{ts}.parquet", index=False)
        accts.to_parquet(base / "sales" / "accounts" / f"dynamics_accounts_{ts}.parquet", index=False)
        conts.to_parquet(base / "sales" / "contacts" / f"dynamics_contacts_{ts}.parquet", index=False)

    print(f"Generated {len(opps)} opportunities, {len(accts)} accounts, {len(conts)} contacts.")
    print(f"Files written under: {base}")
    print("Next: Point your Bronze Auto Loader (or the samples/pipelines/bronze equivalent) at these folders using the MI-backed External Location.")

if __name__ == "__main__":
    main()
