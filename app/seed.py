"""Seed the database with realistic sample data for the plumbing dashboard."""

import random
from datetime import datetime, timedelta

from database import get_db, init_db

PROJECT_TYPES = [
    "PLBG Equipment Replacement",
    "PLBG SANITARY/GREASE INFRASTRUCTURE",
    "PLBG GAS INFRASTRUCTURE",
    "PLBG WATER INFRASTRUCTURE",
]

STATUSES = ["Active", "Active", "Active", "Complete", "On Hold", "Cancelled"]

CITIES = [
    ("Bentonville", "AR"), ("Rogers", "AR"), ("Springdale", "AR"),
    ("Dallas", "TX"), ("Houston", "TX"), ("Austin", "TX"),
    ("Atlanta", "GA"), ("Savannah", "GA"),
    ("Orlando", "FL"), ("Tampa", "FL"), ("Miami", "FL"),
    ("Nashville", "TN"), ("Memphis", "TN"),
    ("Charlotte", "NC"), ("Raleigh", "NC"),
    ("Phoenix", "AZ"), ("Tucson", "AZ"),
    ("Denver", "CO"), ("Oklahoma City", "OK"), ("Kansas City", "MO"),
]

CONTRACTORS = [
    "Acme GC", "PlumbCo National", "FacilityPro Services",
    "BlueLine Mechanical", "Summit Contractors", "Patriot Plumbing Inc",
    "Core Facility Group", "Atlas Building Services",
]

VENDORS = [
    "PlumbCo Supplies", "Ferguson Enterprises", "HD Supply",
    "Grainger", "Winsupply", "National Pipe & Plastics",
    "Core & Main", "Watts Water Technologies",
]

SCOPES = {
    "PLBG Equipment Replacement": [
        "Replace water heater and associated piping.",
        "Install new grease trap and reroute drain lines.",
        "Replace backflow preventer assembly.",
        "Swap out failed sump pump and check valves.",
        "Replace PRV and expansion tank on main supply.",
    ],
    "PLBG SANITARY/GREASE INFRASTRUCTURE": [
        "Reline sanitary sewer main under sales floor.",
        "Install new grease interceptor per code update.",
        "Replace corroded cast-iron drain stack.",
        "Rehabilitate grease infrastructure in deli area.",
        "Video inspect and repair sanitary lateral.",
    ],
    "PLBG GAS INFRASTRUCTURE": [
        "Install new gas meter and regulator.",
        "Replace corroded gas piping in rooftop HVAC run.",
        "Add gas shutoff valve per fire marshal order.",
        "Re-pipe gas line from meter to kitchen equipment.",
        "Leak repair on underground gas service line.",
    ],
    "PLBG WATER INFRASTRUCTURE": [
        "Replace backflow preventer and connect new feed line.",
        "Install new domestic water booster pump.",
        "Re-pipe main water entry with copper.",
        "Replace underground water service line to meter.",
        "Add isolation valves to pharmacy water supply.",
    ],
}


def seed_data(num_projects: int = 30) -> None:
    """Generate and insert sample projects, budgets, and POs."""
    init_db()
    conn = get_db()

    # Clear existing data
    conn.execute("DELETE FROM sap_po")
    conn.execute("DELETE FROM sap_budget")
    conn.execute("DELETE FROM projects")

    base_date = datetime(2026, 2, 10)

    for i in range(1, num_projects + 1):
        project_id = f"LXN-{i:06d}"
        project_type = random.choice(PROJECT_TYPES)
        city, state = random.choice(CITIES)
        store = str(random.randint(1000, 9999))
        seq = f"{random.randint(1, 5):02d}"
        status = random.choice(STATUSES)
        sap_def = f"SAP-PROJ-{random.randint(1000, 9999)}"
        scope = random.choice(SCOPES[project_type])
        gc = random.choice(CONTRACTORS)
        updated = (base_date - timedelta(days=random.randint(0, 30))).isoformat()

        conn.execute(
            """INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (project_id, project_type, store, seq, f"{store}-{seq}",
             city, state, status, sap_def, scope, gc, updated),
        )

        # SAP Budget
        budget_total = round(random.uniform(50_000, 500_000), 2)
        budget_actuals = round(budget_total * random.uniform(0.05, 0.6), 2)
        budget_committed = round(budget_total * random.uniform(0.2, 0.5), 2)
        budget_open = round(budget_total - budget_actuals - budget_committed, 2)

        conn.execute(
            """INSERT OR IGNORE INTO sap_budget VALUES (?, ?, ?, ?, ?, ?)""",
            (sap_def, budget_total, budget_open, budget_committed,
             budget_actuals, updated),
        )

        # SAP POs (1-3 per project)
        for j in range(random.randint(1, 3)):
            po_num = f"45{random.randint(10000000, 99999999)}"
            vendor = random.choice(VENDORS)
            po_total = round(random.uniform(5_000, 150_000), 2)
            invoiced = round(po_total * random.uniform(0, 0.8), 2)
            remaining = round(po_total - invoiced, 2)
            po_status = random.choice(["Open", "Open", "Closed", "Partially Invoiced"])
            created = (base_date - timedelta(days=random.randint(10, 90))).strftime("%Y-%m-%d")

            conn.execute(
                """INSERT OR IGNORE INTO sap_po VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (po_num, sap_def, vendor, po_total, invoiced,
                 remaining, po_status, created, updated),
            )

    conn.commit()
    conn.close()
    print(f"Seeded {num_projects} projects with budgets and POs.")


if __name__ == "__main__":
    seed_data()
