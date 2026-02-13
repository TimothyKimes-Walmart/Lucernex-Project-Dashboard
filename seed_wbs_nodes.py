"""Seed WBS node budget data from the BigQuery results we already have."""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).parent / "dashboard.db"

NODES = [
    {
        "node_key": "WMUS.SG.FAC.UP.PLB",
        "node_label": "Plumbing",
        "description": "Plumbing - Unplanned",
        "original_budget": 52_903_357.75,
        "supplemental_budget": 6_758_994.75,
        "returned_budget": -10_000_000.00,
        "current_budget": 49_662_352.50,
        "actuals": 38_616_359.30,
        "open_commitments": 760_651.37,
        "budget_available": 10_285_341.83,
        "distributed_budget": 98_468.29,
        "project_count": 10_350,
    },
    {
        "node_key": "WMUS.SG.FAC.UP.TANK",
        "node_label": "Tanks",
        "description": "Tanks",
        "original_budget": 0.00,
        "supplemental_budget": 8_000_000.00,
        "returned_budget": 0.00,
        "current_budget": 8_000_000.00,
        "actuals": 705_078.68,
        "open_commitments": 1_491_647.22,
        "budget_available": 5_803_274.10,
        "distributed_budget": 2_206_872.00,
        "project_count": 152,
    },
    {
        "node_key": "WMUS.SG.FAC.UP.LIFT",
        "node_label": "Lift Stations",
        "description": "Not found in SAP",
        "original_budget": 0,
        "supplemental_budget": 0,
        "returned_budget": 0,
        "current_budget": 0,
        "actuals": 0,
        "open_commitments": 0,
        "budget_available": 0,
        "distributed_budget": 0,
        "project_count": 0,
    },
]


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sap_wbs_nodes (
            node_key TEXT PRIMARY KEY,
            node_label TEXT,
            description TEXT,
            original_budget REAL DEFAULT 0,
            supplemental_budget REAL DEFAULT 0,
            returned_budget REAL DEFAULT 0,
            current_budget REAL DEFAULT 0,
            actuals REAL DEFAULT 0,
            open_commitments REAL DEFAULT 0,
            budget_available REAL DEFAULT 0,
            distributed_budget REAL DEFAULT 0,
            project_count INTEGER DEFAULT 0,
            last_updated TEXT
        )
    """)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    conn.execute("DELETE FROM sap_wbs_nodes")

    for n in NODES:
        conn.execute(
            """INSERT INTO sap_wbs_nodes
               (node_key, node_label, description, original_budget,
                supplemental_budget, returned_budget, current_budget,
                actuals, open_commitments, budget_available,
                distributed_budget, project_count, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                n["node_key"], n["node_label"], n["description"],
                n["original_budget"], n["supplemental_budget"],
                n["returned_budget"], n["current_budget"],
                n["actuals"], n["open_commitments"],
                n["budget_available"], n["distributed_budget"],
                n["project_count"], now,
            ),
        )

    conn.commit()
    conn.close()
    print(f"Seeded {len(NODES)} WBS nodes into {DB_PATH}")


if __name__ == "__main__":
    main()
