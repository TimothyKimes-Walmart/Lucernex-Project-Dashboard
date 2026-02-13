"""Seed WBS node budget data (per fiscal year) from BigQuery results."""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).parent / "dashboard.db"

NODES = [
    # PLB by year
    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2022,
     "description": "Plumbing - Unplanned",
     "original_budget": 9_510_611, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 9_510_611, "actuals": 7_636_398, "open_commitments": 414_713,
     "budget_available": 1_459_500, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 2221},
    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2023,
     "description": "Plumbing - Unplanned",
     "original_budget": 10_770_611, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 10_770_611, "actuals": 10_999_964, "open_commitments": 64_594,
     "budget_available": -293_948, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 3169},
    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2024,
     "description": "Plumbing - Unplanned",
     "original_budget": 18_841_221, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 18_841_221, "actuals": 9_610_722, "open_commitments": 35_071,
     "budget_available": 9_195_428, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 2474},
    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2025,
     "description": "Plumbing - Unplanned",
     "original_budget": 9_420_611, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 9_420_611, "actuals": 9_621_087, "open_commitments": 0,
     "budget_available": -200_476, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 2472},
    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2026,
     "description": "Plumbing - Unplanned",
     "original_budget": 9_420_610.50, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 9_420_610.50, "actuals": 748_188, "open_commitments": 246_274,
     "budget_available": 8_426_148.50, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 14},
    # TANK by year (only 2026)
    {"node_key": "WMUS.SG.FAC.UP.TANK", "node_label": "Tanks", "approval_year": 2026,
     "description": "Tanks",
     "original_budget": 0, "supplemental_budget": 8_000_000, "returned_budget": 0,
     "current_budget": 8_000_000, "actuals": 705_079, "open_commitments": 1_491_647,
     "budget_available": 5_803_274, "distributed_budget": 2_206_872,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 152},
    # LIFT — placeholder for all PLB years
    {"node_key": "WMUS.SG.FAC.UP.LIFT", "node_label": "Lift Stations", "approval_year": 2022,
     "description": "Not found in SAP",
     "original_budget": 0, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 0, "actuals": 0, "open_commitments": 0,
     "budget_available": 0, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 0},
    {"node_key": "WMUS.SG.FAC.UP.LIFT", "node_label": "Lift Stations", "approval_year": 2023,
     "description": "Not found in SAP",
     "original_budget": 0, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 0, "actuals": 0, "open_commitments": 0,
     "budget_available": 0, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 0},
    {"node_key": "WMUS.SG.FAC.UP.LIFT", "node_label": "Lift Stations", "approval_year": 2024,
     "description": "Not found in SAP",
     "original_budget": 0, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 0, "actuals": 0, "open_commitments": 0,
     "budget_available": 0, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 0},
    {"node_key": "WMUS.SG.FAC.UP.LIFT", "node_label": "Lift Stations", "approval_year": 2025,
     "description": "Not found in SAP",
     "original_budget": 0, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 0, "actuals": 0, "open_commitments": 0,
     "budget_available": 0, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 0},
    {"node_key": "WMUS.SG.FAC.UP.LIFT", "node_label": "Lift Stations", "approval_year": 2026,
     "description": "Not found in SAP",
     "original_budget": 0, "supplemental_budget": 0, "returned_budget": 0,
     "current_budget": 0, "actuals": 0, "open_commitments": 0,
     "budget_available": 0, "distributed_budget": 0,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0, "project_count": 0},
]


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    # Drop and recreate to match new schema.
    conn.execute("DROP TABLE IF EXISTS sap_wbs_nodes")
    conn.execute("""
        CREATE TABLE sap_wbs_nodes (
            node_key TEXT NOT NULL,
            approval_year INTEGER NOT NULL,
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
            budget_cf_from_prev REAL DEFAULT 0,
            budget_cf_to_next REAL DEFAULT 0,
            project_count INTEGER DEFAULT 0,
            last_updated TEXT,
            PRIMARY KEY (node_key, approval_year)
        )
    """)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    for n in NODES:
        conn.execute(
            """INSERT INTO sap_wbs_nodes
               (node_key, approval_year, node_label, description,
                original_budget, supplemental_budget, returned_budget,
                current_budget, actuals, open_commitments,
                budget_available, distributed_budget,
                budget_cf_from_prev, budget_cf_to_next,
                project_count, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                n["node_key"], n["approval_year"], n["node_label"], n["description"],
                n["original_budget"], n["supplemental_budget"], n["returned_budget"],
                n["current_budget"], n["actuals"], n["open_commitments"],
                n["budget_available"], n["distributed_budget"],
                n["budget_cf_from_prev"], n["budget_cf_to_next"],
                n["project_count"], now,
            ),
        )

    conn.commit()
    conn.close()
    print(f"Seeded {len(NODES)} WBS node-year rows into {DB_PATH}")


if __name__ == "__main__":
    main()
