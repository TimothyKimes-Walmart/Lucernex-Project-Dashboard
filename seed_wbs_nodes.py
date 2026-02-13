"""Seed WBS node budget data (per fiscal year) from verified BigQuery results."""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).parent / "dashboard.db"

# Exact values from vw_rps_rb0224_us_report, verified 2026-02-13.
# Nulls from BQ are stored as 0.
NODES = [
    # ── PLB by year ──────────────────────────────────────────────
    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2022,
     "description": "Unplanned - Plumbing",
     "original_budget": 439_694.75, "supplemental_budget": 3_060_305.25,
     "returned_budget": 0, "current_budget": 3_500_000.00,
     "actuals": 3_082_157.43, "open_commitments": 0,
     "budget_available": 417_842.57, "distributed_budget": 278_147.82,
     "budget_cf_from_prev": 278_147.82, "budget_cf_to_next": 0,
     "project_count": 4},

    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2023,
     "description": "Unplanned - Plumbing",
     "original_budget": 278_147.82, "supplemental_budget": 4_000_000.00,
     "returned_budget": -2_000_000.00, "current_budget": 2_278_147.82,
     "actuals": 2_245_019.68, "open_commitments": 0,
     "budget_available": 33_128.14, "distributed_budget": 449_593.42,
     "budget_cf_from_prev": 449_593.42, "budget_cf_to_next": 0,
     "project_count": 4},

    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2024,
     "description": "Unplanned - Plumbing",
     "original_budget": 899_186.84, "supplemental_budget": 0,
     "returned_budget": 0, "current_budget": 899_186.84,
     "actuals": 9_548.67, "open_commitments": 0,
     "budget_available": 889_638.17, "distributed_budget": 440_350.02,
     "budget_cf_from_prev": 458_836.82, "budget_cf_to_next": 0,
     "project_count": 3},

    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2025,
     "description": "Plumbing - Unplanned",
     "original_budget": 440_350.02, "supplemental_budget": 0,
     "returned_budget": 0, "current_budget": 440_350.02,
     "actuals": 0, "open_commitments": 0,
     "budget_available": 440_350.02, "distributed_budget": 0,
     "budget_cf_from_prev": 440_350.02, "budget_cf_to_next": 0,
     "project_count": 3},

    {"node_key": "WMUS.SG.FAC.UP.PLB", "node_label": "Plumbing", "approval_year": 2026,
     "description": "Plumbing - Unplanned",
     "original_budget": 440_350.02, "supplemental_budget": 8_678_949.98,
     "returned_budget": -8_000_000.00,
     "current_budget": 9_420_610.50,  # SAP budget override (BQ current_budget=1,119,300)
     "actuals": 730_852.36, "open_commitments": 0,
     "budget_available": 8_689_758.14, "distributed_budget": 98_468.29,
     "budget_cf_from_prev": 440_350.02, "budget_cf_to_next": 0,
     "project_count": 7},

    # ── TANK by year (only 2026) ────────────────────────────────
    {"node_key": "WMUS.SG.FAC.UP.TANK", "node_label": "Tanks", "approval_year": 2026,
     "description": "Tanks",
     "original_budget": 0, "supplemental_budget": 8_000_000.00,
     "returned_budget": 0, "current_budget": 8_000_000.00,
     "actuals": 620_420.82, "open_commitments": 0,
     "budget_available": 7_379_579.18, "distributed_budget": 2_206_272.00,
     "budget_cf_from_prev": 0, "budget_cf_to_next": 0,
     "project_count": 76},

    # ── LIFT — not found in SAP, placeholders ───────────────────
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
