"""SQLite database setup and models for Lucernex Plumbing Dashboard."""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "dashboard.db"


def get_db() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create tables if they don't exist."""
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS projects (
            project_id TEXT PRIMARY KEY,
            project_type TEXT NOT NULL,
            store TEXT,
            sequence TEXT,
            store_sequence TEXT,
            city TEXT,
            state TEXT,
            project_status TEXT,
            sap_project_definition TEXT,
            brief_scope_of_work TEXT,
            general_contractor TEXT,
            store_type TEXT,
            banner TEXT,
            created_date TEXT,
            construction_complete_date TEXT,
            pmo_sr_pm_comments TEXT,
            cec_comments TEXT,
            lucernex_updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS sap_budget (
            sap_project_definition TEXT PRIMARY KEY,
            budget_total REAL,
            budget_open REAL,
            budget_committed REAL,
            budget_actuals REAL,
            sap_updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS sap_po (
            po_number TEXT PRIMARY KEY,
            sap_project_definition TEXT,
            vendor TEXT,
            vendor_email TEXT,
            po_total REAL,
            invoiced_to_date REAL,
            remaining_to_invoice REAL,
            po_status TEXT,
            created_date TEXT,
            last_update TEXT
        );

        CREATE TABLE IF NOT EXISTS lucernex_documents (
            doc_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            folder_id TEXT,
            folder_category TEXT,
            sub_folder TEXT,
            doc_name TEXT,
            doc_url TEXT,
            doc_type TEXT,
            doc_size TEXT,
            uploaded_by TEXT,
            uploaded_at TEXT,
            last_checked TEXT,
            is_deleted INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_lxdocs_project
            ON lucernex_documents(project_id);
        CREATE INDEX IF NOT EXISTS idx_lxdocs_folder
            ON lucernex_documents(folder_category, sub_folder);

        -- SAP WBS node-level budget data (program positions), per fiscal year.
        CREATE TABLE IF NOT EXISTS sap_wbs_nodes (
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
        );

        -- Tracks when each BQ source was last updated and when we last pulled.
        CREATE TABLE IF NOT EXISTS refresh_metadata (
            source_key TEXT PRIMARY KEY,
            source_label TEXT,
            source_last_updated TEXT,
            dashboard_refreshed_at TEXT
        );
    """)
    conn.commit()
    conn.close()


def get_refresh_metadata() -> list[dict]:
    """Return refresh metadata rows as a list of dicts."""
    conn = get_db()
    rows = conn.execute(
        "SELECT source_key, source_label, source_last_updated, dashboard_refreshed_at "
        "FROM refresh_metadata ORDER BY source_key"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
