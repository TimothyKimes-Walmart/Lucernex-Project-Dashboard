"""ETL: Pull real plumbing project data from BigQuery into SQLite."""

import re
import sqlite3
from datetime import datetime, timezone

from google.cloud import bigquery
from database import get_db, init_db, DB_PATH

BQ_PROJECT = "re-ods-explorer"

PLBG_PROGRAM_TYPES = [
    "PLBG EQUIPMENT REPLACEMENT",
    "PLBG SANITARY/GREASE INFRASTRUCTURE",
    "PLBG GAS INFRASTRUCTURE",
    "PLBG WATER INFRASTRUCTURE",
]


def pull_projects(client: bigquery.Client) -> list[dict]:
    """Pull plumbing projects from qb_fmpm_project_cur."""
    query = """
        SELECT
            -- Use Lucernex ProjectEntityID as the canonical project ID;
            -- fall back to FMPM Record_ID_Nbr if no Lucernex match.
            COALESCE(CAST(lx.ProjectEntityID AS STRING), CAST(p.Record_ID_Nbr AS STRING)) AS project_id,
            p.Program_Type AS project_type,
            CAST(p.Store_Nbr AS STRING) AS store,
            -- Sequence: use Lucernex Sequence_Number (e.g. "1009")
            COALESCE(lx.Sequence_Number, CAST(p.Record_ID_Nbr AS STRING)) AS sequence,
            -- Store.Sequence: use Lucernex StoreSequenceNbr (e.g. "5624.1009")
            COALESCE(lx.StoreSequenceNbr, CONCAT(CAST(p.Store_Nbr AS STRING), '.', CAST(p.Record_ID_Nbr AS STRING))) AS store_sequence,
            p.City AS city,
            p.State AS state,
            p.Project_Status AS project_status,
            p.SAP_Project_Definition_Nbr AS sap_project_definition,
            COALESCE(lx.Brief_Scope_Of_Work, '') AS brief_scope_of_work,
            -- Contractor: set to NULL initially; backfilled from PO vendor
            -- in load_to_sqlite, with Lucernex GC Firm as final fallback.
            CASE
                WHEN p.Contractor IS NOT NULL AND p.Contractor != ''
                THEN p.Contractor
                ELSE NULL
            END AS general_contractor,
            -- Keep Lucernex GC Firm as a fallback (may have concatenation artifacts).
            CASE
                WHEN lx.GeneralContractor_Firm IS NOT NULL
                     AND lx.GeneralContractor_Firm NOT IN ('', '!Unknown')
                THEN lx.GeneralContractor_Firm
                ELSE NULL
            END AS lx_gc_firm,
            CAST(p.Date_Modified AS STRING) AS lucernex_updated_at,
            -- Budget fields
            CAST(p.SAP_Actuals AS FLOAT64) AS sap_actuals,
            CAST(p.SAP_Open_Commitments AS FLOAT64) AS sap_open_commitments,
            CAST(p.Total_Contract_Amount AS FLOAT64) AS total_contract_amount,
            -- Contractor PO details
            CAST(p.Contractor_SAP_PO_Nbr AS STRING) AS contractor_po_number,
            CAST(p.Contractor_SAP_PO_Amount AS FLOAT64) AS contractor_po_amount,
            p.Contractor_Resource_Assigned AS contractor_resource,
            -- Dates
            CAST(p.Date_Created AS STRING) AS created_date,
            CAST(p.Start_Date_Projected AS STRING) AS start_date_projected,
            CAST(p.Start_Date_Actual AS STRING) AS start_date_actual,
            CAST(p.Completion_Date_Projected AS STRING) AS completion_date_projected,
            CAST(p.Completion_Date_Actual AS STRING) AS construction_complete_date,
            -- FM info
            p.FM_Sub_Region AS fm_sub_region,
            p.Regional_Manager AS regional_manager,
            p.Market_Manager AS market_manager,
            p.Store_Type AS store_type,
            p.Program_Group AS program_group,
            -- Comments from Lucernex
            lx.PMO_SrPM_Comments AS pmo_sr_pm_comments,
            lx.CEC_Comments AS cec_comments
        FROM `re-ods-prod.us_re_ods_prod_pub.qb_fmpm_project_cur` p
        LEFT JOIN `re-ods-prod.us_re_ods_prod_pub.lx_all_projects_curr` lx
            ON p.SAP_Project_Definition_Nbr = lx.SAPProjectDefinition
        WHERE UPPER(p.Program_Type) LIKE '%PLBG%'
          AND p.Is_Active = TRUE
        ORDER BY p.Date_Modified DESC
    """
    print("Pulling projects from BigQuery...")
    results = client.query(query).result()
    rows = [dict(row) for row in results]
    print(f"  Found {len(rows)} plumbing projects")
    return rows


def pull_purchase_orders(client: bigquery.Client) -> list[dict]:
    """Pull PO data from vw_rps_purchase_order matched to PLBG SAP definitions.

    Format conversion: our SAP def 'USFC-009320' → PO table 'USFC00932000000'
    (strip dash, append '00000').
    """
    query = """
        WITH plbg_sap AS (
            SELECT DISTINCT
                SAP_Project_Definition_Nbr,
                CONCAT(
                    REPLACE(SAP_Project_Definition_Nbr, '-', ''),
                    '00000'
                ) AS po_project_def
            FROM `re-ods-prod.us_re_ods_prod_pub.qb_fmpm_project_cur`
            WHERE UPPER(Program_Type) LIKE '%PLBG%'
              AND SAP_Project_Definition_Nbr IS NOT NULL
              AND SAP_Project_Definition_Nbr != ''
        )
        SELECT
            po.po_nbr AS po_number,
            p.SAP_Project_Definition_Nbr AS sap_project_definition,
            po.vendor_name AS vendor,
            -- PO amount and invoice amount live on SEPARATE rows.
            -- net_po_lc_amt holds the PO line value, invoiced_lc_amt
            -- holds the invoice receipt value. They don't overlap on
            -- the same row, so we SUM each independently.
            SUM(CAST(po.net_po_lc_amt AS FLOAT64)) AS po_total,
            SUM(CAST(po.invoiced_lc_amt AS FLOAT64)) AS invoiced_to_date,
            SUM(CAST(po.net_po_lc_amt AS FLOAT64)) -
                SUM(CAST(po.invoiced_lc_amt AS FLOAT64)) AS remaining_to_invoice,
            MAX(po.pur_doc_sts) AS po_status,
            CAST(MIN(po.document_date) AS STRING) AS created_date,
            CAST(MAX(po.ods_updated_datetime) AS STRING) AS last_update
        FROM `re-ods-prod.us_re_ods_prod_pub.vw_rps_purchase_order` po
        INNER JOIN plbg_sap p ON po.project_definition = p.po_project_def
        GROUP BY po.po_nbr, p.SAP_Project_Definition_Nbr, po.vendor_name
        HAVING SUM(CAST(po.net_po_lc_amt AS FLOAT64)) > 0
            OR SUM(CAST(po.invoiced_lc_amt AS FLOAT64)) > 0
        ORDER BY MIN(po.document_date) DESC
    """
    print("Pulling POs from vw_rps_purchase_order...")
    results = client.query(query).result()
    rows = [dict(row) for row in results]
    print(f"  Found {len(rows)} PO records")
    return rows


# Known plumbing vendors used on Sam's Club UCO / ACC tank projects.
_SAMS_PLUMBING_VENDORS = [
    "APTIM Environmental",
    "United Installers",
    "Reynalds Brothers",
    "Kleenco Maintenance",
    "Stokes Plumbing",
]

# Regex to extract the real store number from item_text like "4724UCOTanks".
_ITEM_TEXT_STORE_RE = re.compile(r"^(\d+)UCOTank", re.IGNORECASE)


def pull_sams_umbrella_pos(
    client: bigquery.Client,
    store_to_sap: dict[str, str],
) -> list[dict]:
    """Pull Sam's Club UCO/ACC POs from the USMS-001700 umbrella project.

    These POs are booked under a parent SAP node (USMS-001700) rather
    than individual USFC-* child definitions.  We cross-reference them
    back to the correct USFC project via store number.

    Some vendors (Reynalds Brothers) book ALL POs to a hub store (6439)
    and encode the real store in ``item_text`` (e.g. "4724UCOTanks").
    We parse that pattern first, falling back to ``store_nbr``.

    Args:
        client: BigQuery client.
        store_to_sap: Mapping of store number (str) to the
            sap_project_definition the PO should be attributed to.

    Returns:
        List of PO dicts in the same shape as ``pull_purchase_orders``.
    """
    vendor_clauses = " OR ".join(
        f"UPPER(po.vendor_name) LIKE '%{v.upper()}%'"
        for v in _SAMS_PLUMBING_VENDORS
    )

    query = f"""
        SELECT
            po.store_nbr,
            po.po_nbr         AS po_number,
            po.vendor_name    AS vendor,
            po.item_text,
            SUM(CAST(po.net_po_lc_amt AS FLOAT64))  AS po_total,
            SUM(CAST(po.invoiced_lc_amt AS FLOAT64)) AS invoiced_to_date,
            SUM(CAST(po.net_po_lc_amt AS FLOAT64))
              - SUM(CAST(po.invoiced_lc_amt AS FLOAT64)) AS remaining_to_invoice,
            MAX(po.pur_doc_sts) AS po_status,
            CAST(MIN(po.document_date) AS STRING) AS created_date,
            CAST(MAX(po.ods_updated_datetime) AS STRING) AS last_update
        FROM `re-ods-prod.us_re_ods_prod_pub.vw_rps_purchase_order` po
        WHERE po.project_definition = 'USMS00170000000'
          AND ({vendor_clauses})
        GROUP BY po.store_nbr, po.po_nbr, po.vendor_name, po.item_text
        HAVING SUM(CAST(po.net_po_lc_amt AS FLOAT64)) > 0
            OR SUM(CAST(po.invoiced_lc_amt AS FLOAT64)) > 0
        ORDER BY po.store_nbr, po.po_nbr
    """
    print("Pulling Sam's Club USMS-001700 umbrella POs...")
    results = client.query(query).result()
    raw_rows = [dict(row) for row in results]
    print(f"  Found {len(raw_rows)} raw USMS PO records")

    # Map each PO to the correct USFC sap_project_definition.
    mapped: list[dict] = []
    skipped = 0
    for row in raw_rows:
        # Determine the real target store.
        real_store = None
        m = _ITEM_TEXT_STORE_RE.match(row.get("item_text") or "")
        if m:
            real_store = m.group(1)
        if not real_store:
            real_store = str(row["store_nbr"])

        sap_def = store_to_sap.get(real_store)
        if not sap_def:
            skipped += 1
            continue

        mapped.append({
            "po_number": row["po_number"],
            "sap_project_definition": sap_def,
            "vendor": row["vendor"],
            "po_total": row["po_total"] or 0,
            "invoiced_to_date": row["invoiced_to_date"] or 0,
            "remaining_to_invoice": row["remaining_to_invoice"] or 0,
            "po_status": row["po_status"],
            "created_date": row["created_date"],
            "last_update": row["last_update"],
        })

    print(f"  Mapped {len(mapped)} POs to projects ({skipped} skipped - no project match)")
    return mapped


def _build_store_to_sap_map(projects: list[dict]) -> dict[str, str]:
    """Build a store -> sap_project_definition lookup from Sam's projects.

    When multiple projects exist for the same store, prefer the active
    non-duplicate/non-cancelled one.
    """
    store_map: dict[str, str] = {}
    for p in projects:
        store = str(p.get("store") or "")
        sap_def = p.get("sap_project_definition")
        if not store or not sap_def:
            continue

        scope = (p.get("brief_scope_of_work") or "").lower()
        status = (p.get("project_status") or "").lower()
        is_dupe = "duplicate" in scope or "cancelled" in scope

        # Only overwrite if this project is better (non-duplicate, active).
        if store not in store_map or (not is_dupe and status == "active"):
            store_map[store] = sap_def

    return store_map


# Map raw Store_Type codes to user-friendly banner labels.
_BANNER_MAP = {
    "SUP": "Walmart",
    "WNM": "Walmart",
    "W/M": "Walmart",
    "FASHION": "Walmart",
    "SAM": "Sam's Club",
    "FC": "DC",
    "GROCERY DC": "DC",
    "GDC": "DC",
}


def _resolve_banner(store_type: str | None) -> str:
    """Convert a BQ Store_Type code to a banner label."""
    if not store_type:
        return "Unknown"
    return _BANNER_MAP.get(store_type.upper().strip(), "Walmart")


def load_to_sqlite(projects: list[dict], pos: list[dict]) -> None:
    """Load BigQuery data into SQLite."""
    init_db()
    conn = get_db()

    # Clear existing data
    conn.execute("DELETE FROM sap_po")
    conn.execute("DELETE FROM sap_budget")
    conn.execute("DELETE FROM projects")

    # Insert projects
    for p in projects:
        banner = _resolve_banner(p.get("store_type"))
        conn.execute(
            """INSERT OR IGNORE INTO projects
               (project_id, project_type, store, sequence, store_sequence,
                city, state, project_status, sap_project_definition,
                brief_scope_of_work, general_contractor,
                store_type, banner, created_date,
                construction_complete_date,
                pmo_sr_pm_comments, cec_comments,
                lucernex_updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                p["project_id"], p["project_type"], p["store"],
                p["sequence"], p["store_sequence"],
                p["city"], p["state"], p["project_status"],
                p["sap_project_definition"], p["brief_scope_of_work"],
                p["general_contractor"],
                p.get("store_type"), banner,
                p.get("created_date"),
                p.get("construction_complete_date"),
                p.get("pmo_sr_pm_comments"),
                p.get("cec_comments"),
                p["lucernex_updated_at"],
            ),
        )

        # Build SAP budget from project-level fields
        if p["sap_project_definition"]:
            budget_total = (p["sap_actuals"] or 0) + (p["sap_open_commitments"] or 0)
            conn.execute(
                """INSERT OR REPLACE INTO sap_budget
                   (sap_project_definition, budget_total, budget_open,
                    budget_committed, budget_actuals, sap_updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    p["sap_project_definition"],
                    budget_total if budget_total > 0 else (p["total_contract_amount"] or 0),
                    p["sap_open_commitments"] or 0,
                    p["contractor_po_amount"] or 0,
                    p["sap_actuals"] or 0,
                    p["lucernex_updated_at"],
                ),
            )

    print(f"  Loaded {len(projects)} projects")

    # Insert POs
    po_count = 0
    for po in pos:
        conn.execute(
            """INSERT OR IGNORE INTO sap_po
               (po_number, sap_project_definition, vendor, po_total,
                invoiced_to_date, remaining_to_invoice, po_status,
                created_date, last_update)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                po["po_number"], po["sap_project_definition"],
                po["vendor"], po["po_total"] or 0,
                po["invoiced_to_date"] or 0, po["remaining_to_invoice"] or 0,
                po["po_status"], po["created_date"], po["last_update"],
            ),
        )
        po_count += 1

    print(f"  Loaded {po_count} purchase orders")

    # Backfill contractor from primary PO vendor (highest PO value)
    # for ALL projects that have PO data — PO vendor is the cleanest source.
    po_backfilled = conn.execute("""
        UPDATE projects
        SET general_contractor = (
            SELECT po.vendor
            FROM sap_po po
            WHERE po.sap_project_definition = projects.sap_project_definition
            GROUP BY po.vendor
            ORDER BY SUM(po.po_total) DESC
            LIMIT 1
        )
        WHERE sap_project_definition IN (
            SELECT DISTINCT sap_project_definition FROM sap_po
        )
    """).rowcount
    print(f"  Set contractor from PO vendor for {po_backfilled} projects")

    conn.commit()
    conn.close()


# ── Comment-referenced PO recovery ──────────────────────────────────
# PO numbers written in PMO Sr PM Comments (e.g. "APTIM PO# 40836460")
# reveal POs that may not be matched by SAP def or store-number joins.
# We parse them out, check which are missing, and pull from BQ directly.

_COMMENT_PO_RE = re.compile(
    r"(?:APTIM|INSTALLER)\s+PO#?\s*(\d{8})", re.IGNORECASE
)


def _parse_comment_po_map(projects: list[dict]) -> dict[str, str]:
    """Parse PO numbers from PMO Sr PM Comments.

    Returns:
        dict mapping po_number -> sap_project_definition.
    """
    po_to_sap: dict[str, str] = {}
    for p in projects:
        comments = p.get("pmo_sr_pm_comments") or ""
        sap_def = p.get("sap_project_definition")
        if not comments.strip() or not sap_def:
            continue
        for po_num in _COMMENT_PO_RE.findall(comments):
            po_to_sap[po_num] = sap_def
    return po_to_sap


def pull_comment_referenced_pos(
    client: bigquery.Client,
    po_to_sap: dict[str, str],
    existing_po_numbers: set[str],
) -> list[dict]:
    """Fetch POs from BQ that were referenced in PMO comments but missing.

    Args:
        client: BigQuery client.
        po_to_sap: Mapping from po_number -> sap_project_definition
            (parsed from PMO Sr PM Comments).
        existing_po_numbers: PO numbers already collected.

    Returns:
        List of PO dicts ready for insertion.
    """
    missing = {po for po in po_to_sap if po not in existing_po_numbers}
    if not missing:
        print("  No comment-referenced POs to recover.")
        return []

    print(f"Pulling {len(missing)} comment-referenced POs from BQ...")
    # Build a BQ IN clause — safe because values are all \d{8}
    in_clause = ", ".join(f"'{po}'" for po in sorted(missing))
    query = f"""
        SELECT
            po.po_nbr AS po_number,
            po.vendor_name AS vendor,
            SUM(CAST(po.net_po_lc_amt AS FLOAT64))  AS po_total,
            SUM(CAST(po.invoiced_lc_amt AS FLOAT64)) AS invoiced_to_date,
            SUM(CAST(po.net_po_lc_amt AS FLOAT64))
              - SUM(CAST(po.invoiced_lc_amt AS FLOAT64)) AS remaining_to_invoice,
            MAX(po.pur_doc_sts) AS po_status,
            CAST(MIN(po.document_date) AS STRING) AS created_date,
            CAST(MAX(po.ods_updated_datetime) AS STRING) AS last_update
        FROM `re-ods-prod.us_re_ods_prod_pub.vw_rps_purchase_order` po
        WHERE po.po_nbr IN ({in_clause})
        GROUP BY po.po_nbr, po.vendor_name
        HAVING SUM(CAST(po.net_po_lc_amt AS FLOAT64)) > 0
            OR SUM(CAST(po.invoiced_lc_amt AS FLOAT64)) > 0
    """
    results = client.query(query).result()
    recovered: list[dict] = []
    for row in results:
        po_num = row["po_number"]
        recovered.append({
            "po_number": po_num,
            "sap_project_definition": po_to_sap[po_num],
            "vendor": row["vendor"],
            "po_total": row["po_total"] or 0,
            "invoiced_to_date": row["invoiced_to_date"] or 0,
            "remaining_to_invoice": row["remaining_to_invoice"] or 0,
            "po_status": row["po_status"],
            "created_date": row["created_date"],
            "last_update": row["last_update"],
        })
    print(f"  Recovered {len(recovered)} POs from BQ (of {len(missing)} referenced)")
    return recovered


# ── WBS node-level budget pull ──────────────────────────────────────

# SAP WBS nodes we track for plumbing fund overview.
WBS_NODES = {
    "WMUS.SG.FAC.UP.PLB": "Plumbing",
    "WMUS.SG.FAC.UP.TANK": "Tanks",
    "WMUS.SG.FAC.UP.LIFT": "Lift Stations",
}


def pull_wbs_node_budgets(client: bigquery.Client) -> list[dict]:
    """Pull budget data for tracked WBS program positions, grouped by year."""
    node_keys = ", ".join(f"'{k}'" for k in WBS_NODES)
    query = f"""
        SELECT
            program_position,
            approval_year,
            MAX(program_position_desc) AS description,
            COUNT(DISTINCT project_definition) AS project_count,
            SUM(SAFE_CAST(original_budget AS FLOAT64)) AS original_budget,
            SUM(SAFE_CAST(supplemental_budget AS FLOAT64)) AS supplemental_budget,
            SUM(SAFE_CAST(returned_budget AS FLOAT64)) AS returned_budget,
            SUM(SAFE_CAST(current_budget AS FLOAT64)) AS current_budget,
            SUM(SAFE_CAST(total_actual AS FLOAT64)) AS actuals,
            SUM(SAFE_CAST(total_commitments AS FLOAT64)) AS open_commitments,
            SUM(SAFE_CAST(current_budget_available AS FLOAT64)) AS budget_available,
            SUM(SAFE_CAST(distributed_budget AS FLOAT64)) AS distributed_budget,
            SUM(SAFE_CAST(budget_cf_from_previous_fiscal_year AS FLOAT64)) AS budget_cf_from_prev,
            SUM(SAFE_CAST(budget_cf_to_next_fiscal_year AS FLOAT64)) AS budget_cf_to_next
        FROM `re-ods-prod.us_re_ods_prod_pub.vw_rps_rb0224_us_report`
        WHERE UPPER(program_position) IN ({node_keys})
          AND approval_year IS NOT NULL
        GROUP BY program_position, approval_year
        ORDER BY program_position, approval_year
    """
    print("Pulling WBS node budgets by year from RB0224 report...")
    results = client.query(query).result()
    rows = [dict(row) for row in results]
    years = sorted({r["approval_year"] for r in rows})
    nodes = sorted({r["program_position"] for r in rows})
    print(f"  Found {len(rows)} rows: {len(nodes)} nodes x {len(years)} years ({years})")
    return rows


def load_wbs_nodes(nodes: list[dict]) -> None:
    """Upsert WBS node budget data (per year) into SQLite."""
    conn = get_db()
    conn.execute("DELETE FROM sap_wbs_nodes")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    for n in nodes:
        key = n["program_position"].upper()
        year = n.get("approval_year") or 0
        conn.execute(
            """INSERT OR REPLACE INTO sap_wbs_nodes
               (node_key, approval_year, node_label, description,
                original_budget, supplemental_budget, returned_budget,
                current_budget, actuals, open_commitments,
                budget_available, distributed_budget,
                budget_cf_from_prev, budget_cf_to_next,
                project_count, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                key, year, WBS_NODES.get(key, key), n.get("description", ""),
                n.get("original_budget", 0) or 0,
                n.get("supplemental_budget", 0) or 0,
                n.get("returned_budget", 0) or 0,
                n.get("current_budget", 0) or 0,
                n.get("actuals", 0) or 0,
                n.get("open_commitments", 0) or 0,
                n.get("budget_available", 0) or 0,
                n.get("distributed_budget", 0) or 0,
                n.get("budget_cf_from_prev", 0) or 0,
                n.get("budget_cf_to_next", 0) or 0,
                n.get("project_count", 0) or 0,
                now,
            ),
        )

    # Insert placeholder rows for nodes not found in BQ (like LIFT).
    found_keys = {n["program_position"].upper() for n in nodes}
    all_years = sorted({n.get("approval_year", 0) for n in nodes}) or [0]
    for key, label in WBS_NODES.items():
        if key not in found_keys:
            for year in all_years:
                conn.execute(
                    """INSERT OR IGNORE INTO sap_wbs_nodes
                       (node_key, approval_year, node_label, description, last_updated)
                       VALUES (?, ?, ?, ?, ?)""",
                    (key, year, label, "Not found in SAP", now),
                )

    conn.commit()
    conn.close()
    print(f"  Loaded {len(nodes)} WBS node-year rows")


# BQ source tables and their freshness queries.
_SOURCE_FRESHNESS = {
    "lucernex_projects": {
        "label": "Lucernex Projects",
        "query": "SELECT MAX(ods_updated_datetime) FROM `re-ods-prod.us_re_ods_prod_pub.lx_all_projects_curr`",
    },
    "fmpm_projects": {
        "label": "FMPM Projects",
        "query": "SELECT MAX(Date_Modified) FROM `re-ods-prod.us_re_ods_prod_pub.qb_fmpm_project_cur`",
    },
    "sap_purchase_orders": {
        "label": "SAP Purchase Orders",
        "query": "SELECT MAX(ods_updated_datetime) FROM `re-ods-prod.us_re_ods_prod_pub.vw_rps_purchase_order`",
    },
}


def _record_refresh_metadata(client: bigquery.Client) -> None:
    """Query BQ for each source's last-updated timestamp and persist."""
    conn = get_db()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    for key, info in _SOURCE_FRESHNESS.items():
        try:
            row = next(iter(client.query(info["query"]).result()))
            ts = row[0]
            # Normalize to string
            if hasattr(ts, "strftime"):
                source_ts = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
            else:
                source_ts = str(ts) if ts else "Unknown"
        except Exception:
            source_ts = "Unknown"

        conn.execute(
            """INSERT OR REPLACE INTO refresh_metadata
               (source_key, source_label, source_last_updated, dashboard_refreshed_at)
               VALUES (?, ?, ?, ?)""",
            (key, info["label"], source_ts, now),
        )

    conn.commit()
    conn.close()
    print(f"  Recorded refresh metadata at {now}")


def run_etl() -> None:
    """Execute the full ETL pipeline."""
    print(f"Starting ETL -> {DB_PATH}")
    client = bigquery.Client(project=BQ_PROJECT)

    projects = pull_projects(client)
    pos = pull_purchase_orders(client)

    # Pull Sam's Club POs from the USMS-001700 umbrella project.
    store_to_sap = _build_store_to_sap_map(projects)
    sams_pos = pull_sams_umbrella_pos(client, store_to_sap)

    # Merge Sam's umbrella POs — avoid duplicate PO numbers.
    existing_po_numbers = {po["po_number"] for po in pos}
    new_count = 0
    for spo in sams_pos:
        if spo["po_number"] not in existing_po_numbers:
            pos.append(spo)
            existing_po_numbers.add(spo["po_number"])
            new_count += 1
    print(f"  Merged {new_count} new Sam's umbrella POs (total POs: {len(pos)})")

    # Mine PMO Sr PM Comments for PO numbers we haven't matched yet.
    comment_po_map = _parse_comment_po_map(projects)
    comment_pos = pull_comment_referenced_pos(
        client, comment_po_map, existing_po_numbers
    )
    comment_new = 0
    for cpo in comment_pos:
        if cpo["po_number"] not in existing_po_numbers:
            pos.append(cpo)
            existing_po_numbers.add(cpo["po_number"])
            comment_new += 1
    print(f"  Merged {comment_new} comment-referenced POs (total POs: {len(pos)})")

    load_to_sqlite(projects, pos)

    # Pull WBS node-level budget data.
    wbs_nodes = pull_wbs_node_budgets(client)
    load_wbs_nodes(wbs_nodes)

    # Record source freshness + local refresh timestamp.
    _record_refresh_metadata(client)

    print("\nETL complete! Dashboard data refreshed.")


if __name__ == "__main__":
    run_etl()
