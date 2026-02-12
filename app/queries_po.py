"""PO-specific query functions for the POs tab."""

from __future__ import annotations

import logging
from database import get_db

logger = logging.getLogger(__name__)

# Allowlisted sort columns to prevent SQL injection.
_PO_SORT_COLUMNS = {
    "po_number": "po.po_number",
    "vendor": "po.vendor",
    "store_sequence": "p.store_sequence",
    "city": "p.city",
    "state": "p.state",
    "project_status": "p.project_status",
    "po_status": "po.po_status",
    "po_total": "po.po_total",
    "invoiced_to_date": "po.invoiced_to_date",
    "remaining_to_invoice": "po.remaining_to_invoice",
    "give_back_amount": "give_back_amount",
    "days_since_last_invoice": "days_since_last_invoice",
    "created_date": "po.created_date",
    "last_update": "po.last_update",
    "sap_project_definition": "po.sap_project_definition",
}

# Search fields for the PO tab — mirrors the Projects tab plus PO-specific.
PO_SEARCH_FIELDS: dict[str, dict] = {
    "po_number":      {"label": "PO Number",   "col": "po.po_number"},
    "vendor":         {"label": "Vendor",      "col": "po.vendor"},
    "store":          {"label": "Store",       "col": "p.store"},
    "store_sequence": {"label": "Store+Seq",   "col": "p.store_sequence"},
    "city":           {"label": "City",        "col": "p.city"},
    "state":          {"label": "State",       "col": "p.state"},
    "sap_def":        {"label": "SAP Def",     "col": "po.sap_project_definition"},
    "status":         {"label": "Proj Status", "col": "p.project_status"},
    "project_id":     {"label": "Entity ID",   "col": "p.project_id"},
    "project_type":   {"label": "Proj Type",   "col": "p.project_type"},
    "scope":          {"label": "Scope",       "col": "p.brief_scope_of_work"},
    "contractor":     {"label": "Contractor",  "col": "p.general_contractor"},
    "banner":         {"label": "Banner",      "col": "p.banner"},
}

_ALL_PO_FIELD_KEYS = list(PO_SEARCH_FIELDS.keys())


def _build_po_search_clause(
    search: str | None,
    fields: list[str] | None = None,
) -> tuple[str, list]:
    """Build WHERE fragment for PO multi-term search."""
    if not search:
        return "", []
    terms = [t.strip() for t in search.split(";") if t.strip()]
    if not terms:
        return "", []
    active = fields if fields else _ALL_PO_FIELD_KEYS
    clauses: list[str] = []
    params: list = []
    for term in terms:
        like = f"%{term}%"
        or_parts: list[str] = []
        for key in active:
            meta = PO_SEARCH_FIELDS.get(key)
            if meta:
                or_parts.append(f"{meta['col']} LIKE ?")
                params.append(like)
        if or_parts:
            clauses.append(f"({' OR '.join(or_parts)})")
    if not clauses:
        return "", []
    return f" AND ({' AND '.join(clauses)})", params


# ── Core PO base query (shared across list + summary) ────────────────

_PO_BASE_QUERY = """
    SELECT
        po.po_number,
        po.vendor,
        po.vendor_email,
        p.store,
        p.sequence,
        p.store_sequence,
        p.city,
        p.state,
        p.project_id,
        p.project_status,
        po.po_status,
        COALESCE(po.po_total, 0) AS po_total,
        COALESCE(po.invoiced_to_date, 0) AS invoiced_to_date,
        COALESCE(po.remaining_to_invoice, 0) AS remaining_to_invoice,
        -- Over-invoiced flag
        CASE WHEN COALESCE(po.invoiced_to_date, 0) > COALESCE(po.po_total, 0)
             THEN 1 ELSE 0 END AS is_over_invoiced,
        -- Give-back logic: project complete + remaining > 0
        CASE WHEN LOWER(p.project_status) = 'complete'
                  AND COALESCE(po.remaining_to_invoice, 0) > 0
             THEN 1 ELSE 0 END AS give_back_flag,
        CASE WHEN LOWER(p.project_status) = 'complete'
                  AND COALESCE(po.remaining_to_invoice, 0) > 0
             THEN COALESCE(po.remaining_to_invoice, 0)
             ELSE 0 END AS give_back_amount,
        -- Aging: days since last update
        CAST(
            JULIANDAY('now') - JULIANDAY(
                COALESCE(po.last_update, po.created_date)
            ) AS INTEGER
        ) AS days_since_last_invoice,
        po.created_date,
        po.last_update,
        po.sap_project_definition,
        p.general_contractor,
        p.project_type,
        p.brief_scope_of_work
    FROM sap_po po
    LEFT JOIN projects p
        ON po.sap_project_definition = p.sap_project_definition
"""


def _build_po_filters(
    vendor: str | None = None,
    state: str | None = None,
    project_status: str | None = None,
    po_status: str | None = None,
    has_remaining: bool = False,
    give_back_only: bool = False,
    aging_30: bool = False,
    search: str | None = None,
    search_fields: list[str] | None = None,
    sap_def: str | None = None,
) -> tuple[str, list]:
    """Build WHERE clauses from filter params. Returns (sql, params)."""
    where = " WHERE 1=1"
    params: list = []

    if vendor:
        where += " AND po.vendor = ?"
        params.append(vendor)
    if state:
        where += " AND p.state = ?"
        params.append(state)
    if project_status:
        where += " AND p.project_status = ?"
        params.append(project_status)
    if po_status:
        where += " AND po.po_status = ?"
        params.append(po_status)
    if has_remaining:
        where += " AND COALESCE(po.remaining_to_invoice, 0) > 0"
    if give_back_only:
        where += (
            " AND LOWER(p.project_status) = 'complete'"
            " AND COALESCE(po.remaining_to_invoice, 0) > 0"
        )
    if aging_30:
        where += (
            " AND CAST(JULIANDAY('now') - JULIANDAY("
            "COALESCE(po.last_update, po.created_date)) AS INTEGER) >= 30"
        )
    if sap_def:
        where += " AND po.sap_project_definition = ?"
        params.append(sap_def)

    search_clause, search_params = _build_po_search_clause(
        search, fields=search_fields,
    )
    if search_clause:
        where += search_clause
        params.extend(search_params)

    return where, params


def get_all_pos(
    vendor: str | None = None,
    state: str | None = None,
    project_status: str | None = None,
    po_status: str | None = None,
    has_remaining: bool = False,
    give_back_only: bool = False,
    aging_30: bool = False,
    search: str | None = None,
    search_fields: list[str] | None = None,
    sap_def: str | None = None,
    sort: str | None = None,
    order: str | None = None,
    page: int = 1,
    page_size: int = 100,
) -> tuple[list[dict], int]:
    """Return paginated PO list + total count."""
    where, params = _build_po_filters(
        vendor=vendor, state=state, project_status=project_status,
        po_status=po_status, has_remaining=has_remaining,
        give_back_only=give_back_only, aging_30=aging_30,
        search=search, search_fields=search_fields, sap_def=sap_def,
    )

    conn = get_db()

    # Total count for pagination
    count_sql = f"SELECT COUNT(*) as cnt FROM sap_po po LEFT JOIN projects p ON po.sap_project_definition = p.sap_project_definition{where}"
    total = conn.execute(count_sql, params).fetchone()["cnt"]

    # Main query with sort + pagination
    sort_col = _PO_SORT_COLUMNS.get(sort, "po.po_number")
    sort_dir = "DESC" if order == "desc" else "ASC"
    offset = (page - 1) * page_size

    query = f"{_PO_BASE_QUERY}{where} ORDER BY {sort_col} {sort_dir} LIMIT ? OFFSET ?"
    rows = conn.execute(query, params + [page_size, offset]).fetchall()
    conn.close()
    return [dict(r) for r in rows], total


def get_po_summary_stats(
    vendor: str | None = None,
    state: str | None = None,
    project_status: str | None = None,
    po_status: str | None = None,
    has_remaining: bool = False,
    give_back_only: bool = False,
    aging_30: bool = False,
    search: str | None = None,
    search_fields: list[str] | None = None,
    sap_def: str | None = None,
) -> dict:
    """Summary panel stats scoped to current filters."""
    where, params = _build_po_filters(
        vendor=vendor, state=state, project_status=project_status,
        po_status=po_status, has_remaining=has_remaining,
        give_back_only=give_back_only, aging_30=aging_30,
        search=search, search_fields=search_fields, sap_def=sap_def,
    )
    conn = get_db()
    row = conn.execute(f"""
        SELECT
            COUNT(*) AS total_pos,
            COALESCE(SUM(po.po_total), 0) AS total_po_value,
            COALESCE(SUM(po.invoiced_to_date), 0) AS total_invoiced,
            COALESCE(SUM(po.remaining_to_invoice), 0) AS total_remaining,
            COALESCE(SUM(
                CASE WHEN LOWER(p.project_status) = 'complete'
                          AND COALESCE(po.remaining_to_invoice, 0) > 0
                     THEN po.remaining_to_invoice ELSE 0 END
            ), 0) AS total_give_back,
            COUNT(DISTINCT CASE
                WHEN LOWER(p.project_status) = 'complete'
                     AND COALESCE(po.remaining_to_invoice, 0) > 0
                THEN p.project_id END
            ) AS complete_with_open_pos
        FROM sap_po po
        LEFT JOIN projects p
            ON po.sap_project_definition = p.sap_project_definition
        {where}
    """, params).fetchone()
    conn.close()
    return dict(row)


def get_po_filter_options() -> dict:
    """Return distinct values for filter dropdowns."""
    conn = get_db()
    vendors = [
        r["vendor"] for r in conn.execute(
            "SELECT DISTINCT vendor FROM sap_po WHERE vendor IS NOT NULL AND vendor != '' ORDER BY vendor"
        ).fetchall()
    ]
    states = [
        r["state"] for r in conn.execute(
            "SELECT DISTINCT p.state FROM sap_po po "
            "LEFT JOIN projects p ON po.sap_project_definition = p.sap_project_definition "
            "WHERE p.state IS NOT NULL ORDER BY p.state"
        ).fetchall()
    ]
    project_statuses = [
        r["project_status"] for r in conn.execute(
            "SELECT DISTINCT p.project_status FROM sap_po po "
            "LEFT JOIN projects p ON po.sap_project_definition = p.sap_project_definition "
            "WHERE p.project_status IS NOT NULL ORDER BY p.project_status"
        ).fetchall()
    ]
    po_statuses = [
        r["po_status"] for r in conn.execute(
            "SELECT DISTINCT po_status FROM sap_po WHERE po_status IS NOT NULL ORDER BY po_status"
        ).fetchall()
    ]
    conn.close()
    return {
        "vendors": vendors,
        "states": states,
        "project_statuses": project_statuses,
        "po_statuses": po_statuses,
    }


def get_po_detail(po_number: str) -> dict | None:
    """Return a single PO with linked project info."""
    conn = get_db()
    row = conn.execute(f"{_PO_BASE_QUERY} WHERE po.po_number = ?", (po_number,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_pos_for_email_export(po_numbers: list[str]) -> list[dict]:
    """Return PO details grouped by vendor for email export."""
    if not po_numbers:
        return []
    placeholders = ",".join("?" * len(po_numbers))
    conn = get_db()
    rows = conn.execute(
        f"{_PO_BASE_QUERY} WHERE po.po_number IN ({placeholders}) ORDER BY po.vendor, po.po_number",
        po_numbers,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
