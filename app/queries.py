"""Database query functions for the plumbing dashboard."""

import logging

from database import get_db

logger = logging.getLogger(__name__)


def get_summary_stats(search: str | None = None) -> dict:
    """Return high-level KPI stats for the dashboard."""
    search_clause, search_params = _build_search_clause(search)
    # When searching, scope budget/PO stats to matched projects only.
    proj_filter = f" WHERE 1=1 {search_clause}" if search_clause else ""
    sap_filter = (
        f" WHERE sap_project_definition IN "
        f"(SELECT sap_project_definition FROM projects p WHERE 1=1 {search_clause})"
        if search_clause else ""
    )

    conn = get_db()
    stats = {}

    row = conn.execute(
        f"SELECT COUNT(*) as cnt FROM projects p{proj_filter}", search_params
    ).fetchone()
    stats["total_projects"] = row["cnt"]

    row = conn.execute(
        f"SELECT COUNT(*) as cnt FROM projects p WHERE project_status = 'Active'{search_clause}",
        search_params,
    ).fetchone()
    stats["active_projects"] = row["cnt"]

    row = conn.execute(
        f"SELECT COALESCE(SUM(budget_total), 0) as total FROM sap_budget{sap_filter}",
        search_params,
    ).fetchone()
    stats["total_budget"] = row["total"]

    row = conn.execute(
        f"SELECT COALESCE(SUM(budget_actuals), 0) as total FROM sap_budget{sap_filter}",
        search_params,
    ).fetchone()
    stats["total_actuals"] = row["total"]

    row = conn.execute(
        f"SELECT COALESCE(SUM(remaining_to_invoice), 0) as total FROM sap_po{sap_filter}",
        search_params,
    ).fetchone()
    stats["remaining_to_invoice"] = row["total"]

    row = conn.execute(
        f"SELECT COUNT(*) as cnt FROM sap_po{sap_filter}", search_params
    ).fetchone()
    stats["total_pos"] = row["cnt"]

    conn.close()
    return stats


def get_projects_by_type(search: str | None = None) -> list[dict]:
    """Return project counts grouped by type."""
    search_clause, search_params = _build_search_clause(search)
    conn = get_db()
    rows = conn.execute(
        f"SELECT project_type, COUNT(*) as cnt FROM projects p WHERE 1=1{search_clause} GROUP BY project_type ORDER BY cnt DESC",
        search_params,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_projects_by_status(search: str | None = None) -> list[dict]:
    """Return project counts grouped by status."""
    search_clause, search_params = _build_search_clause(search)
    conn = get_db()
    rows = conn.execute(
        f"SELECT project_status, COUNT(*) as cnt FROM projects p WHERE 1=1{search_clause} GROUP BY project_status ORDER BY cnt DESC",
        search_params,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_budget_by_type(search: str | None = None) -> list[dict]:
    """Return budget totals grouped by project type."""
    search_clause, search_params = _build_search_clause(search)
    conn = get_db()
    rows = conn.execute(f"""
        SELECT p.project_type,
               COALESCE(SUM(b.budget_total), 0) as budget_total,
               COALESCE(SUM(b.budget_actuals), 0) as budget_actuals,
               COALESCE(SUM(b.budget_committed), 0) as budget_committed,
               COALESCE(SUM(b.budget_open), 0) as budget_open
        FROM projects p
        LEFT JOIN sap_budget b ON p.sap_project_definition = b.sap_project_definition
        WHERE 1=1{search_clause}
        GROUP BY p.project_type
        ORDER BY budget_total DESC
    """, search_params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Reusable search-clause builder ───────────────────────────────────

# Mapping of search-field key -> (SQL column expression, is_subquery).
# Order here determines default "all" order; keep it stable.
SEARCH_FIELDS: dict[str, dict] = {
    "project_id":   {"label": "Entity ID",   "col": "{t}.project_id"},
    "store":        {"label": "Store",       "col": "{t}.store"},
    "city":         {"label": "Location",    "col": "{t}.city"},
    "contractor":   {"label": "Contractor",  "col": "{t}.general_contractor"},
    "scope":        {"label": "Scope",       "col": "{t}.brief_scope_of_work"},
    "sap_def":      {"label": "SAP Def",     "col": "{t}.sap_project_definition"},
    "banner":       {"label": "Banner",      "col": "{t}.banner"},
    "status":       {"label": "Status",      "col": "{t}.project_status"},
    "po":           {"label": "PO/Vendor",   "col": "__po_subquery__"},
}

_ALL_FIELD_KEYS = list(SEARCH_FIELDS.keys())


def parse_search_fields(raw: str | None) -> list[str]:
    """Parse a comma-separated list of search field keys.

    Returns only valid keys.  If empty/None, returns all keys.
    """
    if not raw:
        return _ALL_FIELD_KEYS
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    valid = [k for k in keys if k in SEARCH_FIELDS]
    return valid or _ALL_FIELD_KEYS


def _build_search_clause(
    search: str | None,
    table_alias: str = "p",
    fields: list[str] | None = None,
) -> tuple[str, list]:
    """Build a WHERE fragment for multi-term semicolon-delimited search.

    Args:
        search: Raw search string (semicolons separate AND-ed terms).
        table_alias: SQL alias for the projects table.
        fields: List of search-field keys to include (None = all).

    Returns (sql_fragment, params).  The fragment is empty when there is
    nothing to filter.  Multiple terms are combined with AND so every
    term must match *somewhere* in the row.
    """
    if not search:
        return "", []

    terms = [t.strip() for t in search.split(";") if t.strip()]
    if not terms:
        return "", []

    active = fields if fields else _ALL_FIELD_KEYS

    clauses: list[str] = []
    params: list = []
    for term in terms:
        like = f"%{term}%"
        or_parts: list[str] = []
        for key in active:
            meta = SEARCH_FIELDS[key]
            if meta["col"] == "__po_subquery__":
                or_parts.append(
                    f"{table_alias}.sap_project_definition IN ("
                    f"SELECT po.sap_project_definition FROM sap_po po "
                    f"WHERE po.po_number LIKE ? OR po.vendor LIKE ?)"
                )
                params.extend([like, like])
            else:
                col = meta["col"].replace("{t}", table_alias)
                or_parts.append(f"{col} LIKE ?")
                params.append(like)

        if or_parts:
            clauses.append(f"({' OR '.join(or_parts)})")

    if not clauses:
        return "", []
    return f" AND ({' AND '.join(clauses)})", params


# Allowlisted sort columns to prevent SQL injection.
_SORT_COLUMNS = {
    "project_id": "p.project_id",
    "project_type": "p.project_type",
    "store": "p.store",
    "banner": "p.banner",
    "location": "p.city",
    "project_status": "p.project_status",
    "scope": "p.brief_scope_of_work",
    "general_contractor": "p.general_contractor",
    "budget_total": "budget_total",
    "budget_actuals": "budget_actuals",
    "created_date": "p.created_date",
    "construction_complete_date": "p.construction_complete_date",
}


def get_all_projects(
    project_type: str | None = None,
    status: str | None = None,
    contractor: str | None = None,
    banner: str | None = None,
    search: str | None = None,
    search_fields: list[str] | None = None,
    sort: str | None = None,
    order: str | None = None,
) -> list[dict]:
    """Return all projects with budget data, optionally filtered and sorted.

    Budget/actuals are derived from PO data (the accurate source),
    falling back to sap_budget when no POs exist for a project.
    """
    conn = get_db()
    query = """
        SELECT p.*,
            -- Use PO totals as primary source; fall back to sap_budget.
            COALESCE(po_agg.total_po, b.budget_total, 0) AS budget_total,
            COALESCE(po_agg.total_invoiced, b.budget_actuals, 0) AS budget_actuals,
            COALESCE(po_agg.total_po, b.budget_committed, 0) AS budget_committed,
            COALESCE(
                po_agg.total_po - po_agg.total_invoiced,
                b.budget_open, 0
            ) AS budget_open
        FROM projects p
        LEFT JOIN sap_budget b
            ON p.sap_project_definition = b.sap_project_definition
        LEFT JOIN (
            SELECT sap_project_definition,
                   SUM(po_total) AS total_po,
                   SUM(invoiced_to_date) AS total_invoiced
            FROM sap_po
            GROUP BY sap_project_definition
        ) po_agg ON p.sap_project_definition = po_agg.sap_project_definition
        WHERE 1=1
    """
    params: list = []

    if project_type:
        query += " AND p.project_type = ?"
        params.append(project_type)
    if status:
        query += " AND p.project_status = ?"
        params.append(status)
    if contractor:
        query += " AND p.general_contractor = ?"
        params.append(contractor)
    if banner:
        query += " AND p.banner = ?"
        params.append(banner)
    search_clause, search_params = _build_search_clause(
        search, fields=search_fields,
    )
    if search_clause:
        query += search_clause
        params.extend(search_params)

    sort_col = _SORT_COLUMNS.get(sort, "p.project_id")
    sort_dir = "DESC" if order == "desc" else "ASC"
    query += f" ORDER BY {sort_col} {sort_dir}"

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_contractors() -> list[str]:
    """Return distinct contractor names for the filter dropdown."""
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT general_contractor FROM projects WHERE general_contractor IS NOT NULL AND general_contractor != '' ORDER BY general_contractor"
    ).fetchall()
    conn.close()
    return [r["general_contractor"] for r in rows]


def get_all_banners() -> list[str]:
    """Return distinct banner labels for the filter dropdown."""
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT banner FROM projects WHERE banner IS NOT NULL ORDER BY banner"
    ).fetchall()
    conn.close()
    return [r["banner"] for r in rows]


def get_project_detail(project_id: str) -> dict | None:
    """Return a single project with budget and POs."""
    conn = get_db()
    project = conn.execute(
        "SELECT * FROM projects WHERE project_id = ?", (project_id,)
    ).fetchone()
    if not project:
        conn.close()
        return None

    project = dict(project)
    budget = conn.execute(
        "SELECT * FROM sap_budget WHERE sap_project_definition = ?",
        (project["sap_project_definition"],),
    ).fetchone()
    project["budget"] = dict(budget) if budget else None

    pos = conn.execute(
        "SELECT * FROM sap_po WHERE sap_project_definition = ? ORDER BY created_date",
        (project["sap_project_definition"],),
    ).fetchall()
    project["purchase_orders"] = [dict(po) for po in pos]

    conn.close()
    return project


def get_top_contractors(search: str | None = None) -> list[dict]:
    """Return top general contractors by project count."""
    search_clause, search_params = _build_search_clause(search)
    conn = get_db()
    rows = conn.execute(f"""
        SELECT general_contractor, COUNT(*) as project_count,
               COALESCE(SUM(b.budget_total), 0) as total_budget
        FROM projects p
        LEFT JOIN sap_budget b ON p.sap_project_definition = b.sap_project_definition
        WHERE 1=1{search_clause}
        GROUP BY general_contractor
        ORDER BY project_count DESC
        LIMIT 10
    """, search_params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_po_status_summary(search: str | None = None) -> list[dict]:
    """Return PO counts grouped by status."""
    search_clause, search_params = _build_search_clause(search)
    conn = get_db()
    if search_clause:
        # Scope POs to matched projects.
        rows = conn.execute(f"""
            SELECT po_status, COUNT(*) as cnt, SUM(po_total) as total
            FROM sap_po
            WHERE sap_project_definition IN (
                SELECT sap_project_definition FROM projects p WHERE 1=1{search_clause}
            )
            GROUP BY po_status ORDER BY cnt DESC
        """, search_params).fetchall()
    else:
        rows = conn.execute(
            "SELECT po_status, COUNT(*) as cnt, SUM(po_total) as total FROM sap_po GROUP BY po_status ORDER BY cnt DESC"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── SAP WBS Node Budgets ─────────────────────────────────────────

def get_wbs_node_budgets() -> list[dict]:
    """Return all tracked WBS node budget records."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM sap_wbs_nodes ORDER BY node_key"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Lucernex Documents ───────────────────────────────────────────────

def get_project_documents_tree(project_id: str) -> list[dict]:
    """Return a pre-aggregated folder → sub-folder → docs tree.

    Output shape:
    [
      {
        "folder_category": "Drawings",
        "subfolders": [
          {
            "sub_folder": "Plumbing Plans",
            "docs": [{doc_id, doc_name, doc_url, ...}, ...]
          }
        ]
      }
    ]

    Only folders/sub-folders with ≥1 active document are included.
    """
    conn = get_db()
    rows = conn.execute(
        """SELECT doc_id, folder_category, sub_folder, doc_name,
                  doc_url, doc_type, doc_size, uploaded_by, uploaded_at
           FROM lucernex_documents
           WHERE project_id = ? AND is_deleted = 0
           ORDER BY folder_category, sub_folder, doc_name""",
        (project_id,),
    ).fetchall()
    conn.close()

    # Build nested tree from flat rows.
    from collections import OrderedDict

    categories: OrderedDict[str, OrderedDict[str, list[dict]]] = OrderedDict()
    for r in rows:
        cat = r["folder_category"] or "Uncategorised"
        sub = r["sub_folder"] or "General"
        categories.setdefault(cat, OrderedDict()).setdefault(sub, []).append({
            "doc_id": r["doc_id"],
            "doc_name": r["doc_name"],
            "doc_url": r["doc_url"],
            "doc_type": r["doc_type"],
            "doc_size": r["doc_size"],
            "uploaded_by": r["uploaded_by"],
            "uploaded_at": r["uploaded_at"],
        })

    tree: list[dict] = []
    for cat_name, subs in categories.items():
        subfolders = [
            {"sub_folder": sf_name, "docs": docs}
            for sf_name, docs in subs.items()
        ]
        tree.append({"folder_category": cat_name, "subfolders": subfolders})
    return tree


def get_project_doc_last_checked(project_id: str) -> str | None:
    """Return the most recent last_checked timestamp for a project's docs."""
    conn = get_db()
    row = conn.execute(
        "SELECT MAX(last_checked) as ts FROM lucernex_documents WHERE project_id = ?",
        (project_id,),
    ).fetchone()
    conn.close()
    return row["ts"] if row else None


def get_project_doc_count(project_id: str) -> int:
    """Return count of active documents for a project."""
    conn = get_db()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM lucernex_documents WHERE project_id = ? AND is_deleted = 0",
        (project_id,),
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0
