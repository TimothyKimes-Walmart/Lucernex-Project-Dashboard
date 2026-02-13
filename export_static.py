"""Export the Lucernex Plumbing Dashboard as a self-contained static HTML report.

Reads data from the SQLite database and generates a single HTML file with
embedded JSON data + client-side JavaScript for full interactivity (tabs,
search, filters, sorting, modals). No server needed — perfect for SharePoint.

Usage:
    python export_static.py            # writes to ./report.html
    python export_static.py out.html   # custom output path
"""

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

DB_PATH = Path(__file__).parent / "dashboard.db"
TEMPLATE_DIR = Path(__file__).parent / "static_templates"


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ── Data queries ─────────────────────────────────────────────────────

def query_summary_stats(conn: sqlite3.Connection) -> dict:
    stats = {}
    stats["total_projects"] = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    stats["active_projects"] = conn.execute(
        "SELECT COUNT(*) FROM projects WHERE project_status = 'Active'"
    ).fetchone()[0]
    stats["total_budget"] = conn.execute(
        "SELECT COALESCE(SUM(budget_total), 0) FROM sap_budget"
    ).fetchone()[0]
    stats["total_actuals"] = conn.execute(
        "SELECT COALESCE(SUM(budget_actuals), 0) FROM sap_budget"
    ).fetchone()[0]
    stats["remaining_to_invoice"] = conn.execute(
        "SELECT COALESCE(SUM(remaining_to_invoice), 0) FROM sap_po"
    ).fetchone()[0]
    stats["total_pos"] = conn.execute("SELECT COUNT(*) FROM sap_po").fetchone()[0]
    return stats


def query_by_type(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT project_type, COUNT(*) as cnt FROM projects GROUP BY project_type ORDER BY cnt DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def query_by_status(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT project_status, COUNT(*) as cnt FROM projects GROUP BY project_status ORDER BY cnt DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def query_budget_by_type(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT p.project_type,
               COALESCE(SUM(b.budget_total), 0) as budget_total,
               COALESCE(SUM(b.budget_actuals), 0) as budget_actuals,
               COALESCE(SUM(b.budget_committed), 0) as budget_committed,
               COALESCE(SUM(b.budget_open), 0) as budget_open
        FROM projects p
        LEFT JOIN sap_budget b ON p.sap_project_definition = b.sap_project_definition
        GROUP BY p.project_type ORDER BY budget_total DESC
    """).fetchall()
    return [dict(r) for r in rows]


def query_top_contractors(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT general_contractor, COUNT(*) as project_count,
               COALESCE(SUM(b.budget_total), 0) as total_budget
        FROM projects p
        LEFT JOIN sap_budget b ON p.sap_project_definition = b.sap_project_definition
        GROUP BY general_contractor ORDER BY project_count DESC LIMIT 10
    """).fetchall()
    return [dict(r) for r in rows]


def query_po_summary(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT po_status, COUNT(*) as cnt, COALESCE(SUM(po_total), 0) as total
        FROM sap_po GROUP BY po_status ORDER BY cnt DESC
    """).fetchall()
    return [dict(r) for r in rows]


def query_projects(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT p.project_id, p.project_type, p.store, p.store_sequence,
               p.city, p.state, p.project_status, p.brief_scope_of_work,
               p.general_contractor, p.banner, p.sap_project_definition,
               p.construction_complete_date, p.created_date,
               COALESCE(b.budget_total, 0) AS budget_total,
               COALESCE(b.budget_actuals, 0) AS budget_actuals
        FROM projects p
        LEFT JOIN sap_budget b ON p.sap_project_definition = b.sap_project_definition
        ORDER BY p.project_id
    """).fetchall()
    return [dict(r) for r in rows]


def query_pos(conn: sqlite3.Connection) -> list[dict]:
    """Query all POs with project context for the PO tab."""
    rows = conn.execute("""
        SELECT po.po_number, po.vendor, po.po_total, po.invoiced_to_date,
               po.remaining_to_invoice, po.po_status, po.created_date,
               po.last_update, po.sap_project_definition, po.vendor_email,
               p.store, p.project_status, p.project_type,
               p.general_contractor, p.brief_scope_of_work AS scope,
               p.city, p.state, p.banner
        FROM sap_po po
        LEFT JOIN projects p ON po.sap_project_definition = p.sap_project_definition
        ORDER BY po.po_number
    """).fetchall()
    return [dict(r) for r in rows]


def query_wbs_nodes(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM sap_wbs_nodes ORDER BY node_key, approval_year"
    ).fetchall()
    return [dict(r) for r in rows]


def query_give_back(conn: sqlite3.Connection) -> dict:
    row = conn.execute("""
        SELECT
            COUNT(*) AS total_pos,
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
        LEFT JOIN projects p ON po.sap_project_definition = p.sap_project_definition
    """).fetchone()
    return dict(row)


# ── Insights generator ───────────────────────────────────────────────

def _fmt_k(value: float | None) -> str:
    if value is None:
        return "$0"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:,.1f}M"
    if abs(value) >= 1_000:
        return f"${value / 1_000:,.0f}K"
    return f"${value:,.0f}"


def generate_insights(stats, by_type, give_back, contractors) -> list[str]:
    """Generate executive-level bullet insights from data."""
    insights = []
    pct_active = (
        (stats["active_projects"] / stats["total_projects"] * 100)
        if stats["total_projects"] else 0
    )
    insights.append(
        f"The plumbing portfolio has <strong>{stats['total_projects']} projects</strong> "
        f"with <strong>{stats['active_projects']} ({pct_active:.0f}%)</strong> currently active."
    )
    if stats["total_budget"] > 0:
        util = stats["total_actuals"] / stats["total_budget"] * 100
        insights.append(
            f"Budget utilisation is at <strong>{util:.0f}%</strong> "
            f"({_fmt_k(stats['total_actuals'])} spent of {_fmt_k(stats['total_budget'])} total)."
        )
    if stats["remaining_to_invoice"] > 0:
        insights.append(
            f"<strong>{_fmt_k(stats['remaining_to_invoice'])}</strong> remains to be invoiced "
            f"across {stats['total_pos']} purchase orders."
        )
    if give_back["total_give_back"] > 0:
        insights.append(
            f"\U0001f514 <strong>Give-back opportunity:</strong> {give_back['complete_with_open_pos']} "
            f"completed projects still have open PO balances totalling "
            f"<strong>{_fmt_k(give_back['total_give_back'])}</strong>."
        )
    if by_type:
        top = by_type[0]
        insights.append(
            f"<strong>{top['project_type']}</strong> is the largest category "
            f"with {top['cnt']} projects."
        )
    if contractors:
        tc = contractors[0]
        insights.append(
            f"Top contractor is <strong>{tc['general_contractor'] or 'Unassigned'}</strong> "
            f"handling {tc['project_count']} projects ({_fmt_k(tc['total_budget'])} budget)."
        )
    return insights


def generate_recommendations(stats, give_back) -> list[str]:
    return [
        f"Review the <strong>{_fmt_k(give_back['total_give_back'])}</strong> give-back "
        f"opportunity from completed projects with open PO balances to recapture budget.",
        f"Focus invoice reconciliation on the <strong>{stats['total_pos']} active POs</strong> "
        f"to reduce the remaining-to-invoice backlog.",
        "Consider consolidating contractor assignments — the top contractors handle the "
        "majority of projects, presenting negotiation leverage.",
        "This report reflects a point-in-time snapshot. For live data, access the full "
        "dashboard application or re-run the export.",
    ]


# ── Jinja2 template rendering ────────────────────────────────────────

def fmt_currency(value):
    """Jinja2 filter: format as $X,XXX.XX"""
    if value is None:
        return "$0.00"
    return f"${value:,.2f}"


def render_report(data: dict) -> str:
    """Render the static report HTML using Jinja2 templates."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=False,
    )
    env.filters["fmt_currency"] = fmt_currency
    template = env.get_template("report.html")
    return template.render(**data)


# ── Main ─────────────────────────────────────────────────────────────

def main() -> None:
    output_path = (
        Path(sys.argv[1]) if len(sys.argv) > 1
        else Path(__file__).parent / "report.html"
    )

    if not DB_PATH.exists():
        print(f"[ERROR] Database not found: {DB_PATH}")
        sys.exit(1)

    print(f"[*] Reading data from {DB_PATH} ...")
    conn = get_db()

    stats = query_summary_stats(conn)
    by_type = query_by_type(conn)
    by_status = query_by_status(conn)
    budget_by_type = query_budget_by_type(conn)
    contractors = query_top_contractors(conn)
    po_summary = query_po_summary(conn)
    projects = query_projects(conn)
    pos = query_pos(conn)
    wbs_nodes = query_wbs_nodes(conn)
    give_back = query_give_back(conn)
    conn.close()

    generated_at = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    insights = generate_insights(stats, by_type, give_back, contractors)
    recommendations = generate_recommendations(stats, give_back)

    print(f"[*] Building static HTML report ...")
    html = render_report({
        "generated_at": generated_at,
        "stats": stats,
        "insights": insights,
        "recommendations": recommendations,
        "contractors": contractors,
        "po_summary": po_summary,
        "give_back": give_back,
        # JSON for client-side interactivity
        "projects_json": json.dumps(projects),
        "pos_json": json.dumps(pos),
        "wbs_nodes_json": json.dumps(wbs_nodes),
        "by_type_json": json.dumps(by_type),
        "by_status_json": json.dumps(by_status),
        "budget_by_type_json": json.dumps(budget_by_type),
        "contractors_json": json.dumps(contractors),
        "po_summary_json": json.dumps(po_summary),
        "stats_json": json.dumps(stats),
        "give_back_json": json.dumps(give_back),
    })

    output_path.write_text(html, encoding="utf-8")
    print(f"[OK] Report written to {output_path.resolve()}")
    print(f"     {len(projects)} projects · {len(pos)} POs · {len(contractors)} contractors")


if __name__ == "__main__":
    main()
