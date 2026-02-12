"""Export the Lucernex Plumbing Dashboard as a self-contained static HTML report.

Reads data from the SQLite database and generates a single HTML file that can
be opened directly in any browser — no server needed. Perfect for SharePoint.

Usage:
    python export_static.py            # writes to ./report.html
    python export_static.py out.html   # custom output path
"""

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "dashboard.db"


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ── Data queries (self-contained, no app imports needed) ─────────────

def _query_summary_stats(conn: sqlite3.Connection) -> dict:
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


def _query_by_type(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT project_type, COUNT(*) as cnt FROM projects GROUP BY project_type ORDER BY cnt DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def _query_by_status(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT project_status, COUNT(*) as cnt FROM projects GROUP BY project_status ORDER BY cnt DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def _query_budget_by_type(conn: sqlite3.Connection) -> list[dict]:
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


def _query_top_contractors(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT general_contractor, COUNT(*) as project_count,
               COALESCE(SUM(b.budget_total), 0) as total_budget
        FROM projects p
        LEFT JOIN sap_budget b ON p.sap_project_definition = b.sap_project_definition
        GROUP BY general_contractor ORDER BY project_count DESC LIMIT 10
    """).fetchall()
    return [dict(r) for r in rows]


def _query_po_summary(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT po_status, COUNT(*) as cnt, SUM(po_total) as total
        FROM sap_po GROUP BY po_status ORDER BY cnt DESC
    """).fetchall()
    return [dict(r) for r in rows]


def _query_projects_table(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT p.project_id, p.project_type, p.store, p.store_sequence,
               p.city, p.state, p.project_status, p.brief_scope_of_work,
               p.general_contractor, p.banner,
               COALESCE(po_agg.total_po, b.budget_total, 0) AS budget_total,
               COALESCE(po_agg.total_invoiced, b.budget_actuals, 0) AS budget_actuals
        FROM projects p
        LEFT JOIN sap_budget b ON p.sap_project_definition = b.sap_project_definition
        LEFT JOIN (
            SELECT sap_project_definition,
                   SUM(po_total) AS total_po,
                   SUM(invoiced_to_date) AS total_invoiced
            FROM sap_po GROUP BY sap_project_definition
        ) po_agg ON p.sap_project_definition = po_agg.sap_project_definition
        ORDER BY p.project_id
    """).fetchall()
    return [dict(r) for r in rows]


def _query_give_back_summary(conn: sqlite3.Connection) -> dict:
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


def _query_status_by_type_matrix(conn: sqlite3.Connection) -> list[dict]:
    """Status breakdown per project type for the analysis section."""
    rows = conn.execute("""
        SELECT project_type, project_status, COUNT(*) as cnt
        FROM projects
        GROUP BY project_type, project_status
        ORDER BY project_type, cnt DESC
    """).fetchall()
    return [dict(r) for r in rows]


def _fmt(value: float | None) -> str:
    if value is None:
        return "$0.00"
    return f"${value:,.2f}"


def _fmt_k(value: float | None) -> str:
    if value is None:
        return "$0"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:,.1f}M"
    if abs(value) >= 1_000:
        return f"${value / 1_000:,.0f}K"
    return f"${value:,.0f}"


# ── Insights generator ───────────────────────────────────────────────

def _generate_insights(
    stats: dict,
    by_type: list[dict],
    by_status: list[dict],
    budget_by_type: list[dict],
    give_back: dict,
    contractors: list[dict],
) -> list[str]:
    """Generate executive-level bullet insights from the data."""
    insights = []

    # Portfolio overview
    pct_active = (
        (stats["active_projects"] / stats["total_projects"] * 100)
        if stats["total_projects"] else 0
    )
    insights.append(
        f"The plumbing portfolio has <strong>{stats['total_projects']} projects</strong> "
        f"with <strong>{stats['active_projects']} ({pct_active:.0f}%)</strong> currently active."
    )

    # Budget utilisation
    if stats["total_budget"] > 0:
        util = stats["total_actuals"] / stats["total_budget"] * 100
        insights.append(
            f"Budget utilisation is at <strong>{util:.0f}%</strong> "
            f"({_fmt_k(stats['total_actuals'])} spent of {_fmt_k(stats['total_budget'])} total)."
        )

    # Remaining to invoice
    if stats["remaining_to_invoice"] > 0:
        insights.append(
            f"<strong>{_fmt_k(stats['remaining_to_invoice'])}</strong> remains to be invoiced "
            f"across {stats['total_pos']} purchase orders."
        )

    # Give-back opportunity
    if give_back["total_give_back"] > 0:
        insights.append(
            f"🔔 <strong>Give-back opportunity:</strong> {give_back['complete_with_open_pos']} "
            f"completed projects still have open PO balances totalling "
            f"<strong>{_fmt_k(give_back['total_give_back'])}</strong>."
        )

    # Largest type
    if by_type:
        top = by_type[0]
        insights.append(
            f"<strong>{top['project_type']}</strong> is the largest category "
            f"with {top['cnt']} projects."
        )

    # Top contractor
    if contractors:
        tc = contractors[0]
        insights.append(
            f"Top contractor is <strong>{tc['general_contractor'] or 'Unassigned'}</strong> "
            f"handling {tc['project_count']} projects ({_fmt_k(tc['total_budget'])} budget)."
        )

    return insights


# ── HTML builder ─────────────────────────────────────────────────────

def _build_html(
    stats: dict,
    by_type: list[dict],
    by_status: list[dict],
    budget_by_type: list[dict],
    contractors: list[dict],
    po_summary: list[dict],
    projects: list[dict],
    give_back: dict,
    generated_at: str,
) -> str:
    insights = _generate_insights(
        stats, by_type, by_status, budget_by_type, give_back, contractors,
    )

    # Build contractor rows
    contractor_rows = ""
    for gc in contractors:
        contractor_rows += f"""<tr class="border-b border-gray-100 hover:bg-gray-50">
          <td class="py-2 px-3">{gc['general_contractor'] or 'Unassigned'}</td>
          <td class="py-2 px-3 text-right font-medium">{gc['project_count']}</td>
          <td class="py-2 px-3 text-right">{_fmt(gc['total_budget'])}</td>
        </tr>"""

    # Build PO status rows
    po_rows = ""
    status_badge_cls = {
        "Open": "bg-green-50 text-green-700",
        "Closed": "bg-gray-100 text-gray-700",
    }
    for po in po_summary:
        badge = status_badge_cls.get(po["po_status"], "bg-yellow-50 text-yellow-800")
        po_rows += f"""<tr class="border-b border-gray-100 hover:bg-gray-50">
          <td class="py-2 px-3">
            <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium {badge}">
              {po['po_status']}
            </span>
          </td>
          <td class="py-2 px-3 text-right font-medium">{po['cnt']}</td>
          <td class="py-2 px-3 text-right">{_fmt(po['total'])}</td>
        </tr>"""

    # Build projects table rows
    project_rows = ""
    for p in projects:
        status_color = {
            "Active": "bg-green-50 text-green-700",
            "Complete": "bg-blue-50 text-blue-700",
            "On Hold": "bg-yellow-50 text-yellow-800",
            "Cancelled": "bg-red-50 text-red-700",
        }.get(p["project_status"] or "", "bg-gray-100 text-gray-600")
        scope = (p["brief_scope_of_work"] or "")[:60]
        if len(p.get("brief_scope_of_work") or "") > 60:
            scope += "…"
        project_rows += f"""<tr class="border-b border-gray-100 hover:bg-gray-50">
          <td class="py-2 px-3 font-medium text-blue-700">{p['project_id']}</td>
          <td class="py-2 px-3 text-xs">{(p['project_type'] or '').replace('PLBG ', '')}</td>
          <td class="py-2 px-3">{p['store'] or ''}</td>
          <td class="py-2 px-3">{p['city'] or ''}, {p['state'] or ''}</td>
          <td class="py-2 px-3">
            <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium {status_color}">
              {p['project_status'] or 'Unknown'}
            </span>
          </td>
          <td class="py-2 px-3 text-xs">{scope}</td>
          <td class="py-2 px-3">{p['general_contractor'] or ''}</td>
          <td class="py-2 px-3 text-right">{_fmt(p['budget_total'])}</td>
          <td class="py-2 px-3 text-right">{_fmt(p['budget_actuals'])}</td>
        </tr>"""

    # Insights HTML
    insights_html = "\n".join(
        f'<li class="flex items-start gap-2"><span class="text-blue-600 mt-0.5">▸</span><span>{i}</span></li>'
        for i in insights
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Plumbing Dashboard Report | Facility Services</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    body {{ font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; }}
    @media print {{ nav, footer {{ display: none; }} main {{ padding: 0 !important; }} }}
  </style>
</head>
<body class="bg-gray-50 text-gray-900 min-h-screen">

  <!-- Top Nav -->
  <nav class="bg-[#0053e2] text-white shadow-lg print:hidden">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="flex items-center justify-between h-16">
        <div class="flex items-center space-x-3">
          <span class="text-[#ffc220] text-2xl font-bold">✦</span>
          <span class="text-lg font-bold tracking-tight">Plumbing Projects Dashboard</span>
        </div>
        <div class="text-sm opacity-80">Static Report · Generated {generated_at}</div>
      </div>
    </div>
  </nav>

  <main class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">

    <!-- Executive Insights -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6 mb-8">
      <h2 class="text-lg font-semibold mb-3 flex items-center gap-2">
        <span class="text-[#0053e2]">📊</span> Executive Insights
      </h2>
      <ul class="space-y-2 text-sm leading-relaxed">
        {insights_html}
      </ul>
    </div>

    <!-- KPI Cards -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
      <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <p class="text-sm font-medium text-gray-500 uppercase tracking-wide">Total Projects</p>
        <p class="text-3xl font-bold text-[#0053e2] mt-1">{stats['total_projects']}</p>
        <p class="text-sm text-green-600 mt-1">{stats['active_projects']} active</p>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <p class="text-sm font-medium text-gray-500 uppercase tracking-wide">Total Budget</p>
        <p class="text-3xl font-bold text-[#0053e2] mt-1">{_fmt(stats['total_budget'])}</p>
        <p class="text-sm text-gray-500 mt-1">{_fmt(stats['total_actuals'])} spent</p>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <p class="text-sm font-medium text-gray-500 uppercase tracking-wide">Remaining to Invoice</p>
        <p class="text-3xl font-bold text-[#995213] mt-1">{_fmt(stats['remaining_to_invoice'])}</p>
        <p class="text-sm text-gray-500 mt-1">{stats['total_pos']} purchase orders</p>
      </div>
    </div>

    <!-- Charts Row 1 -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
      <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 class="text-lg font-semibold mb-4">Projects by Type</h2>
        <div style="height: 300px;"><canvas id="chartByType"></canvas></div>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 class="text-lg font-semibold mb-4">Projects by Status</h2>
        <div style="height: 300px;"><canvas id="chartByStatus"></canvas></div>
      </div>
    </div>

    <!-- Charts Row 2 -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6 mb-8">
      <h2 class="text-lg font-semibold mb-4">Budget Breakdown by Project Type</h2>
      <div style="height: 350px;"><canvas id="chartBudget"></canvas></div>
    </div>

    <!-- Bottom Tables -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
      <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 class="text-lg font-semibold mb-4">Top General Contractors</h2>
        <div class="overflow-x-auto">
          <table class="w-full text-sm" role="table">
            <thead>
              <tr class="border-b border-gray-300">
                <th class="text-left py-2 px-3 font-semibold" scope="col">Contractor</th>
                <th class="text-right py-2 px-3 font-semibold" scope="col">Projects</th>
                <th class="text-right py-2 px-3 font-semibold" scope="col">Total Budget</th>
              </tr>
            </thead>
            <tbody>{contractor_rows}</tbody>
          </table>
        </div>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h2 class="text-lg font-semibold mb-4">Purchase Order Status</h2>
        <div class="overflow-x-auto">
          <table class="w-full text-sm" role="table">
            <thead>
              <tr class="border-b border-gray-300">
                <th class="text-left py-2 px-3 font-semibold" scope="col">Status</th>
                <th class="text-right py-2 px-3 font-semibold" scope="col">Count</th>
                <th class="text-right py-2 px-3 font-semibold" scope="col">Total Value</th>
              </tr>
            </thead>
            <tbody>{po_rows}</tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Full Projects Table -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-200 p-6 mb-8">
      <h2 class="text-lg font-semibold mb-4">All Projects ({len(projects)})</h2>
      <div class="overflow-x-auto">
        <table class="w-full text-sm" role="table" id="projectsTable">
          <thead class="sticky top-0 bg-white">
            <tr class="border-b-2 border-gray-300">
              <th class="text-left py-2 px-3 font-semibold cursor-pointer" scope="col" onclick="sortTable(0)">Entity ID ↕</th>
              <th class="text-left py-2 px-3 font-semibold cursor-pointer" scope="col" onclick="sortTable(1)">Type ↕</th>
              <th class="text-left py-2 px-3 font-semibold cursor-pointer" scope="col" onclick="sortTable(2)">Store ↕</th>
              <th class="text-left py-2 px-3 font-semibold" scope="col">Location</th>
              <th class="text-left py-2 px-3 font-semibold cursor-pointer" scope="col" onclick="sortTable(4)">Status ↕</th>
              <th class="text-left py-2 px-3 font-semibold" scope="col">Scope</th>
              <th class="text-left py-2 px-3 font-semibold" scope="col">Contractor</th>
              <th class="text-right py-2 px-3 font-semibold cursor-pointer" scope="col" onclick="sortTable(7)">Budget ↕</th>
              <th class="text-right py-2 px-3 font-semibold cursor-pointer" scope="col" onclick="sortTable(8)">Actuals ↕</th>
            </tr>
          </thead>
          <tbody>{project_rows}</tbody>
        </table>
      </div>
    </div>

    <!-- Bottom Analysis -->
    <div class="bg-blue-50 rounded-xl border border-blue-200 p-6">
      <h2 class="text-lg font-semibold mb-3 flex items-center gap-2">
        <span class="text-[#0053e2]">💡</span> Recommendations for Leadership
      </h2>
      <ul class="space-y-2 text-sm leading-relaxed">
        <li class="flex items-start gap-2"><span class="text-blue-600 mt-0.5">▸</span>
          <span>Review the <strong>{_fmt_k(give_back['total_give_back'])}</strong> give-back opportunity from completed projects with open PO balances to recapture budget.</span></li>
        <li class="flex items-start gap-2"><span class="text-blue-600 mt-0.5">▸</span>
          <span>Focus invoice reconciliation on the <strong>{stats['total_pos']} active POs</strong> to reduce the remaining-to-invoice backlog.</span></li>
        <li class="flex items-start gap-2"><span class="text-blue-600 mt-0.5">▸</span>
          <span>Consider consolidating contractor assignments — the top 3 contractors handle the majority of projects, presenting negotiation leverage.</span></li>
        <li class="flex items-start gap-2"><span class="text-blue-600 mt-0.5">▸</span>
          <span>This report reflects a point-in-time snapshot. For live data, access the full dashboard application or re-run the export.</span></li>
      </ul>
    </div>

  </main>

  <footer class="bg-white border-t border-gray-200 mt-12 py-6 print:hidden">
    <div class="max-w-7xl mx-auto px-4 text-center text-gray-500 text-sm">
      Facility Services · Lucernex + SAP Integration · Generated {generated_at} · Built with 🐶 Code Puppy
    </div>
  </footer>

  <script>
    // ── Chart.js init ──────────────────────────────────────────────
    const WM_BLUE = '#0053e2';
    const WM_SPARK = '#ffc220';
    const WM_GREEN = '#2a8703';
    const WM_RED = '#ea1100';
    const PALETTE = [WM_BLUE, WM_SPARK, WM_GREEN, WM_RED, '#80a9f1', '#ffe18f', '#995213', '#cccccc'];
    Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";
    Chart.defaults.plugins.legend.labels.usePointStyle = true;

    const byType = {json.dumps(by_type)};
    new Chart(document.getElementById('chartByType'), {{
      type: 'doughnut',
      data: {{
        labels: byType.map(d => d.project_type.replace('PLBG ', '')),
        datasets: [{{
          data: byType.map(d => d.cnt),
          backgroundColor: PALETTE.slice(0, byType.length),
          borderWidth: 2, borderColor: '#fff'
        }}]
      }},
      options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom' }} }} }}
    }});

    const byStatus = {json.dumps(by_status)};
    const statusColors = {{ 'Active': WM_GREEN, 'Complete': WM_BLUE, 'On Hold': WM_SPARK, 'Cancelled': WM_RED }};
    new Chart(document.getElementById('chartByStatus'), {{
      type: 'doughnut',
      data: {{
        labels: byStatus.map(d => d.project_status),
        datasets: [{{
          data: byStatus.map(d => d.cnt),
          backgroundColor: byStatus.map(d => statusColors[d.project_status] || '#ccc'),
          borderWidth: 2, borderColor: '#fff'
        }}]
      }},
      options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom' }} }} }}
    }});

    const budgetData = {json.dumps(budget_by_type)};
    new Chart(document.getElementById('chartBudget'), {{
      type: 'bar',
      data: {{
        labels: budgetData.map(d => d.project_type.replace('PLBG ', '')),
        datasets: [
          {{ label: 'Actuals', data: budgetData.map(d => d.budget_actuals), backgroundColor: WM_BLUE }},
          {{ label: 'Committed', data: budgetData.map(d => d.budget_committed), backgroundColor: WM_SPARK }},
          {{ label: 'Open', data: budgetData.map(d => d.budget_open), backgroundColor: '#80a9f1' }},
        ]
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        scales: {{
          x: {{ stacked: true, grid: {{ display: false }} }},
          y: {{ stacked: true, ticks: {{ callback: v => '$' + (v / 1000).toFixed(0) + 'K' }} }}
        }},
        plugins: {{ legend: {{ position: 'bottom' }} }}
      }}
    }});

    // ── Client-side table sort ─────────────────────────────────────
    let sortDir = {{}};
    function sortTable(colIdx) {{
      const table = document.getElementById('projectsTable');
      const tbody = table.querySelector('tbody');
      const rows = Array.from(tbody.querySelectorAll('tr'));
      sortDir[colIdx] = !sortDir[colIdx];
      const dir = sortDir[colIdx] ? 1 : -1;
      rows.sort((a, b) => {{
        let aVal = a.cells[colIdx].textContent.trim();
        let bVal = b.cells[colIdx].textContent.trim();
        const aNum = parseFloat(aVal.replace(/[$,]/g, ''));
        const bNum = parseFloat(bVal.replace(/[$,]/g, ''));
        if (!isNaN(aNum) && !isNaN(bNum)) return (aNum - bNum) * dir;
        return aVal.localeCompare(bVal) * dir;
      }});
      rows.forEach(r => tbody.appendChild(r));
    }}
  </script>
</body>
</html>"""


def main() -> None:
    output_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "report.html"

    if not DB_PATH.exists():
        print(f"[ERROR] Database not found: {DB_PATH}")
        sys.exit(1)

    print(f"[*] Reading data from {DB_PATH} ...")
    conn = get_db()

    stats = _query_summary_stats(conn)
    by_type = _query_by_type(conn)
    by_status = _query_by_status(conn)
    budget_by_type = _query_budget_by_type(conn)
    contractors = _query_top_contractors(conn)
    po_summary = _query_po_summary(conn)
    projects = _query_projects_table(conn)
    give_back = _query_give_back_summary(conn)
    conn.close()

    generated_at = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    print(f"[*] Building static HTML report ...")
    html = _build_html(
        stats=stats,
        by_type=by_type,
        by_status=by_status,
        budget_by_type=budget_by_type,
        contractors=contractors,
        po_summary=po_summary,
        projects=projects,
        give_back=give_back,
        generated_at=generated_at,
    )

    output_path.write_text(html, encoding="utf-8")
    print(f"[OK] Report written to {output_path.resolve()}")
    print(f"   {len(projects)} projects · {stats['total_pos']} POs · {len(contractors)} contractors")


if __name__ == "__main__":
    main()
