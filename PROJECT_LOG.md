# 📦 Lucernex Plumbing Dashboard — Project Log

> **Created:** 2026-02-11  
> **Owner:** Timothy (t0k05cq)  
> **Stack:** Python + FastAPI + HTMX + Tailwind + SQLite + Chart.js

---

## 📅 Log

### 2026-02-11 — Project Setup
- ✅ Unzipped `code_puppy_dashboard_handoff.zip`
- ✅ Reviewed README, DDL, and sample data (3 tables: projects, sap_budget, sap_po)
- ✅ Identified 4 plumbing project types to filter on
- ✅ Created project folder `projects/lucernex-plumbing-dashboard/`
- ✅ Built FastAPI app with HTMX + Tailwind + Chart.js (Walmart branded)
- ✅ Created database layer (SQLite): projects, sap_budget, sap_po tables
- ✅ Created query layer with filtering, search, KPIs
- ✅ Seeded 30 sample projects across all 4 plumbing types
- ✅ Dashboard page: KPI cards, doughnut charts (by type/status), stacked bar (budget), contractor & PO tables
- ✅ Projects list page: filterable/searchable table with HTMX live filtering
- ✅ Project detail page: scope, budget progress bar, PO breakdown
- ✅ Server running on http://127.0.0.1:8501

### 2026-02-11 — Real Data Integration
- ✅ Installed Google Cloud SDK and authenticated with Walmart SSO
- ✅ Installed BigQuery Python packages (google-cloud-bigquery, pyarrow, etc.)
- ✅ Searched BigQuery for Lucernex/SAP tables across re-ods-explorer and re-ods-prod
- ✅ Discovered key tables:
  - `re-ods-prod.us_re_ods_prod_pub.qb_fmpm_project_cur` ← Plumbing projects (Program_Type LIKE '%PLBG%')
  - `re-ods-prod.us_re_ods_prod_pub.vw_rps_purchase_order` ← SAP Purchase Orders (project_definition)
- ✅ Solved SAP format mismatch: `USFC-009320` → `USFC00932000000` (strip dash, append 00000)
- ✅ Built `app/etl.py` — Full ETL pipeline from BigQuery → SQLite
- ✅ Loaded **197 real plumbing projects** + **172 real POs** from BigQuery
- ✅ Dashboard now shows REAL Facility Services data!

### 2026-02-11 — PO Invoice Data Fix
- 🐛 **Bug**: PO 40676449 showed $0 invoiced instead of $8,500
- 🔍 **Root cause**: `vw_rps_purchase_order` stores PO amounts and invoice receipts on **separate rows**:
  - Row with `net_po_lc_amt = 17000` (PO line) has `invoiced_lc_amt = 0`
  - Row with `net_po_lc_amt = 0` (invoice receipt) has `invoiced_lc_amt = 8500`
  - Our `WHERE net_po_lc_amt > 0` filter was discarding invoice receipt rows!
- ✅ **Fix**: Removed `WHERE` filter, replaced with `HAVING` on grouped results to include all row types
- ✅ PO 40676449 now correctly shows: Total=$17K, Invoiced=$8.5K, Remaining=$8.5K
- ✅ 53 POs now have real invoice data (was 0 before)

### 2026-02-11 — Brief Scope of Work Fix
- 🐛 **Bug**: Project detail "Scope of Work" showed the Program_Type (e.g. "PLBG EQUIPMENT REPLACEMENT") instead of the actual scope
- 🔍 **Root cause**: `qb_fmpm_project_cur` doesn't have a scope field — it lives in `lx_all_projects_curr.Brief_Scope_Of_Work`
- ✅ **Fix**: Added LEFT JOIN to `lx_all_projects_curr` on `SAPProjectDefinition` in the ETL query
- ✅ 189/197 projects now have real scope (e.g. "Used Cooking Oil - Tank Replacement")

### 2026-02-11 — Documents Integration UI Fix
- 🐛 **Bug**: Documents section was invisible when no docs had been synced (`{% if doc_count > 0 %}` guard)
- 🐛 **Bug**: No way for users to trigger a Lucernex document sync from the UI
- ✅ **Fix**: Documents section now **always shows** on project detail, even with 0 docs
- ✅ **Fix**: Added a "Sync Now" button (Walmart blue, with spinning icon during request)
- ✅ **Fix**: Added empty state with helpful prompt to click Sync Now
- ✅ **Fix**: Button hits `POST /projects/{id}/documents/sync` via HTMX and swaps the panel in-place
- ✅ **Fix**: Error state shown inline if sync fails (e.g. missing Lucernex credentials)
- ✅ **Fix**: Button disables during sync (`hx-disabled-elt`) to prevent double-clicks
- ✅ Extracted `partials/documents_panel.html` for DRY reuse between full page load and HTMX swap
- ✅ Added HTMX indicator CSS to `base.html` for spinner/static icon toggling

#### File Structure
```
app/
  main.py          ← FastAPI routes
  database.py      ← SQLite connection & schema
  queries.py       ← All SQL queries
  seed.py          ← Sample data generator
  templates/
    base.html      ← Layout + nav + Walmart colors
    dashboard.html ← KPIs + charts
    projects.html  ← Filterable table
    project_detail.html ← Single project view
    partials/
      projects_table.html ← HTMX partial for live filtering
```

---

*Updated by Chewie 🐶*
