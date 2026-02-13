"""FastAPI app for the Lucernex Plumbing Projects Dashboard."""

import json
import logging
from pathlib import Path

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from database import init_db, get_refresh_metadata
from queries import (
    SEARCH_FIELDS,
    get_all_banners,
    get_all_contractors,
    get_all_projects,
    parse_search_fields,
    get_budget_by_type,
    get_po_status_summary,
    get_project_detail,
    get_project_doc_count,
    get_project_doc_last_checked,
    get_project_documents_tree,
    get_projects_by_status,
    get_projects_by_type,
    get_summary_stats,
    get_top_contractors,
    get_wbs_node_budgets,
    get_wbs_node_years,
)

from routes_po import router as po_router

logger = logging.getLogger(__name__)

# Track whether an ETL refresh is currently running.
_etl_lock = {"running": False, "message": ""}

app = FastAPI(title="Lucernex Plumbing Dashboard")
app.include_router(po_router)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def fmt_currency(value: float | None) -> str:
    """Format a number as USD currency."""
    if value is None:
        return "$0.00"
    return f"${value:,.2f}"


templates.env.filters["currency"] = fmt_currency


@app.on_event("startup")
def startup() -> None:
    init_db()


def _dashboard_context(request: Request, search: str | None = None) -> dict:
    """Shared context builder for full dashboard and HTMX partial."""
    stats = get_summary_stats(search=search)
    by_type = get_projects_by_type(search=search)
    by_status = get_projects_by_status(search=search)
    budget_by_type = get_budget_by_type(search=search)
    contractors = get_top_contractors(search=search)
    po_summary = get_po_status_summary(search=search)

    wbs_nodes = get_wbs_node_budgets()
    wbs_years = get_wbs_node_years()

    return {
        "request": request,
        "stats": stats,
        "by_type_json": json.dumps(by_type),
        "by_status_json": json.dumps(by_status),
        "budget_by_type_json": json.dumps(budget_by_type),
        "contractors": contractors,
        "po_summary": po_summary,
        "wbs_nodes": wbs_nodes,
        "wbs_years": wbs_years,
        "wbs_selected_year": None,
        "search": search or "",
    }


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, search: str = Query(None)):
    """Main dashboard view with KPIs and charts."""
    ctx = _dashboard_context(request, search)
    return templates.TemplateResponse("dashboard.html", ctx)


@app.get("/dashboard/content", response_class=HTMLResponse)
def dashboard_content_partial(request: Request, search: str = Query(None)):
    """HTMX partial: re-render dashboard cards + charts for search."""
    ctx = _dashboard_context(request, search)
    return templates.TemplateResponse("partials/dashboard_content.html", ctx)


@app.get("/dashboard/wbs-nodes", response_class=HTMLResponse)
def wbs_nodes_partial(request: Request, year: int = Query(None)):
    """HTMX partial: re-render WBS nodes panel for a selected year."""
    wbs_nodes = get_wbs_node_budgets(year=year if year else None)
    wbs_years = get_wbs_node_years()
    return templates.TemplateResponse("partials/wbs_nodes_panel.html", {
        "request": request,
        "wbs_nodes": wbs_nodes,
        "wbs_years": wbs_years,
        "wbs_selected_year": year,
    })


@app.get("/projects", response_class=HTMLResponse)
def projects_list(
    request: Request,
    project_type: str = Query(None),
    status: str = Query(None),
    contractor: str = Query(None),
    banner: str = Query(None),
    search: str = Query(None),
    search_fields: str = Query(None),
    sort: str = Query(None),
    order: str = Query(None),
):
    """Filterable, sortable projects table view."""
    active_fields = parse_search_fields(search_fields)
    projects = get_all_projects(
        project_type=project_type, status=status,
        contractor=contractor, banner=banner,
        search=search, search_fields=active_fields,
        sort=sort, order=order,
    )
    by_type = get_projects_by_type()
    by_status = get_projects_by_status()
    contractors = get_all_contractors()
    banners = get_all_banners()

    return templates.TemplateResponse("projects.html", {
        "request": request,
        "projects": projects,
        "refresh_meta": get_refresh_metadata(),
        "types": [r["project_type"] for r in by_type],
        "statuses": [r["project_status"] for r in by_status],
        "contractors": contractors,
        "banners": banners,
        "selected_type": project_type or "",
        "selected_status": status or "",
        "selected_contractor": contractor or "",
        "selected_banner": banner or "",
        "search": search or "",
        "search_fields_meta": SEARCH_FIELDS,
        "active_search_fields": active_fields,
        "sort": sort or "project_id",
        "order": order or "asc",
    })


@app.get("/projects/table", response_class=HTMLResponse)
def projects_table_partial(
    request: Request,
    project_type: str = Query(None),
    status: str = Query(None),
    contractor: str = Query(None),
    banner: str = Query(None),
    search: str = Query(None),
    search_fields: str = Query(None),
    sort: str = Query(None),
    order: str = Query(None),
):
    """HTMX partial: just the table body for filtering/sorting."""
    active_fields = parse_search_fields(search_fields)
    projects = get_all_projects(
        project_type=project_type, status=status,
        contractor=contractor, banner=banner,
        search=search, search_fields=active_fields,
        sort=sort, order=order,
    )
    return templates.TemplateResponse("partials/projects_table.html", {
        "request": request,
        "projects": projects,
        "sort": sort or "project_id",
        "order": order or "asc",
    })


# ── Document API (must be before catch-all /projects/{project_id}) ───

@app.get("/api/projects/{project_id}/documents")
def api_project_documents(project_id: str):
    """Return pre-aggregated document tree from the reporting DB."""
    tree = get_project_documents_tree(project_id)
    last_checked = get_project_doc_last_checked(project_id)
    return JSONResponse({
        "project_id": project_id,
        "last_checked": last_checked,
        "folders": tree,
    })


@app.post("/api/projects/{project_id}/documents/refresh")
def api_refresh_project_documents(project_id: str):
    """Trigger a live Lucernex document sync for a single project (JSON)."""
    try:
        from etl_documents import sync_project_documents
        count = sync_project_documents(project_id)
    except EnvironmentError as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    except Exception as exc:
        logger.error("Document refresh failed for %s", project_id, exc_info=True)
        return JSONResponse({"error": f"Refresh failed: {exc}"}, status_code=500)

    tree = get_project_documents_tree(project_id)
    last_checked = get_project_doc_last_checked(project_id)
    return JSONResponse({
        "project_id": project_id,
        "doc_count": count,
        "last_checked": last_checked,
        "folders": tree,
    })


@app.post("/projects/{project_id}/documents/sync", response_class=HTMLResponse)
def htmx_sync_project_documents(request: Request, project_id: str):
    """HTMX endpoint: sync documents and return the updated panel partial."""
    sync_error = ""
    try:
        from etl_documents import sync_project_documents
        sync_project_documents(project_id)
    except EnvironmentError as exc:
        sync_error = str(exc)
    except Exception as exc:
        logger.error("Document sync failed for %s", project_id, exc_info=True)
        sync_error = f"Sync failed: {exc}"

    doc_tree = get_project_documents_tree(project_id)
    doc_count = get_project_doc_count(project_id)

    return templates.TemplateResponse("partials/documents_panel.html", {
        "request": request,
        "doc_tree": doc_tree,
        "doc_count": doc_count,
        "sync_error": sync_error,
    })


@app.get("/projects/{project_id}", response_class=HTMLResponse)
def project_detail(request: Request, project_id: str):
    """Single project detail view with budget, POs, and documents."""
    project = get_project_detail(project_id)
    if not project:
        return HTMLResponse("<h1>Project not found</h1>", status_code=404)

    doc_tree = get_project_documents_tree(project_id)
    doc_count = get_project_doc_count(project_id)
    doc_last_checked = get_project_doc_last_checked(project_id)

    return templates.TemplateResponse("project_detail.html", {
        "request": request,
        "project": project,
        "doc_tree": doc_tree,
        "doc_count": doc_count,
        "doc_last_checked": doc_last_checked,
        "refresh_meta": get_refresh_metadata(),
    })


# ── Data Refresh ────────────────────────────────────────────────────

@app.post("/api/refresh", response_class=JSONResponse)
async def api_refresh_data():
    """Trigger a full ETL refresh from BigQuery / Lucernex.

    Returns JSON with status + summary message.
    Prevents concurrent runs with a simple lock.
    """
    if _etl_lock["running"]:
        return JSONResponse(
            {"status": "busy", "message": "A refresh is already in progress."},
            status_code=409,
        )
    _etl_lock["running"] = True
    _etl_lock["message"] = ""
    try:
        import asyncio
        from etl import run_etl
        # Run the (blocking) ETL in a thread so we don't stall the event loop.
        await asyncio.to_thread(run_etl)
        _etl_lock["message"] = "Refresh complete."
        return JSONResponse({"status": "ok", "message": _etl_lock["message"]})
    except Exception as exc:
        logger.error("ETL refresh failed", exc_info=True)
        _etl_lock["message"] = f"Refresh failed: {exc}"
        return JSONResponse(
            {"status": "error", "message": _etl_lock["message"]},
            status_code=500,
        )
    finally:
        _etl_lock["running"] = False


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8501, reload=True)
