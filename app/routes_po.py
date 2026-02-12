"""FastAPI routes for the POs tab."""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from database import get_refresh_metadata
from queries_po import (
    PO_SEARCH_FIELDS,
    get_all_pos,
    get_po_detail,
    get_po_filter_options,
    get_po_summary_stats,
    get_pos_for_email_export,
)

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def fmt_currency(value: float | None) -> str:
    if value is None:
        return "$0.00"
    return f"${value:,.2f}"

templates.env.filters["currency"] = fmt_currency


def _bool_param(val: str | None) -> bool:
    """Parse toggle filter params to bool."""
    return val in ("1", "true", "on", "yes")


def _common_filter_params(
    vendor: str | None = None,
    state: str | None = None,
    project_status: str | None = None,
    po_status: str | None = None,
    has_remaining: str | None = None,
    give_back_only: str | None = None,
    aging_30: str | None = None,
    search: str | None = None,
    sap_def: str | None = None,
) -> dict:
    """Normalize filter params into a dict."""
    return {
        "vendor": vendor or None,
        "state": state or None,
        "project_status": project_status or None,
        "po_status": po_status or None,
        "has_remaining": _bool_param(has_remaining),
        "give_back_only": _bool_param(give_back_only),
        "aging_30": _bool_param(aging_30),
        "search": search or None,
        "sap_def": sap_def or None,
    }


@router.get("/pos", response_class=HTMLResponse)
def pos_list(
    request: Request,
    vendor: str = Query(None),
    state: str = Query(None),
    project_status: str = Query(None),
    po_status: str = Query(None),
    has_remaining: str = Query(None),
    give_back_only: str = Query(None),
    aging_30: str = Query(None),
    search: str = Query(None),
    sap_def: str = Query(None),
    sort: str = Query(None),
    order: str = Query(None),
    page: int = Query(1, ge=1),
):
    """Main POs tab view."""
    filters = _common_filter_params(
        vendor=vendor, state=state, project_status=project_status,
        po_status=po_status, has_remaining=has_remaining,
        give_back_only=give_back_only, aging_30=aging_30,
        search=search, sap_def=sap_def,
    )

    pos, total = get_all_pos(
        **filters, sort=sort, order=order, page=page,
    )
    summary = get_po_summary_stats(**filters)
    filter_opts = get_po_filter_options()
    total_pages = max(1, math.ceil(total / 100))

    return templates.TemplateResponse("pos.html", {
        "request": request,
        "pos": pos,
        "summary": summary,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "filter_opts": filter_opts,
        "search": search or "",
        "sort": sort or "po_number",
        "order": order or "asc",
        # Pass selected filters back
        "selected_vendor": vendor or "",
        "selected_state": state or "",
        "selected_project_status": project_status or "",
        "selected_po_status": po_status or "",
        "has_remaining": _bool_param(has_remaining),
        "give_back_only": _bool_param(give_back_only),
        "aging_30": _bool_param(aging_30),
        "selected_sap_def": sap_def or "",
        "search_fields_meta": PO_SEARCH_FIELDS,
        "refresh_meta": get_refresh_metadata(),
    })


@router.get("/pos/table", response_class=HTMLResponse)
def pos_table_partial(
    request: Request,
    vendor: str = Query(None),
    state: str = Query(None),
    project_status: str = Query(None),
    po_status: str = Query(None),
    has_remaining: str = Query(None),
    give_back_only: str = Query(None),
    aging_30: str = Query(None),
    search: str = Query(None),
    sap_def: str = Query(None),
    sort: str = Query(None),
    order: str = Query(None),
    page: int = Query(1, ge=1),
):
    """HTMX partial: PO table body + pagination."""
    filters = _common_filter_params(
        vendor=vendor, state=state, project_status=project_status,
        po_status=po_status, has_remaining=has_remaining,
        give_back_only=give_back_only, aging_30=aging_30,
        search=search, sap_def=sap_def,
    )
    pos, total = get_all_pos(
        **filters, sort=sort, order=order, page=page,
    )
    summary = get_po_summary_stats(**filters)
    total_pages = max(1, math.ceil(total / 100))

    return templates.TemplateResponse("partials/pos_table.html", {
        "request": request,
        "pos": pos,
        "summary": summary,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "sort": sort or "po_number",
        "order": order or "asc",
    })


@router.get("/pos/{po_number}/detail", response_class=HTMLResponse)
def po_quick_view(request: Request, po_number: str):
    """HTMX partial: PO quick-view modal content."""
    po = get_po_detail(po_number)
    if not po:
        return HTMLResponse("<p class='text-walmart-red-100 p-4'>PO not found.</p>")
    return templates.TemplateResponse("partials/po_detail_modal.html", {
        "request": request,
        "po": po,
    })


@router.post("/pos/export-email", response_class=HTMLResponse)
async def po_export_email(request: Request):
    """Generate email report grouped by vendor for selected POs."""
    form = await request.form()
    raw = form.get("po_numbers", "")
    po_numbers = [n.strip() for n in raw.split(",") if n.strip()]

    if not po_numbers:
        return HTMLResponse(
            '<p class="text-walmart-red-100 p-4">No POs selected.</p>'
        )

    pos = get_pos_for_email_export(po_numbers)
    if not pos:
        return HTMLResponse(
            '<p class="text-walmart-red-100 p-4">No PO data found.</p>'
        )

    # Compute aggregate metrics across all selected POs.
    total_po_value = sum(p.get("po_total", 0) for p in pos)
    total_invoiced = sum(p.get("invoiced_to_date", 0) for p in pos)
    total_remaining = sum(p.get("remaining_to_invoice", 0) for p in pos)
    total_give_back = sum(p.get("give_back_amount", 0) for p in pos)
    pct_invoiced = (total_invoiced / total_po_value * 100) if total_po_value else 0
    pct_remaining = (total_remaining / total_po_value * 100) if total_po_value else 0

    # Group by vendor
    by_vendor: dict[str, list[dict]] = defaultdict(list)
    for po in pos:
        by_vendor[po.get("vendor") or "Unknown Vendor"].append(po)

    # Build vendor sections with per-vendor metrics
    vendor_sections: list[dict] = []
    for vendor_name, vendor_pos in by_vendor.items():
        v_total = sum(p.get("po_total", 0) for p in vendor_pos)
        v_invoiced = sum(p.get("invoiced_to_date", 0) for p in vendor_pos)
        v_remaining = sum(p.get("remaining_to_invoice", 0) for p in vendor_pos)
        v_give_back = sum(p.get("give_back_amount", 0) for p in vendor_pos)
        v_pct = (v_invoiced / v_total * 100) if v_total else 0

        if len(vendor_pos) == 1:
            subject = f"PO Status Update \u2013 {vendor_pos[0]['po_number']}"
        else:
            subject = f"PO Status Update \u2013 {vendor_name}"

        vendor_sections.append({
            "vendor": vendor_name,
            "vendor_email": vendor_pos[0].get("vendor_email") or "",
            "subject": subject,
            "pos": vendor_pos,
            "po_count": len(vendor_pos),
            "total": v_total,
            "invoiced": v_invoiced,
            "remaining": v_remaining,
            "give_back": v_give_back,
            "pct_invoiced": v_pct,
        })

    now_utc = datetime.now(timezone.utc).strftime("%b %d, %Y at %I:%M %p UTC")

    return templates.TemplateResponse("partials/email_drafts.html", {
        "request": request,
        "now_utc": now_utc,
        "vendor_sections": vendor_sections,
        "metrics": {
            "po_count": len(pos),
            "total_value": total_po_value,
            "invoiced": total_invoiced,
            "remaining": total_remaining,
            "give_back": total_give_back,
            "pct_invoiced": pct_invoiced,
            "pct_remaining": pct_remaining,
        },
    })
