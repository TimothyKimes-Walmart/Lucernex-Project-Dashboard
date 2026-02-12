"""Lucernex REST API client for retrieving project documents.

Auth flow:
    POST /rest/jwt  (Basic Auth) → JWT token
    All subsequent calls use Authorization: Bearer <token>

Document retrieval flow:
    1. GET /servlet/TreeLoaderServlet  → folder tree for a project
    2. GET /servlet/JSONDataRequest    → documents inside a folder
"""

import os
import time
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api-walmart.lucernex.com"

# Token cache (module-level singleton)
_token_cache: dict = {"token": None, "expires_at": 0.0}


def _get_credentials() -> tuple[str, str]:
    """Read Lucernex credentials from environment variables."""
    username = os.environ.get("LUCERNEX_USER", "")
    password = os.environ.get("LUCERNEX_PASS", "")
    if not username or not password:
        raise EnvironmentError(
            "LUCERNEX_USER and LUCERNEX_PASS environment variables are required. "
            "Set them before running the document sync."
        )
    return username, password


def _get_token() -> str:
    """Obtain or reuse a cached JWT token from Lucernex."""
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["token"]

    username, password = _get_credentials()
    # Request a token valid for 60 minutes.
    resp = requests.post(
        f"{BASE_URL}/rest/jwt",
        params={"expiryTimeInMinutes": 60},
        auth=(username, password),
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.text.strip().strip('"')
    _token_cache["token"] = token
    _token_cache["expires_at"] = now + 3500  # ~58 min buffer
    logger.info("Lucernex JWT token acquired.")
    return token


def _api_get(url: str, params: dict | None = None) -> requests.Response:
    """Authenticated GET request to Lucernex."""
    token = _get_token()
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp


# ──────────────────────────────────────────────
#  Public API
# ──────────────────────────────────────────────

def get_project_folders(project_entity_id: str) -> list[dict]:
    """Return the folder tree for a Lucernex project.

    Each item: {id, text, numFiles, children: [...]}
    """
    resp = _api_get(
        f"{BASE_URL}/servlet/TreeLoaderServlet",
        params={
            "treeType": "peFolders",
            "peID": project_entity_id,
            "node": "root",
            "_dc": str(int(time.time() * 1000)),
        },
    )
    data = resp.json()
    # Lucernex may return a list or a dict with a list inside.
    if isinstance(data, dict):
        data = data.get("children", data.get("data", []))
    return data if isinstance(data, list) else []


def get_folder_documents(
    folder_id: str,
    page: int = 1,
    limit: int = 200,
) -> list[dict]:
    """Return documents inside a specific folder.

    Each item typically has: ID, name, date, size, version, viewUrl, etc.
    """
    resp = _api_get(
        f"{BASE_URL}/servlet/JSONDataRequest",
        params={
            "reqType": "Documents",
            "folderID": folder_id,
            "page": page,
            "start": (page - 1) * limit,
            "limit": limit,
            "_dc": str(int(time.time() * 1000)),
        },
    )
    data = resp.json()
    if isinstance(data, dict):
        return data.get("data", data.get("rows", []))
    return data if isinstance(data, list) else []


def build_document_url(doc_id: str, folder_id: str) -> str:
    """Build a download URL for a specific document."""
    return (
        f"{BASE_URL}/servlet/DocumentDownload"
        f"?documentID={doc_id}&folderID={folder_id}"
    )


def fetch_all_documents_for_project(
    project_entity_id: str,
) -> list[dict]:
    """Walk the full folder tree and collect every document.

    Returns a flat list of normalised document dicts ready for DB insert.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    documents: list[dict] = []

    folders = get_project_folders(project_entity_id)

    def _walk(nodes: list[dict], category: str = "", depth: int = 0):
        for node in nodes:
            folder_name = node.get("text", "")
            folder_id = str(node.get("id", ""))
            num_files = node.get("numFiles", 0)

            # Determine category vs sub_folder based on depth.
            if depth == 0:
                cur_category = folder_name
                cur_sub = ""
            else:
                cur_category = category
                cur_sub = folder_name

            # Fetch documents if this folder has files.
            if num_files and num_files > 0:
                try:
                    docs = get_folder_documents(folder_id)
                    for doc in docs:
                        doc_id = str(
                            doc.get("ID", doc.get("id", doc.get("documentID", "")))
                        )
                        doc_name = doc.get("name", doc.get("Name", ""))
                        doc_date = doc.get("date", doc.get("Date", ""))
                        doc_size = doc.get("size", doc.get("Size", ""))
                        doc_type = _guess_mime(doc_name)
                        uploaded_by = doc.get(
                            "uploadedBy",
                            doc.get("UploadedBy", doc.get("author", "")),
                        )

                        documents.append({
                            "doc_id": doc_id,
                            "project_id": project_entity_id,
                            "folder_id": folder_id,
                            "folder_category": cur_category,
                            "sub_folder": cur_sub,
                            "doc_name": doc_name,
                            "doc_url": build_document_url(doc_id, folder_id),
                            "doc_type": doc_type,
                            "doc_size": str(doc_size),
                            "uploaded_by": str(uploaded_by),
                            "uploaded_at": str(doc_date),
                            "last_checked": now_iso,
                        })
                except Exception:
                    logger.warning(
                        "Failed to list docs in folder %s (%s/%s)",
                        folder_id, cur_category, cur_sub,
                        exc_info=True,
                    )

            # Recurse into children.
            children = node.get("children", [])
            if isinstance(children, list) and children:
                _walk(children, cur_category, depth + 1)

    _walk(folders)
    logger.info(
        "Fetched %d documents for project %s",
        len(documents), project_entity_id,
    )
    return documents


def _guess_mime(filename: str) -> str:
    """Best-effort MIME type from file extension."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return {
        "pdf": "application/pdf",
        "doc": "application/msword",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls": "application/vnd.ms-excel",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "dwg": "application/acad",
        "zip": "application/zip",
        "msg": "application/vnd.ms-outlook",
        "txt": "text/plain",
        "csv": "text/csv",
    }.get(ext, "application/octet-stream")
