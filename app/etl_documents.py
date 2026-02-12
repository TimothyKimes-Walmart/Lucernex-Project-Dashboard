"""ETL: Sync Lucernex project documents into the reporting database.

This runs separately from the main BQ ETL. It calls the Lucernex
Documents API for each project, stores results in lucernex_documents,
and soft-deletes documents that no longer exist in Lucernex.

Usage:
    python etl_documents.py              # Sync ALL projects
    python etl_documents.py 292726       # Sync a single project
"""

import logging
import sys
from datetime import datetime, timezone

from database import get_db, init_db
from lucernex_client import fetch_all_documents_for_project

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def sync_project_documents(project_id: str) -> int:
    """Fetch and upsert documents for a single project.

    Returns the number of documents synced.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    docs = fetch_all_documents_for_project(project_id)

    conn = get_db()

    # Track which doc_ids we see in this sync.
    seen_ids: set[str] = set()

    for d in docs:
        seen_ids.add(d["doc_id"])
        conn.execute(
            """INSERT INTO lucernex_documents
                   (doc_id, project_id, folder_id, folder_category,
                    sub_folder, doc_name, doc_url, doc_type, doc_size,
                    uploaded_by, uploaded_at, last_checked, is_deleted)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
               ON CONFLICT(doc_id) DO UPDATE SET
                    folder_category = excluded.folder_category,
                    sub_folder      = excluded.sub_folder,
                    doc_name        = excluded.doc_name,
                    doc_url         = excluded.doc_url,
                    doc_type        = excluded.doc_type,
                    doc_size        = excluded.doc_size,
                    uploaded_by     = excluded.uploaded_by,
                    uploaded_at     = excluded.uploaded_at,
                    last_checked    = excluded.last_checked,
                    is_deleted      = 0
            """,
            (
                d["doc_id"], d["project_id"], d["folder_id"],
                d["folder_category"], d["sub_folder"],
                d["doc_name"], d["doc_url"], d["doc_type"],
                d["doc_size"], d["uploaded_by"], d["uploaded_at"],
                d["last_checked"],
            ),
        )

    # Soft-delete documents no longer present in Lucernex.
    if seen_ids:
        placeholders = ",".join("?" for _ in seen_ids)
        conn.execute(
            f"""UPDATE lucernex_documents
                SET is_deleted = 1, last_checked = ?
                WHERE project_id = ?
                  AND doc_id NOT IN ({placeholders})
                  AND is_deleted = 0""",
            [now_iso, project_id, *seen_ids],
        )
    else:
        # No docs returned — soft-delete all existing.
        conn.execute(
            """UPDATE lucernex_documents
               SET is_deleted = 1, last_checked = ?
               WHERE project_id = ? AND is_deleted = 0""",
            (now_iso, project_id),
        )

    conn.commit()
    conn.close()
    return len(docs)


def sync_all_projects() -> None:
    """Sync documents for every project in the database."""
    conn = get_db()
    rows = conn.execute("SELECT project_id FROM projects ORDER BY project_id").fetchall()
    conn.close()

    total = len(rows)
    logger.info("Starting document sync for %d projects...", total)

    synced = 0
    errors = 0
    for i, row in enumerate(rows, 1):
        pid = row["project_id"]
        try:
            count = sync_project_documents(pid)
            synced += count
            logger.info("  [%d/%d] Project %s: %d docs", i, total, pid, count)
        except Exception:
            errors += 1
            logger.error(
                "  [%d/%d] Project %s: FAILED", i, total, pid, exc_info=True
            )

    logger.info(
        "Document sync complete. %d docs synced, %d errors out of %d projects.",
        synced, errors, total,
    )


if __name__ == "__main__":
    init_db()
    if len(sys.argv) > 1:
        pid = sys.argv[1]
        logger.info("Syncing documents for project %s ...", pid)
        count = sync_project_documents(pid)
        logger.info("Done. %d documents synced for project %s.", count, pid)
    else:
        sync_all_projects()
