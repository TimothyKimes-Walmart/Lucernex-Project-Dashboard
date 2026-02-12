"""Seed test documents into lucernex_documents table."""
import sys
sys.path.insert(0, 'app')

from database import init_db, get_db

init_db()
conn = get_db()

# Verify table
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print(f"Tables: {tables}")

test_docs = [
    ('DOC-T001','292726','f1','Drawings','Plumbing Plans','Store151_plumbing_plan.pdf',
     'https://api-walmart.lucernex.com/servlet/DocumentDownload?documentID=DOC-T001&folderID=f1',
     'application/pdf','2.1 MB','jane.doe','2026-01-15','2026-02-11T08:00:00Z',0),
    ('DOC-T002','292726','f1','Drawings','Plumbing Plans','Store151_asbuilt.pdf',
     'https://api-walmart.lucernex.com/servlet/DocumentDownload?documentID=DOC-T002&folderID=f1',
     'application/pdf','4.5 MB','john.smith','2026-01-20','2026-02-11T08:00:00Z',0),
    ('DOC-T003','292726','f2','Drawings','Civil','Site_utilities_layout.pdf',
     'https://api-walmart.lucernex.com/servlet/DocumentDownload?documentID=DOC-T003&folderID=f2',
     'application/pdf','1.2 MB','jane.doe','2026-01-10','2026-02-11T08:00:00Z',0),
    ('DOC-T004','292726','f3','Specs','Pipe Specs','PipeSpecs_v2.docx',
     'https://api-walmart.lucernex.com/servlet/DocumentDownload?documentID=DOC-T004&folderID=f3',
     'application/vnd.openxmlformats','350 KB','mike.jones','2026-01-22','2026-02-11T08:00:00Z',0),
    ('DOC-T005','292726','f4','Construction','Photos','Progress_photo_jan.jpg',
     'https://api-walmart.lucernex.com/servlet/DocumentDownload?documentID=DOC-T005&folderID=f4',
     'image/jpeg','5.8 MB','site.manager','2026-01-25','2026-02-11T08:00:00Z',0),
]

for d in test_docs:
    conn.execute(
        'INSERT OR REPLACE INTO lucernex_documents '
        '(doc_id,project_id,folder_id,folder_category,sub_folder,doc_name,'
        'doc_url,doc_type,doc_size,uploaded_by,uploaded_at,last_checked,is_deleted) '
        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', d
    )
conn.commit()

cnt = conn.execute('SELECT COUNT(*) FROM lucernex_documents').fetchone()[0]
print(f"Documents in DB: {cnt}")

conn.close()
print("Done!")
