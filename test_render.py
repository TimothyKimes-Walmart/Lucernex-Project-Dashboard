import sys
import traceback
sys.path.insert(0, 'app')

from queries import get_project_detail, get_project_documents_tree, get_project_doc_count, get_project_doc_last_checked
from jinja2 import Environment, FileSystemLoader

try:
    project = get_project_detail('273851')
    print(f'project found: {project is not None}')
    doc_tree = get_project_documents_tree('273851')
    doc_count = get_project_doc_count('273851')
    doc_last_checked = get_project_doc_last_checked('273851')
    print(f'docs: count={doc_count}, tree_len={len(doc_tree)}, lc={doc_last_checked}')

    env = Environment(loader=FileSystemLoader('app/templates'))
    env.filters['currency'] = lambda v: f'${v:,.2f}' if v else '$0.00'
    t = env.get_template('project_detail.html')
    html = t.render(
        project=project, doc_tree=doc_tree, doc_count=doc_count,
        doc_last_checked=doc_last_checked, request=None
    )
    print(f'RENDER OK, html length: {len(html)}')
except Exception:
    traceback.print_exc()
