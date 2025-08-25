# ctiapp/cybok.py
import re, html
from bson import ObjectId
from flask import Blueprint, render_template_string, request, url_for
from .db import coll
from .utils import login_required

bp = Blueprint("cybok", __name__)

@bp.get("/cybok/<sid>")
@login_required
def cybok_view(sid):
    cybok_coll = coll.database["cybok_sections"]
    try:
        oid = ObjectId(sid)
    except Exception:
        return render_template_string("<div style='padding:24px'>Invalid ObjectId: {{ sid }}</div>", sid=sid), 400
    doc = cybok_coll.find_one({"_id": oid})
    if not doc:
        return render_template_string("<div style='padding:24px'>Section Not Found</div>"), 404

    title = html.escape(doc.get("title") or "")
    section = html.escape(doc.get("section") or "")
    content = doc.get("content") or ""
    paras = [f"<p>{html.escape(p.strip())}</p>" for p in re.split(r"\n{2,}", content) if p.strip()]
    body_html = "\n".join(paras) if paras else f"<pre class='text-secondary'>{html.escape(content)}</pre>"

    return render_template_string(r"""
    <!doctype html><html lang="en"><head>
      <meta charset="utf-8"><title>CyBOK · {{ section }} {{ title }}</title>
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    </head><body class="bg-light">
      <nav class="navbar navbar-expand-lg bg-white border-bottom">
        <div class="container-fluid">
          <a class="navbar-brand" href="{{ url_for('feed.feed') }}">CTI Portal</a>
          <span class="ms-2 text-muted">CyBOK</span>
        </div>
      </nav>
      <div class="container py-4">
        <h4 class="mb-1">{{ section }} · {{ title }}</h4>
        <div class="card"><div class="card-body">{{ body|safe }}</div></div>
      </div>
    </body></html>
    """, section=section, title=title, body=body_html)

@bp.get("/cybok/byref")
@login_required
def cybok_byref():
    title = (request.args.get("title") or "").strip()
    section = (request.args.get("section") or "").strip()
    version = (request.args.get("version") or "v1").strip()
    if not title and not section:
        return render_template_string("<div style='padding:24px'>Missing params: ?title or ?section</div>"), 400

    cybok_coll = coll.database["cybok_sections"]
    q = {"version": version}
    if title:   q["title"] = title
    if section: q["section"] = section
    doc = cybok_coll.find_one(q)
    if not doc:
        q2 = {"version": version}
        if title:
            q2["title"] = {"$regex": re.escape(title), "$options": "i"}
        if section:
            q2["section"] = {"$regex": f"^{re.escape(section)}", "$options": "i"}
        doc = cybok_coll.find_one(q2)
    if not doc:
        return render_template_string("<div style='padding:24px'>CyBOK section not found</div>"), 404

    safe_title = html.escape(doc.get("title") or "")
    safe_section = html.escape(doc.get("section") or "")
    content = doc.get("content") or ""
    paras = [f"<p>{html.escape(p.strip())}</p>" for p in re.split(r"\n{2,}", content) if p.strip()]
    body_html = "\n".join(paras) if paras else f"<pre class='text-secondary'>{html.escape(content)}</pre>"

    return render_template_string(r"""
    <!doctype html><html lang="en"><head>
      <meta charset="utf-8"><title>CyBOK · {{ section }} {{ title }}</title>
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    </head><body class="bg-light">
      <nav class="navbar navbar-expand-lg bg-white border-bottom">
        <div class="container-fluid">
          <a class="navbar-brand" href="{{ url_for('feed.feed') }}">CTI Portal</a>
          <span class="ms-2 text-muted">CyBOK</span>
        </div>
      </nav>
      <div class="container py-4">
        <h4 class="mb-1">{{ section }} · {{ title }}</h4>
        <div class="card"><div class="card-body">{{ body|safe }}</div></div>
      </div>
    </body></html>
    """, section=safe_section, title=safe_title, body=body_html)
