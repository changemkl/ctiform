# ctiapp/cve.py
from flask import Blueprint, render_template
from .utils import login_required, nvd_get_cve_raw, nvd_parse_summary, current_role

bp = Blueprint("cve", __name__)

@bp.get("/cve/<cve_id>")
@login_required
def cve_detail(cve_id):
    role = current_role()
    data = {}
    try:
        data = nvd_parse_summary(nvd_get_cve_raw(cve_id))
    except Exception:
        data = {}
    return render_template("cve.html", cve_id=cve_id, data=data, role=role)
