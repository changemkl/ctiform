# ctiapp/feed.py
import math, re
from datetime import datetime, timezone
from bson import ObjectId
from flask import Blueprint, render_template, request, make_response, redirect, url_for, flash
from .config import ARTICLE_SOURCES, SOURCE_ROLE, SOURCE_STYLE, ROLES, ROLE_ORDER
from .db import coll, user_rss_sources_coll, user_rss_items_coll
from .utils import (
    parse_dt, role_allows, fmt_ts, current_role, current_username, login_required,
    extract_cves_from_text, brief_for_public, threat_points_for_pro
)

bp = Blueprint("feed", __name__)

@bp.app_context_processor
def inject_helpers():
    return dict(
        SOURCE_STYLE=SOURCE_STYLE,
        extract_cves_from_text=extract_cves_from_text,
        brief_for_public=brief_for_public,
        threat_points_for_pro=threat_points_for_pro,
        fmt_ts=fmt_ts,
        ROLES=ROLES
    )

@bp.route("/feed")
@login_required
def feed():
    role = current_role()
    owner_name = current_username()
    q = (request.args.get("q") or "").strip()
    since = parse_dt(request.args.get("since"))
    until = parse_dt(request.args.get("until"))
    page = max(1, int(request.args.get("page", 1)))
    page_size = min(100, max(5, int(request.args.get("page_size", 20))))

    all_filters = ["rss"] + ARTICLE_SOURCES
    sel_sources = request.args.getlist("source")
    if not sel_sources:
        sel_sources = ARTICLE_SOURCES[:]
    rss_mode = ("rss" in sel_sources) and (set(sel_sources) == {"rss"})

    if rss_mode:
        filt = {"owner_username": owner_name}
        if q:
            filt["$or"] = [
                {"title": {"$regex": q, "$options": "i"}},
                {"content": {"$regex": q, "$options": "i"}},
            ]
        if since or until:
            rng = {}
            if since: rng["$gte"] = since
            if until: rng["$lte"] = until
            filt["timestamp"] = rng

        total = user_rss_items_coll.count_documents(filt)
        items = list(
            user_rss_items_coll.find(
                filt,
                {"title":1,"url":1,"content":1,"timestamp":1,"feed_url":1}
            )
            .sort([("timestamp", -1)])
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
    else:
        req_sources = [s for s in sel_sources if s in ARTICLE_SOURCES]
        allowed_sources = [s for s in req_sources if role_allows(role, SOURCE_ROLE.get(s, "public"))]
        items = []; total = 0
        if allowed_sources:
            branch = {"source": {"$in": allowed_sources}, "allowed_roles": role}
            if q:
                branch["$or"] = [
                    {"title": {"$regex": q, "$options": "i"}},
                    {"content": {"$regex": q, "$options": "i"}},
                ]
            if since or until:
                rng = {}
                if since: rng["$gte"] = since
                if until: rng["$lte"] = until
                branch["timestamp"] = rng
            filt = branch
            total = coll.count_documents(filt)
            items = list(
                coll.find(
                    filt,
                    {
                        "title":1,"url":1,"content":1,"timestamp":1,"source":1,"min_role":1,
                        "nvd_cvss":1,"nvd_cwes":1,"nvd_refs":1,
                        "edb_id":1,"edb_cves":1,
                        "recommendations.cybok": 1,
                    }
                )
                .sort([("timestamp", -1)])
                .skip((page - 1) * page_size)
                .limit(page_size)
            )

    pages = max(1, math.ceil(total / page_size))
    pager = {"total": total, "page": page, "pages": pages,
             "page_size": page_size, "has_prev": page > 1, "has_next": page < pages,
             "prev": page - 1, "next": page + 1}

    rss_list = list(
        user_rss_sources_coll.find({"owner_username": owner_name}).sort([("updated_at", -1)])
    )

    resp = make_response(render_template(
        "feed.html",
        items=items, pager=pager, q=q,
        sources=sel_sources,
        source_label={k: v["name"] for k, v in SOURCE_STYLE.items()},
        all_sources=ARTICLE_SOURCES,
        all_filters=all_filters,
        rss_list=rss_list,
        rss_mode=rss_mode
    ))
    return resp

@bp.get("/item/<id>")
@login_required
def item_detail(id):
    from flask import abort
    try:
        oid = ObjectId(id)
    except Exception:
        abort(404)
    doc = coll.find_one({"_id": oid})
    if not doc:
        abort(404)
    st = SOURCE_STYLE.get(doc.get("source"), {"name": doc.get("source","Other"), "badge":"secondary", "icon":"📰"})
    return render_template("item.html", it=doc, st=st)

@bp.post("/add_rss")
@login_required
def add_rss():
    from worker.tasks import run_fetch_user_rss_once
    owner_name = current_username()
    rss_url = (request.form.get("rss_url") or "").strip()
    role_sel = (request.form.get("rss_role") or "public").strip().lower()
    if role_sel not in ROLES:
        role_sel = "public"
    if not rss_url or not re.match(r"^https?://", rss_url, re.I):
        flash("Invalid RSS URL.", "warning")
        return redirect(url_for("feed.feed"))

    now = datetime.now(timezone.utc)
    user_rss_sources_coll.update_one(
        {"owner_username": owner_name, "url": rss_url},
        {"$set": {
            "owner_username": owner_name,
            "url": rss_url,
            "mode": "rss",
            "min_role": role_sel,
            "allowed_roles": [r for r in ROLES if ROLE_ORDER[r] >= ROLE_ORDER[role_sel]],
            "enabled": True,
            "updated_at": now,
        }, "$setOnInsert": {
            "created_at": now,
            "last_crawled": None,
            "last_status": None,
        }},
        upsert=True
    )

    ar = run_fetch_user_rss_once.delay(owner_name, rss_url, 200)
    flash(f"RSS saved & initial fetch queued (task: {ar.id})", "success")
    return redirect(url_for("feed.feed"))

@bp.post("/sources/<sid>/toggle")
@login_required
def source_toggle(sid):
    owner_name = current_username()
    try:
        from bson import ObjectId
        oid = ObjectId(sid)
    except Exception:
        flash("Invalid source id.", "warning")
        return redirect(url_for("feed.feed"))
    doc = user_rss_sources_coll.find_one({"_id": oid, "owner_username": owner_name})
    if not doc:
        flash("Source not found or no permission.", "warning")
        return redirect(url_for("feed.feed"))
    new_enabled = not bool(doc.get("enabled", True))
    user_rss_sources_coll.update_one({"_id": oid, "owner_username": owner_name}, {"$set": {"enabled": new_enabled, "updated_at": datetime.now(timezone.utc)}})
    flash(("Enabled" if new_enabled else "Disabled") + " RSS source.", "info")
    return redirect(url_for("feed.feed"))

@bp.post("/sources/<sid>/delete")
@login_required
def source_delete(sid):
    owner_name = current_username()
    try:
        from bson import ObjectId
        oid = ObjectId(sid)
    except Exception:
        flash("Invalid source id.", "warning")
        return redirect(url_for("feed.feed"))
    res = user_rss_sources_coll.delete_one({"_id": oid, "owner_username": owner_name})
    if res.deleted_count == 0:
        flash("Source not found or no permission.", "warning")
    else:
        flash("RSS source deleted.", "danger")
    return redirect(url_for("feed.feed"))
