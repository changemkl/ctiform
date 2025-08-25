# ctiapp/auth.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone
from .config import ROLES
from .db import users_coll

bp = Blueprint("auth", __name__)

@bp.get("/login")
def login_get():
    return render_template("auth.html", mode="login", next=request.args.get("next") or "")

@bp.post("/login")
def login_post():
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()
    next_url = (request.form.get("next") or "").strip() or url_for("feed.feed")
    u = users_coll.find_one({"username": username})
    if not u or not check_password_hash(u.get("password",""), password):
        flash("Invalid username or password", "danger")
        return render_template("auth.html", mode="login", next=next_url)
    session["uid"] = str(u["_id"])
    return redirect(next_url)

@bp.get("/register")
def register_get():
    return render_template("auth.html", mode="register", next=request.args.get("next") or "")

@bp.post("/register")
def register_post():
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()
    role = (request.form.get("role") or "public").strip()
    next_url = (request.form.get("next") or "").strip() or url_for("feed.feed")
    if role not in ROLES:
        role = "public"
    if not username or not password:
        flash("Please enter username and password", "warning")
        return render_template("auth.html", mode="register", next=next_url)
    if users_coll.find_one({"username": username}):
        flash("Username already exists", "warning")
        return render_template("auth.html", mode="register", next=next_url)

    u = {
        "username": username,
        "password": generate_password_hash(password),
        "role": role,
        "created_at": datetime.now(timezone.utc)
    }
    r = users_coll.insert_one(u)
    session["uid"] = str(r.inserted_id)
    return redirect(next_url)

@bp.get("/logout")
def logout():
    session.pop("uid", None)
    return redirect(url_for("auth.login_get"))
