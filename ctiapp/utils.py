# ctiapp/utils.py
import re, requests
from datetime import datetime
from functools import wraps, lru_cache
from flask import session, redirect, url_for, request
from bson import ObjectId
from dateutil import parser as dateparser
from .config import ROLES, ROLE_ORDER, NVD_API_KEY
from .db import users_coll

def parse_dt(s):
    if not s: return None
    try: return dateparser.parse(s)
    except Exception: return None

def fmt_ts(ts, fmt="%Y-%m-%d %H:%M:%S"):
    if not ts: return ""
    if isinstance(ts, datetime): return ts.strftime(fmt)
    if isinstance(ts, str):
        try:
            dt = dateparser.parse(ts); return dt.strftime(fmt) if dt else ""
        except Exception: return ts
    return ""

def role_allows(current_role: str, min_role: str) -> bool:
    return ROLE_ORDER.get(current_role, 0) >= ROLE_ORDER.get(min_role, 0)

def get_current_user():
    uid = session.get("uid")
    if not uid: return None
    try:
        return users_coll.find_one({"_id": ObjectId(uid)})
    except Exception:
        return None

def current_username():
    u = get_current_user()
    return u["username"] if u else None

def current_role():
    u = get_current_user()
    return u["role"] if u and u.get("role") in ROLES else "public"

def login_required(view):
    @wraps(view)
    def _wrapped(*args, **kwargs):
        if not get_current_user():
            return redirect(url_for("auth.login_get", next=request.path))
        return view(*args, **kwargs)
    return _wrapped

CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.I)
def extract_cves_from_text(txt: str):
    if not txt: return []
    return sorted(set(m.upper() for m in CVE_RE.findall(txt)))

def brief_for_public(text: str, length=200):
    if not text: return ""
    t = re.sub(r"<[^>]+>", " ", text)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:length] + ("…" if len(t) > length else "")

def threat_points_for_pro(text: str):
    if not text: return ""
    body = re.sub(r"<[^>]+>", " ", text)
    body = re.sub(r"\s+", " ", body)
    sentences = re.split(r"(?<=[。.!?])\s+", body)
    SIGNALS = ("critical","remote execution","RCE","exploit","zero-day","in the wild","privilege escalation","bypass","vulnerability","attack")
    picked = [s for s in sentences if any(k.lower() in s.lower() for k in SIGNALS)]
    if not picked and sentences: picked = sentences[:2]
    return " ".join(picked[:2])

try:
    from readability import Document
except Exception:
    Document = None
try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

def extract_main_content(html: str):
    title = ""; text = ""
    if Document:
        try:
            doc = Document(html)
            title = (doc.short_title() or "").strip()
            summary_html = doc.summary()
            import re as _r
            text = _r.sub(r"<[^>]+>", " ", summary_html or "")
        except Exception:
            pass
    if not text and BeautifulSoup:
        try:
            soup = BeautifulSoup(html, "html.parser")
            article = soup.find("article") or soup
            paras = [p.get_text(" ", strip=True) for p in article.find_all("p")]
            text = " ".join(paras).strip()
            if not title and soup.title and soup.title.string:
                title = soup.title.string.strip()
        except Exception:
            pass
    if not text:
        import re as _r
        text = _r.sub(r"<[^>]+>", " ", html or "")
    import re as _r
    text = _r.sub(r"\s+", " ", text).strip()
    return title, text

NVD_API = "https://services.nvd.nist.gov/rest/json/cves/2.0"
def _nvd_headers():
    h = {"User-Agent": "cti-portal/1.0"}
    if NVD_API_KEY: h["apiKey"] = NVD_API_KEY
    return h

@lru_cache(maxsize=256)
def nvd_get_cve_raw(cve_id: str):
    r = requests.get(NVD_API, params={"cveId": cve_id}, headers=_nvd_headers(), timeout=15)
    r.raise_for_status()
    return r.json()

def nvd_parse_summary(nvd_json: dict):
    vulns = (nvd_json or {}).get("vulnerabilities") or []
    if not vulns: return {}
    cve = vulns[0].get("cve") or {}
    descriptions = cve.get("descriptions") or []
    desc = ""
    for d in descriptions:
        if d.get("lang") == "en":
            desc = d.get("value","")
            break
    metrics = cve.get("metrics") or {}
    cvss = {}
    for key in ("cvssMetricV31","cvssMetricV30","cvssMetricV2"):
        if metrics.get(key):
            m = metrics[key][0]; data = m.get("cvssData", {})
            cvss = {
                "version": data.get("version"),
                "baseScore": data.get("baseScore"),
                "baseSeverity": m.get("baseSeverity"),
                "vectorString": data.get("vectorString"),
                "exploitabilityScore": m.get("exploitabilityScore"),
                "impactScore": m.get("impactScore"),
            }
            break
    weaknesses = []
    for w in (cve.get("weaknesses") or []):
        for d in (w.get("description") or []):
            if d.get("value"): weaknesses.append(d["value"])
    weaknesses = sorted(set(weaknesses))
    refs = []
    for r in (cve.get("references") or []):
        refs.append({"url": r.get("url"), "tags": r.get("tags") or []})
    return {"id": cve.get("id"), "description": desc, "cvss": cvss, "weaknesses": weaknesses, "references": refs}
