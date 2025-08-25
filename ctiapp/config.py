# ctiapp/config.py
import os

MONGODB_URI = os.getenv(
    "MONGODB_URI",
    "mongodb+srv://yzhang850:a237342160@cluster0.cficuai.mongodb.net/?retryWrites=true&w=majority&authSource=admin"
)
DB_NAME   = os.getenv("DB_NAME", "cti_platform")
COLL_NAME = os.getenv("COLL_NAME", "threats")
SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-key")
NVD_API_KEY = os.getenv("NVD_API_KEY", "").strip()

ROLES = ["public", "pro", "admin"]
ROLE_ORDER = {r: i for i, r in enumerate(ROLES)}

ARTICLE_SOURCES = ["krebsonsecurity", "msrc_blog", "cisa_kev", "nvd", "exploitdb"]

SOURCE_ROLE = {
    "krebsonsecurity": "public",
    "msrc_blog": "public",
    "cisa_kev": "pro",
    "nvd": "admin",
    "exploitdb": "admin",
}

SOURCE_STYLE = {
    "krebsonsecurity": {"name": "KrebsOnSecurity", "badge": "success", "icon": "🕵️"},
    "msrc_blog":       {"name": "MSRC Blog",       "badge": "primary", "icon": "🛡"},
    "cisa_kev":        {"name": "CISA KEV",        "badge": "warning", "icon": "⚠️"},
    "nvd":             {"name": "NVD (CVE)",       "badge": "danger",  "icon": "📊"},
    "exploitdb":       {"name": "Exploit-DB",      "badge": "dark",    "icon": "💥"},
}
