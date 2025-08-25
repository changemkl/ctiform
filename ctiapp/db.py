# ctiapp/db.py
from pymongo import MongoClient
from .config import MONGODB_URI, DB_NAME, COLL_NAME

mongo = None
coll = None
users_coll = None
sources_coll = None
user_rss_sources_coll = None
user_rss_items_coll = None

def init_mongo(app=None):
    global mongo, coll, users_coll, sources_coll, user_rss_items_coll, user_rss_sources_coll
    mongo = MongoClient(MONGODB_URI)
    db = mongo[DB_NAME]
    coll = db[COLL_NAME]
    users_coll = db["users"]
    sources_coll = db["custom_sources"]
    user_rss_sources_coll = db["user_rss_sources"]
    user_rss_items_coll   = db["user_rss_items"]
