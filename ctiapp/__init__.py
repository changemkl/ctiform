# ctiapp/__init__.py
from flask import Flask, redirect, url_for
from .config import SECRET_KEY
from .db import init_mongo
from .auth import bp as auth_bp
from .feed import bp as feed_bp
from .cve import bp as cve_bp
from .cybok import bp as cybok_bp
from .tasks_api import bp as tasks_bp

def create_app():
    app = Flask(__name__, template_folder="templates")
    app.secret_key = SECRET_KEY

    init_mongo(app)

    app.register_blueprint(auth_bp, url_prefix="/auth")
    app.register_blueprint(feed_bp)
    app.register_blueprint(cve_bp)
    app.register_blueprint(cybok_bp)
    app.register_blueprint(tasks_bp)

    @app.route("/")
    def index():
        return redirect(url_for("feed.feed"))

    return app
