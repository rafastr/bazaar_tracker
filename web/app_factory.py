from __future__ import annotations

import os
import secrets
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask

from web.db_context import close_db, get_hero_colors_map
from web.routes.achievements import achievements_bp
from web.routes.api import api_bp
from web.routes.heroes import heroes_bp
from web.routes.items import items_bp
from web.routes.main import main_bp
from web.routes.manage import manage_bp
from web.routes.runs import runs_bp
from core.config import APP_NAME, APP_VERSION
from core.bootstrap import ensure_resources


def get_bundle_root() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent.parent


APP_ROOT = get_bundle_root()
BUNDLED_RESOURCES = APP_ROOT / "resources"
TEMPLATES_DIR = APP_ROOT / "web" / "templates"
STATIC_DIR = APP_ROOT / "web" / "static"

ensure_resources(BUNDLED_RESOURCES)

load_dotenv()


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(TEMPLATES_DIR),
        static_folder=str(STATIC_DIR),
    )
    app.secret_key = secrets.token_hex(32)

    app.teardown_appcontext(close_db)

    app.register_blueprint(main_bp)
    app.register_blueprint(runs_bp)
    app.register_blueprint(items_bp)
    app.register_blueprint(heroes_bp)
    app.register_blueprint(achievements_bp)
    app.register_blueprint(manage_bp)
    app.register_blueprint(api_bp)

    @app.template_filter("datetime_ymd")
    def datetime_ymd_filter(ts: int) -> str:
        try:
            return datetime.fromtimestamp(int(ts)).strftime("%Y/%m/%d")
        except Exception:
            return ""

    @app.context_processor
    def inject_app_meta():
        return {
            "app_name": APP_NAME,
            "app_version": APP_VERSION,
        }

    @app.context_processor
    def inject_hero_colors():
        return {
            "hero_colors": get_hero_colors_map(),
        }

    return app
