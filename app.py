"""
Oracle Migration Coordinator
Flask application — entry point
"""

import os
from flask import Flask
from config import Config
from extensions import db, migrate


def _apply_schema_migrations(app, db):
    """
    Применяет недостающие ALTER TABLE миграции к существующим таблицам.
    Безопасно: проверяет наличие колонки перед добавлением.
    """
    migrations = [
        # migration_jobs: добавить debezium_connector_name если отсутствует
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='migration_jobs'
                  AND column_name='debezium_connector_name'
            ) THEN
                ALTER TABLE migration_jobs ADD COLUMN debezium_connector_name VARCHAR(300);
            END IF;
        END$$;
        """,
        # Создать таблицы migration_preflight_results и debezium_connectors если не существуют
        # (db.create_all() уже делает это, но на случай если они были пропущены)
    ]

    try:
        with db.engine.connect() as conn:
            for sql in migrations:
                conn.execute(db.text(sql))
            conn.commit()
        app.logger.info("Schema migrations applied successfully")
    except Exception as e:
        app.logger.warning(f"Schema migration warning (non-fatal): {e}")


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    # Initialize extensions
    db.init_app(app)
    migrate.init_app(app, db)

    # Import models so Flask-Migrate can detect them
    with app.app_context():
        from models import (  # noqa: F401
            DbConnection, KafkaConnection, KafkaConnectConnection,
            MigrationJob, MigrationPreflightResult, DebeziumConnector,
        )
        try:
            db.create_all()
            _apply_schema_migrations(app, db)
        except Exception as e:
            app.logger.warning(f"Could not create DB tables (DB may be unavailable): {e}")

    # Register blueprints
    from routes.api import api_bp
    from routes.ui import ui_bp
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(ui_bp)

    return app


if __name__ == "__main__":
    app = create_app()
    print("Oracle Migration Coordinator → http://localhost:5000")
    app.run(debug=True, host="0.0.0.0", port=5000)
