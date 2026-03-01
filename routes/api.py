import re
import uuid
import random
from datetime import datetime
from flask import Blueprint, jsonify, request, current_app

api_bp = Blueprint("api", __name__)

# ─────────────────────────────────────────────────────────────────────────────
# Mock state — Jobs & Workers remain in-memory
# ─────────────────────────────────────────────────────────────────────────────

JOBS = [
    {
        "id": 1,
        "name": "PROD_ORDERS → DWH",
        "source": "PROD_DB",
        "target": "DWH_DB",
        "table": "ORDERS",
        "status": "running",
        "progress": 67,
        "chunks_total": 1200,
        "chunks_done": 804,
        "chunks_failed": 3,
        "rows_migrated": 8_420_317,
        "rows_total": 12_567_000,
        "started_at": "2025-02-26 08:14:22",
        "eta": "2025-02-26 11:42:00",
        "scn": "1847392847",
        "workers": 4,
        "throughput": "12,400 rows/s",
    },
    {
        "id": 2,
        "name": "PROD_CUSTOMERS → DWH",
        "source": "PROD_DB",
        "target": "DWH_DB",
        "table": "CUSTOMERS",
        "status": "completed",
        "progress": 100,
        "chunks_total": 240,
        "chunks_done": 240,
        "chunks_failed": 0,
        "rows_migrated": 2_100_000,
        "rows_total": 2_100_000,
        "started_at": "2025-02-26 06:00:00",
        "eta": "2025-02-26 07:55:00",
        "scn": "1847100021",
        "workers": 2,
        "throughput": "—",
    },
    {
        "id": 3,
        "name": "AUDIT_LOG → ARCHIVE",
        "source": "PROD_DB",
        "target": "ARCHIVE_DB",
        "table": "AUDIT_LOG",
        "status": "failed",
        "progress": 23,
        "chunks_total": 800,
        "chunks_done": 184,
        "chunks_failed": 12,
        "rows_migrated": 920_000,
        "rows_total": 4_000_000,
        "started_at": "2025-02-26 09:30:00",
        "eta": None,
        "scn": "1847401002",
        "workers": 0,
        "throughput": "—",
    },
    {
        "id": 4,
        "name": "TRANSACTIONS → DWH",
        "source": "PROD_DB",
        "target": "DWH_DB",
        "table": "TRANSACTIONS",
        "status": "pending",
        "progress": 0,
        "chunks_total": 3600,
        "chunks_done": 0,
        "chunks_failed": 0,
        "rows_migrated": 0,
        "rows_total": 36_000_000,
        "started_at": None,
        "eta": None,
        "scn": None,
        "workers": 0,
        "throughput": "—",
    },
]

WORKERS = [
    {"id": "worker_01", "container": "migrator_worker_1", "status": "active",  "job": "job_001", "chunk": "ROWID 0001-0240", "cpu": 38, "mem": 512, "rows_s": 3200, "uptime": "2h 14m", "heartbeat": "2s ago"},
    {"id": "worker_02", "container": "migrator_worker_2", "status": "active",  "job": "job_001", "chunk": "ROWID 0241-0480", "cpu": 42, "mem": 498, "rows_s": 3100, "uptime": "2h 14m", "heartbeat": "1s ago"},
    {"id": "worker_03", "container": "migrator_worker_3", "status": "active",  "job": "job_001", "chunk": "ROWID 0481-0720", "cpu": 35, "mem": 520, "rows_s": 3050, "uptime": "2h 14m", "heartbeat": "3s ago"},
    {"id": "worker_04", "container": "migrator_worker_4", "status": "active",  "job": "job_001", "chunk": "ROWID 0721-0960", "cpu": 41, "mem": 505, "rows_s": 3150, "uptime": "2h 14m", "heartbeat": "1s ago"},
    {"id": "worker_05", "container": "migrator_worker_5", "status": "idle",   "job": None,      "chunk": None,             "cpu": 1,  "mem": 128, "rows_s": 0,    "uptime": "2h 14m", "heartbeat": "5s ago"},
    {"id": "worker_06", "container": "migrator_worker_6", "status": "idle",   "job": None,      "chunk": None,             "cpu": 0,  "mem": 124, "rows_s": 0,    "uptime": "2h 14m", "heartbeat": "4s ago"},
    {"id": "worker_07", "container": "migrator_worker_7", "status": "error",  "job": "job_003", "chunk": "ROWID 0181-0200","cpu": 0,  "mem": 210, "rows_s": 0,    "uptime": "57m",    "heartbeat": "48s ago"},
]


# ─────────────────────────────────────────────────────────────────────────────
# Helper: safe DB access
# ─────────────────────────────────────────────────────────────────────────────

def _db_available():
    """Check if DB is accessible."""
    try:
        from extensions import db
        db.session.execute(db.text("SELECT 1"))
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Stats / Jobs / Workers (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

@api_bp.get("/stats")
def get_stats():
    return jsonify({
        "total_rows_migrated": 11_440_317,
        "active_jobs": sum(1 for j in JOBS if j["status"] == "running"),
        "active_workers": sum(1 for w in WORKERS if w["status"] == "active"),
        "total_throughput": "12,400 rows/s",
        "uptime": "2h 14m",
        "pg_state_size": "248 MB",
    })


@api_bp.get("/jobs")
def get_jobs():
    """
    Возвращает список jobs.
    Если БД доступна — из таблицы migration_jobs.
    Fallback — mock-данные для демо.
    """
    try:
        from models import MigrationJob
        jobs = MigrationJob.query.order_by(MigrationJob.created_at.desc()).all()
        return jsonify([j.to_dict() for j in jobs])
    except Exception as e:
        current_app.logger.warning(f"DB unavailable for /api/jobs, using mock: {e}")
        return jsonify(JOBS)


@api_bp.post("/jobs")
def create_job():
    """
    Создать новый migration job.
    Обязательные поля: name, source_conn_id, target_conn_id, source_table.
    Опциональные: target_table, chunk_size, chunk_strategy, migration_mode, filter_clause.
    """
    try:
        from models import MigrationJob, DbConnection
        from extensions import db

        data = request.get_json(force=True) or {}

        # Валидация обязательных полей
        required = ["name", "source_conn_id", "target_conn_id", "source_table"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return jsonify({"ok": False, "error": f"Missing required fields: {', '.join(missing)}"}), 400

        # Проверяем что подключения существуют
        source_conn = DbConnection.query.get(data["source_conn_id"])
        if not source_conn:
            return jsonify({"ok": False, "error": f"Source connection id={data['source_conn_id']} not found"}), 404

        target_conn = DbConnection.query.get(data["target_conn_id"])
        if not target_conn:
            return jsonify({"ok": False, "error": f"Target connection id={data['target_conn_id']} not found"}), 404

        # Валидация chunk_strategy
        valid_strategies = ("rowid", "pk_range", "date_range", "hash")
        chunk_strategy = data.get("chunk_strategy", "rowid")
        if chunk_strategy not in valid_strategies:
            return jsonify({"ok": False, "error": f"Invalid chunk_strategy. Valid: {valid_strategies}"}), 400

        # Валидация migration_mode
        valid_modes = ("bulk_cdc", "bulk_only", "cdc_only")
        migration_mode = data.get("migration_mode", "bulk_cdc")
        if migration_mode not in valid_modes:
            return jsonify({"ok": False, "error": f"Invalid migration_mode. Valid: {valid_modes}"}), 400

        # Валидация chunk_size
        try:
            chunk_size = int(data.get("chunk_size", 10000))
            if chunk_size < 100:
                return jsonify({"ok": False, "error": "chunk_size must be >= 100"}), 400
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "chunk_size must be an integer"}), 400

        job = MigrationJob(
            name            = data["name"].strip(),
            source_conn_id  = int(data["source_conn_id"]),
            target_conn_id  = int(data["target_conn_id"]),
            source_table    = data["source_table"].strip().upper(),
            target_table    = (data.get("target_table") or "").strip().upper() or None,
            chunk_size      = chunk_size,
            chunk_strategy  = chunk_strategy,
            migration_mode  = migration_mode,
            filter_clause   = (data.get("filter_clause") or "").strip() or None,
            status          = "pending",
        )
        db.session.add(job)
        db.session.commit()

        current_app.logger.info(f"Created migration job id={job.id} name='{job.name}'")
        return jsonify({"ok": True, "job": job.to_dict()}), 201

    except Exception as e:
        current_app.logger.error(f"create_job error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@api_bp.get("/jobs/<int:job_id>")
def get_job(job_id):
    """Получить один job по ID."""
    try:
        from models import MigrationJob
        job = MigrationJob.query.get_or_404(job_id)
        return jsonify(job.to_dict())
    except Exception as e:
        current_app.logger.error(f"get_job error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.delete("/jobs/<int:job_id>")
def delete_job(job_id):
    """Удалить job (только если статус pending или failed или cancelled)."""
    try:
        from models import MigrationJob, DebeziumConnector
        from extensions import db
        job = MigrationJob.query.get_or_404(job_id)

        if job.status not in ("pending", "failed", "cancelled"):
            return jsonify({
                "ok": False,
                "error": f"Cannot delete job in status '{job.status}'. "
                         f"Only pending/failed/cancelled jobs can be deleted."
            }), 409

        # Удалить связанные Debezium коннекторы
        DebeziumConnector.query.filter_by(job_id=job.id).delete()

        db.session.delete(job)
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        current_app.logger.error(f"delete_job error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.post("/jobs/<int:job_id>/action")
def job_action(job_id):
    """
    Выполнить действие над job.
    Поддерживаемые действия: cancel, retry.
    (start/pause — будут реализованы при интеграции с воркерами)
    """
    try:
        from models import MigrationJob
        from extensions import db

        data = request.get_json(force=True) or {}
        action = data.get("action")

        job = MigrationJob.query.get_or_404(job_id)

        # Допустимые переходы статусов
        transitions = {
            "cancel": {
                "allowed_from": ("pending", "running", "cdc_starting", "bulk_done"),
                "new_status": "cancelled",
            },
            "retry": {
                "allowed_from": ("failed", "cancelled"),
                "new_status": "pending",
            },
        }

        if action not in transitions:
            return jsonify({"ok": False, "error": f"Unknown action '{action}'. Valid: {list(transitions.keys())}"}), 400

        transition = transitions[action]
        if job.status not in transition["allowed_from"]:
            return jsonify({
                "ok": False,
                "error": f"Action '{action}' not allowed for job in status '{job.status}'. "
                         f"Allowed from: {transition['allowed_from']}"
            }), 409

        job.status = transition["new_status"]
        if action == "retry":
            # Сбрасываем прогресс при повторном запуске
            job.cdc_owner_worker_id = None
            job.cdc_started_at = None
            job.cdc_heartbeat_at = None
        job.updated_at = datetime.utcnow()
        db.session.commit()

        return jsonify({"ok": True, "job_id": job_id, "action": action, "new_status": job.status})

    except Exception as e:
        current_app.logger.error(f"job_action error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@api_bp.get("/workers")
def get_workers():
    for w in WORKERS:
        if w["status"] == "active":
            w["cpu"]    = max(10, min(90, w["cpu"] + random.randint(-5, 5)))
            w["rows_s"] = max(500, w["rows_s"] + random.randint(-200, 200))
    return jsonify(WORKERS)


# ─────────────────────────────────────────────────────────────────────────────
# Legacy /api/connections — backward compatibility
# ─────────────────────────────────────────────────────────────────────────────

@api_bp.get("/connections")
def get_connections():
    """Return all connections in legacy format for backward compatibility."""
    try:
        from models import DbConnection, KafkaConnection, KafkaConnectConnection
        result = []

        for c in DbConnection.query.all():
            result.append({
                "name":    c.name,
                "type":    c.db_type.capitalize() if c.db_type else "Unknown",
                "host":    c.host,
                "port":    c.port,
                "sid":     c.database or c.service_name or "—",
                "user":    c.username,
                "status":  c.status,
                "version": "—",
                "role":    c.role,
            })

        for k in KafkaConnection.query.all():
            result.append({
                "name":    k.name,
                "type":    "Kafka",
                "host":    k.bootstrap_servers,
                "port":    None,
                "sid":     "—",
                "user":    k.sasl_username or "—",
                "status":  k.status,
                "version": "—",
            })

        for kc in KafkaConnectConnection.query.all():
            result.append({
                "name":    kc.name,
                "type":    "KafkaConnect",
                "host":    kc.url,
                "port":    None,
                "sid":     "—",
                "user":    kc.username or "—",
                "status":  kc.status,
                "version": "—",
            })

        return jsonify(result)
    except Exception as e:
        current_app.logger.warning(f"DB unavailable for /api/connections: {e}")
        return jsonify([])


# ─────────────────────────────────────────────────────────────────────────────
# DB Connections CRUD
# ─────────────────────────────────────────────────────────────────────────────

@api_bp.get("/connections/db")
def get_db_connections():
    try:
        from models import DbConnection
        conns = DbConnection.query.order_by(DbConnection.created_at.desc()).all()
        return jsonify([c.to_dict() for c in conns])
    except Exception as e:
        current_app.logger.error(f"get_db_connections error: {e}")
        return jsonify({"error": str(e)}), 503


@api_bp.post("/connections/db")
def create_db_connection():
    try:
        from models import DbConnection
        from extensions import db
        data = request.get_json(force=True) or {}

        db_type = (data.get("db_type") or "").lower()

        # Базовые обязательные поля
        required = ["name", "db_type", "host", "port", "username", "password"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

        # Для PostgreSQL поле database обязательно
        if db_type == "postgresql" and not data.get("database"):
            return jsonify({"error": "Missing required fields: database"}), 400

        # Для Oracle нужен хотя бы SID или Service Name
        if db_type == "oracle" and not data.get("database") and not data.get("service_name"):
            return jsonify({"error": "For Oracle connection specify SID (database) or Service Name"}), 400

        conn = DbConnection(
            name         = data["name"],
            role         = data.get("role", "SOURCE"),
            db_type      = db_type,
            host         = data["host"],
            port         = int(data["port"]),
            database     = data.get("database") or None,
            username     = data["username"],
            password     = data["password"],
            service_name = data.get("service_name") or None,
            ssl_mode     = data.get("ssl_mode") or None,
            status       = "disconnected",
        )
        db.session.add(conn)
        db.session.commit()
        return jsonify(conn.to_dict()), 201
    except Exception as e:
        current_app.logger.error(f"create_db_connection error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.put("/connections/db/<int:conn_id>")
def update_db_connection(conn_id):
    try:
        from models import DbConnection
        from extensions import db
        conn = DbConnection.query.get_or_404(conn_id)
        data = request.get_json(force=True) or {}

        for field in ["name", "role", "host", "database", "username", "password", "service_name", "ssl_mode"]:
            if field in data:
                setattr(conn, field, data[field])
        if "db_type" in data:
            conn.db_type = data["db_type"].lower()
        if "port" in data:
            conn.port = int(data["port"])

        conn.updated_at = datetime.utcnow()
        db.session.commit()
        return jsonify(conn.to_dict())
    except Exception as e:
        current_app.logger.error(f"update_db_connection error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.delete("/connections/db/<int:conn_id>")
def delete_db_connection(conn_id):
    try:
        from models import DbConnection
        from extensions import db
        conn = DbConnection.query.get_or_404(conn_id)
        db.session.delete(conn)
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        current_app.logger.error(f"delete_db_connection error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.get("/connections/db/<int:conn_id>/schemas")
def get_db_schemas(conn_id):
    """
    Получить список схем (owners/namespaces) из Oracle или PostgreSQL.
    Для Oracle: ALL_TABLES DISTINCT owner (только схемы с таблицами, без системных).
    Для PostgreSQL: information_schema.schemata.
    """
    try:
        from models import DbConnection
        conn = DbConnection.query.get_or_404(conn_id)
        schemas = _fetch_schemas(conn)
        return jsonify({"schemas": schemas})
    except Exception as e:
        current_app.logger.error(f"get_db_schemas error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.get("/connections/db/<int:conn_id>/tables")
def get_db_tables(conn_id):
    """
    Получить список таблиц в указанной схеме.
    Query param: ?schema=SCHEMA_NAME
    """
    schema = request.args.get("schema", "").strip().upper()
    if not schema:
        return jsonify({"error": "Query parameter 'schema' is required"}), 400
    try:
        from models import DbConnection
        conn = DbConnection.query.get_or_404(conn_id)
        tables = _fetch_tables(conn, schema)
        return jsonify({"schema": schema, "tables": tables})
    except Exception as e:
        current_app.logger.error(f"get_db_tables error: {e}")
        return jsonify({"error": str(e)}), 500


def _fetch_schemas(conn):
    """Получить список схем из БД."""
    try:
        if conn.db_type == "oracle":
            import oracledb
            if conn.service_name:
                dsn = oracledb.makedsn(conn.host, conn.port, service_name=conn.service_name)
            elif conn.database:
                dsn = oracledb.makedsn(conn.host, conn.port, sid=conn.database)
            else:
                raise ValueError("Oracle connection requires Service Name or SID")

            c = oracledb.connect(user=conn.username, password=conn.password, dsn=dsn)
            cur = c.cursor()
            # Схемы у которых есть хотя бы одна таблица, без системных
            cur.execute("""
                SELECT DISTINCT owner
                FROM all_tables
                WHERE owner NOT IN (
                    'SYS','SYSTEM','OUTLN','DBSNMP','APPQOSSYS','DBSFWUSER',
                    'GGSYS','ANONYMOUS','CTXSYS','DVSYS','DVF','GSMADMIN_INTERNAL',
                    'MDSYS','OLAPSYS','XDB','WMSYS','OJVMSYS','ORDSYS','ORDDATA',
                    'ORDPLUGINS','SI_INFORMTN_SCHEMA','LBACSYS','APEX_PUBLIC_USER',
                    'FLOWS_FILES','APEX_030200','APEX_040000','APEX_040200',
                    'AUDSYS','REMOTE_SCHEDULER_AGENT','SYSBACKUP','SYSDG',
                    'SYSKM','SYSRAC','SYS$UMF','MDDATA','SPATIAL_CSW_ADMIN_USR',
                    'SPATIAL_WFS_ADMIN_USR','ORDS_METADATA','ORDS_PUBLIC_USER'
                )
                ORDER BY owner
            """)
            schemas = [row[0] for row in cur.fetchall()]
            c.close()
            return schemas

        elif conn.db_type == "postgresql":
            import psycopg2
            kwargs = dict(
                host=conn.host, port=conn.port, dbname=conn.database,
                user=conn.username, password=conn.password, connect_timeout=5,
            )
            if conn.ssl_mode:
                kwargs["sslmode"] = conn.ssl_mode
            c = psycopg2.connect(**kwargs)
            cur = c.cursor()
            cur.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast',
                                          'pg_temp_1','pg_toast_temp_1')
                  AND schema_name NOT LIKE 'pg_%'
                ORDER BY schema_name
            """)
            schemas = [row[0] for row in cur.fetchall()]
            c.close()
            return schemas

        else:
            raise ValueError(f"Unknown db_type: {conn.db_type}")

    except Exception as e:
        raise RuntimeError(f"Failed to fetch schemas: {e}") from e


def _fetch_tables(conn, schema):
    """Получить список таблиц в схеме."""
    try:
        if conn.db_type == "oracle":
            import oracledb
            if conn.service_name:
                dsn = oracledb.makedsn(conn.host, conn.port, service_name=conn.service_name)
            elif conn.database:
                dsn = oracledb.makedsn(conn.host, conn.port, sid=conn.database)
            else:
                raise ValueError("Oracle connection requires Service Name or SID")

            c = oracledb.connect(user=conn.username, password=conn.password, dsn=dsn)
            cur = c.cursor()
            cur.execute("""
                SELECT table_name
                FROM all_tables
                WHERE owner = :schema_name
                ORDER BY table_name
            """, schema_name=schema)
            tables = [row[0] for row in cur.fetchall()]
            c.close()
            return tables

        elif conn.db_type == "postgresql":
            import psycopg2
            kwargs = dict(
                host=conn.host, port=conn.port, dbname=conn.database,
                user=conn.username, password=conn.password, connect_timeout=5,
            )
            if conn.ssl_mode:
                kwargs["sslmode"] = conn.ssl_mode
            c = psycopg2.connect(**kwargs)
            cur = c.cursor()
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (schema.lower(),))
            tables = [row[0].upper() for row in cur.fetchall()]
            c.close()
            return tables

        else:
            raise ValueError(f"Unknown db_type: {conn.db_type}")

    except Exception as e:
        raise RuntimeError(f"Failed to fetch tables: {e}") from e


@api_bp.post("/connections/db/<int:conn_id>/test")
def test_db_connection(conn_id):
    try:
        from models import DbConnection
        from extensions import db
        conn = DbConnection.query.get_or_404(conn_id)

        status, message = _test_db_conn(conn)
        conn.status = status
        conn.last_tested_at = datetime.utcnow()
        db.session.commit()
        return jsonify({"status": status, "message": message})
    except Exception as e:
        current_app.logger.error(f"test_db_connection error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


def _test_db_conn(conn):
    """Attempt real connection to Oracle or PostgreSQL."""
    try:
        if conn.db_type == "oracle":
            import oracledb
            # Приоритет: Service Name > SID
            # Service Name — современный и рекомендуемый способ подключения к Oracle (9i+)
            # SID — устаревший способ, поддерживается для совместимости
            if conn.service_name:
                dsn = oracledb.makedsn(conn.host, conn.port, service_name=conn.service_name)
            elif conn.database:
                dsn = oracledb.makedsn(conn.host, conn.port, sid=conn.database)
            else:
                return "error", "Oracle connection requires either Service Name or SID"
            c = oracledb.connect(user=conn.username, password=conn.password, dsn=dsn)
            ver = c.version
            c.close()
            return "connected", f"Oracle {ver}"

        elif conn.db_type == "postgresql":
            import psycopg2
            kwargs = dict(
                host=conn.host,
                port=conn.port,
                dbname=conn.database,
                user=conn.username,
                password=conn.password,
                connect_timeout=5,
            )
            if conn.ssl_mode:
                kwargs["sslmode"] = conn.ssl_mode
            c = psycopg2.connect(**kwargs)
            cur = c.cursor()
            cur.execute("SELECT version()")
            ver = cur.fetchone()[0]
            c.close()
            return "connected", ver

        else:
            return "error", f"Unknown db_type: {conn.db_type}"

    except Exception as e:
        return "error", str(e)


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Connections CRUD
# ─────────────────────────────────────────────────────────────────────────────

@api_bp.get("/connections/kafka")
def get_kafka_connections():
    try:
        from models import KafkaConnection
        conns = KafkaConnection.query.order_by(KafkaConnection.created_at.desc()).all()
        return jsonify([c.to_dict() for c in conns])
    except Exception as e:
        current_app.logger.error(f"get_kafka_connections error: {e}")
        return jsonify({"error": str(e)}), 503


@api_bp.post("/connections/kafka")
def create_kafka_connection():
    try:
        from models import KafkaConnection
        from extensions import db
        data = request.get_json(force=True) or {}

        required = ["name", "bootstrap_servers"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

        conn = KafkaConnection(
            name              = data["name"],
            bootstrap_servers = data["bootstrap_servers"],
            security_protocol = data.get("security_protocol", "PLAINTEXT"),
            sasl_mechanism    = data.get("sasl_mechanism"),
            sasl_username     = data.get("sasl_username"),
            sasl_password     = data.get("sasl_password"),
            ssl_cafile        = data.get("ssl_cafile"),
            ssl_certfile      = data.get("ssl_certfile"),
            ssl_keyfile       = data.get("ssl_keyfile"),
            status            = "disconnected",
        )
        db.session.add(conn)
        db.session.commit()
        return jsonify(conn.to_dict()), 201
    except Exception as e:
        current_app.logger.error(f"create_kafka_connection error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.put("/connections/kafka/<int:conn_id>")
def update_kafka_connection(conn_id):
    try:
        from models import KafkaConnection
        from extensions import db
        conn = KafkaConnection.query.get_or_404(conn_id)
        data = request.get_json(force=True) or {}

        for field in ["name", "bootstrap_servers", "security_protocol", "sasl_mechanism",
                      "sasl_username", "sasl_password", "ssl_cafile", "ssl_certfile", "ssl_keyfile"]:
            if field in data:
                setattr(conn, field, data[field])

        conn.updated_at = datetime.utcnow()
        db.session.commit()
        return jsonify(conn.to_dict())
    except Exception as e:
        current_app.logger.error(f"update_kafka_connection error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.delete("/connections/kafka/<int:conn_id>")
def delete_kafka_connection(conn_id):
    try:
        from models import KafkaConnection
        from extensions import db
        conn = KafkaConnection.query.get_or_404(conn_id)
        db.session.delete(conn)
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        current_app.logger.error(f"delete_kafka_connection error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.post("/connections/kafka/<int:conn_id>/test")
def test_kafka_connection(conn_id):
    try:
        from models import KafkaConnection
        from extensions import db
        conn = KafkaConnection.query.get_or_404(conn_id)

        status, message = _test_kafka_conn(conn)
        conn.status = status
        conn.last_tested_at = datetime.utcnow()
        db.session.commit()
        return jsonify({"status": status, "message": message})
    except Exception as e:
        current_app.logger.error(f"test_kafka_connection error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


def _test_kafka_conn(conn):
    """Attempt connection to Kafka bootstrap servers."""
    try:
        from kafka import KafkaAdminClient
        kwargs = dict(
            bootstrap_servers=conn.bootstrap_servers,
            security_protocol=conn.security_protocol,
            request_timeout_ms=5000,
        )
        if conn.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL") and conn.sasl_mechanism:
            kwargs["sasl_mechanism"] = conn.sasl_mechanism
            kwargs["sasl_plain_username"] = conn.sasl_username or ""
            kwargs["sasl_plain_password"] = conn.sasl_password or ""
        if conn.security_protocol in ("SSL", "SASL_SSL"):
            if conn.ssl_cafile:
                kwargs["ssl_cafile"] = conn.ssl_cafile
            if conn.ssl_certfile:
                kwargs["ssl_certfile"] = conn.ssl_certfile
            if conn.ssl_keyfile:
                kwargs["ssl_keyfile"] = conn.ssl_keyfile

        client = KafkaAdminClient(**kwargs)
        topics = client.list_topics()
        client.close()
        return "connected", f"OK — {len(topics)} topics"
    except Exception as e:
        return "error", str(e)


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Connect Connections CRUD
# ─────────────────────────────────────────────────────────────────────────────

@api_bp.get("/connections/kafka-connect")
def get_kafka_connect_connections():
    try:
        from models import KafkaConnectConnection
        conns = KafkaConnectConnection.query.order_by(KafkaConnectConnection.created_at.desc()).all()
        return jsonify([c.to_dict() for c in conns])
    except Exception as e:
        current_app.logger.error(f"get_kafka_connect_connections error: {e}")
        return jsonify({"error": str(e)}), 503


@api_bp.post("/connections/kafka-connect")
def create_kafka_connect_connection():
    try:
        from models import KafkaConnectConnection
        from extensions import db
        data = request.get_json(force=True) or {}

        required = ["name", "url"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

        conn = KafkaConnectConnection(
            name     = data["name"],
            url      = data["url"],
            username = data.get("username"),
            password = data.get("password"),
            status   = "disconnected",
        )
        db.session.add(conn)
        db.session.commit()
        return jsonify(conn.to_dict()), 201
    except Exception as e:
        current_app.logger.error(f"create_kafka_connect_connection error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.put("/connections/kafka-connect/<int:conn_id>")
def update_kafka_connect_connection(conn_id):
    try:
        from models import KafkaConnectConnection
        from extensions import db
        conn = KafkaConnectConnection.query.get_or_404(conn_id)
        data = request.get_json(force=True) or {}

        for field in ["name", "url", "username", "password"]:
            if field in data:
                setattr(conn, field, data[field])

        conn.updated_at = datetime.utcnow()
        db.session.commit()
        return jsonify(conn.to_dict())
    except Exception as e:
        current_app.logger.error(f"update_kafka_connect_connection error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.delete("/connections/kafka-connect/<int:conn_id>")
def delete_kafka_connect_connection(conn_id):
    try:
        from models import KafkaConnectConnection
        from extensions import db
        conn = KafkaConnectConnection.query.get_or_404(conn_id)
        db.session.delete(conn)
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        current_app.logger.error(f"delete_kafka_connect_connection error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.post("/connections/kafka-connect/<int:conn_id>/test")
def test_kafka_connect_connection(conn_id):
    try:
        from models import KafkaConnectConnection
        from extensions import db
        conn = KafkaConnectConnection.query.get_or_404(conn_id)

        status, message = _test_kafka_connect_conn(conn)
        conn.status = status
        conn.last_tested_at = datetime.utcnow()
        db.session.commit()
        return jsonify({"status": status, "message": message})
    except Exception as e:
        current_app.logger.error(f"test_kafka_connect_connection error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


def _test_kafka_connect_conn(conn):
    """Attempt GET /connectors to Kafka Connect REST API."""
    try:
        import requests as req
        url = conn.url.rstrip("/") + "/connectors"
        kwargs = dict(timeout=5)
        if conn.username and conn.password:
            kwargs["auth"] = (conn.username, conn.password)
        resp = req.get(url, **kwargs)
        resp.raise_for_status()
        connectors = resp.json()
        return "connected", f"OK — {len(connectors)} connectors"
    except Exception as e:
        return "error", str(e)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: Oracle connection factory
# ─────────────────────────────────────────────────────────────────────────────

def _get_oracle_conn(conn):
    """
    Создать oracledb соединение из объекта DbConnection.
    Возвращает oracledb.Connection.
    Бросает исключение если подключение не удалось.
    """
    import oracledb
    if conn.service_name:
        dsn = oracledb.makedsn(conn.host, conn.port or 1521, service_name=conn.service_name)
    elif conn.database:
        dsn = oracledb.makedsn(conn.host, conn.port or 1521, sid=conn.database)
    else:
        raise ValueError("Oracle connection requires Service Name or SID")
    return oracledb.connect(user=conn.username, password=conn.password, dsn=dsn)


# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight checks
# ─────────────────────────────────────────────────────────────────────────────

def _run_preflight_checks(source_conn, target_conn, source_schema, source_table,
                          target_schema, target_table):
    """
    Выполнить все pre-flight проверки.
    Возвращает список dict: {check, status, errors, warnings}
    """
    results = []

    # ── 1. Таблица существует ────────────────────────────────────────────────
    def check_table_exists():
        errors = []
        warnings = []
        try:
            c = _get_oracle_conn(source_conn)
            cur = c.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER=:schema_name AND TABLE_NAME=:tbl_name",
                schema_name=source_schema.upper(), tbl_name=source_table.upper()
            )
            src_count = cur.fetchone()[0]
            c.close()

            c = _get_oracle_conn(target_conn)
            cur = c.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER=:schema_name AND TABLE_NAME=:tbl_name",
                schema_name=target_schema.upper(), tbl_name=target_table.upper()
            )
            tgt_count = cur.fetchone()[0]
            c.close()

            if src_count == 0:
                errors.append(f"Таблица {source_schema}.{source_table} не найдена в source")
            if tgt_count == 0:
                errors.append(f"Таблица {target_schema}.{target_table} не найдена в target")

            status = "ERROR" if errors else "OK"
        except Exception as e:
            return {"check": "Таблица существует", "status": "EXCEPTION",
                    "errors": [str(e)], "warnings": []}
        return {"check": "Таблица существует", "status": status,
                "errors": errors, "warnings": warnings}

    # ── 2. Схема и колонки ───────────────────────────────────────────────────
    def check_columns():
        errors = []
        warnings = []
        try:
            c = _get_oracle_conn(source_conn)
            cur = c.cursor()
            cur.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
                FROM ALL_TAB_COLUMNS
                WHERE OWNER=:schema_name AND TABLE_NAME=:tbl_name
                ORDER BY COLUMN_ID
            """, schema_name=source_schema.upper(), tbl_name=source_table.upper())
            src_cols = {row[0]: row for row in cur.fetchall()}
            c.close()

            c = _get_oracle_conn(target_conn)
            cur = c.cursor()
            cur.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE
                FROM ALL_TAB_COLUMNS
                WHERE OWNER=:schema_name AND TABLE_NAME=:tbl_name
                ORDER BY COLUMN_ID
            """, schema_name=target_schema.upper(), tbl_name=target_table.upper())
            tgt_cols = {row[0]: row for row in cur.fetchall()}
            c.close()

            # Колонки в source, которых нет в target
            for col in src_cols:
                if col not in tgt_cols:
                    errors.append(f"Колонка {col}: есть в source, отсутствует в target")

            # Колонки в target, которых нет в source
            for col in tgt_cols:
                if col not in src_cols:
                    warnings.append(f"Колонка {col}: есть в target, отсутствует в source")

            # Сравнение типов и nullable
            for col in src_cols:
                if col in tgt_cols:
                    s = src_cols[col]
                    t = tgt_cols[col]
                    if s[1] != t[1]:
                        warnings.append(f"Колонка {col}: тип {s[1]} в source, {t[1]} в target")
                    # Nullable: source NULL, target NOT NULL — потенциальная проблема
                    if s[2] == 'Y' and t[2] == 'N':
                        warnings.append(
                            f"Колонка {col}: NOT NULL в target, но NULL в source"
                        )

            status = "ERROR" if errors else ("WARNING" if warnings else "OK")
        except Exception as e:
            return {"check": "Схема и колонки", "status": "EXCEPTION",
                    "errors": [str(e)], "warnings": []}
        return {"check": "Схема и колонки", "status": status,
                "errors": errors, "warnings": warnings}

    # ── 3. Constraints ───────────────────────────────────────────────────────
    def check_constraints():
        errors = []
        warnings = []
        try:
            c = _get_oracle_conn(target_conn)
            cur = c.cursor()
            cur.execute("""
                SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE, STATUS, VALIDATED
                FROM ALL_CONSTRAINTS
                WHERE OWNER=:schema_name AND TABLE_NAME=:tbl_name
                  AND CONSTRAINT_TYPE IN ('P','U','R','C')
            """, schema_name=target_schema.upper(), tbl_name=target_table.upper())
            rows = cur.fetchall()
            c.close()

            for name, ctype, status, validated in rows:
                if ctype == 'P':
                    # PK должен быть ENABLED
                    if status != 'ENABLED':
                        errors.append(f"PRIMARY KEY {name}: должен быть ENABLED, сейчас {status}")
                else:
                    # UNIQUE/FK/CHECK должны быть DISABLED
                    type_label = {'U': 'UNIQUE', 'R': 'FOREIGN KEY', 'C': 'CHECK'}.get(ctype, ctype)
                    if status == 'ENABLED':
                        errors.append(
                            f"{type_label} constraint {name}: должен быть DISABLED"
                        )

            status_result = "ERROR" if errors else ("WARNING" if warnings else "OK")
        except Exception as e:
            return {"check": "Constraints", "status": "EXCEPTION",
                    "errors": [str(e)], "warnings": []}
        return {"check": "Constraints", "status": status_result,
                "errors": errors, "warnings": warnings}

    # ── 4. Триггеры ──────────────────────────────────────────────────────────
    def check_triggers():
        errors = []
        warnings = []
        try:
            c = _get_oracle_conn(target_conn)
            cur = c.cursor()
            cur.execute("""
                SELECT TRIGGER_NAME, STATUS
                FROM ALL_TRIGGERS
                WHERE OWNER=:schema_name AND TABLE_NAME=:tbl_name
            """, schema_name=target_schema.upper(), tbl_name=target_table.upper())
            rows = cur.fetchall()
            c.close()

            for name, status in rows:
                if status == 'ENABLED':
                    errors.append(f"Триггер {name}: должен быть DISABLED")

            status_result = "ERROR" if errors else "OK"
        except Exception as e:
            return {"check": "Триггеры", "status": "EXCEPTION",
                    "errors": [str(e)], "warnings": []}
        return {"check": "Триггеры", "status": status_result,
                "errors": errors, "warnings": warnings}

    # ── 5. Индексы ───────────────────────────────────────────────────────────
    def check_indexes():
        errors = []
        warnings = []
        try:
            c = _get_oracle_conn(target_conn)
            cur = c.cursor()
            # Получаем индексы, исключая PK-индексы
            cur.execute("""
                SELECT i.INDEX_NAME, i.STATUS, i.UNIQUENESS
                FROM ALL_INDEXES i
                WHERE i.TABLE_OWNER=:schema_name AND i.TABLE_NAME=:tbl_name
                  AND NOT EXISTS (
                      SELECT 1 FROM ALL_CONSTRAINTS c
                      WHERE c.OWNER=i.TABLE_OWNER
                        AND c.TABLE_NAME=i.TABLE_NAME
                        AND c.INDEX_NAME=i.INDEX_NAME
                        AND c.CONSTRAINT_TYPE='P'
                  )
            """, schema_name=target_schema.upper(), tbl_name=target_table.upper())
            rows = cur.fetchall()
            c.close()

            for name, status, uniqueness in rows:
                if status != 'UNUSABLE':
                    errors.append(
                        f"Индекс {name} ({uniqueness}): должен быть UNUSABLE, сейчас {status}"
                    )

            status_result = "ERROR" if errors else "OK"
        except Exception as e:
            return {"check": "Индексы", "status": "EXCEPTION",
                    "errors": [str(e)], "warnings": []}
        return {"check": "Индексы", "status": status_result,
                "errors": errors, "warnings": warnings}

    # ── 6. Права доступа ─────────────────────────────────────────────────────
    def check_privileges():
        errors = []
        warnings = []
        try:
            # Проверяем права для пользователя target соединения на target таблицу
            c = _get_oracle_conn(target_conn)
            cur = c.cursor()
            cur.execute("""
                SELECT PRIVILEGE
                FROM ALL_TAB_PRIVS
                WHERE TABLE_SCHEMA=:schema_name AND TABLE_NAME=:tbl_name
                  AND GRANTEE=:grantee
            """, schema_name=target_schema.upper(), tbl_name=target_table.upper(),
                grantee=target_conn.username.upper())
            granted = {row[0] for row in cur.fetchall()}
            c.close()

            required_privs = {'INSERT', 'UPDATE', 'DELETE', 'SELECT'}
            missing_privs = required_privs - granted

            # Также проверяем через USER_TAB_PRIVS если пользователь — владелец схемы
            if missing_privs and target_conn.username.upper() == target_schema.upper():
                # Владелец схемы имеет все права по умолчанию
                missing_privs = set()

            for priv in sorted(missing_privs):
                errors.append(
                    f"Отсутствует право {priv} на {target_schema}.{target_table} "
                    f"для пользователя {target_conn.username}"
                )

            status_result = "ERROR" if errors else ("WARNING" if warnings else "OK")
        except Exception as e:
            return {"check": "Права доступа", "status": "EXCEPTION",
                    "errors": [str(e)], "warnings": []}
        return {"check": "Права доступа", "status": status_result,
                "errors": errors, "warnings": warnings}

    # ── 7. Партиционирование ─────────────────────────────────────────────────
    def check_partitioning():
        errors = []
        warnings = []
        try:
            c = _get_oracle_conn(target_conn)
            cur = c.cursor()
            cur.execute("""
                SELECT PARTITIONED, ROW_MOVEMENT
                FROM ALL_TABLES
                WHERE OWNER=:schema_name AND TABLE_NAME=:tbl_name
            """, schema_name=target_schema.upper(), tbl_name=target_table.upper())
            row = cur.fetchone()
            c.close()

            if row:
                partitioned, row_movement = row
                if partitioned == 'YES' and row_movement != 'ENABLED':
                    errors.append(
                        f"Таблица партиционирована, но ROW MOVEMENT не включён "
                        f"(сейчас: {row_movement}). "
                        f"Выполните: ALTER TABLE {target_schema}.{target_table} ENABLE ROW MOVEMENT"
                    )

            status_result = "ERROR" if errors else "OK"
        except Exception as e:
            return {"check": "Партиционирование", "status": "EXCEPTION",
                    "errors": [str(e)], "warnings": []}
        return {"check": "Партиционирование", "status": status_result,
                "errors": errors, "warnings": warnings}

    # Выполняем все проверки
    for fn in [check_table_exists, check_columns, check_constraints,
               check_triggers, check_indexes, check_privileges, check_partitioning]:
        results.append(fn())

    return results


@api_bp.post("/jobs/preflight")
def run_preflight():
    """
    POST /api/jobs/preflight — запустить pre-flight проверки.
    """
    try:
        from models import DbConnection, MigrationPreflightResult
        from extensions import db

        data = request.get_json(force=True) or {}
        required = ["job_name", "source_conn_id", "source_schema", "source_table",
                    "target_conn_id", "target_schema", "target_table"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

        job_name      = data["job_name"]
        source_schema = data["source_schema"].strip().upper()
        source_table  = data["source_table"].strip().upper()
        target_schema = data["target_schema"].strip().upper()
        target_table  = data["target_table"].strip().upper()

        source_conn = DbConnection.query.get(data["source_conn_id"])
        if not source_conn:
            return jsonify({"error": f"Source connection id={data['source_conn_id']} not found"}), 404

        target_conn = DbConnection.query.get(data["target_conn_id"])
        if not target_conn:
            return jsonify({"error": f"Target connection id={data['target_conn_id']} not found"}), 404

        # Запускаем проверки
        check_results = _run_preflight_checks(
            source_conn, target_conn,
            source_schema, source_table,
            target_schema, target_table
        )

        # Сохраняем результаты в БД
        src_table_full = f"{source_schema}.{source_table}"
        tgt_table_full = f"{target_schema}.{target_table}"

        for r in check_results:
            record = MigrationPreflightResult(
                job_name       = job_name,
                source_table   = src_table_full,
                target_table   = tgt_table_full,
                source_conn_id = int(data["source_conn_id"]),
                target_conn_id = int(data["target_conn_id"]),
                check_name     = r["check"],
                status         = r["status"],
                errors         = r["errors"],
                warnings       = r["warnings"],
                checked_at     = datetime.utcnow(),
            )
            db.session.add(record)
        db.session.commit()

        # Формируем ответ
        summary = {"total": len(check_results), "ok": 0, "warnings": 0, "errors": 0, "exceptions": 0}
        for r in check_results:
            if r["status"] == "OK":
                summary["ok"] += 1
            elif r["status"] == "WARNING":
                summary["warnings"] += 1
            elif r["status"] == "ERROR":
                summary["errors"] += 1
            elif r["status"] == "EXCEPTION":
                summary["exceptions"] += 1

        can_proceed = summary["errors"] == 0 and summary["exceptions"] == 0

        return jsonify({
            "can_proceed": can_proceed,
            "results": [
                {
                    "check":    r["check"],
                    "status":   r["status"],
                    "errors":   r["errors"],
                    "warnings": r["warnings"],
                }
                for r in check_results
            ],
            "summary": summary,
        })

    except Exception as e:
        current_app.logger.error(f"run_preflight error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.get("/jobs/preflight")
def get_preflight_results():
    """
    GET /api/jobs/preflight — получить последние результаты pre-flight проверок.
    Query params: source_table, target_table (опционально)
    """
    try:
        from models import MigrationPreflightResult

        source_table = request.args.get("source_table", "").strip().upper() or None
        target_table = request.args.get("target_table", "").strip().upper() or None

        query = MigrationPreflightResult.query

        if source_table:
            query = query.filter(MigrationPreflightResult.source_table.ilike(f"%{source_table}%"))
        if target_table:
            query = query.filter(MigrationPreflightResult.target_table.ilike(f"%{target_table}%"))

        # Берём последние результаты, сортируем по дате убывания
        records = query.order_by(MigrationPreflightResult.checked_at.desc()).limit(200).all()

        return jsonify([r.to_dict() for r in records])

    except Exception as e:
        current_app.logger.error(f"get_preflight_results error: {e}")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Target DDL objects management
# ─────────────────────────────────────────────────────────────────────────────

@api_bp.get("/jobs/target-objects")
def get_target_objects():
    """
    GET /api/jobs/target-objects — получить индексы, констрейнты и триггеры таблицы.
    Query params: conn_id, schema, table
    """
    try:
        from models import DbConnection

        conn_id = request.args.get("conn_id")
        schema  = (request.args.get("schema") or "").strip().upper()
        table   = (request.args.get("table") or "").strip().upper()

        if not conn_id or not schema or not table:
            return jsonify({"error": "Query params conn_id, schema, table are required"}), 400

        conn = DbConnection.query.get_or_404(int(conn_id))
        c = _get_oracle_conn(conn)

        try:
            cur = c.cursor()

            # ── Индексы ──────────────────────────────────────────────────────
            cur.execute("""
                SELECT i.INDEX_NAME, i.INDEX_TYPE, i.UNIQUENESS, i.STATUS,
                       NVL((SELECT 'Y' FROM ALL_CONSTRAINTS c
                            WHERE c.OWNER=i.TABLE_OWNER
                              AND c.TABLE_NAME=i.TABLE_NAME
                              AND c.INDEX_NAME=i.INDEX_NAME
                              AND c.CONSTRAINT_TYPE='P'
                              AND ROWNUM=1), 'N') AS IS_PK
                FROM ALL_INDEXES i
                WHERE i.TABLE_OWNER=:schema_name AND i.TABLE_NAME=:tbl_name
                ORDER BY i.INDEX_NAME
            """, schema_name=schema, tbl_name=table)
            idx_rows = cur.fetchall()

            # Колонки индексов
            cur.execute("""
                SELECT INDEX_NAME, COLUMN_NAME
                FROM ALL_IND_COLUMNS
                WHERE TABLE_OWNER=:schema_name AND TABLE_NAME=:tbl_name
                ORDER BY INDEX_NAME, COLUMN_POSITION
            """, schema_name=schema, tbl_name=table)
            idx_cols_raw = cur.fetchall()

            # Группируем колонки по индексу
            idx_cols = {}
            for idx_name, col_name in idx_cols_raw:
                idx_cols.setdefault(idx_name, []).append(col_name)

            indexes = []
            for idx_name, idx_type, uniqueness, status, is_pk in idx_rows:
                indexes.append({
                    "name":       idx_name,
                    "type":       idx_type,
                    "uniqueness": uniqueness,
                    "status":     status,
                    "is_pk":      is_pk == 'Y',
                    "columns":    idx_cols.get(idx_name, []),
                })

            # ── Констрейнты ──────────────────────────────────────────────────
            type_labels = {'P': 'PRIMARY KEY', 'U': 'UNIQUE', 'R': 'FOREIGN KEY', 'C': 'CHECK'}
            cur.execute("""
                SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE, STATUS, VALIDATED
                FROM ALL_CONSTRAINTS
                WHERE OWNER=:schema_name AND TABLE_NAME=:tbl_name
                  AND CONSTRAINT_TYPE IN ('P','U','R','C')
                ORDER BY CONSTRAINT_TYPE, CONSTRAINT_NAME
            """, schema_name=schema, tbl_name=table)
            constraints = [
                {
                    "name":       row[0],
                    "type":       row[1],
                    "type_label": type_labels.get(row[1], row[1]),
                    "status":     row[2],
                    "validated":  row[3],
                }
                for row in cur.fetchall()
            ]

            # ── Триггеры ─────────────────────────────────────────────────────
            cur.execute("""
                SELECT TRIGGER_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, STATUS
                FROM ALL_TRIGGERS
                WHERE OWNER=:schema_name AND TABLE_NAME=:tbl_name
                ORDER BY TRIGGER_NAME
            """, schema_name=schema, tbl_name=table)
            triggers = [
                {
                    "name":   row[0],
                    "type":   row[1],
                    "event":  row[2],
                    "status": row[3],
                }
                for row in cur.fetchall()
            ]

        finally:
            c.close()

        return jsonify({
            "indexes":     indexes,
            "constraints": constraints,
            "triggers":    triggers,
        })

    except Exception as e:
        current_app.logger.error(f"get_target_objects error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.post("/jobs/target-objects/action")
def target_objects_action():
    """
    POST /api/jobs/target-objects/action — выполнить DDL действие на target.
    """
    try:
        from models import DbConnection

        data = request.get_json(force=True) or {}
        conn_id = data.get("conn_id")
        schema  = (data.get("schema") or "").strip().upper()
        actions = data.get("actions", [])

        if not conn_id or not schema:
            return jsonify({"error": "Fields conn_id and schema are required"}), 400
        if not isinstance(actions, list) or not actions:
            return jsonify({"error": "Field actions must be a non-empty list"}), 400

        conn = DbConnection.query.get_or_404(int(conn_id))
        c = _get_oracle_conn(conn)

        results = []
        success_count = 0
        error_count = 0

        try:
            cur = c.cursor()

            for action in actions:
                action_type = action.get("type", "")
                name        = (action.get("name") or "").strip().upper()
                table       = (action.get("table") or "").strip().upper()

                sql = None
                try:
                    if action_type == "disable_constraint":
                        if not table:
                            raise ValueError("Field 'table' is required for disable_constraint")
                        sql = f"ALTER TABLE {schema}.{table} DISABLE CONSTRAINT {name}"
                    elif action_type == "enable_constraint":
                        if not table:
                            raise ValueError("Field 'table' is required for enable_constraint")
                        sql = f"ALTER TABLE {schema}.{table} ENABLE CONSTRAINT {name}"
                    elif action_type == "disable_trigger":
                        sql = f"ALTER TRIGGER {schema}.{name} DISABLE"
                    elif action_type == "enable_trigger":
                        sql = f"ALTER TRIGGER {schema}.{name} ENABLE"
                    elif action_type == "unusable_index":
                        sql = f"ALTER INDEX {schema}.{name} UNUSABLE"
                    elif action_type == "rebuild_index":
                        sql = f"ALTER INDEX {schema}.{name} REBUILD"
                    else:
                        raise ValueError(f"Unknown action type: {action_type}")

                    cur.execute(sql)
                    results.append({
                        "action": action_type,
                        "name":   name,
                        "status": "OK",
                        "sql":    sql,
                    })
                    success_count += 1

                except Exception as e:
                    results.append({
                        "action": action_type,
                        "name":   name,
                        "status": "ERROR",
                        "sql":    sql,
                        "error":  str(e),
                    })
                    error_count += 1

        finally:
            c.close()

        return jsonify({
            "results":       results,
            "success_count": success_count,
            "error_count":   error_count,
        })

    except Exception as e:
        current_app.logger.error(f"target_objects_action error: {e}")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Debezium connectors
# ─────────────────────────────────────────────────────────────────────────────

def _generate_connector_name(job_name: str, source_table: str) -> str:
    """
    Генерирует уникальное имя коннектора.
    Формат: migration-{job_slug}-{table_slug}-{short_id}
    Пример: migration-orders-sales-orders-a1b2c3
    """
    def slugify(s):
        s = s.lower()
        s = re.sub(r'[^a-z0-9]+', '-', s)
        return s.strip('-')[:50]

    job_slug   = slugify(job_name)
    table_slug = slugify(source_table.replace('.', '-'))
    short_id   = str(uuid.uuid4()).replace('-', '')[:6]
    return f"migration-{job_slug}-{table_slug}-{short_id}"


def _kafka_connect_request(kc_conn, method, path, json_body=None):
    """
    Выполнить HTTP запрос к Kafka Connect REST API.
    Возвращает (response_json, status_code).
    """
    import requests as req
    url = kc_conn.url.rstrip("/") + path
    kwargs = dict(timeout=10)
    if kc_conn.username and kc_conn.password:
        kwargs["auth"] = (kc_conn.username, kc_conn.password)
    if json_body is not None:
        kwargs["json"] = json_body

    resp = getattr(req, method.lower())(url, **kwargs)
    try:
        body = resp.json()
    except Exception:
        body = {"raw_text": resp.text}
    return body, resp.status_code


@api_bp.post("/connectors")
def create_connector():
    """
    POST /api/connectors — создать Debezium коннектор.
    """
    try:
        from models import MigrationJob, DbConnection, KafkaConnection, KafkaConnectConnection, DebeziumConnector
        from extensions import db

        data = request.get_json(force=True) or {}
        required = ["job_id", "kafka_connect_conn_id", "kafka_conn_id", "source_conn_id"]
        missing = [f for f in required if data.get(f) is None]
        if missing:
            return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

        job = MigrationJob.query.get(data["job_id"])
        if not job:
            return jsonify({"error": f"MigrationJob id={data['job_id']} not found"}), 404

        source_conn = DbConnection.query.get(data["source_conn_id"])
        if not source_conn:
            return jsonify({"error": f"Source connection id={data['source_conn_id']} not found"}), 404

        kc_conn = KafkaConnectConnection.query.get(data["kafka_connect_conn_id"])
        if not kc_conn:
            return jsonify({"error": f"KafkaConnect connection id={data['kafka_connect_conn_id']} not found"}), 404

        kafka_conn = KafkaConnection.query.get(data["kafka_conn_id"])
        if not kafka_conn:
            return jsonify({"error": f"Kafka connection id={data['kafka_conn_id']} not found"}), 404

        scn_cutoff   = data.get("scn_cutoff") or job.scn_cutoff
        topic_prefix = data.get("topic_prefix", "migration")

        # Разбираем source_table: SCHEMA.TABLE
        source_table_full = job.source_table or ""
        if "." in source_table_full:
            source_schema, source_table = source_table_full.split(".", 1)
        else:
            source_schema = source_conn.username.upper()
            source_table  = source_table_full.upper()

        # Генерируем имя коннектора
        connector_name = _generate_connector_name(job.name, source_table_full)

        # Формируем конфиг
        config = {
            "connector.class":              "io.debezium.connector.oracle.OracleConnector",
            "tasks.max":                    "1",
            "database.hostname":            source_conn.host,
            "database.port":                str(source_conn.port or 1521),
            "database.user":                source_conn.username,
            "database.password":            source_conn.password,
            "database.dbname":              source_conn.service_name or source_conn.database or "",
            "topic.prefix":                 topic_prefix,
            "table.include.list":           f"{source_schema}.{source_table}",
            "snapshot.mode":                "schema_only",
            "log.mining.strategy":          "online_catalog",
            "log.mining.start.scn":         str(scn_cutoff) if scn_cutoff else "0",
            "heartbeat.interval.ms":        "30000",
            "schema.history.internal.kafka.bootstrap.servers": kafka_conn.bootstrap_servers,
            "schema.history.internal.kafka.topic":             f"schema-history.migration.{job.id}",
            "key.converter":                "org.apache.kafka.connect.json.JsonConverter",
            "value.converter":              "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
        }

        # Отправляем в Kafka Connect
        debezium_response = None
        debezium_status   = "CREATING"
        error_message     = None

        try:
            resp_body, resp_code = _kafka_connect_request(
                kc_conn, "POST", "/connectors",
                json_body={"name": connector_name, "config": config}
            )
            debezium_response = resp_body
            if resp_code in (200, 201):
                debezium_status = "CREATING"
            else:
                debezium_status = "FAILED"
                error_message   = str(resp_body)
        except Exception as e:
            debezium_status = "FAILED"
            error_message   = str(e)

        # Сохраняем в БД
        connector = DebeziumConnector(
            connector_name        = connector_name,
            job_id                = job.id,
            job_name              = job.name,
            source_table          = source_table_full,
            target_table          = job.target_table,
            kafka_connect_conn_id = kc_conn.id,
            scn_cutoff            = int(scn_cutoff) if scn_cutoff else None,
            config_snapshot       = config,
            status                = debezium_status,
            error_message         = error_message,
        )
        db.session.add(connector)

        # Обновляем job
        job.debezium_connector_name = connector_name
        db.session.commit()

        return jsonify({
            "connector_name":    connector_name,
            "status":            debezium_status,
            "debezium_response": debezium_response,
            "connector_id":      connector.id,
        }), 201

    except Exception as e:
        current_app.logger.error(f"create_connector error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.get("/connectors")
def list_connectors():
    """GET /api/connectors — список всех коннекторов из БД."""
    try:
        from models import DebeziumConnector
        connectors = DebeziumConnector.query.order_by(DebeziumConnector.created_at.desc()).all()
        return jsonify([c.to_dict() for c in connectors])
    except Exception as e:
        current_app.logger.error(f"list_connectors error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.get("/connectors/refresh-all")
def refresh_all_connectors():
    """
    GET /api/connectors/refresh-all — обновить статусы всех коннекторов из Debezium.
    """
    try:
        from models import DebeziumConnector, KafkaConnectConnection
        from extensions import db

        connectors = DebeziumConnector.query.filter(
            DebeziumConnector.status.notin_(["DESTROYED"])
        ).all()

        updated = 0
        errors  = []

        for connector in connectors:
            try:
                kc_conn = KafkaConnectConnection.query.get(connector.kafka_connect_conn_id)
                if not kc_conn:
                    continue

                resp_body, resp_code = _kafka_connect_request(
                    kc_conn, "GET",
                    f"/connectors/{connector.connector_name}/status"
                )

                if resp_code == 200:
                    connector_state = (
                        resp_body.get("connector", {}).get("state", "UNKNOWN")
                    )
                    connector.status          = connector_state
                    connector.last_status_check = datetime.utcnow()
                    updated += 1
                elif resp_code == 404:
                    connector.status          = "DESTROYED"
                    connector.last_status_check = datetime.utcnow()
                    updated += 1
                else:
                    errors.append(
                        f"{connector.connector_name}: HTTP {resp_code}"
                    )
            except Exception as e:
                errors.append(f"{connector.connector_name}: {e}")

        db.session.commit()

        return jsonify({
            "updated": updated,
            "errors":  errors,
            "total":   len(connectors),
        })

    except Exception as e:
        current_app.logger.error(f"refresh_all_connectors error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.get("/connectors/<int:connector_id>")
def get_connector(connector_id):
    """GET /api/connectors/<id> — детали коннектора."""
    try:
        from models import DebeziumConnector
        connector = DebeziumConnector.query.get_or_404(connector_id)
        return jsonify(connector.to_dict())
    except Exception as e:
        current_app.logger.error(f"get_connector error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.get("/connectors/<int:connector_id>/status")
def get_connector_status(connector_id):
    """
    GET /api/connectors/<id>/status — получить актуальный статус из Debezium.
    """
    try:
        from models import DebeziumConnector, KafkaConnectConnection
        from extensions import db

        connector = DebeziumConnector.query.get_or_404(connector_id)
        kc_conn   = KafkaConnectConnection.query.get(connector.kafka_connect_conn_id)
        if not kc_conn:
            return jsonify({"error": "KafkaConnect connection not found"}), 404

        resp_body, resp_code = _kafka_connect_request(
            kc_conn, "GET",
            f"/connectors/{connector.connector_name}/status"
        )

        if resp_code == 200:
            connector_state = resp_body.get("connector", {}).get("state", "UNKNOWN")
            tasks           = resp_body.get("tasks", [])

            connector.status          = connector_state
            connector.last_status_check = datetime.utcnow()
            db.session.commit()

            return jsonify({
                "connector_name":  connector.connector_name,
                "status":          connector_state,
                "connector_state": connector_state,
                "tasks":           tasks,
                "raw":             resp_body,
            })
        elif resp_code == 404:
            connector.status          = "DESTROYED"
            connector.last_status_check = datetime.utcnow()
            db.session.commit()
            return jsonify({"error": "Connector not found in Debezium", "status": "DESTROYED"}), 404
        else:
            return jsonify({"error": f"Debezium returned HTTP {resp_code}", "raw": resp_body}), resp_code

    except Exception as e:
        current_app.logger.error(f"get_connector_status error: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.post("/connectors/<int:connector_id>/action")
def connector_action(connector_id):
    """
    POST /api/connectors/<id>/action — действие с коннектором.
    Body: {"action": "pause" | "resume" | "restart" | "delete"}
    """
    try:
        from models import DebeziumConnector, KafkaConnectConnection
        from extensions import db

        connector = DebeziumConnector.query.get_or_404(connector_id)
        kc_conn   = KafkaConnectConnection.query.get(connector.kafka_connect_conn_id)
        if not kc_conn:
            return jsonify({"error": "KafkaConnect connection not found"}), 404

        data   = request.get_json(force=True) or {}
        action = data.get("action", "")

        valid_actions = ("pause", "resume", "restart", "delete")
        if action not in valid_actions:
            return jsonify({
                "error": f"Unknown action '{action}'. Valid: {list(valid_actions)}"
            }), 400

        base_path = f"/connectors/{connector.connector_name}"

        if action == "pause":
            resp_body, resp_code = _kafka_connect_request(kc_conn, "PUT", f"{base_path}/pause")
            if resp_code in (200, 202, 204):
                connector.status = "PAUSED"
                db.session.commit()
            return jsonify({"ok": resp_code in (200, 202, 204), "raw": resp_body,
                            "status_code": resp_code})

        elif action == "resume":
            resp_body, resp_code = _kafka_connect_request(kc_conn, "PUT", f"{base_path}/resume")
            if resp_code in (200, 202, 204):
                connector.status = "RUNNING"
                db.session.commit()
            return jsonify({"ok": resp_code in (200, 202, 204), "raw": resp_body,
                            "status_code": resp_code})

        elif action == "restart":
            resp_body, resp_code = _kafka_connect_request(kc_conn, "POST", f"{base_path}/restart")
            if resp_code in (200, 202, 204):
                connector.status = "RUNNING"
                db.session.commit()
            return jsonify({"ok": resp_code in (200, 202, 204), "raw": resp_body,
                            "status_code": resp_code})

        elif action == "delete":
            resp_body, resp_code = _kafka_connect_request(kc_conn, "DELETE", base_path)
            if resp_code in (200, 204, 404):
                connector.status = "DESTROYED"
                db.session.commit()
            return jsonify({"ok": resp_code in (200, 204, 404), "raw": resp_body,
                            "status_code": resp_code})

    except Exception as e:
        current_app.logger.error(f"connector_action error: {e}")
        return jsonify({"error": str(e)}), 500
