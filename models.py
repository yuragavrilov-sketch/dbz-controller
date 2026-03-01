"""
ORA·MIGRATE — Database Models
SQLAlchemy models for connection management and migration jobs.
"""

from datetime import datetime
from extensions import db


class DbConnection(db.Model):
    """Database connection (Oracle or PostgreSQL)."""
    __tablename__ = "db_connections"

    id            = db.Column(db.Integer, primary_key=True)
    name          = db.Column(db.String(128), nullable=False, unique=True)
    role          = db.Column(db.String(16), nullable=False, default="SOURCE")   # SOURCE / TARGET
    db_type       = db.Column(db.String(32), nullable=False)                     # oracle / postgresql
    host          = db.Column(db.String(256), nullable=False)
    port          = db.Column(db.Integer, nullable=False)
    database      = db.Column(db.String(128), nullable=True)                     # SID (Oracle, optional if service_name set) or dbname (PostgreSQL)
    username      = db.Column(db.String(128), nullable=False)
    password      = db.Column(db.String(256), nullable=False)
    service_name  = db.Column(db.String(128), nullable=True)                     # Oracle only
    ssl_mode      = db.Column(db.String(32), nullable=True)                      # PostgreSQL only
    status        = db.Column(db.String(32), nullable=False, default="disconnected")
    last_tested_at = db.Column(db.DateTime, nullable=True)
    created_at    = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at    = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id":             self.id,
            "name":           self.name,
            "role":           self.role,
            "db_type":        self.db_type,
            "host":           self.host,
            "port":           self.port,
            "database":       self.database,
            "username":       self.username,
            "service_name":   self.service_name,
            "ssl_mode":       self.ssl_mode,
            "status":         self.status,
            "last_tested_at": self.last_tested_at.isoformat() if self.last_tested_at else None,
            "created_at":     self.created_at.isoformat() if self.created_at else None,
            "updated_at":     self.updated_at.isoformat() if self.updated_at else None,
        }


class KafkaConnection(db.Model):
    """Kafka broker connection."""
    __tablename__ = "kafka_connections"

    id                = db.Column(db.Integer, primary_key=True)
    name              = db.Column(db.String(128), nullable=False, unique=True)
    bootstrap_servers = db.Column(db.String(512), nullable=False)
    security_protocol = db.Column(db.String(32), nullable=False, default="PLAINTEXT")
    sasl_mechanism    = db.Column(db.String(32), nullable=True)
    sasl_username     = db.Column(db.String(128), nullable=True)
    sasl_password     = db.Column(db.String(256), nullable=True)
    ssl_cafile        = db.Column(db.String(512), nullable=True)
    ssl_certfile      = db.Column(db.String(512), nullable=True)
    ssl_keyfile       = db.Column(db.String(512), nullable=True)
    status            = db.Column(db.String(32), nullable=False, default="disconnected")
    last_tested_at    = db.Column(db.DateTime, nullable=True)
    created_at        = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at        = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id":                self.id,
            "name":              self.name,
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
            "sasl_mechanism":    self.sasl_mechanism,
            "sasl_username":     self.sasl_username,
            "ssl_cafile":        self.ssl_cafile,
            "ssl_certfile":      self.ssl_certfile,
            "ssl_keyfile":       self.ssl_keyfile,
            "status":            self.status,
            "last_tested_at":    self.last_tested_at.isoformat() if self.last_tested_at else None,
            "created_at":        self.created_at.isoformat() if self.created_at else None,
            "updated_at":        self.updated_at.isoformat() if self.updated_at else None,
        }


class KafkaConnectConnection(db.Model):
    """Kafka Connect REST API connection."""
    __tablename__ = "kafka_connect_connections"

    id             = db.Column(db.Integer, primary_key=True)
    name           = db.Column(db.String(128), nullable=False, unique=True)
    url            = db.Column(db.String(512), nullable=False)
    username       = db.Column(db.String(128), nullable=True)
    password       = db.Column(db.String(256), nullable=True)
    status         = db.Column(db.String(32), nullable=False, default="disconnected")
    last_tested_at = db.Column(db.DateTime, nullable=True)
    created_at     = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at     = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id":             self.id,
            "name":           self.name,
            "url":            self.url,
            "username":       self.username,
            "status":         self.status,
            "last_tested_at": self.last_tested_at.isoformat() if self.last_tested_at else None,
            "created_at":     self.created_at.isoformat() if self.created_at else None,
            "updated_at":     self.updated_at.isoformat() if self.updated_at else None,
        }


class MigrationJob(db.Model):
    """
    Migration job — одна запись = миграция одной таблицы.
    Соответствует migration_system.migration_jobs из спецификации,
    но хранится в координационной БД (PostgreSQL) через SQLAlchemy.
    """
    __tablename__ = "migration_jobs"

    # Идентификатор
    id                    = db.Column(db.Integer, primary_key=True)

    # Название задания (произвольное, для UI)
    name                  = db.Column(db.String(256), nullable=False)

    # Подключения (FK к DbConnection)
    source_conn_id        = db.Column(db.Integer, db.ForeignKey("db_connections.id"), nullable=False)
    target_conn_id        = db.Column(db.Integer, db.ForeignKey("db_connections.id"), nullable=False)

    # Таблицы
    source_table          = db.Column(db.String(200), nullable=False)   # SCHEMA.TABLE в source
    target_table          = db.Column(db.String(200), nullable=True)    # SCHEMA.TABLE в target (если отличается)

    # Параметры чанкования
    chunk_size            = db.Column(db.Integer, nullable=False, default=10000)
    # Стратегия: rowid / pk_range / date_range / hash
    chunk_strategy        = db.Column(db.String(32), nullable=False, default="rowid")

    # Параметры CDC
    # Режим: bulk_cdc / bulk_only / cdc_only
    migration_mode        = db.Column(db.String(32), nullable=False, default="bulk_cdc")

    # Опциональный WHERE-фильтр для bulk SELECT
    filter_clause         = db.Column(db.Text, nullable=True)

    # Статус: pending / cdc_starting / running / bulk_done / completed / failed / cancelled
    status                = db.Column(db.String(32), nullable=False, default="pending")

    # Прогресс (обновляется воркерами)
    scn_cutoff            = db.Column(db.BigInteger, nullable=True)     # Oracle SCN точки разреза
    max_rowid             = db.Column(db.String(100), nullable=True)    # верхняя граница ROWID для bulk
    chunks_total          = db.Column(db.Integer, nullable=False, default=0)
    chunks_done           = db.Column(db.Integer, nullable=False, default=0)
    chunks_failed         = db.Column(db.Integer, nullable=False, default=0)
    rows_migrated         = db.Column(db.BigInteger, nullable=False, default=0)

    # CDC ownership (заполняется воркером)
    cdc_owner_worker_id   = db.Column(db.String(100), nullable=True)
    cdc_started_at        = db.Column(db.DateTime, nullable=True)
    cdc_heartbeat_at      = db.Column(db.DateTime, nullable=True)

    # Debezium коннектор
    debezium_connector_name = db.Column(db.String(300), nullable=True)  # имя Debezium коннектора

    # Временные метки
    created_at            = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    started_at            = db.Column(db.DateTime, nullable=True)
    completed_at          = db.Column(db.DateTime, nullable=True)
    updated_at            = db.Column(db.DateTime, nullable=False, default=datetime.utcnow,
                                      onupdate=datetime.utcnow)

    # Relationships
    source_conn           = db.relationship("DbConnection", foreign_keys=[source_conn_id])
    target_conn           = db.relationship("DbConnection", foreign_keys=[target_conn_id])

    def to_dict(self):
        # Вычисляем прогресс в процентах
        progress = 0
        if self.chunks_total and self.chunks_total > 0:
            progress = round(self.chunks_done / self.chunks_total * 100)

        return {
            "id":                       self.id,
            "name":                     self.name,
            "source_conn_id":           self.source_conn_id,
            "target_conn_id":           self.target_conn_id,
            "source":                   self.source_conn.name if self.source_conn else None,
            "target":                   self.target_conn.name if self.target_conn else None,
            "source_table":             self.source_table,
            "target_table":             self.target_table or self.source_table,
            "chunk_size":               self.chunk_size,
            "chunk_strategy":           self.chunk_strategy,
            "migration_mode":           self.migration_mode,
            "filter_clause":            self.filter_clause,
            "status":                   self.status,
            "progress":                 progress,
            "scn_cutoff":               str(self.scn_cutoff) if self.scn_cutoff else None,
            "max_rowid":                self.max_rowid,
            "chunks_total":             self.chunks_total,
            "chunks_done":              self.chunks_done,
            "chunks_failed":            self.chunks_failed,
            "rows_migrated":            self.rows_migrated,
            "cdc_owner_worker_id":      self.cdc_owner_worker_id,
            "cdc_started_at":           self.cdc_started_at.isoformat() if self.cdc_started_at else None,
            "cdc_heartbeat_at":         self.cdc_heartbeat_at.isoformat() if self.cdc_heartbeat_at else None,
            "debezium_connector_name":  self.debezium_connector_name,
            "created_at":               self.created_at.isoformat() if self.created_at else None,
            "started_at":               self.started_at.isoformat() if self.started_at else None,
            "completed_at":             self.completed_at.isoformat() if self.completed_at else None,
            "updated_at":               self.updated_at.isoformat() if self.updated_at else None,
        }


class MigrationPreflightResult(db.Model):
    """Результаты pre-flight проверок перед созданием job."""
    __tablename__ = 'migration_preflight_results'

    id             = db.Column(db.Integer, primary_key=True, autoincrement=True)
    job_name       = db.Column(db.String(200), nullable=False)   # имя будущего job
    source_table   = db.Column(db.String(200), nullable=False)
    target_table   = db.Column(db.String(200), nullable=False)
    source_conn_id = db.Column(db.Integer, db.ForeignKey('db_connections.id'))
    target_conn_id = db.Column(db.Integer, db.ForeignKey('db_connections.id'))
    check_name     = db.Column(db.String(100), nullable=False)
    status         = db.Column(db.String(20), nullable=False)    # OK / WARNING / ERROR / EXCEPTION
    errors         = db.Column(db.JSON, default=list)
    warnings       = db.Column(db.JSON, default=list)
    checked_at     = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id":             self.id,
            "job_name":       self.job_name,
            "source_table":   self.source_table,
            "target_table":   self.target_table,
            "source_conn_id": self.source_conn_id,
            "target_conn_id": self.target_conn_id,
            "check_name":     self.check_name,
            "status":         self.status,
            "errors":         self.errors or [],
            "warnings":       self.warnings or [],
            "checked_at":     self.checked_at.isoformat() if self.checked_at else None,
        }


class DebeziumConnector(db.Model):
    """Debezium коннекторы привязанные к migration jobs."""
    __tablename__ = 'debezium_connectors'

    id                    = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # Имя коннектора: migration-{job_name}-{source_schema}-{source_table}-{short_uuid}
    # Пример: migration-orders-sales-orders-a1b2c3
    connector_name        = db.Column(db.String(300), nullable=False, unique=True)
    job_id                = db.Column(db.Integer, db.ForeignKey('migration_jobs.id'), nullable=False)
    job_name              = db.Column(db.String(200), nullable=False)  # денормализация для удобства поиска
    source_table          = db.Column(db.String(200), nullable=False)
    target_table          = db.Column(db.String(200))
    kafka_connect_conn_id = db.Column(db.Integer, db.ForeignKey('kafka_connect_connections.id'))
    scn_cutoff            = db.Column(db.BigInteger)
    config_snapshot       = db.Column(db.JSON)   # полный конфиг отправленный в Debezium
    status                = db.Column(db.String(50), default='CREATING')  # CREATING/RUNNING/PAUSED/FAILED/DESTROYED
    last_status_check     = db.Column(db.DateTime)
    error_message         = db.Column(db.Text)
    created_at            = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at            = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    job                   = db.relationship("MigrationJob", foreign_keys=[job_id])
    kafka_connect_conn    = db.relationship("KafkaConnectConnection", foreign_keys=[kafka_connect_conn_id])

    def to_dict(self):
        return {
            "id":                    self.id,
            "connector_name":        self.connector_name,
            "job_id":                self.job_id,
            "job_name":              self.job_name,
            "source_table":          self.source_table,
            "target_table":          self.target_table,
            "kafka_connect_conn_id": self.kafka_connect_conn_id,
            "scn_cutoff":            str(self.scn_cutoff) if self.scn_cutoff else None,
            "config_snapshot":       self.config_snapshot,
            "status":                self.status,
            "last_status_check":     self.last_status_check.isoformat() if self.last_status_check else None,
            "error_message":         self.error_message,
            "created_at":            self.created_at.isoformat() if self.created_at else None,
            "updated_at":            self.updated_at.isoformat() if self.updated_at else None,
        }
