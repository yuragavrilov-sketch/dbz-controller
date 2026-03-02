# Oracle Online Migration System
## Техническая постановка задачи — Версия 1.0

> Онлайн-миграция Oracle → Oracle без остановки продакшн-нагрузки

---

## Содержание

1. [Общее описание](#1-общее-описание)
2. [Архитектура системы](#2-архитектура-системы)
3. [Алгоритм миграции таблицы](#3-алгоритм-миграции-таблицы)
4. [Схема базы данных (PostgreSQL — координация)](#4-схема-базы-данных-postgresql--координация)
5. [Компоненты воркера (Python)](#5-компоненты-воркера-python)
   - 5.6 [QueryTemplateCache](#56-querytemplatecache--кэш-sql-шаблонов)
6. [Координатор (Flask)](#6-координатор-flask)
   - 6.2 [Pre-migration checks](#62-pre-migration-checks)
7. [Отказоустойчивость](#7-отказоустойчивость)
8. [Конфигурация](#8-конфигурация)
9. [Мониторинг и алерты](#9-мониторинг-и-алерты)
10. [Процедура Cutover](#10-процедура-cutover)

---

## 1. Общее описание

Система обеспечивает онлайн-миграцию данных из Oracle (source) в Oracle (target) без остановки продакшн-нагрузки. Миграция может продолжаться несколько дней или недель — все изменения в source непрерывно отражаются на target.

### 1.1 Ключевые требования

- **Нулевое время простоя** — продакшн-нагрузка не прерывается на время миграции
- **Гарантированная консистентность** — ни одна строка не потеряется и не задвоится
- **Отказоустойчивость** — любой компонент можно перезапустить без потери прогресса
- **Масштабируемость** — количество воркеров регулируется без остановки системы
- **Чистая целевая схема** — никаких служебных колонок в бизнес-таблицах target Oracle

### 1.2 Принцип работы

Миграция каждой таблицы состоит из двух параллельных процессов:

| Процесс | Что делает | Когда завершается |
|---------|-----------|-------------------|
| **Bulk** (холодные данные) | Перенос исторических данных существовавших до момента фиксации границы. ROWID-чанки, параллельно N воркеров | За конечное время, после переноса всех чанков |
| **CDC** (горячие данные) | Непрерывный стриминг всех изменений после момента фиксации границы через Debezium → Kafka | До момента cutover |

**Ключевое условие**: CDC запускается **до** начала bulk — чтобы ни одно изменение не было пропущено.

### 1.3 Разделение баз данных

```
Oracle Source  ──►  Debezium  ──►  Kafka  ──►  CDCConsumer  ──►  Oracle Target
     │                                                                  ▲
     └──────────────────  BulkWorker  ───────────────────────────────►─┘

PostgreSQL (координационная БД):
  - migration_jobs, migration_chunks, migration_boundaries
  - _migration_tombstones, _migration_offsets
  - НЕ содержит бизнес-данных
```

Oracle Target используется **только для бизнес-данных**. Все служебные таблицы живут в PostgreSQL.

---

## 2. Архитектура системы

### 2.1 Компоненты

| Компонент | Технология | Назначение |
|-----------|-----------|-----------|
| Координатор | Flask + PostgreSQL | Web UI, управление заданиями, мониторинг, heartbeat |
| Воркер | Python + Docker | Выполнение bulk и CDC; гомогенны — любой берёт любую роль |
| CDC стриминг | Debezium + Kafka | Захват изменений из Oracle redo logs source |
| Координационная БД | PostgreSQL | Чанки, прогресс, Kafka offsets, tombstones |
| Source DB | Oracle | Исходная база, не изменяется в процессе миграции |
| Target DB | Oracle | Целевая база, только бизнес-данные |

### 2.2 Роли воркеров

Все воркеры гомогенны. Роль определяется динамически при взятии задания из PostgreSQL.

| Режим | Условие | Что делает |
|-------|---------|-----------|
| **CDC owner** | Взял ownership на job (UPDATE WHERE cdc_owner IS NULL) | CDC-поток для своей таблицы + параллельно берёт bulk-чанки |
| **Bulk helper** | Нет свободных job, есть свободные чанки | Берёт bulk-чанки любых таблиц |
| **Idle** | Нет ни job ни чанков | Polling каждые 5 сек |

**Ограничение**: одна таблица = один CDC owner. Bulk-чанки одной таблицы могут обрабатывать N воркеров через `SELECT FOR UPDATE SKIP LOCKED`.

---

## 3. Алгоритм миграции таблицы

### 3.1 Последовательность — координатор

| Шаг | Действие |
|-----|----------|
| **1. Фиксация границы** | Атомарно в одной транзакции Oracle source: `SELECT CURRENT_SCN FROM V$DATABASE` и `SELECT MAX(ROWID) FROM <table>`. Сохранить в `migration_boundaries`. Это точка разреза: до неё — bulk, после — CDC. |
| **2. Запуск Debezium** | Создать коннектор через Kafka Connect REST API: `snapshot.mode=never`, `log.mining.start.scn=SCN_cutoff`. Debezium начинает стримить изменения в Kafka начиная с зафиксированного SCN. |
| **3. Ожидание старта** | Polling статуса коннектора до `RUNNING`. Таймаут 60 сек. Только после `RUNNING` продолжать — иначе возможна потеря событий. |
| **4. Нарезка чанков** | Создать ROWID-чанки через `DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID`. Только чанки с `ROWID <= max_rowid`. Записать в `migration_chunks` со статусом `pending`. |
| **4а. Построение SQL-шаблонов** | Вызвать `QueryTemplateCache.build_and_save()`: получить метаданные колонок из Oracle source (ALL_TAB_COLUMNS), исключить виртуальные колонки, построить все 4 шаблона и сохранить в `migration_jobs.config`. |
| **5. Создание job** | Создать запись в `migration_jobs` со статусом `pending`. Воркеры подхватят автоматически. |

### 3.2 Последовательность — воркеры

| Шаг | Действие |
|-----|----------|
| **6. CDC ownership** | Один воркер: `UPDATE migration_jobs SET cdc_owner=worker_id WHERE cdc_owner IS NULL`. Запускает `CDCConsumer` в daemon-потоке. |
| **7. CDC применение** | CDCConsumer читает Kafka батчами. MERGE для create/update, DELETE + tombstone для delete. Offsets сохраняются в PostgreSQL атомарно с данными. |
| **8. Bulk обработка** | Все свободные воркеры (включая CDC-owner) берут bulk-чанки. Для каждой строки: проверить tombstone → MERGE (WHEN NOT MATCHED THEN INSERT). |
| **9. Завершение bulk** | CDC-owner замечает что все чанки `completed`. Очищает tombstones. Обновляет `job.status = 'bulk_done'`. |
| **10. Ожидание cutover** | CDC продолжает работать. Координатор или оператор принимает решение о cutover после верификации. |

### 3.3 Стратегии чанкования

Стратегия определяется автоматически координатором при создании job:

| Условие | Стратегия | Реализация |
|---------|-----------|-----------|
| Heap table (стандартный случай) | **ROWID-чанки** (приоритет) | `DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID` — равномерные чанки по блокам |
| Числовой одиночный PK | Range by PK | `MIN/MAX(pk)` + равномерное деление на N диапазонов |
| Составной PK с датой создания | Range by date | Граница по временной колонке, например `created_at < cutoff_date` |
| Составной PK без временной колонки | Hash-чанки | `MOD(ORA_HASH(pk1||'|'||pk2), N) = chunk_id`, full scan |
| View или ROWID недоступен | Hash-чанки | Тот же подход |

**Приоритет**: ROWID → числовой PK → дата → hash.

### 3.4 Работа с tombstones

Tombstones решают единственный edge case: строка существовала в bulk-зоне (ROWID <= max_rowid), но была **удалена через CDC** после `SCN_cutoff`. Без tombstone bulk воскресит её.

```
CDC получил DELETE для id=1000  →  tombstone.add(id=1000)  →  DELETE from target
Bulk доходит до id=1000         →  tombstone.exists(id=1000) = True  →  SKIP
```

Tombstones актуальны только пока идёт bulk. После завершения bulk — удаляются полностью.

---

## 4. Схема базы данных (PostgreSQL — координация)

Все таблицы создаются в схеме `migration_system`. Бизнес-таблицы Oracle target не затрагиваются.

### 4.1 migration_jobs

```sql
CREATE TABLE migration_system.migration_jobs (
    job_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name            VARCHAR(200) NOT NULL,       -- schema.table в source
    target_table_name     VARCHAR(200),                -- schema.table в target (если отличается)
    scn_cutoff            BIGINT NOT NULL,             -- Oracle SCN точки разреза
    max_rowid             VARCHAR(100),                -- верхняя граница ROWID для bulk
    cdc_owner_worker_id   VARCHAR(100),                -- ID воркера-владельца CDC
    cdc_started_at        TIMESTAMP,
    cdc_heartbeat_at      TIMESTAMP,                   -- обновляется каждые 30 сек
    status                VARCHAR(20) NOT NULL DEFAULT 'pending',
    --   pending / cdc_starting / running / bulk_done / completed / failed
    config                JSONB,                       -- chunk_size, batch_size, strategy, sql_templates, columns, pk_columns
    created_at            TIMESTAMP DEFAULT NOW(),
    completed_at          TIMESTAMP
);
```

### 4.2 migration_chunks

```sql
CREATE TABLE migration_system.migration_chunks (
    chunk_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                UUID NOT NULL REFERENCES migration_jobs(job_id),
    table_name            VARCHAR(200) NOT NULL,
    start_rowid           VARCHAR(100),
    end_rowid             VARCHAR(100),
    assigned_worker_id    VARCHAR(100),
    status                VARCHAR(20) NOT NULL DEFAULT 'pending',
    --   pending / in_progress / completed / failed
    rows_processed        BIGINT DEFAULT 0,
    rows_skipped          BIGINT DEFAULT 0,            -- tombstone + DO NOTHING
    error_message         TEXT,
    assigned_at           TIMESTAMP,
    completed_at          TIMESTAMP
);

CREATE INDEX ON migration_system.migration_chunks (job_id, status);
```

### 4.3 migration_boundaries

```sql
CREATE TABLE migration_system.migration_boundaries (
    job_id          UUID PRIMARY KEY REFERENCES migration_jobs(job_id),
    table_name      VARCHAR(200) NOT NULL,
    scn_cutoff      BIGINT NOT NULL,
    max_rowid       VARCHAR(100),
    captured_at     TIMESTAMP DEFAULT NOW()
);
```

### 4.4 _migration_tombstones

Временная таблица. Удаляется после завершения bulk для job.

```sql
CREATE TABLE migration_system._migration_tombstones (
    job_id          UUID NOT NULL,
    table_name      VARCHAR(200) NOT NULL,
    pk_value        VARCHAR(500) NOT NULL,   -- JSON: {"id": 1234} или {"tenant_id": 5, "seq": 99}
    deleted_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (job_id, table_name, pk_value)
);

CREATE INDEX ON migration_system._migration_tombstones (job_id, table_name);
```

### 4.5 _migration_offsets

Kafka consumer offsets. Сохраняются **в той же транзакции что и данные** — гарантия exactly-once при рестарте.

```sql
CREATE TABLE migration_system._migration_offsets (
    consumer_group  VARCHAR(200) NOT NULL,   -- migration-consumer-{job_id}
    topic           VARCHAR(200) NOT NULL,   -- migration.{schema}.{table}
    partition       INTEGER NOT NULL,
    offset          BIGINT NOT NULL,
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition)
);
```

---

## 5. Компоненты воркера (Python)

### 5.1 MigrationWorker — главный оркестратор

Точка входа процесса воркера. Запускается как Docker-контейнер. Управляет всеми внутренними компонентами.

**Главный цикл** (бесконечный, до SIGTERM):

```python
def run(self):
    while not self.shutdown_flag:

        # Приоритет 1: взять CDC ownership
        job = self.try_acquire_cdc_ownership()
        if job:
            self.start_cdc_thread(job)

        # Приоритет 2: взять bulk-чанк (любого job)
        chunk = self.try_acquire_chunk()
        if chunk:
            self.process_bulk_chunk(chunk)
        elif not job:
            time.sleep(IDLE_POLL_INTERVAL)

        # Heartbeat каждые 30 сек
        if self.cdc_job and time.time() - self.last_heartbeat > 30:
            self.update_heartbeat()
            self.check_bulk_completion()

def try_acquire_cdc_ownership(self):
    return pg.fetchone("""
        UPDATE migration_system.migration_jobs
        SET cdc_owner_worker_id = %s,
            status = 'cdc_starting',
            cdc_started_at = NOW()
        WHERE status = 'pending'
          AND cdc_owner_worker_id IS NULL
        RETURNING *
    """, [self.worker_id])

def try_acquire_chunk(self):
    return pg.fetchone("""
        UPDATE migration_system.migration_chunks
        SET assigned_worker_id = %s,
            status = 'in_progress',
            assigned_at = NOW()
        WHERE chunk_id = (
            SELECT chunk_id FROM migration_system.migration_chunks
            WHERE status = 'pending'
            ORDER BY job_id, chunk_id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING *
    """, [self.worker_id])
```

**При SIGTERM**: установить `shutdown_flag = True`, дождаться завершения текущего чанка, отправить CDCConsumer сигнал остановиться после текущего батча, обновить статусы.

**Определение завершения bulk** (проверяется CDC-owner после каждого чанка):
```sql
SELECT COUNT(*) FROM migration_system.migration_chunks
WHERE job_id = %s AND status != 'completed'
```
Если 0 → `TombstoneRegistry.cleanup()` → `job.status = 'bulk_done'`.

---

### 5.2 CDCConsumer — применение изменений из Kafka

Работает в отдельном daemon-потоке. Читает события Debezium, применяет на Oracle target.

**Инициализация**:
```python
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=config.kafka.bootstrap_servers,
    group_id=f'migration-consumer-{job_id}',
    enable_auto_commit=False,       # только ручной коммит
    auto_offset_reset='earliest',
    value_deserializer=json.loads
)

# Восстановление позиции после рестарта
offsets = pg.fetchall("""
    SELECT partition, offset FROM migration_system._migration_offsets
    WHERE consumer_group = %s AND topic = %s
""", [group_id, topic])

for partition, offset in offsets:
    tp = TopicPartition(topic, partition)
    consumer.seek(tp, offset + 1)
```

**Цикл обработки батча**:

```python
def process_loop(self):
    while not self.stop_flag:
        messages = consumer.poll(timeout_ms=1000, max_records=500)
        if not messages:
            continue

        # 1. Дедупликация внутри батча: победитель — максимальный SCN
        latest = {}
        for tp, msgs in messages.items():
            for msg in msgs:
                key = json.dumps(msg.value['key'], sort_keys=True)
                scn = msg.value.get('scn', 0)
                if key not in latest or scn > latest[key]['scn']:
                    latest[key] = {'msg': msg, 'scn': scn}

        # 2. Применить в транзакции Oracle target + сохранить offsets в PostgreSQL
        with oracle_target.transaction(), pg.transaction():
            for item in latest.values():
                self.apply_event(item['msg'].value)

            # Offsets в PostgreSQL — атомарно с данными target
            self.save_offsets(messages)

        # 3. Kafka commit — только после успешного двойного коммита
        consumer.commit()
```

**Применение событий**:

| op | Операция | Действие на Oracle target |
|----|---------|--------------------------|
| `c` | Create | MERGE (WHEN NOT MATCHED THEN INSERT + WHEN MATCHED THEN UPDATE) |
| `r` | Read (snapshot) | MERGE |
| `u` | Update | MERGE |
| `d` | Delete | `DELETE WHERE pk=X` + `tombstone.add(pk)` |
| `t` | Truncate | `raise DangerousOperationError` — не обрабатывать автоматически |

**MERGE шаблон** для Oracle target:

```python
def build_merge(table, pk_cols, all_cols):
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in all_cols if c not in pk_cols)
    ins_cols = ", ".join(all_cols)
    ins_vals = ", ".join(f"s.{c}" for c in all_cols)
    on_clause = " AND ".join(f"t.{c} = s.{c}" for c in pk_cols)
    dual_select = ", ".join(f":{c} {c}" for c in all_cols)

    return f"""
        MERGE INTO {table} t
        USING (SELECT {dual_select} FROM dual) s
        ON ({on_clause})
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({ins_cols}) VALUES ({ins_vals})
    """
```

---

### 5.3 BulkChunkProcessor — перенос исторических данных

Обрабатывает один ROWID-чанк. Не знает про CDC и Kafka.

```python
def process_chunk(self, chunk):
    oracle_src = get_oracle_source_conn()
    oracle_tgt = get_oracle_target_conn()

    # Читаем из source по ROWID
    cursor_src = oracle_src.cursor()
    cursor_src.arraysize = 10000
    cursor_src.execute(
        f"SELECT * FROM {chunk.table_name} "
        f"WHERE ROWID BETWEEN :start_rowid AND :end_rowid",
        start_rowid=chunk.start_rowid,
        end_rowid=chunk.end_rowid
    )

    cols = [d[0].lower() for d in cursor_src.description]
    pk_cols = get_pk_columns(chunk.table_name)   # из метаданных job
    rows_processed = 0
    rows_skipped = 0
    batch = []

    for row in cursor_src:
        row_dict = dict(zip(cols, row))
        pk_dict = {c: row_dict[c] for c in pk_cols}

        # Проверка tombstone
        if self.tombstone.exists(chunk.job_id, chunk.table_name, pk_dict):
            rows_skipped += 1
            continue

        batch.append(row_dict)

        if len(batch) >= 1000:
            rows_inserted, rows_no_insert = self._flush_batch(
                oracle_tgt, chunk.table_name, pk_cols, cols, batch
            )
            rows_processed += rows_inserted
            rows_skipped += rows_no_insert
            batch.clear()

    if batch:
        rows_inserted, rows_no_insert = self._flush_batch(
            oracle_tgt, chunk.table_name, pk_cols, cols, batch
        )
        rows_processed += rows_inserted
        rows_skipped += rows_no_insert

    # Обновить статус чанка
    pg.execute("""
        UPDATE migration_system.migration_chunks
        SET status='completed', rows_processed=%s, rows_skipped=%s, completed_at=NOW()
        WHERE chunk_id=%s
    """, [rows_processed, rows_skipped, chunk.chunk_id])
```

**Bulk INSERT на Oracle target** — `MERGE WHEN NOT MATCHED` через `executemany`:

```python
def _flush_batch(self, oracle_tgt, cache: QueryTemplateCache, batch):
    # SQL шаблон уже готов — берём из кэша job
    cursor = oracle_tgt.cursor()
    cursor.executemany(cache.bulk_merge, batch)
    oracle_tgt.commit()
    rows_inserted = cursor.rowcount
    rows_no_insert = len(batch) - rows_inserted
    return rows_inserted, rows_no_insert
```

**При ошибке**:
```sql
UPDATE migration_system.migration_chunks
SET status='failed', error_message=:msg
WHERE chunk_id=:id
```
Воркер берёт следующий чанк. Координатор может сбросить `failed` → `pending` через Web UI.

---

### 5.4 TombstoneRegistry

```python
class TombstoneRegistry:

    def add(self, job_id, table_name, pk_dict):
        pg.execute("""
            INSERT INTO migration_system._migration_tombstones
                (job_id, table_name, pk_value)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, [job_id, table_name, self._key(pk_dict)])

    def exists(self, job_id, table_name, pk_dict) -> bool:
        row = pg.fetchone("""
            SELECT 1 FROM migration_system._migration_tombstones
            WHERE job_id=%s AND table_name=%s AND pk_value=%s
        """, [job_id, table_name, self._key(pk_dict)])
        return row is not None

    def cleanup(self, job_id, table_name):
        pg.execute("""
            DELETE FROM migration_system._migration_tombstones
            WHERE job_id=%s AND table_name=%s
        """, [job_id, table_name])

    def _key(self, pk_dict) -> str:
        # Стабильная сериализация — sort_keys гарантирует одинаковый результат
        return json.dumps(pk_dict, sort_keys=True, default=str)
```

---

### 5.5 DebeziumManager

```python
class DebeziumManager:

    def connector_name(self, job_id) -> str:
        return f"migration-{job_id}"

    def create_connector(self, job_id, scn_cutoff, table_name, schema):
        name = self.connector_name(job_id)

        # Идемпотентен: проверить существование
        status = self.get_status(job_id)
        if status == 'RUNNING':
            return

        config = {
            "connector.class": "io.debezium.connector.oracle.OracleConnector",
            "tasks.max": "1",
            "database.hostname": self.config.oracle_source.host,
            "database.port": self.config.oracle_source.port,
            "database.user": self.config.oracle_source.user,
            "database.password": self.config.oracle_source.password,
            "database.dbname": self.config.oracle_source.service,
            "database.pdb.name": self.config.oracle_source.pdb,
            "topic.prefix": "migration",
            "table.include.list": f"{schema}.{table_name}",
            "snapshot.mode": "no_data",
            "log.mining.strategy": "online_catalog",
            "log.mining.start.scn": str(scn_cutoff),
            "heartbeat.interval.ms": "30000",
        }
        requests.post(f"{self.connect_url}/connectors",
                      json={"name": name, "config": config})

    def wait_for_running(self, job_id, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.get_status(job_id) == 'RUNNING':
                return True
            time.sleep(3)
        raise TimeoutError(f"Debezium connector not RUNNING after {timeout}s")

    def get_status(self, job_id) -> str | None:
        resp = requests.get(f"{self.connect_url}/connectors/{self.connector_name(job_id)}/status")
        if resp.status_code == 404:
            return None
        return resp.json()['connector']['state']

    def get_lag_seconds(self, job_id) -> float:
        # Текущий SCN Oracle source
        current_scn = oracle_source.fetchone("SELECT CURRENT_SCN FROM V$DATABASE")[0]
        # Последний обработанный SCN коннектора
        resp = requests.get(f"{self.connect_url}/connectors/{self.connector_name(job_id)}/status")
        connector_scn = int(resp.json()['tasks'][0].get('offset', {}).get('scn', current_scn))
        # Примерный пересчёт SCN → секунды (зависит от нагрузки на Oracle)
        return (current_scn - connector_scn) / self.config.scn_per_second

    def delete_connector(self, job_id):
        requests.delete(f"{self.connect_url}/connectors/{self.connector_name(job_id)}")
```

---

### 5.6 QueryTemplateCache — кэш SQL-шаблонов

#### Концепция

При создании job координатор один раз строит все SQL-шаблоны для таблицы (на основе метаданных схемы) и сохраняет их в поле `config` JSONB таблицы `migration_jobs`. Воркеры при старте читают шаблоны из job и используют их без повторной генерации.

Это устраняет динамическую сборку SQL при каждом вызове `_flush_batch()` и `apply_event()` — критично для таблиц с сотнями колонок и миллионами строк.

#### Хранимые шаблоны

В `migration_jobs.config['sql_templates']` хранятся четыре шаблона:

| Ключ | Назначение | Используется в |
|------|-----------|---------------|
| `bulk_merge` | MERGE WHEN NOT MATCHED для bulk INSERT | `BulkChunkProcessor._flush_batch()` |
| `cdc_merge` | Полный MERGE (WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT) для CDC create/update | `CDCConsumer.apply_event()` |
| `cdc_delete` | DELETE WHERE pk=... для CDC delete | `CDCConsumer.apply_event()` |
| `select_chunk` | SELECT * FROM table WHERE ROWID BETWEEN :start AND :end | `BulkChunkProcessor.process_chunk()` |

#### Структура в config JSONB

```json
{
  "sql_templates": {
    "bulk_merge": "MERGE INTO schema.TABLE t USING (SELECT :col1 col1, :col2 col2 FROM dual) s ON (t.id = s.id) WHEN NOT MATCHED THEN INSERT (col1, col2) VALUES (s.col1, s.col2)",
    "cdc_merge": "MERGE INTO schema.TABLE t USING (SELECT :col1 col1, :col2 col2 FROM dual) s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.col2 = s.col2 WHEN NOT MATCHED THEN INSERT (col1, col2) VALUES (s.col1, s.col2)",
    "cdc_delete": "DELETE FROM schema.TABLE WHERE id = :id",
    "select_chunk": "SELECT * FROM schema.TABLE WHERE ROWID BETWEEN :start_rowid AND :end_rowid"
  },
  "columns": ["col1", "col2"],
  "pk_columns": ["id"],
  "virtual_columns": []
}
```

#### Python класс QueryTemplateCache

```python
class QueryTemplateCache:

    def __init__(self, job: dict):
        templates = job['config']['sql_templates']
        self.bulk_merge   = templates['bulk_merge']
        self.cdc_merge    = templates['cdc_merge']
        self.cdc_delete   = templates['cdc_delete']
        self.select_chunk = templates['select_chunk']
        self.columns      = job['config']['columns']
        self.pk_columns   = job['config']['pk_columns']

    @staticmethod
    def build_and_save(job_id: str, table_name: str,
                       columns: list, pk_cols: list,
                       virtual_cols: list) -> dict:
        """Строится координатором при создании job. Сохраняется в migration_jobs.config."""
        non_virtual = [c for c in columns if c not in virtual_cols]
        non_pk      = [c for c in non_virtual if c not in pk_cols]

        on_clause    = " AND ".join(f"t.{c} = s.{c}" for c in pk_cols)
        dual_select  = ", ".join(f":{c} {c}" for c in non_virtual)
        ins_cols     = ", ".join(non_virtual)
        ins_vals     = ", ".join(f"s.{c}" for c in non_virtual)
        set_clause   = ", ".join(f"t.{c} = s.{c}" for c in non_pk)
        pk_where     = " AND ".join(f"{c} = :{c}" for c in pk_cols)

        templates = {
            "bulk_merge": (
                f"MERGE INTO {table_name} t "
                f"USING (SELECT {dual_select} FROM dual) s "
                f"ON ({on_clause}) "
                f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})"
            ),
            "cdc_merge": (
                f"MERGE INTO {table_name} t "
                f"USING (SELECT {dual_select} FROM dual) s "
                f"ON ({on_clause}) "
                f"WHEN MATCHED THEN UPDATE SET {set_clause} "
                f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})"
            ),
            "cdc_delete": f"DELETE FROM {table_name} WHERE {pk_where}",
            "select_chunk": (
                f"SELECT * FROM {table_name} "
                f"WHERE ROWID BETWEEN :start_rowid AND :end_rowid"
            ),
        }

        config_patch = {
            "sql_templates": templates,
            "columns":        non_virtual,
            "pk_columns":     pk_cols,
            "virtual_columns": virtual_cols,
        }

        pg.execute("""
            UPDATE migration_system.migration_jobs
            SET config = config || %s::jsonb
            WHERE job_id = %s
        """, [json.dumps(config_patch), job_id])

        return config_patch
```

#### Где используется

| Место | Действие |
|-------|---------|
| Координатор, шаг 4а | Вызывает `QueryTemplateCache.build_and_save()` после нарезки чанков — один раз на весь job |
| Воркер, старт обработки job | Создаёт `QueryTemplateCache(job)` из данных job, прочитанных из PostgreSQL |
| `BulkChunkProcessor._flush_batch()` | Использует `self.cache.bulk_merge` вместо динамической генерации SQL |
| `CDCConsumer.apply_event()` | Использует `self.cache.cdc_merge` и `self.cache.cdc_delete` |

#### Преимущества

- **Экономия CPU**: SQL строится один раз на весь job — не при каждом батче из миллионов строк
- **Прозрачность**: шаблон виден в UI (в поле `config` job) — удобно для отладки и аудита
- **Исключение виртуальных колонок**: происходит один раз при создании job, а не при каждом батче
- **Независимость воркеров**: воркеры не нуждаются в прямом доступе к метаданным Oracle для построения SQL

---

## 6. Координатор (Flask)

### 6.1 Ответственность

- **Pre-migration checks**: полная проверка готовности target Oracle перед созданием job
- **Создание job**: захват SCN + max_rowid, создание Debezium коннектора, ожидание `RUNNING`, нарезка чанков
- **Мониторинг heartbeat**: если `cdc_heartbeat_at` не обновлялся > 2 мин → `cdc_owner = NULL`, статус → `pending`
- **Мониторинг чанков**: если chunk в `in_progress` > 10 мин → сбросить в `pending`
- **Мониторинг Debezium**: polling статуса коннекторов, алерт при `FAILED`
- **Cutover**: дождаться `consumer_lag = 0`, пометить job `completed`, удалить коннекторы
- **Верификация**: сравнение row counts source vs target по запросу оператора

---

### 6.2 Pre-migration checks

Выполняются координатором **до создания job** при нажатии кнопки "Create Job" в Web UI. Все проверки выполняются последовательно. При любой ошибке — создание job блокируется с подробным сообщением о причине.

Результат каждой проверки сохраняется в таблицу `migration_preflight_results` для отображения в UI.

#### 6.2.1 Схема и колонки

**Цель**: убедиться что таблица на target существует и структурно совместима с source.

| Проверка | Запрос к target | Ошибка если |
|---------|----------------|------------|
| Таблица существует | `SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER=:s AND TABLE_NAME=:t` | Таблица не найдена |
| Все колонки source присутствуют | `SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE ...` | Колонка из source отсутствует в target |
| Типы данных совместимы | Сравнение `DATA_TYPE`, `DATA_LENGTH`, `DATA_PRECISION`, `DATA_SCALE` | Тип несовместим (например, VARCHAR2(50) vs VARCHAR2(10)) |
| Nullable совпадает | Сравнение `NULLABLE` для каждой колонки | NOT NULL в target для колонки которая NULL в source |
| Виртуальные колонки | `SELECT COLUMN_NAME FROM ALL_TAB_COLS WHERE VIRTUAL_COLUMN='YES'` | Виртуальная колонка включена в список для INSERT — нужно исключить |

```python
def check_schema(self, source_table, target_table):
    errors = []

    src_cols = self.get_columns(oracle_source, source_table)
    tgt_cols = self.get_columns(oracle_target, target_table)
    tgt_col_map = {c['name']: c for c in tgt_cols}

    for col in src_cols:
        if col['name'] not in tgt_col_map:
            errors.append(f"Колонка {col['name']} отсутствует в target")
            continue
        tgt = tgt_col_map[col['name']]
        if not types_compatible(col, tgt):
            errors.append(
                f"Колонка {col['name']}: тип source={col['data_type']}({col['length']}) "
                f"несовместим с target={tgt['data_type']}({tgt['length']})"
            )
        if col['nullable'] == 'N' and tgt['nullable'] == 'Y':
            pass  # source строже — ок
        if col['nullable'] == 'Y' and tgt['nullable'] == 'N' and col.get('default') is None:
            errors.append(
                f"Колонка {col['name']}: NOT NULL в target, но NULL в source "
                f"и нет DEFAULT — bulk INSERT упадёт"
            )
    return errors
```

#### 6.2.2 Constraints

**Цель**: убедиться что constraints настроены правильно для миграции.

| Constraint | Ожидаемый статус | Причина |
|-----------|-----------------|---------|
| PRIMARY KEY | `ENABLED` + `VALIDATED` | Нужен для MERGE ON (...) |
| UNIQUE | `DISABLED` | Данные из source могут временно нарушать уникальность при частичной миграции |
| FOREIGN KEY | `DISABLED` | Референсные таблицы мигрируют независимо, временные нарушения неизбежны |
| CHECK | `DISABLED` | Данные из source могут не проходить check во время частичной миграции |
| NOT NULL | Проверяется отдельно в п. 6.2.1 | — |

```python
def check_constraints(self, target_table):
    errors = []
    warnings = []

    constraints = oracle_target.fetchall("""
        SELECT constraint_name, constraint_type, status, validated
        FROM all_constraints
        WHERE owner = :owner AND table_name = :table
          AND constraint_type IN ('P', 'U', 'R', 'C')
    """, owner=target_schema, table=target_table)

    for c in constraints:
        name, ctype, status, validated = c
        if ctype == 'P':  # Primary key
            if status != 'ENABLED':
                errors.append(
                    f"PK constraint {name}: должен быть ENABLED — нужен для MERGE"
                )
        elif ctype == 'U':  # Unique
            if status == 'ENABLED':
                errors.append(
                    f"UNIQUE constraint {name}: должен быть DISABLED — "
                    f"ALTER TABLE {target_table} DISABLE CONSTRAINT {name}"
                )
        elif ctype == 'R':  # Foreign key
            if status == 'ENABLED':
                errors.append(
                    f"FK constraint {name}: должен быть DISABLED — "
                    f"ALTER TABLE {target_table} DISABLE CONSTRAINT {name}"
                )
        elif ctype == 'C':  # Check
            if status == 'ENABLED':
                warnings.append(
                    f"CHECK constraint {name}: рекомендуется DISABLED на время миграции"
                )
    return errors, warnings
```

#### 6.2.3 Триггеры

**Цель**: все триггеры на таблице должны быть отключены.

Активные триггеры опасны: они выполняются при каждом INSERT/UPDATE/DELETE и могут писать в другие таблицы, вызывать `SEQUENCE.NEXTVAL`, выполнять бизнес-логику — что приведёт к дублям или некорректным данным.

```python
def check_triggers(self, target_table):
    errors = []

    triggers = oracle_target.fetchall("""
        SELECT trigger_name, trigger_type, triggering_event, status
        FROM all_triggers
        WHERE owner = :owner AND table_name = :table
    """, owner=target_schema, table=target_table)

    for t in triggers:
        name, ttype, event, status = t
        if status == 'ENABLED':
            errors.append(
                f"Триггер {name} ({event}): должен быть DISABLED — "
                f"ALTER TRIGGER {name} DISABLE"
            )
    return errors
```

#### 6.2.4 Индексы

**Цель**: не-PK индексы должны быть отключены для ускорения bulk INSERT.

Активные индексы при bulk INSERT в 100M строк могут замедлить вставку в 5–10 раз. Рекомендуется перевести их в `UNUSABLE` перед миграцией и пересоздать после завершения bulk.

| Тип индекса | Рекомендация | Причина |
|------------|-------------|---------|
| PK index | Оставить `VALID` | Нужен для MERGE ON |
| Unique index | `UNUSABLE` | Связан с UNIQUE constraint который должен быть DISABLED |
| Non-unique index | `UNUSABLE` | Замедляет bulk, пересоздать после |
| Function-based index | `UNUSABLE` | Замедляет bulk |
| Bitmap index | `UNUSABLE` | Особенно дорог при частых изменениях |

```python
def check_indexes(self, target_table):
    warnings = []

    indexes = oracle_target.fetchall("""
        SELECT i.index_name, i.index_type, i.uniqueness, i.status,
               c.constraint_type
        FROM all_indexes i
        LEFT JOIN all_constraints c
            ON c.index_name = i.index_name AND c.owner = i.owner
        WHERE i.owner = :owner AND i.table_name = :table
    """, owner=target_schema, table=target_table)

    for idx in indexes:
        name, itype, uniqueness, status, ctype = idx
        is_pk_index = (ctype == 'P')

        if not is_pk_index and status != 'UNUSABLE':
            warnings.append(
                f"Индекс {name} ({itype}, {uniqueness}): рекомендуется UNUSABLE — "
                f"ALTER INDEX {name} UNUSABLE"
            )
    return warnings
```

#### 6.2.5 Sequences и Identity columns

**Цель**: избежать конфликта между sequence на target и значениями PK мигрируемых данных.

```python
def check_sequences(self, source_table, target_table):
    errors = []

    # Получить max PK в source
    pk_cols = get_pk_columns(oracle_source, source_table)
    if len(pk_cols) == 1 and is_numeric(pk_cols[0]):
        max_pk = oracle_source.fetchone(
            f"SELECT MAX({pk_cols[0]}) FROM {source_table}"
        )[0]

        # Найти sequence связанный с PK на target (через DEFAULT или identity)
        seq = get_sequence_for_column(oracle_target, target_table, pk_cols[0])
        if seq:
            next_val = oracle_target.fetchone(
                f"SELECT {seq}.NEXTVAL FROM dual"
            )[0]
            if next_val <= max_pk:
                errors.append(
                    f"Sequence {seq}: текущее значение {next_val} <= MAX PK в source {max_pk}. "
                    f"После миграции sequence будет генерировать дублирующиеся PK. "
                    f"Выполните: ALTER SEQUENCE {seq} RESTART START WITH {max_pk + 10000}"
                )
    return errors
```

#### 6.2.6 Пространство (Tablespace)

**Цель**: убедиться что на target достаточно места для данных.

```python
def check_space(self, source_table, target_table):
    warnings = []

    # Размер source таблицы в байтах
    source_size = oracle_source.fetchone("""
        SELECT SUM(bytes) FROM dba_segments
        WHERE owner = :owner AND segment_name = :table
    """, owner=source_schema, table=source_table)[0] or 0

    # Свободное место в tablespace target таблицы
    target_ts = oracle_target.fetchone("""
        SELECT tablespace_name FROM all_tables
        WHERE owner = :owner AND table_name = :table
    """, owner=target_schema, table=target_table)[0]

    free_space = oracle_target.fetchone("""
        SELECT SUM(bytes) FROM dba_free_space
        WHERE tablespace_name = :ts
    """, ts=target_ts)[0] or 0

    # Коэффициент запаса x1.5 (данные + overhead)
    required = source_size * 1.5
    if free_space < required:
        warnings.append(
            f"Tablespace {target_ts}: свободно {free_space // 1024**3} GB, "
            f"требуется ~{required // 1024**3} GB (x1.5 от размера source). "
            f"Возможна ошибка ORA-01654 во время bulk."
        )

    # LOB сегменты
    lob_size = oracle_source.fetchone("""
        SELECT SUM(s.bytes) FROM dba_segments s
        JOIN dba_lobs l ON l.segment_name = s.segment_name
        WHERE l.owner = :owner AND l.table_name = :table
    """, owner=source_schema, table=source_table)[0] or 0

    if lob_size > 0:
        lob_ts = get_lob_tablespace(oracle_target, target_table)
        lob_free = get_free_space(oracle_target, lob_ts)
        if lob_free < lob_size * 1.5:
            warnings.append(
                f"LOB tablespace {lob_ts}: недостаточно места для LOB-сегментов "
                f"(требуется ~{lob_size * 1.5 // 1024**3} GB)"
            )
    return warnings
```

#### 6.2.7 Права доступа воркера

**Цель**: убедиться что пользователь воркера имеет все необходимые права на target.

```python
def check_permissions(self, target_table):
    errors = []
    required_privs = ['INSERT', 'UPDATE', 'DELETE', 'SELECT']

    granted = oracle_target.fetchall("""
        SELECT privilege FROM all_tab_privs
        WHERE table_name = :table
          AND table_schema = :owner
          AND grantee = :user
    """, table=target_table, owner=target_schema, user=worker_db_user)

    granted_set = {r[0] for r in granted}
    for priv in required_privs:
        if priv not in granted_set:
            errors.append(
                f"Пользователь {worker_db_user} не имеет привилегии {priv} "
                f"на таблице {target_table} — "
                f"GRANT {priv} ON {target_table} TO {worker_db_user}"
            )
    return errors
```

#### 6.2.8 Партиционирование

**Цель**: если target таблица партиционирована — убедиться что все данные из source попадут в существующие партиции.

```python
def check_partitioning(self, source_table, target_table):
    errors = []
    warnings = []

    is_partitioned = oracle_target.fetchone("""
        SELECT partitioning_type FROM all_part_tables
        WHERE owner = :owner AND table_name = :table
    """, owner=target_schema, table=target_table)

    if not is_partitioned:
        return errors, warnings

    part_type = is_partitioned[0]
    part_col = get_partition_column(oracle_target, target_table)

    # Проверить ROW MOVEMENT
    row_movement = oracle_target.fetchone("""
        SELECT row_movement FROM all_tables
        WHERE owner = :owner AND table_name = :table
    """, owner=target_schema, table=target_table)[0]

    if row_movement != 'ENABLED':
        warnings.append(
            f"Таблица {target_table} партиционирована, но ROW MOVEMENT отключён. "
            f"При UPDATE может возникнуть ORA-14402. "
            f"Выполните: ALTER TABLE {target_table} ENABLE ROW MOVEMENT"
        )

    # Проверить диапазон партиций покрывает данные source
    if part_type == 'RANGE':
        src_min, src_max = oracle_source.fetchone(
            f"SELECT MIN({part_col}), MAX({part_col}) FROM {source_table}"
        )
        partition_bounds = get_partition_bounds(oracle_target, target_table)
        if src_max > max(partition_bounds):
            errors.append(
                f"Значение {part_col}={src_max} в source выходит за границы "
                f"партиций target. Данные не поместятся — ORA-14400."
            )
    return errors, warnings
```

#### 6.2.9 Итоговая процедура pre-flight

```python
def run_preflight(self, source_table, target_table) -> PreflightResult:
    all_errors = []
    all_warnings = []

    checks = [
        ("Схема и колонки",        self.check_schema),
        ("Constraints",            self.check_constraints),
        ("Триггеры",               self.check_triggers),
        ("Индексы",                self.check_indexes),
        ("Sequences",              self.check_sequences),
        ("Пространство",           self.check_space),
        ("Права доступа воркера",  self.check_permissions),
        ("Партиционирование",      self.check_partitioning),
    ]

    results = []
    for check_name, check_fn in checks:
        try:
            errors, warnings = check_fn(source_table, target_table)
            all_errors.extend(errors)
            all_warnings.extend(warnings)
            results.append({
                "check": check_name,
                "status": "ERROR" if errors else ("WARNING" if warnings else "OK"),
                "errors": errors,
                "warnings": warnings,
            })
        except Exception as e:
            all_errors.append(f"Ошибка при выполнении проверки '{check_name}': {e}")
            results.append({"check": check_name, "status": "EXCEPTION", "error": str(e)})

    # Сохранить результаты в PostgreSQL для отображения в UI
    save_preflight_results(source_table, target_table, results)

    return PreflightResult(
        can_proceed=len(all_errors) == 0,
        errors=all_errors,
        warnings=all_warnings,
        details=results
    )
```

Создание job блокируется если `can_proceed=False`. Warnings отображаются оператору, но не блокируют создание.

#### 6.2.10 Таблица результатов pre-flight (PostgreSQL)

```sql
CREATE TABLE migration_system.migration_preflight_results (
    id              SERIAL PRIMARY KEY,
    source_table    VARCHAR(200) NOT NULL,
    target_table    VARCHAR(200) NOT NULL,
    check_name      VARCHAR(100) NOT NULL,
    status          VARCHAR(20) NOT NULL,   -- OK / WARNING / ERROR / EXCEPTION
    errors          JSONB DEFAULT '[]',
    warnings        JSONB DEFAULT '[]',
    checked_at      TIMESTAMP DEFAULT NOW()
);
```

---

### 6.3 Web UI — вкладки

| Вкладка | Содержимое |
|---------|-----------|
| **Jobs** | Список заданий: прогресс чанков (N/total), CDC лаг (сек), скорость (строк/сек). Кнопки: создать, сбросить failed чанки, начать cutover |
| **Pre-flight** | Результаты последних проверок по каждой таблице: список checks с OK / WARNING / ERROR и подробными сообщениями. Кнопка "Re-run checks" |
| **Workers** | Активные воркеры: режим (CDC owner / bulk helper / idle), heartbeat, чанков обработано за сессию |
| **Database config** | Подключения к Oracle source, Oracle target, PostgreSQL, Kafka, Kafka Connect. Тест соединений |
| **Monitoring** | CDC лаг, Kafka consumer lag, скорость bulk, ошибки — графики и последние события |

### 6.4 Таймауты

| Событие | Таймаут | Действие координатора |
|---------|---------|----------------------|
| CDC heartbeat молчит | 2 минуты | `cdc_owner = NULL`, `status = 'pending'` — новый воркер подхватит |
| Chunk `in_progress` завис | 10 минут | `status = 'pending'`, `assigned_worker = NULL` |
| Debezium не стартует | 60 секунд | Ошибка создания job, удалить коннектор |
| Debezium статус `FAILED` | Немедленно | Алерт оператору, попытка рестарта коннектора |

---

## 7. Отказоустойчивость

### 7.1 Сценарии отказов

| Сценарий | Что происходит | Восстановление |
|----------|---------------|---------------|
| Воркер упал в середине bulk-чанка | Чанк остаётся `in_progress` | Координатор по таймауту сбрасывает в `pending`. MERGE WHEN NOT MATCHED идемпотентен |
| Воркер упал в середине CDC-батча | Kafka offset не закоммичен | При рестарте батч перечитывается. MERGE идемпотентен. Дедупликация по SCN |
| CDC-воркер упал | Heartbeat перестаёт обновляться | Координатор через 2 мин освобождает ownership. Новый воркер берёт CDC с offset из `_migration_offsets`. Debezium писал в Kafka всё это время |
| Oracle target недоступен | CDC и bulk останавливаются | Kafka хранит события (настроенный retention). После восстановления продолжают с сохранённого offset |
| Kafka недоступна | CDCConsumer не читает | Воркер логирует, переподключается каждые 30 сек. Bulk продолжает независимо |
| Debezium упал | Коннектор в статусе `FAILED` | Координатор обнаруживает. Воркер пересоздаёт коннектор с тем же `SCN_cutoff` из `migration_boundaries` |
| Oracle source archived log удалён | Debezium не может читать с SCN | Критическая ошибка, алерт. Требует ручного решения. Профилактика: мониторинг лага |

### 7.2 Идемпотентность всех операций

| Операция | Гарантия |
|----------|---------|
| CDC MERGE (c/r/u) | MERGE ON (pk) → повторное применение даёт тот же результат |
| CDC DELETE | `DELETE WHERE pk=X` — если строки нет, ничего не происходит |
| Tombstone add | `INSERT ... ON CONFLICT DO NOTHING` |
| Bulk MERGE | `WHEN NOT MATCHED` — повторная обработка чанка безопасна |
| Kafka offset | Сохраняется атомарно с данными в двух транзакциях (Oracle target + PostgreSQL) |
| CDC ownership | `UPDATE WHERE cdc_owner IS NULL` — только один воркер пройдёт |

### 7.3 Двойной коммит в CDCConsumer

CDCConsumer коммитит **две** независимые транзакции последовательно:

```
1. oracle_target.commit()   — бизнес-данные
2. pg.commit()              — offsets в _migration_offsets
3. consumer.commit()        — Kafka offset
```

Если между шагами 1 и 2 произойдёт сбой — данные на target будут, но offset не сохранится. При рестарте батч перечитается и MERGE пройдёт повторно без вреда. Это допустимо: at-least-once + идемпотентный MERGE.

---

## 8. Конфигурация

### 8.1 config.yaml воркера

```yaml
oracle_source:
  dsn: "host:port/service_name"
  user: migration_user
  password: "..."
  fetch_size: 10000            # cx_Oracle arraysize для bulk SELECT

oracle_target:
  dsn: "host:port/service_name"
  user: migration_user
  password: "..."
  bulk_batch_size: 1000        # строк в одном executemany

coordination_db:               # PostgreSQL
  dsn: "postgresql://user:pass@host:5432/migration_db"

kafka:
  bootstrap_servers: "kafka:9092"
  consumer_batch_size: 500
  poll_timeout_ms: 1000

debezium:
  connect_url: "http://kafka-connect:8083"
  heartbeat_interval_ms: 30000
  scn_per_second: 1000         # для расчёта лага в секундах

worker:
  heartbeat_interval_sec: 30
  idle_poll_interval_sec: 5

monitoring:
  max_cdc_lag_seconds: 300
  max_kafka_lag_messages: 100000
```

### 8.2 Требования к Oracle source

- Supplemental logging для мигрируемых таблиц:
  ```sql
  ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
  ```
- Пользователь имеет SELECT на `V$DATABASE`, `V$LOGMNR_CONTENTS`, `DBMS_PARALLEL_EXECUTE`
- Archive log mode включён
- Retention archived logs ≥ длительность миграции + 2 дня:
  ```
  RMAN> CONFIGURE ARCHIVELOG RETENTION POLICY TO RECOVERY WINDOW OF 14 DAYS;
  ```
- Дополнительная нагрузка: supplemental logging увеличивает объём redo на ~20–50%

### 8.3 Требования к Kafka

- Retention топика ≥ длительность миграции + запас — **настроить до старта Debezium**:
  ```
  kafka-configs --alter --topic migration.<schema>.<table> \
    --add-config retention.ms=1209600000   # 2 недели
  ```
- Партиционирование по PK: каждая строка всегда в одном партишне — гарантия порядка событий

---

## 9. Мониторинг и алерты

| Метрика | Источник | Порог алерта | Критичность |
|---------|---------|-------------|------------|
| Debezium connector status | Kafka Connect REST API | `FAILED` или `PAUSED` | 🔴 Critical |
| Oracle source archived log retention | `V$RECOVERY_FILE_DEST` | < 2 дней до истечения | 🔴 Critical |
| Oracle source redo disk | `V$RECOVERY_FILE_DEST.PERCENT_SPACE_USED` | > 80% | 🔴 Critical |
| CDC лаг (секунды) | SCN delta source vs Debezium offset | > 300 сек | 🟠 High |
| CDC heartbeat | `migration_jobs.cdc_heartbeat_at` | > 2 мин без обновления | 🟠 High |
| Kafka consumer lag | Consumer group offsets | > 100 000 сообщений | 🟠 High |
| Kafka disk | Kafka broker JMX metrics | > 80% | 🟠 High |
| Bulk скорость | `rows_processed / elapsed_time` | < 1000 строк/сек | 🟡 Medium |
| Чанки в статусе `failed` | `migration_chunks` | > 0 | 🟡 Medium |

---

## 10. Процедура Cutover

Cutover — финальное переключение трафика с Oracle source на Oracle target. Выполняется оператором через Web UI.

### 10.1 Предусловия

- Все bulk-чанки в статусе `completed`
- CDC лаг < 30 секунд
- Нет чанков в статусе `failed`
- Row counts на target сопоставимы с source

### 10.2 Шаги

1. Оператор нажимает **Begin Cutover** в Web UI
2. Остановить запись в source (maintenance window, обычно секунды)
3. Ждать пока CDCConsumer дочитает Kafka до конца (`consumer_lag = 0`)
4. Финальная верификация: сравнить row counts и контрольные суммы
5. Переключить connection strings приложений на Oracle target
6. `DebeziumManager.delete_connector()` для каждого job
7. Обновить статус всех job в `completed`

### 10.3 Откат

Если верификация провалилась или обнаружены проблемы: вернуть трафик на Oracle source (source не изменялась). CDC можно перезапустить с нового SCN для повторной попытки cutover.

### 10.4 Вне scope данной постановки

- Настройка Kafka топика (retention, партиционирование) — инфраструктурная задача
- Включение supplemental logging на Oracle source — выполняется до создания job
- Переключение connection strings приложений — решается отдельно
- Верификация данных после cutover — отдельный компонент
- Создание объектов схемы на Oracle target (таблицы, индексы, constraints) — выполняется заранее

---

*Документ описывает архитектуру и логику системы. Детали реализации отдельных подсистем (Docker-compose, CI/CD, schema migration) выходят за рамки данной постановки.*
