import logging
import sqlite3
import time
import uuid
from typing import Protocol, runtime_checkable

from structlog.typing import FilteringBoundLogger

from sovereign import config, stats
from sovereign.v2.logging import capture_exception, get_named_logger
from sovereign.v2.types import QueueJob, queue_job_type_adapter


@runtime_checkable
class QueueProtocol(Protocol):
    def put(self, job: QueueJob) -> str | None: ...

    def get(self) -> QueueJob | None: ...

    def size(self) -> int: ...


class InMemoryQueue(QueueProtocol):
    def __init__(self) -> None:
        self.logger: FilteringBoundLogger = get_named_logger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            level=logging.DEBUG,
        )

        self._messages: dict[str, QueueJob] = {}

    def put(self, job: QueueJob) -> str | None:
        message_id = str(uuid.uuid4())
        self._messages[message_id] = job
        self.logger.debug(
            "Putting job in queue",
            job=job,
            job_type=type(job).__name__,
            message_id=message_id,
            queue_size=len(self._messages),
        )
        return message_id

    def get(self) -> QueueJob | None:
        timeout = 30
        start_time = int(time.time())
        poll_interval_seconds = 0.5

        while int(time.time()) - start_time < timeout:
            for message_id, job in self._messages.items():
                del self._messages[message_id]
                self.logger.debug("Retrieved job from queue", message_id=message_id)
                return job

            time.sleep(poll_interval_seconds)

        return None

    def is_empty(self) -> bool:
        return not self._messages

    def size(self) -> int:
        return len(self._messages)


class SqliteQueue(QueueProtocol):
    def __init__(self):
        self.logger: FilteringBoundLogger = get_named_logger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            level=logging.INFO,
        )
        self.db_path = config.worker_v2_queue_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        # check_same_thread=False allows SQLite connections to be shared across threads
        # and means that we need to ensure thread safety ourselves.
        # isolation_level=None uses autocommit mode,
        # which prevents "cannot commit - no transaction is active" errors in multi-threaded contexts.
        conn = sqlite3.connect(
            self.db_path, check_same_thread=False, isolation_level=None
        )
        # Enable WAL mode for better concurrent read/write performance
        conn.execute("PRAGMA journal_mode=WAL")
        # Set busy timeout to 5 seconds to retry on lock contention instead of failing immediately
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        try:
            with self._get_connection() as conn:
                conn.execute("""
                             CREATE TABLE IF NOT EXISTS queue
                             (
                                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                                 data TEXT NOT NULL
                             )
                             """)
                conn.commit()
        except Exception as e:
            self.logger.exception("Failed to initialise SQLite queue database")
            capture_exception(e)
            raise

    def put(self, job: QueueJob) -> str | None:
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    "INSERT INTO queue (data) VALUES (?)",
                    (job.model_dump_json(),),
                )
                job_id = str(cursor.lastrowid)
                self.logger.debug("Put job in SQLite queue", job=job, job_id=job_id)
                stats.increment(
                    "v2.worker.job_queued", tags=[f"job_type:{type(job).__name__}"]
                )
                return str(job_id)
        except Exception as e:
            self.logger.exception("Failed to put job in SQLite queue", job=job)
            capture_exception(e)
            return None

    def get(self) -> QueueJob | None:
        timeout = 30
        start_time = time.time()
        poll_interval_seconds = 0.5

        while time.time() - start_time < timeout:
            try:
                with self._get_connection() as conn:
                    cursor = conn.execute(
                        """
                        DELETE FROM queue
                        WHERE id = (SELECT id FROM queue ORDER BY id ASC LIMIT 1)
                        RETURNING id, data
                        """
                    )
                    row = cursor.fetchone()
                    if row:
                        conn.commit()
                        job = queue_job_type_adapter.validate_json(row["data"])
                        self.logger.debug(
                            "Retrieved job from queue",
                            job_id=row["id"],
                            job_type=type(job).__name__,
                        )
                        return job
            except Exception as e:
                self.logger.exception("Failed to get job from SQLite queue")
                capture_exception(e)
                return None

            time.sleep(poll_interval_seconds)

        return None

    def size(self) -> int:
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM queue")
                row = cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            self.logger.exception("Failed to get queue size from SQLite queue")
            capture_exception(e)
            return 0
