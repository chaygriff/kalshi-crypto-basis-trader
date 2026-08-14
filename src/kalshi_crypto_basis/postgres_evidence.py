"""Least-privilege PostgreSQL connection and durable evidence storage."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files
from types import TracebackType
from typing import Self
from uuid import UUID

import psycopg
from psycopg import Connection

from kalshi_crypto_basis.snapshots import SnapshotEnvelope, SnapshotError


class PostgresEvidenceError(RuntimeError):
    """Raised when durable evidence storage cannot preserve its contract."""


@dataclass(frozen=True, slots=True)
class PostgresConnectionIdentity:
    """Verified database identity and applied schema version."""

    user: str
    database: str
    schema_version: int


@dataclass(frozen=True, slots=True)
class CollectionRunRecord:
    """Replayed collection-run state and ordered evidence lineage."""

    run_id: str
    provider: str
    scope: tuple[str, ...]
    started_at: datetime
    state: str
    completed_at: datetime | None
    snapshot_ids: tuple[str, ...]
    gaps: tuple[str, ...]
    expected_snapshot_count: int | None


class PostgresEvidenceStore:
    """Runtime connection to the append-only evidence schema."""

    def __init__(self, connection: Connection[tuple[object, ...]]) -> None:
        self._connection = connection

    @classmethod
    def connect(cls, service_name: str, *, database_name: str | None = None) -> Self:
        if type(service_name) is not str or not service_name:
            raise PostgresEvidenceError("service_name must be a non-empty string")
        database_name = _validated_database_name(database_name)
        try:
            if database_name is None:
                connection = psycopg.connect(service=service_name)
            else:
                connection = psycopg.connect(service=service_name, dbname=database_name)
        except psycopg.Error as error:
            raise PostgresEvidenceError("PostgreSQL runtime connection failed") from error
        return cls(connection)

    def connection_identity(self) -> PostgresConnectionIdentity:
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT current_user, current_database(), max(version)
                    FROM evidence.schema_migrations
                    GROUP BY current_user, current_database()
                    """
                )
                row = cursor.fetchone()
        except psycopg.Error as error:
            raise PostgresEvidenceError("PostgreSQL schema identity check failed") from error
        if (
            row is None
            or type(row[0]) is not str
            or type(row[1]) is not str
            or type(row[2]) is not int
        ):
            raise PostgresEvidenceError("PostgreSQL schema identity is incomplete")
        return PostgresConnectionIdentity(user=row[0], database=row[1], schema_version=row[2])

    def put(self, snapshot: SnapshotEnvelope, *, raw_payload: bytes) -> SnapshotEnvelope:
        """Atomically persist exact raw bytes and a revalidated snapshot envelope."""
        validated = _validated_snapshot(snapshot)
        if type(raw_payload) is not bytes:
            raise SnapshotError("raw_payload must be bytes")
        if hashlib.sha256(raw_payload).hexdigest() != validated.raw_sha256:
            raise SnapshotError("raw payload hash mismatch")
        envelope_json = validated.to_canonical_json()
        try:
            with self._connection.transaction():
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO evidence.raw_payloads (raw_sha256, payload)
                        VALUES (%s, %s)
                        ON CONFLICT (raw_sha256) DO NOTHING
                        """,
                        (validated.raw_sha256, raw_payload),
                    )
                    cursor.execute(
                        "SELECT payload FROM evidence.raw_payloads WHERE raw_sha256 = %s",
                        (validated.raw_sha256,),
                    )
                    raw_row = cursor.fetchone()
                    if raw_row is None or _database_bytes(raw_row[0]) != raw_payload:
                        raise SnapshotError("raw payload identity conflict")
                    cursor.execute(
                        """
                        INSERT INTO evidence.snapshots (
                            snapshot_id,
                            idempotency_key,
                            raw_sha256,
                            source,
                            request_fingerprint,
                            observed_at,
                            ingested_at,
                            parser_version,
                            envelope_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (idempotency_key) DO NOTHING
                        """,
                        (
                            validated.snapshot_id,
                            validated.idempotency_key,
                            validated.raw_sha256,
                            validated.source,
                            validated.request_fingerprint,
                            validated.observed_at,
                            validated.ingested_at,
                            validated.parser_version,
                            envelope_json,
                        ),
                    )
                    cursor.execute(
                        """
                        SELECT envelope_json, raw_sha256
                        FROM evidence.snapshots
                        WHERE idempotency_key = %s
                        """,
                        (validated.idempotency_key,),
                    )
                    snapshot_row = cursor.fetchone()
                    if snapshot_row is None:
                        raise PostgresEvidenceError("persisted snapshot could not be read")
                    cursor.execute(
                        "SELECT payload FROM evidence.raw_payloads WHERE raw_sha256 = %s",
                        (snapshot_row[1],),
                    )
                    stored_raw_row = cursor.fetchone()
                    if stored_raw_row is None:
                        raise PostgresEvidenceError("persisted raw evidence could not be read")
                    stored = SnapshotEnvelope.from_canonical_json(
                        _database_bytes(snapshot_row[0]),
                        raw_payload=_database_bytes(stored_raw_row[0]),
                    )
                    if stored.normalized != validated.normalized:
                        raise SnapshotError("idempotency conflict: parser output changed")
                    return stored
        except (psycopg.Error, TypeError, ValueError) as error:
            if isinstance(error, SnapshotError):
                raise
            raise PostgresEvidenceError("PostgreSQL snapshot persistence failed") from error

    def get(self, snapshot_id: str) -> SnapshotEnvelope | None:
        if type(snapshot_id) is not str or not snapshot_id:
            raise SnapshotError("snapshot_id must be a non-empty string")
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT snapshot.envelope_json, raw.payload
                    FROM evidence.snapshots AS snapshot
                    JOIN evidence.raw_payloads AS raw USING (raw_sha256)
                    WHERE snapshot.snapshot_id = %s
                    """,
                    (snapshot_id,),
                )
                row = cursor.fetchone()
        except psycopg.Error as error:
            raise PostgresEvidenceError("PostgreSQL snapshot read failed") from error
        if row is None:
            return None
        return SnapshotEnvelope.from_canonical_json(
            _database_bytes(row[0]), raw_payload=_database_bytes(row[1])
        )

    def get_raw(self, raw_sha256: str) -> bytes | None:
        if type(raw_sha256) is not str or not raw_sha256:
            raise SnapshotError("raw_sha256 must be a non-empty string")
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    "SELECT payload FROM evidence.raw_payloads WHERE raw_sha256 = %s",
                    (raw_sha256,),
                )
                row = cursor.fetchone()
        except psycopg.Error as error:
            raise PostgresEvidenceError("PostgreSQL raw evidence read failed") from error
        return None if row is None else _database_bytes(row[0])

    def start_run(
        self,
        *,
        run_id: str,
        provider: str,
        scope: tuple[str, ...],
        started_at: datetime,
    ) -> None:
        run_id = _canonical_uuid(run_id)
        provider = _required_text(provider, "provider")
        scope_json = _canonical_text_tuple(scope, "scope")
        _require_utc(started_at, "started_at")
        gaps_json = b"[]"
        try:
            with self._connection.transaction():
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO evidence.collection_runs (
                            run_id, provider, scope_json, started_at
                        ) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (run_id) DO NOTHING
                        """,
                        (run_id, provider, scope_json, started_at),
                    )
                    cursor.execute(
                        """
                        SELECT provider, scope_json, started_at
                        FROM evidence.collection_runs WHERE run_id = %s
                        """,
                        (run_id,),
                    )
                    row = cursor.fetchone()
                    if row is None or (row[0], _database_bytes(row[1]), row[2]) != (
                        provider,
                        scope_json,
                        started_at,
                    ):
                        raise PostgresEvidenceError("collection run identity conflict")
                    cursor.execute(
                        """
                        INSERT INTO evidence.collection_run_events (
                            run_id, sequence, state, occurred_at, gaps_json
                        ) VALUES (%s, 0, 'started', %s, %s)
                        ON CONFLICT (run_id, sequence) DO NOTHING
                        """,
                        (run_id, started_at, gaps_json),
                    )
        except psycopg.Error as error:
            raise PostgresEvidenceError("PostgreSQL collection run start failed") from error

    def attach_snapshot(self, *, run_id: str, snapshot_id: str, ordinal: int) -> None:
        run_id = _canonical_uuid(run_id)
        snapshot_id = _required_text(snapshot_id, "snapshot_id")
        if type(ordinal) is not int or ordinal < 0:
            raise PostgresEvidenceError("ordinal must be a non-negative integer")
        try:
            with self._connection.transaction():
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT 1 FROM evidence.collection_runs WHERE run_id = %s",
                        (run_id,),
                    )
                    if cursor.fetchone() is None:
                        raise PostgresEvidenceError("collection run does not exist")
                    cursor.execute(
                        """
                        SELECT 1 FROM evidence.collection_run_events
                        WHERE run_id = %s AND sequence = 1
                        """,
                        (run_id,),
                    )
                    if cursor.fetchone() is not None:
                        raise PostgresEvidenceError(
                            "terminal collection run lineage cannot be extended"
                        )
                    cursor.execute(
                        """
                        SELECT snapshot_id FROM evidence.collection_run_snapshots
                        WHERE run_id = %s AND ordinal = %s
                        """,
                        (run_id, ordinal),
                    )
                    existing = cursor.fetchone()
                    if existing is not None:
                        if existing[0] == snapshot_id:
                            return
                        raise PostgresEvidenceError("collection snapshot ordinal conflict")
                    cursor.execute(
                        """
                        SELECT count(*) FROM evidence.collection_run_snapshots
                        WHERE run_id = %s
                        """,
                        (run_id,),
                    )
                    count_row = cursor.fetchone()
                    retained_snapshot_count = None if count_row is None else count_row[0]
                    if ordinal != retained_snapshot_count:
                        raise PostgresEvidenceError(
                            "snapshot ordinal must be the next contiguous value"
                        )
                    cursor.execute(
                        """
                        INSERT INTO evidence.collection_run_snapshots (
                            run_id, ordinal, snapshot_id
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT (run_id, ordinal) DO NOTHING
                        """,
                        (run_id, ordinal, snapshot_id),
                    )
                    cursor.execute(
                        """
                        SELECT snapshot_id FROM evidence.collection_run_snapshots
                        WHERE run_id = %s AND ordinal = %s
                        """,
                        (run_id, ordinal),
                    )
                    row = cursor.fetchone()
                    if row is None or row[0] != snapshot_id:
                        raise PostgresEvidenceError("collection snapshot ordinal conflict")
        except psycopg.Error as error:
            if error.sqlstate == "55000":
                raise PostgresEvidenceError(
                    "terminal collection run lineage cannot be extended"
                ) from error
            if error.sqlstate == "23514":
                raise PostgresEvidenceError(
                    "snapshot ordinal must be the next contiguous value"
                ) from error
            raise PostgresEvidenceError("PostgreSQL collection snapshot link failed") from error

    def finish_run(
        self,
        *,
        run_id: str,
        state: str,
        completed_at: datetime,
        gaps: tuple[str, ...],
        expected_snapshot_count: int | None,
    ) -> None:
        run_id = _canonical_uuid(run_id)
        if state not in {"complete", "incomplete", "failed"}:
            raise PostgresEvidenceError("terminal state must be complete, incomplete, or failed")
        _require_utc(completed_at, "completed_at")
        if expected_snapshot_count is not None and (
            type(expected_snapshot_count) is not int or expected_snapshot_count < 0
        ):
            raise PostgresEvidenceError(
                "expected_snapshot_count must be a non-negative integer or None"
            )
        if state == "complete" and (
            type(expected_snapshot_count) is not int or expected_snapshot_count <= 0
        ):
            raise PostgresEvidenceError("complete run requires exact nonzero snapshot count")
        gaps_json = _canonical_text_tuple(gaps, "gaps", allow_empty=True)
        if state == "complete" and gaps_json != b"[]":
            raise PostgresEvidenceError("complete run cannot retain gaps")
        try:
            with self._connection.transaction():
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT started_at FROM evidence.collection_runs
                        WHERE run_id = %s
                        """,
                        (run_id,),
                    )
                    run_row = cursor.fetchone()
                    if run_row is None:
                        raise PostgresEvidenceError("collection run does not exist")
                    stored_started_at = _database_datetime(run_row[0], "started_at")
                    if completed_at < stored_started_at:
                        raise PostgresEvidenceError("completed_at precedes started_at")
                    cursor.execute(
                        """
                        SELECT count(*) FROM evidence.collection_run_snapshots
                        WHERE run_id = %s
                        """,
                        (run_id,),
                    )
                    count_row = cursor.fetchone()
                    retained_snapshot_count = None if count_row is None else count_row[0]
                    if state == "complete" and retained_snapshot_count != expected_snapshot_count:
                        raise PostgresEvidenceError(
                            "complete run requires exact nonzero snapshot count"
                        )
                    cursor.execute(
                        """
                        INSERT INTO evidence.collection_run_events (
                            run_id, sequence, state, occurred_at, gaps_json,
                            expected_snapshot_count
                        ) VALUES (%s, 1, %s, %s, %s, %s)
                        ON CONFLICT (run_id, sequence) DO NOTHING
                        """,
                        (
                            run_id,
                            state,
                            completed_at,
                            gaps_json,
                            expected_snapshot_count,
                        ),
                    )
                    cursor.execute(
                        """
                        SELECT state, occurred_at, gaps_json, expected_snapshot_count
                        FROM evidence.collection_run_events
                        WHERE run_id = %s AND sequence = 1
                        """,
                        (run_id,),
                    )
                    terminal = cursor.fetchone()
                    if terminal is None or (
                        terminal[0],
                        terminal[1],
                        _database_bytes(terminal[2]),
                        terminal[3],
                    ) != (
                        state,
                        completed_at,
                        gaps_json,
                        expected_snapshot_count,
                    ):
                        raise PostgresEvidenceError("collection terminal state conflict")
        except psycopg.Error as error:
            if error.sqlstate == "23514":
                raise PostgresEvidenceError(
                    "complete run requires exact nonzero snapshot count"
                ) from error
            raise PostgresEvidenceError("PostgreSQL collection run finish failed") from error

    def get_run(self, run_id: str) -> CollectionRunRecord | None:
        run_id = _canonical_uuid(run_id)
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run.provider, run.scope_json, run.started_at,
                           event.state, event.occurred_at, event.gaps_json,
                           event.expected_snapshot_count
                    FROM evidence.collection_runs AS run
                    JOIN LATERAL (
                        SELECT state, occurred_at, gaps_json, expected_snapshot_count
                        FROM evidence.collection_run_events
                        WHERE run_id = run.run_id
                        ORDER BY sequence DESC LIMIT 1
                    ) AS event ON true
                    WHERE run.run_id = %s
                    """,
                    (run_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                cursor.execute(
                    """
                    SELECT snapshot_id FROM evidence.collection_run_snapshots
                    WHERE run_id = %s ORDER BY ordinal
                    """,
                    (run_id,),
                )
                snapshot_ids = tuple(str(item[0]) for item in cursor.fetchall())
        except psycopg.Error as error:
            raise PostgresEvidenceError("PostgreSQL collection run read failed") from error
        scope = _decode_text_tuple(_database_bytes(row[1]), "scope")
        gaps = _decode_text_tuple(_database_bytes(row[5]), "gaps")
        state = str(row[3])
        started_at = _database_datetime(row[2], "started_at")
        completed_at = None if state == "started" else _database_datetime(row[4], "completed_at")
        expected_snapshot_count = _database_optional_integer(row[6], "expected_snapshot_count")
        return CollectionRunRecord(
            run_id=run_id,
            provider=str(row[0]),
            scope=scope,
            started_at=started_at,
            state=state,
            completed_at=completed_at,
            snapshot_ids=snapshot_ids,
            gaps=gaps,
            expected_snapshot_count=expected_snapshot_count,
        )

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


def apply_postgres_migrations(service_name: str, *, database_name: str | None = None) -> None:
    """Apply the reviewed transactional migration through the migrator login."""
    if type(service_name) is not str or not service_name:
        raise PostgresEvidenceError("service_name must be a non-empty string")
    database_name = _validated_database_name(database_name)
    migration = (
        files("kalshi_crypto_basis")
        .joinpath("migrations", "0001_postgres_evidence.sql")
        .read_text(encoding="utf-8")
    )
    try:
        if database_name is None:
            connection = psycopg.connect(service=service_name, autocommit=True)
        else:
            connection = psycopg.connect(
                service=service_name,
                dbname=database_name,
                autocommit=True,
            )
        with connection:
            connection.execute(migration)
    except psycopg.Error as error:
        raise PostgresEvidenceError("PostgreSQL migration failed") from error


def _validated_snapshot(snapshot: SnapshotEnvelope) -> SnapshotEnvelope:
    try:
        return SnapshotEnvelope(
            source=snapshot.source,
            request_fingerprint=snapshot.request_fingerprint,
            observed_at=snapshot.observed_at,
            ingested_at=snapshot.ingested_at,
            parser_version=snapshot.parser_version,
            raw_sha256=snapshot.raw_sha256,
            normalized=snapshot.normalized,
            snapshot_id=snapshot.snapshot_id,
            idempotency_key=snapshot.idempotency_key,
        )
    except AttributeError as error:
        raise SnapshotError("snapshot envelope is missing required attributes") from error


def _canonical_uuid(value: object) -> str:
    if type(value) is not str:
        raise PostgresEvidenceError("run_id must be a canonical UUID string")
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError) as error:
        raise PostgresEvidenceError("run_id must be a canonical UUID string") from error
    if str(parsed) != value:
        raise PostgresEvidenceError("run_id must be a canonical UUID string")
    return value


def _validated_database_name(value: object) -> str | None:
    if value is None:
        return None
    if type(value) is not str or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value) is None:
        raise PostgresEvidenceError("database_name must be a safe PostgreSQL identifier")
    return value


def _required_text(value: object, field: str) -> str:
    if type(value) is not str or not value:
        raise PostgresEvidenceError(f"{field} must be a non-empty string")
    return value


def _require_utc(value: object, field: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise PostgresEvidenceError(f"{field} must be a timezone-aware UTC datetime")
    offset = value.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise PostgresEvidenceError(f"{field} must be normalized to UTC")
    return value


def _canonical_text_tuple(value: object, field: str, *, allow_empty: bool = False) -> bytes:
    if type(value) is not tuple:
        raise PostgresEvidenceError(f"{field} must be a tuple of non-empty strings")
    if not value and not allow_empty:
        raise PostgresEvidenceError(f"{field} must not be empty")
    if not all(type(item) is str and item for item in value):
        raise PostgresEvidenceError(f"{field} must be a tuple of non-empty strings")
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _decode_text_tuple(value: bytes, field: str) -> tuple[str, ...]:
    try:
        parsed = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PostgresEvidenceError(f"stored {field} is invalid") from error
    if not isinstance(parsed, list) or not all(type(item) is str and item for item in parsed):
        raise PostgresEvidenceError(f"stored {field} is invalid")
    return tuple(parsed)


def _database_bytes(value: object) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise PostgresEvidenceError("stored binary evidence has an invalid database type")


def _database_datetime(value: object, field: str) -> datetime:
    if type(value) is not datetime:
        raise PostgresEvidenceError(f"stored {field} has an invalid database type")
    return value


def _database_optional_integer(value: object, field: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise PostgresEvidenceError(f"stored {field} has an invalid database type")
    return value
