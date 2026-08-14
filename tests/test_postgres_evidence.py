import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest

from kalshi_crypto_basis.postgres_evidence import (
    PostgresEvidenceError,
    PostgresEvidenceStore,
    apply_postgres_migrations,
)
from kalshi_crypto_basis.snapshots import SnapshotEnvelope, canonical_request_fingerprint

MIGRATOR_SERVICE = os.environ.get("KCB_POSTGRES_MIGRATOR_SERVICE")
RUNTIME_SERVICE = os.environ.get("KCB_POSTGRES_RUNTIME_SERVICE")

pytestmark = pytest.mark.skipif(
    not MIGRATOR_SERVICE or not RUNTIME_SERVICE,
    reason="local PostgreSQL services are not configured",
)


def test_postgres_runtime_connects_after_reviewed_migration() -> None:
    assert MIGRATOR_SERVICE is not None
    assert RUNTIME_SERVICE is not None

    apply_postgres_migrations(MIGRATOR_SERVICE)
    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        identity = store.connection_identity()

    assert identity.user == "kalshi_crypto_basis_runtime"
    assert identity.database == "kalshi_crypto_basis_dev"
    assert identity.schema_version == 1


def test_postgres_snapshot_replays_after_connection_restart() -> None:
    assert MIGRATOR_SERVICE is not None
    assert RUNTIME_SERVICE is not None
    apply_postgres_migrations(MIGRATOR_SERVICE)
    raw_payload = b'{"jsonrpc":"2.0","result":[]}'
    snapshot = SnapshotEnvelope.create(
        source="deribit",
        request_fingerprint=canonical_request_fingerprint(
            "GET",
            "/public/get_instruments",
            {"currency": "BTC", "expired": False, "kind": "option"},
        ),
        observed_at=datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 20, 0, 1, tzinfo=UTC),
        parser_version="deribit-instruments-v1",
        raw_payload=raw_payload,
        normalized={"instruments": []},
    )

    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        stored = store.put(snapshot, raw_payload=raw_payload)

    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as reopened:
        replayed = reopened.get(snapshot.snapshot_id)
        replayed_raw = reopened.get_raw(snapshot.raw_sha256)

    assert stored == snapshot
    assert replayed == snapshot
    assert replayed_raw == raw_payload


def test_postgres_collection_run_replays_terminal_state_and_snapshot_order() -> None:
    assert MIGRATOR_SERVICE is not None
    assert RUNTIME_SERVICE is not None
    apply_postgres_migrations(MIGRATOR_SERVICE)
    raw_payload = b'{"jsonrpc":"2.0","result":[]}'
    snapshot = SnapshotEnvelope.create(
        source="deribit",
        request_fingerprint=canonical_request_fingerprint(
            "GET", "/public/get_book_summary_by_currency", {"currency": "BTC", "kind": "option"}
        ),
        observed_at=datetime(2026, 8, 13, 20, 1, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 20, 1, 1, tzinfo=UTC),
        parser_version="deribit-summary-v1",
        raw_payload=raw_payload,
        normalized={"summaries": []},
    )
    run_id = str(uuid4())

    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        store.put(snapshot, raw_payload=raw_payload)
        store.start_run(
            run_id=run_id,
            provider="deribit",
            scope=("BTC", "ETH"),
            started_at=datetime(2026, 8, 13, 20, 1, tzinfo=UTC),
        )
        store.attach_snapshot(run_id=run_id, snapshot_id=snapshot.snapshot_id, ordinal=0)
        store.attach_snapshot(run_id=run_id, snapshot_id=snapshot.snapshot_id, ordinal=0)
        store.finish_run(
            run_id=run_id,
            state="complete",
            completed_at=datetime(2026, 8, 13, 20, 1, 2, tzinfo=UTC),
            gaps=(),
            expected_snapshot_count=1,
        )

    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as reopened:
        replayed = reopened.get_run(run_id)

    assert replayed is not None
    assert replayed.provider == "deribit"
    assert replayed.scope == ("BTC", "ETH")
    assert replayed.state == "complete"
    assert replayed.snapshot_ids == (snapshot.snapshot_id,)
    assert replayed.gaps == ()
    assert replayed.expected_snapshot_count == 1


def test_postgres_complete_requires_nonzero_exact_snapshot_count() -> None:
    assert MIGRATOR_SERVICE is not None
    assert RUNTIME_SERVICE is not None
    apply_postgres_migrations(MIGRATOR_SERVICE)
    run_id = str(uuid4())

    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        store.start_run(
            run_id=run_id,
            provider="deribit",
            scope=("BTC",),
            started_at=datetime(2026, 8, 13, 20, 2, tzinfo=UTC),
        )
        with pytest.raises(PostgresEvidenceError, match="complete run requires"):
            store.finish_run(
                run_id=run_id,
                state="complete",
                completed_at=datetime(2026, 8, 13, 20, 2, 1, tzinfo=UTC),
                gaps=(),
                expected_snapshot_count=1,
            )


def test_postgres_terminal_run_rejects_later_snapshot_lineage() -> None:
    assert MIGRATOR_SERVICE is not None
    assert RUNTIME_SERVICE is not None
    apply_postgres_migrations(MIGRATOR_SERVICE)
    raw_payload = b'{"jsonrpc":"2.0","result":["terminal-lineage"]}'
    snapshot = SnapshotEnvelope.create(
        source="deribit",
        request_fingerprint=canonical_request_fingerprint(
            "GET", "/public/get_order_book", {"depth": 1, "instrument_name": "BTC-X"}
        ),
        observed_at=datetime(2026, 8, 13, 20, 3, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 20, 3, 1, tzinfo=UTC),
        parser_version="deribit-order-book-v1",
        raw_payload=raw_payload,
        normalized={"book": {"instrument_name": "BTC-X"}},
    )
    run_id = str(uuid4())

    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        store.put(snapshot, raw_payload=raw_payload)
        store.start_run(
            run_id=run_id,
            provider="deribit",
            scope=("BTC",),
            started_at=datetime(2026, 8, 13, 20, 3, tzinfo=UTC),
        )
        store.attach_snapshot(run_id=run_id, snapshot_id=snapshot.snapshot_id, ordinal=0)
        store.finish_run(
            run_id=run_id,
            state="complete",
            completed_at=datetime(2026, 8, 13, 20, 3, 2, tzinfo=UTC),
            gaps=(),
            expected_snapshot_count=1,
        )
        with pytest.raises(PostgresEvidenceError, match="terminal collection run"):
            store.attach_snapshot(run_id=run_id, snapshot_id=snapshot.snapshot_id, ordinal=1)


def test_database_guards_reject_direct_invalid_terminal_inserts() -> None:
    assert MIGRATOR_SERVICE is not None
    assert RUNTIME_SERVICE is not None
    apply_postgres_migrations(MIGRATOR_SERVICE)
    zero_run_id = str(uuid4())
    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        store.start_run(
            run_id=zero_run_id,
            provider="deribit",
            scope=("BTC",),
            started_at=datetime(2026, 8, 13, 20, 4, tzinfo=UTC),
        )

    with psycopg.connect(f"service={RUNTIME_SERVICE}") as connection:
        with pytest.raises(psycopg.errors.CheckViolation, match="exact nonzero"):
            connection.execute(
                """
                INSERT INTO evidence.collection_run_events (
                    run_id, sequence, state, occurred_at, gaps_json,
                    expected_snapshot_count
                ) VALUES (%s, 1, 'complete', %s, %s, 1)
                """,
                (
                    zero_run_id,
                    datetime(2026, 8, 13, 20, 4, 1, tzinfo=UTC),
                    b"[]",
                ),
            )

    raw_payload = b'{"jsonrpc":"2.0","result":["direct-terminal-guard"]}'
    snapshot = SnapshotEnvelope.create(
        source="deribit",
        request_fingerprint=canonical_request_fingerprint(
            "GET", "/public/get_order_book", {"depth": 1, "instrument_name": "BTC-Y"}
        ),
        observed_at=datetime(2026, 8, 13, 20, 5, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 20, 5, 1, tzinfo=UTC),
        parser_version="deribit-order-book-v1",
        raw_payload=raw_payload,
        normalized={"book": {"instrument_name": "BTC-Y"}},
    )
    terminal_run_id = str(uuid4())
    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        store.put(snapshot, raw_payload=raw_payload)
        store.start_run(
            run_id=terminal_run_id,
            provider="deribit",
            scope=("BTC",),
            started_at=datetime(2026, 8, 13, 20, 5, tzinfo=UTC),
        )
        store.attach_snapshot(run_id=terminal_run_id, snapshot_id=snapshot.snapshot_id, ordinal=0)
        store.finish_run(
            run_id=terminal_run_id,
            state="complete",
            completed_at=datetime(2026, 8, 13, 20, 5, 2, tzinfo=UTC),
            gaps=(),
            expected_snapshot_count=1,
        )

    with psycopg.connect(f"service={RUNTIME_SERVICE}") as connection:
        with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="terminal"):
            connection.execute(
                """
                INSERT INTO evidence.collection_run_snapshots (
                    run_id, ordinal, snapshot_id
                ) VALUES (%s, 1, %s)
                """,
                (terminal_run_id, snapshot.snapshot_id),
            )


def test_postgres_complete_rejects_explicit_gaps() -> None:
    assert MIGRATOR_SERVICE is not None
    assert RUNTIME_SERVICE is not None
    apply_postgres_migrations(MIGRATOR_SERVICE)
    raw_payload = b'{"jsonrpc":"2.0","result":["gapped-complete"]}'
    snapshot = SnapshotEnvelope.create(
        source="deribit",
        request_fingerprint=canonical_request_fingerprint(
            "GET", "/public/get_order_book", {"depth": 1, "instrument_name": "BTC-GAP"}
        ),
        observed_at=datetime(2026, 8, 13, 20, 2, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 20, 2, 1, tzinfo=UTC),
        parser_version="deribit-order-book-v1",
        raw_payload=raw_payload,
        normalized={"book": {"instrument_name": "BTC-GAP"}},
    )
    run_id = str(uuid4())
    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        store.put(snapshot, raw_payload=raw_payload)
        store.start_run(
            run_id=run_id,
            provider="deribit",
            scope=("BTC",),
            started_at=datetime(2026, 8, 13, 20, 2, tzinfo=UTC),
        )
        store.attach_snapshot(run_id=run_id, snapshot_id=snapshot.snapshot_id, ordinal=0)
        with pytest.raises(PostgresEvidenceError, match="complete run cannot retain gaps"):
            store.finish_run(
                run_id=run_id,
                state="complete",
                completed_at=datetime(2026, 8, 13, 20, 2, 2, tzinfo=UTC),
                gaps=("missing_bid:BTC-GAP",),
                expected_snapshot_count=1,
            )

    with psycopg.connect(f"service={RUNTIME_SERVICE}") as connection:
        with pytest.raises(psycopg.errors.CheckViolation, match="cannot retain gaps"):
            connection.execute(
                """
                INSERT INTO evidence.collection_run_events (
                    run_id, sequence, state, occurred_at, gaps_json,
                    expected_snapshot_count
                ) VALUES (%s, 1, 'complete', %s, %s, 1)
                """,
                (
                    run_id,
                    datetime(2026, 8, 13, 20, 2, 2, tzinfo=UTC),
                    b'["missing_bid:BTC-GAP"]',
                ),
            )


def test_postgres_snapshot_ordinals_must_be_contiguous() -> None:
    assert MIGRATOR_SERVICE is not None
    assert RUNTIME_SERVICE is not None
    apply_postgres_migrations(MIGRATOR_SERVICE)
    raw_payload = b'{"jsonrpc":"2.0","result":["ordinal-gap"]}'
    snapshot = SnapshotEnvelope.create(
        source="deribit",
        request_fingerprint=canonical_request_fingerprint(
            "GET", "/public/get_order_book", {"depth": 1, "instrument_name": "BTC-ORD"}
        ),
        observed_at=datetime(2026, 8, 13, 20, 2, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 20, 2, 1, tzinfo=UTC),
        parser_version="deribit-order-book-v1",
        raw_payload=raw_payload,
        normalized={"book": {"instrument_name": "BTC-ORD"}},
    )
    run_id = str(uuid4())
    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        store.put(snapshot, raw_payload=raw_payload)
        store.start_run(
            run_id=run_id,
            provider="deribit",
            scope=("BTC",),
            started_at=datetime(2026, 8, 13, 20, 2, tzinfo=UTC),
        )
        with pytest.raises(PostgresEvidenceError, match="next contiguous value"):
            store.attach_snapshot(run_id=run_id, snapshot_id=snapshot.snapshot_id, ordinal=7)

    with psycopg.connect(f"service={RUNTIME_SERVICE}") as connection:
        with pytest.raises(psycopg.errors.CheckViolation, match="next contiguous value"):
            connection.execute(
                """
                INSERT INTO evidence.collection_run_snapshots (
                    run_id, ordinal, snapshot_id
                ) VALUES (%s, 7, %s)
                """,
                (run_id, snapshot.snapshot_id),
            )


def test_postgres_attach_and_finish_race_preserves_terminal_lineage() -> None:
    assert MIGRATOR_SERVICE is not None
    assert RUNTIME_SERVICE is not None
    runtime_service: str = RUNTIME_SERVICE
    apply_postgres_migrations(MIGRATOR_SERVICE)
    raw_payload = b'{"jsonrpc":"2.0","result":["concurrent-lineage"]}'
    snapshot = SnapshotEnvelope.create(
        source="deribit",
        request_fingerprint=canonical_request_fingerprint(
            "GET", "/public/get_order_book", {"depth": 1, "instrument_name": "BTC-RACE"}
        ),
        observed_at=datetime(2026, 8, 13, 20, 6, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 20, 6, 1, tzinfo=UTC),
        parser_version="deribit-order-book-v1",
        raw_payload=raw_payload,
        normalized={"book": {"instrument_name": "BTC-RACE"}},
    )
    run_id = str(uuid4())
    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        store.put(snapshot, raw_payload=raw_payload)
        store.start_run(
            run_id=run_id,
            provider="deribit",
            scope=("BTC",),
            started_at=datetime(2026, 8, 13, 20, 6, tzinfo=UTC),
        )

    barrier = threading.Barrier(2)

    def attach() -> str:
        with PostgresEvidenceStore.connect(runtime_service) as store:
            barrier.wait()
            store.attach_snapshot(run_id=run_id, snapshot_id=snapshot.snapshot_id, ordinal=0)
        return "attached"

    def finish() -> str:
        with PostgresEvidenceStore.connect(runtime_service) as store:
            barrier.wait()
            try:
                store.finish_run(
                    run_id=run_id,
                    state="complete",
                    completed_at=datetime(2026, 8, 13, 20, 6, 2, tzinfo=UTC),
                    gaps=(),
                    expected_snapshot_count=1,
                )
            except PostgresEvidenceError:
                return "not_finished"
        return "finished"

    with ThreadPoolExecutor(max_workers=2) as executor:
        attach_future = executor.submit(attach)
        finish_future = executor.submit(finish)
        assert attach_future.result(timeout=10) == "attached"
        finish_result = finish_future.result(timeout=10)

    with PostgresEvidenceStore.connect(RUNTIME_SERVICE) as store:
        replayed = store.get_run(run_id)
    assert replayed is not None
    assert replayed.snapshot_ids == (snapshot.snapshot_id,)
    if finish_result == "finished":
        assert replayed.state == "complete"
        assert replayed.expected_snapshot_count == 1
    else:
        assert replayed.state == "started"
        assert replayed.expected_snapshot_count is None
