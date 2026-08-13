from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest

from kalshi_crypto_basis.snapshots import (
    InMemorySnapshotStore,
    SnapshotEnvelope,
    SnapshotError,
    canonical_request_fingerprint,
    migrate_canonical_json,
)


def test_semantically_identical_snapshot_has_stable_identity_and_bytes() -> None:
    request_a = {"currency": "BTC", "limit": 1000, "active": True}
    request_b = {"active": True, "limit": 1000, "currency": "BTC"}
    normalized_a = {
        "ask": Decimal("0.4200"),
        "bid": Decimal("0.410"),
        "ticker": "KXBTC",
    }
    normalized_b = {
        "ticker": "KXBTC",
        "bid": Decimal("0.410"),
        "ask": Decimal("0.4200"),
    }

    first = SnapshotEnvelope.create(
        source="kalshi",
        request_fingerprint=canonical_request_fingerprint("GET", "/markets", request_a),
        observed_at=datetime(2026, 8, 13, 18, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 18, 0, 1, tzinfo=UTC),
        parser_version="kalshi-market-v1",
        raw_payload=b'{"ticker":"KXBTC","yes_bid":41,"yes_ask":42}',
        normalized=normalized_a,
    )
    second = SnapshotEnvelope.create(
        source="kalshi",
        request_fingerprint=canonical_request_fingerprint("get", "/markets", request_b),
        observed_at=datetime(2026, 8, 13, 18, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 18, 0, 1, tzinfo=UTC),
        parser_version="kalshi-market-v1",
        raw_payload=b'{"ticker":"KXBTC","yes_bid":41,"yes_ask":42}',
        normalized=normalized_b,
    )

    assert first.snapshot_id == second.snapshot_id
    assert first.idempotency_key == second.idempotency_key
    assert first.to_canonical_json() == second.to_canonical_json()
    assert b'"ask":{"$decimal":"0.4200"}' in first.to_canonical_json()


def test_decimal_and_text_have_distinct_canonical_identity() -> None:
    decimal_snapshot = _snapshot(normalized={"value": Decimal("0.41")})
    text_snapshot = _snapshot(normalized={"value": "0.41"})

    assert decimal_snapshot.snapshot_id != text_snapshot.snapshot_id
    assert decimal_snapshot.to_canonical_json() != text_snapshot.to_canonical_json()
    replayed = SnapshotEnvelope.from_canonical_json(
        decimal_snapshot.to_canonical_json(), raw_payload=b"{}"
    )
    replayed_values = cast(dict[str, Any], replayed.normalized)
    assert replayed_values["value"] == Decimal("0.41")


def test_reserved_decimal_marker_cannot_be_supplied_as_data() -> None:
    with pytest.raises(SnapshotError, match="reserved canonical key"):
        _snapshot(normalized={"value": {"$decimal": "0.41"}})


def test_request_fingerprint_rejects_float_values() -> None:
    with pytest.raises(SnapshotError, match="float values are forbidden"):
        canonical_request_fingerprint("GET", "/markets", {"price": 0.42})


@pytest.mark.parametrize(
    ("method", "path", "message"),
    [
        ("", "/markets", "method"),
        (" GET ", "/markets", "method"),
        (42, "/markets", "method"),
        ("GET", "", "path"),
        ("GET", "markets", "path"),
        ("GET", "/markets?limit=1", "path"),
        ("GET", 42, "path"),
    ],
)
def test_request_fingerprint_rejects_ambiguous_route_inputs(
    method: Any, path: Any, message: str
) -> None:
    with pytest.raises(SnapshotError, match=message):
        canonical_request_fingerprint(method, path, {})

    with pytest.raises(SnapshotError, match="parameters"):
        canonical_request_fingerprint("GET", "/markets", ["limit", 100])


def test_snapshot_replay_requires_exact_raw_payload_and_preserves_input_immutably() -> None:
    normalized = {"levels": [{"price": Decimal("0.41"), "size": 3}]}
    raw_payload = b'{"orderbook":{"yes":[[41,3]]}}'
    snapshot = SnapshotEnvelope.create(
        source="kalshi",
        request_fingerprint=canonical_request_fingerprint("GET", "/orderbook", {}),
        observed_at=datetime(2026, 8, 13, 18, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 8, 13, 18, 0, 1, tzinfo=UTC),
        parser_version="kalshi-book-v1",
        raw_payload=raw_payload,
        normalized=normalized,
    )
    normalized["levels"][0]["size"] = 999

    replayed = SnapshotEnvelope.from_canonical_json(
        snapshot.to_canonical_json(), raw_payload=raw_payload
    )

    assert replayed == snapshot
    replayed_values = cast(dict[str, Any], replayed.normalized)
    assert replayed_values["levels"][0]["price"] == Decimal("0.41")
    assert replayed_values["levels"][0]["size"] == 3
    with pytest.raises(TypeError):
        replayed_values["levels"][0]["size"] = 4
    with pytest.raises(SnapshotError, match="raw payload hash mismatch"):
        SnapshotEnvelope.from_canonical_json(snapshot.to_canonical_json(), raw_payload=b"tampered")


def test_snapshot_replay_rejects_duplicate_keys_and_future_schema() -> None:
    with pytest.raises(SnapshotError, match="duplicate JSON key"):
        SnapshotEnvelope.from_canonical_json(
            b'{"schema_version":1,"schema_version":1}', raw_payload=b""
        )
    with pytest.raises(SnapshotError, match="unsupported schema version: 2"):
        SnapshotEnvelope.from_canonical_json(b'{"schema_version":2}', raw_payload=b"")
    with pytest.raises(SnapshotError, match="unsupported schema version: True"):
        SnapshotEnvelope.from_canonical_json(b'{"schema_version":true}', raw_payload=b"")


def test_snapshot_rejects_invalid_clock_order_and_non_utc_time() -> None:
    observed_at = datetime(2026, 8, 13, 18, 0, 1, tzinfo=UTC)
    with pytest.raises(SnapshotError, match="ingested_at precedes observed_at"):
        _snapshot(
            observed_at=observed_at,
            ingested_at=observed_at - timedelta(microseconds=1),
        )
    with pytest.raises(SnapshotError, match="normalized to UTC"):
        _snapshot(
            observed_at=observed_at,
            ingested_at=datetime.fromisoformat("2026-08-13T14:00:02-04:00"),
        )


def test_store_is_idempotent_and_rejects_parser_nondeterminism() -> None:
    request_fingerprint = canonical_request_fingerprint(
        "GET", "/public/get_book_summary_by_currency", {"currency": "BTC"}
    )
    first = _snapshot(
        source="deribit",
        request_fingerprint=request_fingerprint,
        parser_version="deribit-summary-v1",
        raw_payload=b'{"result":[]}',
        normalized={"instruments": []},
    )
    divergent = _snapshot(
        source="deribit",
        request_fingerprint=request_fingerprint,
        parser_version="deribit-summary-v1",
        raw_payload=b'{"result":[]}',
        normalized={"instruments": ["invented"]},
    )
    retried_later = _snapshot(
        source="deribit",
        request_fingerprint=request_fingerprint,
        ingested_at=datetime(2026, 8, 13, 18, 0, 2, tzinfo=UTC),
        parser_version="deribit-summary-v1",
        raw_payload=b'{"result":[]}',
        normalized={"instruments": []},
    )
    store = InMemorySnapshotStore()

    stored = store.put(first, raw_payload=b'{"result":[]}')
    assert stored == first
    assert stored is not first
    assert store.put(first, raw_payload=b'{"result":[]}') is stored
    assert store.put(retried_later, raw_payload=b'{"result":[]}') is stored
    assert store.get(first.snapshot_id) is stored
    assert store.get_raw(first.raw_sha256) == b'{"result":[]}'
    with pytest.raises(SnapshotError, match="raw payload hash mismatch"):
        store.put(first, raw_payload=b"tampered")
    with pytest.raises(SnapshotError, match="idempotency conflict"):
        store.put(divergent, raw_payload=b'{"result":[]}')


def test_schema_migration_is_explicit_and_fails_closed() -> None:
    snapshot = _snapshot(normalized={"price": Decimal("0.4200")})
    encoded = snapshot.to_canonical_json()

    assert migrate_canonical_json(encoded, target_version=1) == encoded
    with pytest.raises(SnapshotError, match="unsupported target schema version: 2"):
        migrate_canonical_json(encoded, target_version=2)
    with pytest.raises(SnapshotError, match="unsupported target schema version: True"):
        migrate_canonical_json(encoded, target_version=True)
    with pytest.raises(SnapshotError, match="no migration path from schema version 0"):
        migrate_canonical_json(b'{"schema_version":0}', target_version=1)
    with pytest.raises(SnapshotError, match="fields do not match schema version 1"):
        migrate_canonical_json(b'{"schema_version":1}', target_version=1)
    with pytest.raises(SnapshotError, match="no migration path from schema version True"):
        migrate_canonical_json(b'{"schema_version":true}', target_version=1)


def test_direct_envelope_construction_cannot_forge_or_retain_mutable_state() -> None:
    valid = _snapshot(normalized={"levels": [{"price": Decimal("0.41")}]})
    mutable = {"levels": [{"price": Decimal("0.41")}]}

    reconstructed = SnapshotEnvelope(
        source=valid.source,
        request_fingerprint=valid.request_fingerprint,
        observed_at=valid.observed_at,
        ingested_at=valid.ingested_at,
        parser_version=valid.parser_version,
        raw_sha256=valid.raw_sha256,
        normalized=mutable,
        snapshot_id=valid.snapshot_id,
        idempotency_key=valid.idempotency_key,
    )
    mutable["levels"][0]["price"] = Decimal("0.99")
    reconstructed_values = cast(dict[str, Any], reconstructed.normalized)

    assert reconstructed_values["levels"][0]["price"] == Decimal("0.41")
    with pytest.raises(TypeError):
        reconstructed_values["levels"][0]["price"] = Decimal("0.50")
    with pytest.raises(SnapshotError, match="snapshot identity mismatch"):
        SnapshotEnvelope(
            source=valid.source,
            request_fingerprint=valid.request_fingerprint,
            observed_at=valid.observed_at,
            ingested_at=valid.ingested_at,
            parser_version=valid.parser_version,
            raw_sha256=valid.raw_sha256,
            normalized={"levels": []},
            snapshot_id="sha256:" + "0" * 64,
            idempotency_key=valid.idempotency_key,
        )


def test_store_revalidates_object_new_bypass_before_persistence() -> None:
    valid = _snapshot(normalized={"price": Decimal("0.41")})
    forged = object.__new__(SnapshotEnvelope)
    for field, value in {
        "source": valid.source,
        "request_fingerprint": valid.request_fingerprint,
        "observed_at": valid.observed_at,
        "ingested_at": valid.ingested_at,
        "parser_version": valid.parser_version,
        "raw_sha256": valid.raw_sha256,
        "normalized": {"price": Decimal("0.41")},
        "snapshot_id": "sha256:" + "0" * 64,
        "idempotency_key": "sha256:" + "1" * 64,
    }.items():
        object.__setattr__(forged, field, value)

    store = InMemorySnapshotStore()
    with pytest.raises(SnapshotError, match="snapshot identity mismatch"):
        store.put(forged, raw_payload=b"{}")
    assert store.get(forged.snapshot_id) is None
    assert store.get_raw(forged.raw_sha256) is None


def test_replay_rejects_malformed_decimal_marker_as_snapshot_error() -> None:
    snapshot = _snapshot(normalized={"price": Decimal("0.42")})
    malformed = snapshot.to_canonical_json().replace(
        b'{"$decimal":"0.42"}', b'{"$decimal":"not-a-number"}'
    )

    with pytest.raises(SnapshotError, match="invalid canonical Decimal value"):
        SnapshotEnvelope.from_canonical_json(malformed, raw_payload=b"{}")


@pytest.mark.parametrize("field", ["source", "request_fingerprint", "parser_version"])
def test_snapshot_creation_rejects_empty_provenance_fields(field: str) -> None:
    values = {
        "source": "kalshi",
        "request_fingerprint": canonical_request_fingerprint("GET", "/markets", {}),
        "parser_version": "kalshi-market-v1",
    }
    values[field] = ""

    with pytest.raises(SnapshotError, match=field):
        SnapshotEnvelope.create(
            source=values["source"],
            request_fingerprint=values["request_fingerprint"],
            observed_at=datetime(2026, 8, 13, 18, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 8, 13, 18, 0, 1, tzinfo=UTC),
            parser_version=values["parser_version"],
            raw_payload=b"{}",
            normalized={},
        )


def _snapshot(
    *,
    source: str = "kalshi",
    request_fingerprint: str | None = None,
    observed_at: datetime = datetime(2026, 8, 13, 18, 0, tzinfo=UTC),
    ingested_at: datetime = datetime(2026, 8, 13, 18, 0, 1, tzinfo=UTC),
    parser_version: str = "kalshi-market-v1",
    raw_payload: bytes = b"{}",
    normalized: object = None,
) -> SnapshotEnvelope:
    return SnapshotEnvelope.create(
        source=source,
        request_fingerprint=request_fingerprint
        or canonical_request_fingerprint("GET", "/markets", {}),
        observed_at=observed_at,
        ingested_at=ingested_at,
        parser_version=parser_version,
        raw_payload=raw_payload,
        normalized={} if normalized is None else normalized,
    )
