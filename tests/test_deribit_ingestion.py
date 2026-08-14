from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

import pytest

from kalshi_crypto_basis.deribit_ingestion import (
    DeribitIngestionError,
    DeribitOptionsCollector,
    DeribitPublicHttpTransport,
    DeribitReadOnlyResponse,
    parse_deribit_instruments,
    parse_deribit_order_book,
)
from kalshi_crypto_basis.snapshots import SnapshotEnvelope


class RecordingDeribitTransport:
    def __init__(self, responses: dict[tuple[str, tuple[tuple[str, object], ...]], bytes]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, tuple[tuple[str, object], ...]]] = []
        self.received_at = datetime(2026, 8, 13, 20, 0, 2, tzinfo=UTC)

    def get(self, path: str, params: dict[str, object]) -> DeribitReadOnlyResponse:
        key = (path, tuple(sorted(params.items())))
        self.requests.append(key)
        return DeribitReadOnlyResponse(
            status_code=200,
            content_type="application/json",
            body=self.responses[key],
            received_at=self.received_at,
        )


class RecordingEvidenceStore:
    def __init__(self) -> None:
        self.snapshots: list[SnapshotEnvelope] = []
        self.raw: dict[str, bytes] = {}
        self.run: dict[str, Any] = {}

    def put(self, snapshot: SnapshotEnvelope, *, raw_payload: bytes) -> SnapshotEnvelope:
        self.snapshots.append(snapshot)
        self.raw[snapshot.raw_sha256] = raw_payload
        return snapshot

    def start_run(self, **values: object) -> None:
        self.run.update(values)
        self.run["state"] = "started"
        self.run["snapshot_ids"] = []

    def attach_snapshot(self, *, run_id: str, snapshot_id: str, ordinal: int) -> None:
        assert run_id == self.run["run_id"]
        assert ordinal == len(self.run["snapshot_ids"])
        self.run["snapshot_ids"].append(snapshot_id)

    def finish_run(self, **values: object) -> None:
        assert values["run_id"] == self.run["run_id"]
        self.run.update(values)


def _rpc(result: object, *, us_out: int = 1_786_651_200_000_000) -> bytes:
    return json.dumps(
        {"jsonrpc": "2.0", "id": 1, "result": result, "usOut": us_out},
        separators=(",", ":"),
    ).encode()


def _instrument(currency: str, name: str) -> dict[str, object]:
    return {
        "kind": "option",
        "base_currency": currency,
        "quote_currency": "USD",
        "settlement_currency": currency,
        "instrument_name": name,
        "instrument_id": 101,
        "is_active": True,
        "state": "open",
        "settlement_period": "week",
        "creation_timestamp": 1_786_000_000_000,
        "expiration_timestamp": 1_789_000_000_000,
        "strike": 100000,
        "option_type": "call",
        "contract_size": 1,
        "min_trade_amount": 0.1,
        "tick_size": 0.0001,
        "price_index": f"{currency.lower()}_usd",
        "underlying_type": "crypto",
    }


def _book(name: str) -> dict[str, object]:
    return {
        "instrument_name": name,
        "timestamp": 1_786_651_200_000,
        "state": "open",
        "stats": {"volume": 1.5, "high": 0.2, "low": 0.1},
        "open_interest": 22.5,
        "best_bid_price": 0.10,
        "best_bid_amount": 3.0,
        "best_ask_price": 0.12,
        "best_ask_amount": 4.0,
        "index_price": 100000,
        "min_price": 0.0001,
        "max_price": 1.0,
        "mark_price": 0.11,
        "last_price": 0.105,
        "underlying_price": 100100,
        "underlying_index": "index_price",
        "interest_rate": 0.0,
        "bid_iv": 48.1,
        "ask_iv": 49.2,
        "mark_iv": 48.6,
        "greeks": {"delta": 0.5, "gamma": 0.1, "rho": 0.2, "theta": -0.3, "vega": 0.4},
        "bids": [[0.10, 3.0]],
        "asks": [[0.12, 4.0]],
    }


def test_bounded_collector_persists_complete_btc_eth_chain() -> None:
    names = {"BTC": "BTC-28AUG26-100000-C", "ETH": "ETH-28AUG26-4000-C"}
    responses: dict[tuple[str, tuple[tuple[str, object], ...]], bytes] = {}
    for currency, name in names.items():
        responses[
            (
                "/public/get_instruments",
                (("currency", currency), ("expired", False), ("kind", "option")),
            )
        ] = _rpc([_instrument(currency, name)])
        responses[
            (
                "/public/get_order_book",
                (("depth", 1), ("instrument_name", name)),
            )
        ] = _rpc(_book(name))
    transport = RecordingDeribitTransport(responses)
    store = RecordingEvidenceStore()
    waits: list[float] = []
    clock_values = iter(
        (
            datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
            datetime(2026, 8, 13, 20, 0, 3, tzinfo=UTC),
        )
    )
    collector = DeribitOptionsCollector(
        transport=transport,
        evidence_store=store,
        clock=lambda: next(clock_values),
        wait=waits.append,
        max_instruments=10,
        max_synchronization_seconds=5,
    )

    result = collector.collect(
        currencies=("BTC", "ETH"), run_id="00000000-0000-4000-8000-000000000106"
    )

    assert result.state == "complete"
    assert result.gaps == ()
    assert result.instrument_count == 2
    assert result.book_count == 2
    assert len(store.snapshots) == 4
    assert store.run["state"] == "complete"
    assert waits == [1.0]
    assert all(snapshot.source == "deribit" for snapshot in store.snapshots)
    normalized = cast(Mapping[str, object], store.snapshots[1].normalized)
    btc_book = normalized["book"]
    assert isinstance(btc_book, Mapping)
    greeks = btc_book["greeks"]
    stats = btc_book["stats"]
    assert isinstance(greeks, Mapping)
    assert isinstance(stats, Mapping)
    assert greeks["delta"] == Decimal("0.5")
    assert stats["volume"] == Decimal("1.5")


def test_instrument_parser_rejects_duplicate_json_keys() -> None:
    body = b'{"jsonrpc":"2.0","result":[],"result":[],"usOut":1786651200000000}'
    response = DeribitReadOnlyResponse(
        status_code=200,
        content_type="application/json",
        body=body,
        received_at=datetime(2026, 8, 13, 20, 0, 2, tzinfo=UTC),
    )

    with pytest.raises(DeribitIngestionError, match="duplicate JSON key"):
        parse_deribit_instruments(response, expected_currency="BTC")


def test_crossed_book_produces_incomplete_run() -> None:
    name = "BTC-28AUG26-100000-C"
    crossed = _book(name)
    crossed["best_bid_price"] = 0.13
    responses: dict[tuple[str, tuple[tuple[str, object], ...]], bytes] = {
        (
            "/public/get_instruments",
            (("currency", "BTC"), ("expired", False), ("kind", "option")),
        ): _rpc([_instrument("BTC", name)]),
        (
            "/public/get_order_book",
            (("depth", 1), ("instrument_name", name)),
        ): _rpc(crossed),
    }
    store = RecordingEvidenceStore()
    collector = DeribitOptionsCollector(
        transport=RecordingDeribitTransport(responses),
        evidence_store=store,
        clock=lambda: datetime(2026, 8, 13, 20, 0, 2, tzinfo=UTC),
        wait=lambda _: None,
        max_instruments=10,
        max_synchronization_seconds=5,
    )

    result = collector.collect(currencies=("BTC",), run_id="00000000-0000-4000-8000-000000000107")

    assert result.state == "incomplete"
    assert result.gaps == (f"crossed_book:{name}",)


def test_order_book_accepts_null_trade_range_statistics() -> None:
    name = "BTC-28AUG26-100000-C"
    book = _book(name)
    stats = book["stats"]
    assert isinstance(stats, dict)
    stats["high"] = None
    stats["low"] = None
    response = DeribitReadOnlyResponse(
        status_code=200,
        content_type="application/json",
        body=_rpc(book),
        received_at=datetime(2026, 8, 13, 20, 0, 2, tzinfo=UTC),
    )

    parsed = parse_deribit_order_book(response, expected_instrument_name=name)

    normalized_book = parsed.normalized["book"]
    assert isinstance(normalized_book, Mapping)
    normalized_stats = normalized_book["stats"]
    assert isinstance(normalized_stats, Mapping)
    assert normalized_stats["high"] is None
    assert normalized_stats["low"] is None


def test_order_book_rejects_timestamp_later_than_receipt_clock() -> None:
    name = "BTC-28AUG26-100000-C"
    book = _book(name)
    book["timestamp"] = 1_786_651_220_000
    response = DeribitReadOnlyResponse(
        status_code=200,
        content_type="application/json",
        body=_rpc(book, us_out=1_786_651_200_000_000),
        received_at=datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
    )

    with pytest.raises(DeribitIngestionError, match="later than receipt clock"):
        parse_deribit_order_book(response, expected_instrument_name=name)


@pytest.mark.parametrize("provider_timestamp", [10**100, -(10**100)])
def test_provider_timestamp_overflow_is_normalized(provider_timestamp: int) -> None:
    response = DeribitReadOnlyResponse(
        status_code=200,
        content_type="application/json",
        body=_rpc([], us_out=provider_timestamp),
        received_at=datetime(2026, 8, 13, 20, 0, tzinfo=UTC),
    )
    with pytest.raises(
        DeribitIngestionError,
        match=r"outside supported range|must not be negative",
    ):
        parse_deribit_instruments(response, expected_currency="BTC")

    name = "BTC-28AUG26-100000-C"
    book = _book(name)
    book["timestamp"] = provider_timestamp
    response = DeribitReadOnlyResponse(
        status_code=200,
        content_type="application/json",
        body=_rpc(book),
        received_at=datetime(2026, 8, 13, 20, 0, 2, tzinfo=UTC),
    )
    with pytest.raises(
        DeribitIngestionError,
        match=r"outside supported range|must not be negative",
    ):
        parse_deribit_order_book(response, expected_instrument_name=name)


def test_public_http_transport_uses_allowlisted_get_with_bounded_body() -> None:
    body = _rpc([_instrument("BTC", "BTC-28AUG26-100000-C")])

    class Handler(BaseHTTPRequestHandler):
        request_path = ""

        def do_GET(self) -> None:
            type(self).request_path = self.path
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        transport = DeribitPublicHttpTransport(
            origin=f"http://127.0.0.1:{server.server_port}/api/v2",
            allow_loopback_http=True,
            max_response_bytes=len(body),
            clock=lambda: datetime(2026, 8, 13, 20, 0, 2, tzinfo=UTC),
        )
        response = transport.get(
            "/public/get_instruments",
            {"currency": "BTC", "expired": False, "kind": "option"},
        )
    finally:
        server.shutdown()
        thread.join()
        server.server_close()

    assert response.body == body
    assert Handler.request_path == (
        "/api/v2/public/get_instruments?currency=BTC&expired=false&kind=option"
    )
    with pytest.raises(DeribitIngestionError, match="path is not allowlisted"):
        transport.get("/private/buy", {})


def test_transport_rejects_boolean_depth_and_nonfinite_timeout() -> None:
    transport = DeribitPublicHttpTransport()
    with pytest.raises(DeribitIngestionError, match="depth-one"):
        transport.get(
            "/public/get_order_book",
            {"depth": True, "instrument_name": "BTC-X"},
        )
    for timeout in (float("nan"), float("inf")):
        with pytest.raises(DeribitIngestionError, match="finite"):
            DeribitPublicHttpTransport(timeout_seconds=timeout)


@pytest.mark.parametrize(
    "origin",
    [object(), [], 123, "https://www.deribit.com:bad/api/v2"],
)
def test_transport_normalizes_malformed_origins(origin: object) -> None:
    with pytest.raises(DeribitIngestionError, match="origin is invalid"):
        DeribitPublicHttpTransport(origin=origin)  # type: ignore[arg-type]


def test_parsers_revalidate_forged_response_fields_at_consumption() -> None:
    valid = DeribitReadOnlyResponse(
        status_code=200,
        content_type="application/json",
        body=_rpc([]),
        received_at=datetime(2026, 8, 13, 20, 0, 2, tzinfo=UTC),
    )
    invalid_fields: tuple[tuple[str, object], ...] = (
        ("body", []),
        ("content_type", []),
        ("received_at", []),
    )
    for field, value in invalid_fields:
        forged = object.__new__(DeribitReadOnlyResponse)
        for valid_field in ("status_code", "content_type", "body", "received_at"):
            object.__setattr__(forged, valid_field, getattr(valid, valid_field))
        object.__setattr__(forged, field, value)
        with pytest.raises(DeribitIngestionError, match="response evidence is invalid"):
            parse_deribit_instruments(forged, expected_currency="BTC")

    malformed_path: Any = []
    with pytest.raises(DeribitIngestionError, match="path is not allowlisted"):
        DeribitPublicHttpTransport().get(malformed_path, {})
