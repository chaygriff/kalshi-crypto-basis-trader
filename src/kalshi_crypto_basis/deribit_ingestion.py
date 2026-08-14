"""Bounded public read-only Deribit options evidence ingestion."""

from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from http.client import HTTPConnection, HTTPSConnection
from types import MappingProxyType
from typing import Protocol, cast
from urllib.parse import urlencode, urlsplit

from kalshi_crypto_basis.snapshots import SnapshotEnvelope, canonical_request_fingerprint

INSTRUMENTS_PATH = "/public/get_instruments"
ORDER_BOOK_PATH = "/public/get_order_book"
SUPPORTED_CURRENCIES = frozenset({"BTC", "ETH"})


class DeribitIngestionError(RuntimeError):
    """Raised when Deribit evidence cannot satisfy the ingestion contract."""


@dataclass(frozen=True, slots=True)
class DeribitReadOnlyResponse:
    """Exact public HTTP response evidence."""

    status_code: int
    content_type: str
    body: bytes
    received_at: datetime

    def __post_init__(self) -> None:
        if type(self.status_code) is not int:
            raise DeribitIngestionError("status_code must be an integer")
        if type(self.content_type) is not str:
            raise DeribitIngestionError("content_type must be a string")
        if type(self.body) is not bytes:
            raise DeribitIngestionError("body must be bytes")
        _require_utc(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class ParsedDeribitEvidence:
    normalized: Mapping[str, object]
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class DeribitCollectionResult:
    run_id: str
    state: str
    currencies: tuple[str, ...]
    instrument_count: int
    book_count: int
    snapshot_ids: tuple[str, ...]
    gaps: tuple[str, ...]


class DeribitPublicTransport(Protocol):
    def get(self, path: str, params: dict[str, object]) -> DeribitReadOnlyResponse: ...


class DurableEvidenceStore(Protocol):
    def put(self, snapshot: SnapshotEnvelope, *, raw_payload: bytes) -> SnapshotEnvelope: ...

    def start_run(
        self, *, run_id: str, provider: str, scope: tuple[str, ...], started_at: datetime
    ) -> None: ...

    def attach_snapshot(self, *, run_id: str, snapshot_id: str, ordinal: int) -> None: ...

    def finish_run(
        self,
        *,
        run_id: str,
        state: str,
        completed_at: datetime,
        gaps: tuple[str, ...],
        expected_snapshot_count: int | None,
    ) -> None: ...


class DeribitPublicHttpTransport:
    """Concrete GET-only transport for two public Deribit market-data methods."""

    def __init__(
        self,
        *,
        origin: str = "https://www.deribit.com/api/v2",
        allow_loopback_http: bool = False,
        timeout_seconds: float = 10.0,
        max_response_bytes: int = 8_000_000,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if type(origin) is not str:
            raise DeribitIngestionError("origin is invalid")
        try:
            parsed = urlsplit(origin)
            port = parsed.port
        except ValueError as error:
            raise DeribitIngestionError("origin is invalid") from error
        production = (
            parsed.scheme == "https"
            and parsed.hostname == "www.deribit.com"
            and port is None
            and parsed.path == "/api/v2"
        )
        loopback = (
            allow_loopback_http
            and parsed.scheme == "http"
            and parsed.hostname in {"127.0.0.1", "::1"}
            and port is not None
            and parsed.path == "/api/v2"
        )
        if not production and not loopback:
            raise DeribitIngestionError("origin must be production Deribit or explicit loopback")
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise DeribitIngestionError("origin must not contain credentials, query, or fragment")
        if (
            type(timeout_seconds) not in (int, float)
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise DeribitIngestionError("timeout_seconds must be positive and finite")
        if type(max_response_bytes) is not int or max_response_bytes <= 0:
            raise DeribitIngestionError("max_response_bytes must be positive")
        self._scheme = parsed.scheme
        self._host = parsed.hostname or ""
        self._port = port
        self._base_path = parsed.path
        self._timeout = float(timeout_seconds)
        self._max_response_bytes = max_response_bytes
        self._clock = clock

    def get(self, path: str, params: dict[str, object]) -> DeribitReadOnlyResponse:
        _validate_transport_request(path, params)
        query_values = {
            key: "true" if value is True else "false" if value is False else str(value)
            for key, value in params.items()
        }
        target = f"{self._base_path}{path}?{urlencode(query_values)}"
        connection_type = HTTPSConnection if self._scheme == "https" else HTTPConnection
        connection = connection_type(self._host, self._port, timeout=self._timeout)
        try:
            connection.request(
                "GET",
                target,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "User-Agent": "kalshi-crypto-basis-trader/deribit-read-only",
                },
            )
            response = connection.getresponse()
            content_encoding = response.getheader("Content-Encoding", "identity").lower()
            if content_encoding not in {"", "identity"}:
                raise DeribitIngestionError("compressed responses are not accepted")
            content_length = response.getheader("Content-Length")
            if content_length is not None:
                try:
                    declared_length = int(content_length)
                except ValueError as error:
                    raise DeribitIngestionError("Content-Length is invalid") from error
                if declared_length < 0 or declared_length > self._max_response_bytes:
                    raise DeribitIngestionError("response exceeds configured byte bound")
            body = response.read(self._max_response_bytes + 1)
            if len(body) > self._max_response_bytes:
                raise DeribitIngestionError("response exceeds configured byte bound")
            received_at = _require_utc(self._clock(), "received_at")
            return DeribitReadOnlyResponse(
                status_code=response.status,
                content_type=response.getheader("Content-Type", ""),
                body=body,
                received_at=received_at,
            )
        except (OSError, TimeoutError) as error:
            raise DeribitIngestionError("Deribit public HTTP request failed") from error
        finally:
            connection.close()


class DeribitOptionsCollector:
    """One-shot BTC/ETH option-chain collector with explicit bounds."""

    def __init__(
        self,
        *,
        transport: DeribitPublicTransport,
        evidence_store: DurableEvidenceStore,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        wait: Callable[[float], None] = time.sleep,
        max_instruments: int = 2_000,
        max_synchronization_seconds: int = 30,
        max_age_seconds: int = 30,
    ) -> None:
        if type(max_instruments) is not int or max_instruments <= 0:
            raise DeribitIngestionError("max_instruments must be a positive integer")
        if type(max_synchronization_seconds) is not int or max_synchronization_seconds <= 0:
            raise DeribitIngestionError("max_synchronization_seconds must be positive")
        if type(max_age_seconds) is not int or max_age_seconds <= 0:
            raise DeribitIngestionError("max_age_seconds must be positive")
        self._transport = transport
        self._store = evidence_store
        self._clock = clock
        self._wait = wait
        self._max_instruments = max_instruments
        self._max_sync = timedelta(seconds=max_synchronization_seconds)
        self._max_age = timedelta(seconds=max_age_seconds)

    def collect(self, *, currencies: tuple[str, ...], run_id: str) -> DeribitCollectionResult:
        currencies = _validated_currencies(currencies)
        started_at = _require_utc(self._clock(), "started_at")
        self._store.start_run(
            run_id=run_id, provider="deribit", scope=currencies, started_at=started_at
        )
        snapshot_ids: list[str] = []
        gaps: list[str] = []
        instrument_count = 0
        book_count = 0
        observed_times: list[datetime] = []
        received_times: list[datetime] = []
        try:
            for currency_index, currency in enumerate(currencies):
                if currency_index:
                    self._wait(1.0)
                instrument_params: dict[str, object] = {
                    "currency": currency,
                    "expired": False,
                    "kind": "option",
                }
                instrument_response = self._transport.get(INSTRUMENTS_PATH, instrument_params)
                parsed_instruments = parse_deribit_instruments(
                    instrument_response, expected_currency=currency
                )
                instrument_snapshot = self._persist_response(
                    run_id=run_id,
                    ordinal=len(snapshot_ids),
                    path=INSTRUMENTS_PATH,
                    params=instrument_params,
                    response=instrument_response,
                    parsed=parsed_instruments,
                    parser_version="deribit-instruments-v1",
                )
                snapshot_ids.append(instrument_snapshot.snapshot_id)
                observed_times.append(parsed_instruments.observed_at)
                received_times.append(instrument_response.received_at)
                instruments = parsed_instruments.normalized["instruments"]
                if not isinstance(instruments, tuple):
                    raise DeribitIngestionError("normalized instruments must be a tuple")
                instrument_count += len(instruments)
                if instrument_count > self._max_instruments:
                    raise DeribitIngestionError("instrument count exceeds configured bound")
                for instrument in instruments:
                    if not isinstance(instrument, Mapping):
                        raise DeribitIngestionError("normalized instrument is invalid")
                    name = instrument["instrument_name"]
                    if type(name) is not str:
                        raise DeribitIngestionError("instrument name is invalid")
                    book_params: dict[str, object] = {"depth": 1, "instrument_name": name}
                    book_response = self._transport.get(ORDER_BOOK_PATH, book_params)
                    parsed_book = parse_deribit_order_book(
                        book_response, expected_instrument_name=name
                    )
                    book_snapshot = self._persist_response(
                        run_id=run_id,
                        ordinal=len(snapshot_ids),
                        path=ORDER_BOOK_PATH,
                        params=book_params,
                        response=book_response,
                        parsed=parsed_book,
                        parser_version="deribit-order-book-v1",
                    )
                    snapshot_ids.append(book_snapshot.snapshot_id)
                    observed_times.append(parsed_book.observed_at)
                    received_times.append(book_response.received_at)
                    book_count += 1
                    book = parsed_book.normalized["book"]
                    if not isinstance(book, Mapping):
                        raise DeribitIngestionError("normalized book is invalid")
                    gaps.extend(_book_gaps(name, book))
            if observed_times and max(observed_times) - min(observed_times) > self._max_sync:
                gaps.append("synchronization_window_exceeded")
            completed_at = _require_utc(self._clock(), "completed_at")
            if received_times and completed_at < max(received_times):
                raise DeribitIngestionError("completion clock precedes evidence receipt")
            if observed_times and completed_at - min(observed_times) > self._max_age:
                gaps.append("stale_evidence")
            terminal_gaps = tuple(sorted(gaps))
            state = "complete" if not terminal_gaps else "incomplete"
            self._store.finish_run(
                run_id=run_id,
                state=state,
                completed_at=completed_at,
                gaps=terminal_gaps,
                expected_snapshot_count=len(snapshot_ids),
            )
            return DeribitCollectionResult(
                run_id=run_id,
                state=state,
                currencies=currencies,
                instrument_count=instrument_count,
                book_count=book_count,
                snapshot_ids=tuple(snapshot_ids),
                gaps=terminal_gaps,
            )
        except Exception as error:
            try:
                self._store.finish_run(
                    run_id=run_id,
                    state="failed",
                    completed_at=_require_utc(self._clock(), "failed_at"),
                    gaps=(f"collection_error:{type(error).__name__}",),
                    expected_snapshot_count=None,
                )
            except Exception:
                pass
            if isinstance(error, DeribitIngestionError):
                raise
            raise DeribitIngestionError("Deribit collection failed") from error

    def _persist_response(
        self,
        *,
        run_id: str,
        ordinal: int,
        path: str,
        params: dict[str, object],
        response: DeribitReadOnlyResponse,
        parsed: ParsedDeribitEvidence,
        parser_version: str,
    ) -> SnapshotEnvelope:
        snapshot = SnapshotEnvelope.create(
            source="deribit",
            request_fingerprint=canonical_request_fingerprint("GET", path, params),
            observed_at=parsed.observed_at,
            ingested_at=response.received_at,
            parser_version=parser_version,
            raw_payload=response.body,
            normalized=parsed.normalized,
        )
        stored = self._store.put(snapshot, raw_payload=response.body)
        self._store.attach_snapshot(run_id=run_id, snapshot_id=stored.snapshot_id, ordinal=ordinal)
        return stored


def parse_deribit_instruments(
    response: DeribitReadOnlyResponse, *, expected_currency: str
) -> ParsedDeribitEvidence:
    expected_currency = _validated_currency(expected_currency)
    result, observed_at = _parse_rpc(response)
    if not isinstance(result, list):
        raise DeribitIngestionError("instrument result must be a list")
    names: set[str] = set()
    normalized: list[Mapping[str, object]] = []
    for raw in result:
        item = _require_mapping(raw, "instrument")
        name = _required_text(item, "instrument_name")
        if name in names:
            raise DeribitIngestionError("duplicate instrument identity")
        names.add(name)
        if _required_text(item, "kind") != "option":
            raise DeribitIngestionError("instrument kind is not option")
        if _required_text(item, "base_currency") != expected_currency:
            raise DeribitIngestionError("instrument currency mismatch")
        normalized.append(
            MappingProxyType(
                {
                    "instrument_name": name,
                    "instrument_id": _optional_integer(item, "instrument_id"),
                    "base_currency": expected_currency,
                    "quote_currency": _required_text(item, "quote_currency"),
                    "settlement_currency": _required_text(item, "settlement_currency"),
                    "kind": "option",
                    "is_active": _required_bool(item, "is_active"),
                    "state": _required_text(item, "state"),
                    "settlement_period": _required_text(item, "settlement_period"),
                    "creation_timestamp": _required_integer(item, "creation_timestamp"),
                    "expiration_timestamp": _required_integer(item, "expiration_timestamp"),
                    "strike": _required_decimal(item, "strike"),
                    "option_type": _enum_text(item, "option_type", {"call", "put"}),
                    "contract_size": _required_decimal(item, "contract_size"),
                    "min_trade_amount": _required_decimal(item, "min_trade_amount"),
                    "tick_size": _required_decimal(item, "tick_size"),
                    "price_index": _required_text(item, "price_index"),
                    "underlying_type": _required_text(item, "underlying_type"),
                }
            )
        )
    normalized.sort(key=lambda item: str(item["instrument_name"]))
    return ParsedDeribitEvidence(
        normalized=MappingProxyType(
            {"currency": expected_currency, "instruments": tuple(normalized)}
        ),
        observed_at=observed_at,
    )


def parse_deribit_order_book(
    response: DeribitReadOnlyResponse, *, expected_instrument_name: str
) -> ParsedDeribitEvidence:
    expected_instrument_name = _nonempty_text(expected_instrument_name, "instrument_name")
    result, provider_observed_at = _parse_rpc(response)
    item = _require_mapping(result, "order book")
    if _required_text(item, "instrument_name") != expected_instrument_name:
        raise DeribitIngestionError("order book instrument mismatch")
    book_observed_at = _milliseconds_datetime(_required_integer(item, "timestamp"), "timestamp")
    if book_observed_at > response.received_at:
        raise DeribitIngestionError("order book timestamp is later than receipt clock")
    if abs(book_observed_at - provider_observed_at) > timedelta(seconds=30):
        raise DeribitIngestionError("order book provider clocks are incoherent")
    fields = {
        "instrument_name": expected_instrument_name,
        "timestamp": _required_integer(item, "timestamp"),
        "state": _required_text(item, "state"),
        "open_interest": _required_decimal(item, "open_interest"),
        "best_bid_price": _optional_decimal(item, "best_bid_price"),
        "best_bid_amount": _optional_decimal(item, "best_bid_amount"),
        "best_ask_price": _optional_decimal(item, "best_ask_price"),
        "best_ask_amount": _optional_decimal(item, "best_ask_amount"),
        "index_price": _required_decimal(item, "index_price"),
        "mark_price": _required_decimal(item, "mark_price"),
        "last_price": _optional_decimal(item, "last_price"),
        "min_price": _required_decimal(item, "min_price"),
        "max_price": _required_decimal(item, "max_price"),
        "underlying_price": _required_decimal(item, "underlying_price"),
        "underlying_index": _required_text(item, "underlying_index"),
        "interest_rate": _required_decimal(item, "interest_rate"),
        "bid_iv": _optional_decimal(item, "bid_iv"),
        "ask_iv": _optional_decimal(item, "ask_iv"),
        "mark_iv": _required_decimal(item, "mark_iv"),
        "stats": _deribit_stats(item),
        "greeks": _deribit_greeks(item),
        "bids": _price_levels(item, "bids"),
        "asks": _price_levels(item, "asks"),
    }
    return ParsedDeribitEvidence(
        normalized=MappingProxyType({"book": MappingProxyType(fields)}),
        observed_at=book_observed_at,
    )


def _parse_rpc(response: DeribitReadOnlyResponse) -> tuple[object, datetime]:
    response = _validated_response(response)
    if response.status_code != 200:
        raise DeribitIngestionError("Deribit HTTP status is not 200")
    if response.content_type.split(";", 1)[0].strip().lower() != "application/json":
        raise DeribitIngestionError("Deribit content type is not application/json")
    try:
        payload = json.loads(
            response.body,
            parse_float=Decimal,
            parse_int=int,
            parse_constant=lambda value: _raise_nonfinite(value),
            object_pairs_hook=_unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DeribitIngestionError("Deribit response is not strict JSON") from error
    root = _require_mapping(payload, "JSON-RPC response")
    if root.get("jsonrpc") != "2.0" or "error" in root or "result" not in root:
        raise DeribitIngestionError("Deribit JSON-RPC response is unsuccessful")
    us_out = _required_integer(root, "usOut")
    observed_at = _microseconds_datetime(us_out, "usOut")
    if observed_at > response.received_at + timedelta(seconds=5):
        raise DeribitIngestionError("provider clock is later than receipt clock")
    return root["result"], observed_at


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DeribitIngestionError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _raise_nonfinite(value: str) -> object:
    raise DeribitIngestionError(f"non-finite JSON number: {value}")


def _book_gaps(name: str, book: Mapping[str, object]) -> list[str]:
    gaps: list[str] = []
    bid = book["best_bid_price"]
    ask = book["best_ask_price"]
    bid_amount = book["best_bid_amount"]
    ask_amount = book["best_ask_amount"]
    if bid is None or bid_amount is None:
        gaps.append(f"missing_bid:{name}")
    if ask is None or ask_amount is None:
        gaps.append(f"missing_ask:{name}")
    if isinstance(bid, Decimal) and isinstance(ask, Decimal) and bid > ask:
        gaps.append(f"crossed_book:{name}")
    return gaps


def _validate_transport_request(path: object, params: object) -> None:
    if type(path) is not str or path not in {INSTRUMENTS_PATH, ORDER_BOOK_PATH}:
        raise DeribitIngestionError("path is not allowlisted")
    if type(params) is not dict:
        raise DeribitIngestionError("params must be a plain dictionary")
    if path == INSTRUMENTS_PATH:
        if set(params) != {"currency", "expired", "kind"}:
            raise DeribitIngestionError("instrument parameters do not match the public contract")
        _validated_currency(params["currency"])
        if params["expired"] is not False or params["kind"] != "option":
            raise DeribitIngestionError("only active option discovery is allowed")
        return
    if (
        set(params) != {"depth", "instrument_name"}
        or type(params["depth"]) is not int
        or params["depth"] != 1
    ):
        raise DeribitIngestionError("only depth-one order-book parameters are allowed")
    instrument_name = params["instrument_name"]
    if type(instrument_name) is not str or not re.fullmatch(
        r"[A-Z0-9][A-Z0-9_-]{0,99}", instrument_name
    ):
        raise DeribitIngestionError("instrument_name is invalid")


def _validated_currencies(value: object) -> tuple[str, ...]:
    if type(value) is not tuple or not value:
        raise DeribitIngestionError("currencies must be a non-empty tuple")
    currencies = tuple(_validated_currency(item) for item in value)
    if len(set(currencies)) != len(currencies):
        raise DeribitIngestionError("currencies must be unique")
    return currencies


def _validated_currency(value: object) -> str:
    if type(value) is not str or value not in SUPPORTED_CURRENCIES:
        raise DeribitIngestionError("currency must be BTC or ETH")
    return value


def _require_mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise DeribitIngestionError(f"{field} must be an object")
    return value


def _nonempty_text(value: object, field: str) -> str:
    if type(value) is not str or not value:
        raise DeribitIngestionError(f"{field} must be a non-empty string")
    return value


def _required_text(item: Mapping[str, object], field: str) -> str:
    return _nonempty_text(item.get(field), field)


def _enum_text(item: Mapping[str, object], field: str, allowed: set[str]) -> str:
    value = _required_text(item, field)
    if value not in allowed:
        raise DeribitIngestionError(f"{field} is unsupported")
    return value


def _required_bool(item: Mapping[str, object], field: str) -> bool:
    value = item.get(field)
    if type(value) is not bool:
        raise DeribitIngestionError(f"{field} must be a boolean")
    return value


def _required_integer(item: Mapping[str, object], field: str) -> int:
    value = item.get(field)
    if type(value) is not int:
        raise DeribitIngestionError(f"{field} must be an integer")
    return value


def _optional_integer(item: Mapping[str, object], field: str) -> int | None:
    value = item.get(field)
    if value is None:
        return None
    if type(value) is not int:
        raise DeribitIngestionError(f"{field} must be an integer or null")
    return value


def _required_decimal(item: Mapping[str, object], field: str) -> Decimal:
    value = _optional_decimal(item, field)
    if value is None:
        raise DeribitIngestionError(f"{field} must be numeric")
    return value


def _optional_decimal(item: Mapping[str, object], field: str) -> Decimal | None:
    value = item.get(field)
    if value is None:
        return None
    if type(value) is int:
        result = Decimal(value)
    elif type(value) is Decimal:
        result = value
    else:
        raise DeribitIngestionError(f"{field} must be numeric or null")
    if not result.is_finite():
        raise DeribitIngestionError(f"{field} must be finite")
    return result


def _price_levels(item: Mapping[str, object], field: str) -> tuple[tuple[Decimal, Decimal], ...]:
    value = item.get(field)
    if not isinstance(value, list) or len(value) > 1:
        raise DeribitIngestionError(f"{field} must contain at most one depth level")
    levels: list[tuple[Decimal, Decimal]] = []
    for level in value:
        if not isinstance(level, list) or len(level) != 2:
            raise DeribitIngestionError(f"{field} contains an invalid level")
        levels.append(
            (
                _decimal_value(level[0], f"{field} price"),
                _decimal_value(level[1], f"{field} amount"),
            )
        )
    return tuple(levels)


def _decimal_value(value: object, field: str) -> Decimal:
    if type(value) is int:
        result = Decimal(value)
    elif type(value) is Decimal:
        result = value
    else:
        raise DeribitIngestionError(f"{field} must be numeric")
    if not result.is_finite():
        raise DeribitIngestionError(f"{field} must be finite")
    return result


def _deribit_stats(item: Mapping[str, object]) -> Mapping[str, object]:
    stats = _require_mapping(item.get("stats"), "stats")
    return MappingProxyType(
        {
            "volume": _required_decimal(stats, "volume"),
            "high": _optional_decimal(stats, "high"),
            "low": _optional_decimal(stats, "low"),
            "price_change": _optional_decimal(stats, "price_change"),
            "volume_usd": _optional_decimal(stats, "volume_usd"),
        }
    )


def _deribit_greeks(item: Mapping[str, object]) -> Mapping[str, object]:
    greeks = _require_mapping(item.get("greeks"), "greeks")
    return MappingProxyType(
        {
            name: _required_decimal(greeks, name)
            for name in ("delta", "gamma", "rho", "theta", "vega")
        }
    )


def _milliseconds_datetime(value: int, field: str) -> datetime:
    if value < 0:
        raise DeribitIngestionError(f"{field} must not be negative")
    try:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)
    except OverflowError as error:
        raise DeribitIngestionError(f"{field} is outside supported range") from error


def _microseconds_datetime(value: int, field: str) -> datetime:
    if value < 0:
        raise DeribitIngestionError(f"{field} must not be negative")
    try:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value)
    except OverflowError as error:
        raise DeribitIngestionError(f"{field} is outside supported range") from error


def _require_utc(value: object, field: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise DeribitIngestionError(f"{field} must be timezone-aware")
    offset = value.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise DeribitIngestionError(f"{field} must be normalized to UTC")
    return value


def _validated_response(response: object) -> DeribitReadOnlyResponse:
    unvalidated_response = cast(DeribitReadOnlyResponse, response)
    try:
        return DeribitReadOnlyResponse(
            status_code=unvalidated_response.status_code,
            content_type=unvalidated_response.content_type,
            body=unvalidated_response.body,
            received_at=unvalidated_response.received_at,
        )
    except (AttributeError, DeribitIngestionError) as error:
        raise DeribitIngestionError("response evidence is invalid") from error
