"""Read-only, provenance-backed Kalshi market ingestion."""

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, Protocol

from kalshi_crypto_basis.snapshots import (
    InMemorySnapshotStore,
    SnapshotEnvelope,
    SnapshotError,
    canonical_request_fingerprint,
)


class KalshiIngestionError(ValueError):
    """Raised when Kalshi evidence cannot be ingested deterministically."""


@dataclass(frozen=True, slots=True)
class ReadOnlyResponse:
    """Exact response bytes and the clocks attached by a read-only transport."""

    body: bytes
    observed_at: datetime
    received_at: datetime

    def __post_init__(self) -> None:
        if type(self.body) is not bytes:
            raise KalshiIngestionError("response body must be bytes")
        _require_utc_datetime_value(self.observed_at, "observed_at")
        _require_utc_datetime_value(self.received_at, "received_at")
        if self.received_at < self.observed_at:
            raise KalshiIngestionError("received_at cannot precede observed_at")


@dataclass(frozen=True, slots=True)
class ReviewedSeries:
    """Owner-reviewed binding between a native Series ticker and an underlying."""

    ticker: str
    underlying: Literal["BTC", "ETH"]

    def __post_init__(self) -> None:
        if type(self.ticker) is not str or not self.ticker or self.ticker != self.ticker.strip():
            raise KalshiIngestionError("reviewed series ticker must be non-empty and unpadded")
        if type(self.underlying) is not str or self.underlying not in {"BTC", "ETH"}:
            raise KalshiIngestionError("reviewed series underlying must be BTC or ETH")


class ReadOnlyTransport(Protocol):
    """Narrow transport capability: this adapter can only issue GET requests."""

    def get(self, path: str, parameters: dict[str, object]) -> ReadOnlyResponse: ...


@dataclass(frozen=True, slots=True)
class KalshiMarket:
    ticker: str
    event_ticker: str
    series_ticker: str
    underlying: Literal["BTC", "ETH"]
    status: str
    market_type: str
    yes_bid: Decimal
    no_bid: Decimal


@dataclass(frozen=True, slots=True)
class MarketDiscoveryBatch:
    markets: tuple[KalshiMarket, ...]
    pages: tuple[SnapshotEnvelope, ...]
    complete: bool


@dataclass(frozen=True, slots=True)
class OrderbookLevel:
    price: Decimal
    count: Decimal


@dataclass(frozen=True, slots=True)
class KalshiOrderbook:
    ticker: str
    yes_bids: tuple[OrderbookLevel, ...]
    no_bids: tuple[OrderbookLevel, ...]
    yes_asks: tuple[OrderbookLevel, ...]
    no_asks: tuple[OrderbookLevel, ...]
    snapshot: SnapshotEnvelope
    raw_payload: bytes


@dataclass(frozen=True, slots=True)
class KalshiSeriesDetail:
    ticker: str
    category: str
    settlement_sources: tuple[tuple[str, str], ...]
    contract_url: str
    contract_terms_url: str
    fee_type: str
    fee_multiplier: Decimal


@dataclass(frozen=True, slots=True)
class KalshiEventDetail:
    event_ticker: str
    series_ticker: str
    settlement_sources: tuple[tuple[str, str], ...]
    fee_type_override: str | None
    fee_multiplier_override: Decimal | None


@dataclass(frozen=True, slots=True)
class PriceRange:
    start: Decimal
    end: Decimal
    step: Decimal


@dataclass(frozen=True, slots=True)
class KalshiMarketDetail:
    ticker: str
    event_ticker: str
    series_ticker: str
    underlying: Literal["BTC", "ETH"]
    market_type: str
    lifecycle_status: str
    strike_type: str
    floor_strike: Decimal | None
    cap_strike: Decimal | None
    rules_primary: str
    rules_secondary: str
    can_close_early: bool
    close_time: datetime
    expected_expiration_time: datetime
    price_level_structure: str
    price_ranges: tuple[PriceRange, ...]
    snapshot: SnapshotEnvelope
    raw_payload: bytes


@dataclass(frozen=True, slots=True)
class LiveMarketEvidence:
    series: KalshiSeriesDetail
    event: KalshiEventDetail
    market: KalshiMarketDetail
    snapshots: tuple[SnapshotEnvelope, SnapshotEnvelope, SnapshotEnvelope]


@dataclass(frozen=True, slots=True)
class HistoricalCutoff:
    market_settled_at: datetime
    snapshot: SnapshotEnvelope


@dataclass(frozen=True, slots=True)
class RoutedMarketDetail:
    source_tier: Literal["live", "historical"]
    cutoff: HistoricalCutoff
    market: KalshiMarketDetail
    snapshots: tuple[SnapshotEnvelope, SnapshotEnvelope]


@dataclass(frozen=True, slots=True)
class ValidatedOrderbookEvidence:
    book: KalshiOrderbook
    market: KalshiMarketDetail


@dataclass(frozen=True, slots=True)
class IngestionGap:
    ticker: str
    code: Literal[
        "DETAIL_MISSING",
        "ORDERBOOK_MISSING",
        "NO_EXECUTABLE_LIQUIDITY",
    ]


@dataclass(frozen=True, slots=True)
class GapReport:
    gaps: tuple[IngestionGap, ...]
    complete: bool


class KalshiReadOnlyIngestor:
    """Discover only markets belonging to explicitly reviewed Series tickers."""

    def __init__(
        self,
        *,
        transport: ReadOnlyTransport,
        reviewed_series: tuple[ReviewedSeries, ...],
        max_pages: int = 100,
        snapshot_store: InMemorySnapshotStore | None = None,
    ) -> None:
        if type(reviewed_series) is not tuple:
            raise KalshiIngestionError("reviewed_series must be a tuple")
        if snapshot_store is not None and type(snapshot_store) is not InMemorySnapshotStore:
            raise KalshiIngestionError("snapshot_store must be an InMemorySnapshotStore")
        validated_series: list[ReviewedSeries] = []
        for series in reviewed_series:
            if type(series) is not ReviewedSeries:
                raise KalshiIngestionError("reviewed_series members must be ReviewedSeries")
            try:
                ticker = series.ticker
                underlying = series.underlying
            except AttributeError as error:
                raise KalshiIngestionError("reviewed series is incomplete") from error
            _required_path_segment(ticker, "reviewed series ticker")
            if type(underlying) is not str or underlying not in {"BTC", "ETH"}:
                raise KalshiIngestionError("reviewed series underlying must be BTC or ETH")
            validated_series.append(ReviewedSeries(ticker=ticker, underlying=underlying))
        by_ticker = {series.ticker: series for series in validated_series}
        if len(by_ticker) != len(reviewed_series):
            raise KalshiIngestionError("reviewed series tickers must be unique")
        if type(max_pages) is not int or max_pages < 1:
            raise KalshiIngestionError("max_pages must be a positive integer")
        self._transport = transport
        self._reviewed_series = by_ticker
        self._max_pages = max_pages
        self.__snapshot_store = (
            snapshot_store if snapshot_store is not None else InMemorySnapshotStore()
        )

    def discover_markets(self, *, series_ticker: str) -> MarketDiscoveryBatch:
        series_ticker = _required_path_segment(series_ticker, "series_ticker")
        try:
            series = self._reviewed_series[series_ticker]
        except KeyError as error:
            raise KalshiIngestionError("series ticker is not reviewed") from error
        base_parameters: dict[str, object] = {
            "series_ticker": series.ticker,
            "status": "open",
            "limit": 1000,
        }
        path = "/trade-api/v2/markets"
        all_markets: list[KalshiMarket] = []
        markets_by_ticker: dict[str, KalshiMarket] = {}
        pages: list[SnapshotEnvelope] = []
        seen_cursors: set[str] = set()
        cursor = ""
        while True:
            parameters = dict(base_parameters)
            if cursor:
                parameters["cursor"] = cursor
            response = self._get_response(path, parameters)
            payload = _parse_object(response.body)
            raw_markets = payload.get("markets")
            next_cursor = payload.get("cursor")
            if not isinstance(raw_markets, list) or not isinstance(next_cursor, str):
                raise KalshiIngestionError("market page has invalid markets or cursor")
            markets = tuple(_parse_market(item, series) for item in raw_markets)
            normalized = {
                "series_ticker": series.ticker,
                "underlying": series.underlying,
                "markets": [
                    {
                        "ticker": market.ticker,
                        "event_ticker": market.event_ticker,
                        "status": market.status,
                        "market_type": market.market_type,
                        "yes_bid": market.yes_bid,
                        "no_bid": market.no_bid,
                    }
                    for market in markets
                ],
                "cursor": next_cursor,
            }
            try:
                snapshot = SnapshotEnvelope.create(
                    source="kalshi",
                    request_fingerprint=canonical_request_fingerprint("GET", path, parameters),
                    observed_at=response.observed_at,
                    ingested_at=response.received_at,
                    parser_version="kalshi-markets-v1",
                    raw_payload=response.body,
                    normalized=normalized,
                )
            except SnapshotError as error:
                raise KalshiIngestionError("market page provenance is invalid") from error
            for market in markets:
                existing_market = markets_by_ticker.get(market.ticker)
                if existing_market is None:
                    markets_by_ticker[market.ticker] = market
                    all_markets.append(market)
                elif existing_market != market:
                    raise KalshiIngestionError("conflicting duplicate ticker")
            pages.append(snapshot)
            if not next_cursor:
                return MarketDiscoveryBatch(
                    markets=tuple(all_markets), pages=tuple(pages), complete=True
                )
            if len(pages) >= self._max_pages:
                raise KalshiIngestionError("page budget exhausted")
            if next_cursor in seen_cursors:
                raise KalshiIngestionError("cursor loop detected")
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    def get_orderbook(self, *, ticker: str, market_type: str) -> KalshiOrderbook:
        if type(market_type) is not str or market_type != "binary":
            raise KalshiIngestionError("market_type must be binary")
        ticker = _required_path_segment(ticker, "ticker")
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        parameters: dict[str, object] = {"depth": 0}
        response = self._get_response(path, parameters)
        payload = _parse_object(response.body)
        orderbook = payload.get("orderbook_fp")
        if not isinstance(orderbook, Mapping):
            raise KalshiIngestionError("orderbook_fp must be an object")
        yes_bids = _parse_levels(orderbook.get("yes_dollars"), "yes_dollars")
        no_bids = _parse_levels(orderbook.get("no_dollars"), "no_dollars")
        if yes_bids and no_bids and yes_bids[-1].price + no_bids[-1].price > Decimal("1"):
            raise KalshiIngestionError("binary orderbook is crossed")
        yes_asks = _complement_asks(no_bids)
        no_asks = _complement_asks(yes_bids)
        normalized = {
            "ticker": ticker,
            "pricing_basis": "side_leg",
            "yes_bids": _levels_value(yes_bids),
            "no_bids": _levels_value(no_bids),
            "yes_asks": _levels_value(yes_asks),
            "no_asks": _levels_value(no_asks),
        }
        try:
            snapshot = SnapshotEnvelope.create(
                source="kalshi",
                request_fingerprint=canonical_request_fingerprint("GET", path, parameters),
                observed_at=response.observed_at,
                ingested_at=response.received_at,
                parser_version="kalshi-orderbook-v1",
                raw_payload=response.body,
                normalized=normalized,
            )
        except SnapshotError as error:
            raise KalshiIngestionError("orderbook provenance is invalid") from error
        try:
            snapshot = self.__snapshot_store.put(snapshot, raw_payload=response.body)
        except SnapshotError as error:
            raise KalshiIngestionError("orderbook persistence failed") from error
        return KalshiOrderbook(
            ticker=ticker,
            yes_bids=yes_bids,
            no_bids=no_bids,
            yes_asks=yes_asks,
            no_asks=no_asks,
            snapshot=snapshot,
            raw_payload=response.body,
        )

    def get_live_market_evidence(
        self,
        *,
        series_ticker: str,
        event_ticker: str,
        market_ticker: str,
    ) -> LiveMarketEvidence:
        series_ticker = _required_path_segment(series_ticker, "series_ticker")
        try:
            reviewed = self._reviewed_series[series_ticker]
        except KeyError as error:
            raise KalshiIngestionError("series ticker is not reviewed") from error
        event_ticker = _required_path_segment(event_ticker, "event_ticker")
        market_ticker = _required_path_segment(market_ticker, "market_ticker")
        series_payload, series_snapshot, _series_raw = self._get_detail(
            path=f"/trade-api/v2/series/{series_ticker}",
            parameters={},
            root="series",
            parser_version="kalshi-series-v1",
        )
        series = _parse_series_detail(series_payload)
        if series.ticker != reviewed.ticker:
            raise KalshiIngestionError("series response identity mismatch")
        event_payload, event_snapshot, _event_raw = self._get_detail(
            path=f"/trade-api/v2/events/{event_ticker}",
            parameters={"with_nested_markets": False},
            root="event",
            parser_version="kalshi-event-v1",
        )
        event = _parse_event_detail(event_payload)
        if event.event_ticker != event_ticker or event.series_ticker != series.ticker:
            raise KalshiIngestionError("event response identity mismatch")
        market_payload, market_snapshot, market_raw = self._get_detail(
            path=f"/trade-api/v2/markets/{market_ticker}",
            parameters={},
            root="market",
            parser_version="kalshi-market-detail-v1",
        )
        market = _parse_market_detail(
            market_payload,
            reviewed.underlying,
            snapshot=market_snapshot,
            raw_payload=market_raw,
        )
        if (
            market.ticker != market_ticker
            or market.event_ticker != event.event_ticker
            or market.series_ticker != reviewed.ticker
        ):
            raise KalshiIngestionError("market response identity mismatch")
        if not series.settlement_sources or not event.settlement_sources:
            raise KalshiIngestionError("settlement sources are required")
        try:
            market_snapshot = self.__snapshot_store.put(market_snapshot, raw_payload=market_raw)
        except SnapshotError as error:
            raise KalshiIngestionError("market detail persistence failed") from error
        return LiveMarketEvidence(
            series=series,
            event=event,
            market=market,
            snapshots=(series_snapshot, event_snapshot, market_snapshot),
        )

    def get_routed_market_detail(
        self,
        *,
        ticker: str,
        settlement_time: datetime | None,
        underlying: Literal["BTC", "ETH"],
    ) -> RoutedMarketDetail:
        ticker = _required_path_segment(ticker, "ticker")
        if type(underlying) is not str or underlying not in {"BTC", "ETH"}:
            raise KalshiIngestionError("underlying must be BTC or ETH")
        if underlying not in {series.underlying for series in self._reviewed_series.values()}:
            raise KalshiIngestionError("underlying is not reviewed")
        if settlement_time is None:
            raise KalshiIngestionError("PARTITION_UNKNOWN: settlement time is required")
        _require_utc_datetime_value(settlement_time, "settlement_time")
        cutoff_response = self._get_response("/trade-api/v2/historical/cutoff", {})
        cutoff_payload = _parse_object(cutoff_response.body)
        market_settled_at = _required_utc_datetime(
            cutoff_payload.get("market_settled_ts"), "market_settled_ts"
        )
        try:
            cutoff_snapshot = SnapshotEnvelope.create(
                source="kalshi",
                request_fingerprint=canonical_request_fingerprint(
                    "GET", "/trade-api/v2/historical/cutoff", {}
                ),
                observed_at=cutoff_response.observed_at,
                ingested_at=cutoff_response.received_at,
                parser_version="kalshi-historical-cutoff-v1",
                raw_payload=cutoff_response.body,
                normalized=cutoff_payload,
            )
        except SnapshotError as error:
            raise KalshiIngestionError("historical cutoff provenance is invalid") from error
        cutoff = HistoricalCutoff(market_settled_at=market_settled_at, snapshot=cutoff_snapshot)
        source_tier: Literal["live", "historical"]
        if settlement_time < market_settled_at:
            source_tier = "historical"
            path = f"/trade-api/v2/historical/markets/{ticker}"
        else:
            source_tier = "live"
            path = f"/trade-api/v2/markets/{ticker}"
        market_payload, market_snapshot, market_raw = self._get_detail(
            path=path,
            parameters={},
            root="market",
            parser_version="kalshi-market-detail-v1",
        )
        returned_series_ticker = _required_path_segment(
            market_payload.get("series_ticker"), "market.series_ticker"
        )
        try:
            reviewed = self._reviewed_series[returned_series_ticker]
        except KeyError as error:
            raise KalshiIngestionError("market series is not reviewed") from error
        if reviewed.underlying != underlying:
            raise KalshiIngestionError("market underlying identity mismatch")
        market = _parse_market_detail(
            market_payload,
            reviewed.underlying,
            snapshot=market_snapshot,
            raw_payload=market_raw,
        )
        if market.ticker != ticker:
            raise KalshiIngestionError("market response identity mismatch")
        try:
            market_snapshot = self.__snapshot_store.put(market_snapshot, raw_payload=market_raw)
        except SnapshotError as error:
            raise KalshiIngestionError("market detail persistence failed") from error
        return RoutedMarketDetail(
            source_tier=source_tier,
            cutoff=cutoff,
            market=market,
            snapshots=(cutoff_snapshot, market_snapshot),
        )

    def validate_orderbook_for_market(
        self, *, orderbook_snapshot_id: str, market_snapshot_id: str
    ) -> ValidatedOrderbookEvidence:
        orderbook_snapshot_id = _required_text(orderbook_snapshot_id, "orderbook_snapshot_id")
        market_snapshot_id = _required_text(market_snapshot_id, "market_snapshot_id")
        try:
            stored_book = self.__snapshot_store.get(orderbook_snapshot_id)
            stored_market = self.__snapshot_store.get(market_snapshot_id)
            if stored_book is None or stored_market is None:
                raise KalshiIngestionError("record is not persisted evidence")
            stored_book_raw = self.__snapshot_store.get_raw(stored_book.raw_sha256)
            stored_market_raw = self.__snapshot_store.get_raw(stored_market.raw_sha256)
            if stored_book_raw is None or stored_market_raw is None:
                raise KalshiIngestionError("persisted raw evidence is missing")
        except (SnapshotError, TypeError, ValueError) as error:
            raise KalshiIngestionError("record is not persisted evidence") from error
        if not isinstance(stored_book.normalized, Mapping):
            raise KalshiIngestionError("orderbook normalized evidence is invalid")
        book_ticker = _required_path_segment(
            stored_book.normalized.get("ticker"), "orderbook ticker"
        )
        book_snapshot = _replay_snapshot(
            snapshot=stored_book,
            raw_payload=stored_book_raw,
            source="kalshi",
            request_fingerprints=(
                canonical_request_fingerprint(
                    "GET", f"/trade-api/v2/markets/{book_ticker}/orderbook", {"depth": 0}
                ),
            ),
            parser_version="kalshi-orderbook-v1",
            error_message="orderbook snapshot provenance is invalid",
        )
        raw_book = _parse_object(stored_book_raw).get("orderbook_fp")
        if not isinstance(raw_book, Mapping):
            raise KalshiIngestionError("orderbook snapshot raw evidence is invalid")
        raw_yes_bids = _parse_levels(raw_book.get("yes_dollars"), "yes_dollars")
        raw_no_bids = _parse_levels(raw_book.get("no_dollars"), "no_dollars")
        raw_yes_asks = _complement_asks(raw_no_bids)
        raw_no_asks = _complement_asks(raw_yes_bids)
        raw_normalized = {
            "ticker": book_ticker,
            "pricing_basis": "side_leg",
            "yes_bids": _frozen_levels_value(raw_yes_bids),
            "no_bids": _frozen_levels_value(raw_no_bids),
            "yes_asks": _frozen_levels_value(raw_yes_asks),
            "no_asks": _frozen_levels_value(raw_no_asks),
        }
        if book_snapshot.normalized != raw_normalized:
            raise KalshiIngestionError("orderbook snapshot does not match raw evidence")
        book = KalshiOrderbook(
            ticker=book_ticker,
            yes_bids=raw_yes_bids,
            no_bids=raw_no_bids,
            yes_asks=raw_yes_asks,
            no_asks=raw_no_asks,
            snapshot=book_snapshot,
            raw_payload=stored_book_raw,
        )

        raw_market_payload = _parse_object(stored_market_raw).get("market")
        if not isinstance(raw_market_payload, Mapping):
            raise KalshiIngestionError("market detail evidence is invalid")
        market_ticker = _required_path_segment(raw_market_payload.get("ticker"), "market.ticker")
        market_snapshot = _replay_snapshot(
            snapshot=stored_market,
            raw_payload=stored_market_raw,
            source="kalshi",
            request_fingerprints=(
                canonical_request_fingerprint("GET", f"/trade-api/v2/markets/{market_ticker}", {}),
                canonical_request_fingerprint(
                    "GET", f"/trade-api/v2/historical/markets/{market_ticker}", {}
                ),
            ),
            parser_version="kalshi-market-detail-v1",
            error_message="market detail evidence is invalid",
        )
        returned_series = _required_path_segment(
            raw_market_payload.get("series_ticker"), "market.series_ticker"
        )
        try:
            reviewed = self._reviewed_series[returned_series]
        except KeyError as error:
            raise KalshiIngestionError("market series is not reviewed") from error
        market = _parse_market_detail(
            raw_market_payload,
            reviewed.underlying,
            snapshot=market_snapshot,
            raw_payload=stored_market_raw,
        )
        if book.ticker != market.ticker:
            raise KalshiIngestionError("orderbook and market ticker mismatch")
        if market.market_type != "binary":
            raise KalshiIngestionError("orderbook requires a binary market")
        for level in (*book.yes_bids, *book.no_bids, *book.yes_asks, *book.no_asks):
            if not _price_matches_grid(level.price, market.price_ranges):
                raise KalshiIngestionError("orderbook level violates market price grid")
        return ValidatedOrderbookEvidence(book=book, market=market)

    def _get_detail(
        self,
        *,
        path: str,
        parameters: dict[str, object],
        root: str,
        parser_version: str,
    ) -> tuple[Mapping[str, object], SnapshotEnvelope, bytes]:
        response = self._get_response(path, parameters)
        payload = _parse_object(response.body)
        detail = payload.get(root)
        if not isinstance(detail, Mapping):
            raise KalshiIngestionError(f"{root} detail must be an object")
        normalized = dict(detail)
        try:
            snapshot = SnapshotEnvelope.create(
                source="kalshi",
                request_fingerprint=canonical_request_fingerprint("GET", path, parameters),
                observed_at=response.observed_at,
                ingested_at=response.received_at,
                parser_version=parser_version,
                raw_payload=response.body,
                normalized=normalized,
            )
        except SnapshotError as error:
            raise KalshiIngestionError(f"{root} detail provenance is invalid") from error
        return detail, snapshot, response.body

    def _get_response(self, path: str, parameters: dict[str, object]) -> ReadOnlyResponse:
        try:
            response = self._transport.get(path, parameters)
        except KalshiIngestionError:
            raise
        except (AttributeError, TypeError) as error:
            raise KalshiIngestionError("transport GET capability is invalid") from error
        if type(response) is not ReadOnlyResponse:
            raise KalshiIngestionError("transport must return ReadOnlyResponse")
        try:
            return ReadOnlyResponse(
                body=response.body,
                observed_at=response.observed_at,
                received_at=response.received_at,
            )
        except KalshiIngestionError:
            raise
        except (AttributeError, TypeError) as error:
            raise KalshiIngestionError("transport response evidence is invalid") from error


def build_gap_report(
    *,
    markets: tuple[KalshiMarket, ...],
    detail_tickers: set[str],
    orderbooks: Mapping[str, KalshiOrderbook],
) -> GapReport:
    gaps: list[IngestionGap] = []
    for market in markets:
        if market.ticker not in detail_tickers:
            gaps.append(IngestionGap(ticker=market.ticker, code="DETAIL_MISSING"))
            continue
        book = orderbooks.get(market.ticker)
        if book is None:
            gaps.append(IngestionGap(ticker=market.ticker, code="ORDERBOOK_MISSING"))
        elif not book.yes_bids and not book.no_bids:
            gaps.append(IngestionGap(ticker=market.ticker, code="NO_EXECUTABLE_LIQUIDITY"))
    ordered = tuple(sorted(gaps, key=lambda gap: (gap.ticker, gap.code)))
    return GapReport(gaps=ordered, complete=not ordered)


def _price_matches_grid(price: Decimal, ranges: tuple[PriceRange, ...]) -> bool:
    for price_range in ranges:
        if price_range.start <= price <= price_range.end:
            return (price - price_range.start) % price_range.step == 0
    return False


def _replay_snapshot(
    *,
    snapshot: SnapshotEnvelope,
    raw_payload: bytes,
    source: str,
    request_fingerprints: tuple[str, ...],
    parser_version: str,
    error_message: str,
) -> SnapshotEnvelope:
    if not isinstance(raw_payload, bytes):
        raise KalshiIngestionError(error_message)
    try:
        replayed = SnapshotEnvelope.from_canonical_json(
            snapshot.to_canonical_json(), raw_payload=raw_payload
        )
    except (AttributeError, SnapshotError, TypeError, ValueError) as error:
        raise KalshiIngestionError(error_message) from error
    if (
        replayed.source != source
        or replayed.request_fingerprint not in request_fingerprints
        or replayed.parser_version != parser_version
    ):
        raise KalshiIngestionError(error_message)
    return replayed


def _parse_object(body: bytes) -> dict[str, object]:
    try:
        payload = json.loads(
            body,
            parse_float=Decimal,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_json_constant,
        )
    except NonStandardJsonConstantError as error:
        raise KalshiIngestionError("non-standard JSON constant") from error
    except DuplicateJsonKeyError as error:
        raise KalshiIngestionError("duplicate JSON object key") from error
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise KalshiIngestionError("market page is invalid JSON") from error
    if not isinstance(payload, dict):
        raise KalshiIngestionError("market page must be an object")
    return payload


def _parse_market(value: object, series: ReviewedSeries) -> KalshiMarket:
    if not isinstance(value, Mapping):
        raise KalshiIngestionError("market must be an object")
    returned_series = _required_text(value.get("series_ticker"), "series_ticker")
    if returned_series != series.ticker:
        raise KalshiIngestionError("market series identity mismatch")
    status = _required_text(value.get("status"), "status")
    if status != "open":
        raise KalshiIngestionError("market discovery status must be open")
    market_type = _required_text(value.get("market_type"), "market_type")
    if market_type != "binary":
        raise KalshiIngestionError("market discovery requires binary markets")
    return KalshiMarket(
        ticker=_required_path_segment(value.get("ticker"), "ticker"),
        event_ticker=_required_path_segment(value.get("event_ticker"), "event_ticker"),
        series_ticker=returned_series,
        underlying=series.underlying,
        status=status,
        market_type=market_type,
        yes_bid=_required_decimal(value.get("yes_bid_dollars"), "yes_bid_dollars"),
        no_bid=_required_decimal(value.get("no_bid_dollars"), "no_bid_dollars"),
    )


def _parse_series_detail(value: Mapping[str, object]) -> KalshiSeriesDetail:
    fee_type = _required_text(value.get("fee_type"), "fee_type")
    if fee_type not in {"quadratic", "quadratic_with_maker_fees", "flat"}:
        raise KalshiIngestionError("unsupported series fee_type")
    return KalshiSeriesDetail(
        ticker=_required_text(value.get("ticker"), "series.ticker"),
        category=_required_text(value.get("category"), "series.category"),
        settlement_sources=_parse_settlement_sources(
            value.get("settlement_sources"), "series.settlement_sources"
        ),
        contract_url=_required_text(value.get("contract_url"), "series.contract_url"),
        contract_terms_url=_required_text(
            value.get("contract_terms_url"), "series.contract_terms_url"
        ),
        fee_type=fee_type,
        fee_multiplier=_decimal_number(value.get("fee_multiplier"), "series.fee_multiplier"),
    )


def _parse_event_detail(value: Mapping[str, object]) -> KalshiEventDetail:
    fee_type_value = value.get("fee_type_override")
    fee_multiplier_value = value.get("fee_multiplier_override")
    if (fee_type_value is None) != (fee_multiplier_value is None):
        raise KalshiIngestionError("event fee override fields must be jointly populated")
    fee_type: str | None = None
    fee_multiplier: Decimal | None = None
    if fee_type_value is not None:
        fee_type = _required_text(fee_type_value, "event.fee_type_override")
        if fee_type not in {"quadratic", "quadratic_with_maker_fees", "flat"}:
            raise KalshiIngestionError("unsupported event fee_type_override")
        fee_multiplier = _decimal_number(fee_multiplier_value, "event.fee_multiplier_override")
    return KalshiEventDetail(
        event_ticker=_required_text(value.get("event_ticker"), "event.event_ticker"),
        series_ticker=_required_text(value.get("series_ticker"), "event.series_ticker"),
        settlement_sources=_parse_settlement_sources(
            value.get("settlement_sources"), "event.settlement_sources"
        ),
        fee_type_override=fee_type,
        fee_multiplier_override=fee_multiplier,
    )


def _parse_market_detail(
    value: Mapping[str, object],
    underlying: Literal["BTC", "ETH"],
    *,
    snapshot: SnapshotEnvelope,
    raw_payload: bytes,
) -> KalshiMarketDetail:
    market_type = _required_text(value.get("market_type"), "market.market_type")
    if market_type != "binary":
        raise KalshiIngestionError("market must be binary")
    status = _required_text(value.get("status"), "market.status")
    if status not in {
        "initialized",
        "inactive",
        "active",
        "closed",
        "determined",
        "disputed",
        "amended",
        "finalized",
    }:
        raise KalshiIngestionError("unsupported market lifecycle status")
    strike_type = _required_text(value.get("strike_type"), "market.strike_type")
    floor_strike = _optional_decimal_number(value.get("floor_strike"), "market.floor_strike")
    cap_strike = _optional_decimal_number(value.get("cap_strike"), "market.cap_strike")
    if strike_type in {"greater", "greater_or_equal"}:
        if floor_strike is None or cap_strike is not None:
            raise KalshiIngestionError("greater strike requires only floor_strike")
    elif strike_type in {"less", "less_or_equal"}:
        if cap_strike is None or floor_strike is not None:
            raise KalshiIngestionError("less strike requires only cap_strike")
    elif strike_type == "between":
        if floor_strike is None or cap_strike is None or floor_strike >= cap_strike:
            raise KalshiIngestionError("between strike requires ordered bounds")
    else:
        raise KalshiIngestionError("unsupported market strike_type")
    can_close_early = value.get("can_close_early")
    if not isinstance(can_close_early, bool):
        raise KalshiIngestionError("market.can_close_early must be boolean")
    price_ranges = _parse_price_ranges(value.get("price_ranges"))
    return KalshiMarketDetail(
        ticker=_required_text(value.get("ticker"), "market.ticker"),
        event_ticker=_required_text(value.get("event_ticker"), "market.event_ticker"),
        series_ticker=_required_path_segment(value.get("series_ticker"), "market.series_ticker"),
        underlying=underlying,
        market_type=market_type,
        lifecycle_status=status,
        strike_type=strike_type,
        floor_strike=floor_strike,
        cap_strike=cap_strike,
        rules_primary=_required_text(value.get("rules_primary"), "market.rules_primary"),
        rules_secondary=_required_text(value.get("rules_secondary"), "market.rules_secondary"),
        can_close_early=can_close_early,
        close_time=_required_utc_datetime(value.get("close_time"), "market.close_time"),
        expected_expiration_time=_required_utc_datetime(
            value.get("expected_expiration_time"), "market.expected_expiration_time"
        ),
        price_level_structure=_required_text(
            value.get("price_level_structure"), "market.price_level_structure"
        ),
        price_ranges=price_ranges,
        snapshot=snapshot,
        raw_payload=raw_payload,
    )


def _required_text(value: object, field: str) -> str:
    if type(value) is not str or not value:
        raise KalshiIngestionError(f"{field} must be a non-empty string")
    return value


def _required_path_segment(value: object, field: str) -> str:
    segment = _required_text(value, field)
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", segment, flags=re.ASCII) is None:
        raise KalshiIngestionError(f"{field} must be one safe path segment")
    return segment


class DuplicateJsonKeyError(ValueError):
    """A JSON object repeated the same exact member name."""


class NonStandardJsonConstantError(ValueError):
    """JSON contained a non-standard NaN or infinity constant."""


def _reject_json_constant(value: str) -> object:
    raise NonStandardJsonConstantError(value)


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJsonKeyError(key)
        result[key] = value
    return result


def _decimal_number(value: object, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, str | int | Decimal):
        raise KalshiIngestionError(f"{field} must be an exact decimal number")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise KalshiIngestionError(f"{field} must be an exact decimal number") from error
    if not parsed.is_finite():
        raise KalshiIngestionError(f"{field} must be finite")
    return parsed


def _optional_decimal_number(value: object, field: str) -> Decimal | None:
    if value is None:
        return None
    return _decimal_number(value, field)


def _parse_settlement_sources(value: object, field: str) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        raise KalshiIngestionError(f"{field} must be a list")
    sources: list[tuple[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise KalshiIngestionError(f"{field} entries must be objects")
        sources.append(
            (
                _required_text(item.get("name"), f"{field}.name"),
                _required_text(item.get("url"), f"{field}.url"),
            )
        )
    return tuple(sources)


def _parse_price_ranges(value: object) -> tuple[PriceRange, ...]:
    if not isinstance(value, list) or not value:
        raise KalshiIngestionError("market.price_ranges must be a non-empty list")
    ranges: list[PriceRange] = []
    previous_end: Decimal | None = None
    for item in value:
        if not isinstance(item, Mapping):
            raise KalshiIngestionError("market.price_ranges entries must be objects")
        start = _plain_decimal(item.get("start"), "market.price_ranges.start")
        end = _plain_decimal(item.get("end"), "market.price_ranges.end")
        step = _plain_decimal(item.get("step"), "market.price_ranges.step")
        if start < 0 or end > 1 or start > end or step <= 0:
            raise KalshiIngestionError("market.price_ranges bounds or step are invalid")
        if (end - start) % step != 0:
            raise KalshiIngestionError("market.price_ranges endpoints must align to step")
        if previous_end is None:
            if start != 0:
                raise KalshiIngestionError("market.price_ranges must begin at zero")
        elif start != previous_end + step:
            raise KalshiIngestionError("market.price_ranges must be contiguous")
        ranges.append(PriceRange(start=start, end=end, step=step))
        previous_end = end
    if previous_end != 1:
        raise KalshiIngestionError("market.price_ranges must end at one")
    return tuple(ranges)


def _plain_decimal(value: object, field: str) -> Decimal:
    if not isinstance(value, str) or re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value) is None:
        raise KalshiIngestionError(f"{field} must use plain decimal syntax")
    return _decimal_number(value, field)


def _required_utc_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise KalshiIngestionError(f"{field} must use canonical UTC format")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise KalshiIngestionError(f"{field} must use canonical UTC format") from error
    canonical = parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
    if canonical != value:
        raise KalshiIngestionError(f"{field} must use canonical UTC format")
    return parsed


def _require_utc_datetime_value(value: datetime, field: str) -> None:
    if type(value) is not datetime:
        raise KalshiIngestionError(f"{field} must be a datetime")
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise KalshiIngestionError(f"{field} must be timezone-aware UTC")


def _required_decimal(value: object, field: str) -> Decimal:
    if not isinstance(value, str):
        raise KalshiIngestionError(f"{field} must be a decimal string")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise KalshiIngestionError(f"{field} must be a decimal string") from error
    if not parsed.is_finite() or parsed < 0 or parsed > 1:
        raise KalshiIngestionError(f"{field} must be between zero and one")
    return parsed


def _parse_levels(value: object, field: str) -> tuple[OrderbookLevel, ...]:
    if not isinstance(value, list):
        raise KalshiIngestionError(f"{field} must be a list")
    levels: list[OrderbookLevel] = []
    previous_price: Decimal | None = None
    for item in value:
        if not isinstance(item, list) or len(item) != 2:
            raise KalshiIngestionError(f"{field} levels must be two-string tuples")
        price = _required_decimal(item[0], f"{field}.price")
        count = _required_positive_decimal(item[1], f"{field}.count")
        if previous_price is not None and price <= previous_price:
            raise KalshiIngestionError(f"{field} prices must be strictly ascending")
        levels.append(OrderbookLevel(price=price, count=count))
        previous_price = price
    return tuple(levels)


def _required_positive_decimal(value: object, field: str) -> Decimal:
    if not isinstance(value, str):
        raise KalshiIngestionError(f"{field} must be a decimal string")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise KalshiIngestionError(f"{field} must be a decimal string") from error
    if not parsed.is_finite() or parsed <= 0:
        raise KalshiIngestionError(f"{field} must be positive")
    return parsed


def _complement_asks(bids: tuple[OrderbookLevel, ...]) -> tuple[OrderbookLevel, ...]:
    one = Decimal("1.00")
    return tuple(
        OrderbookLevel(price=one - level.price, count=level.count) for level in reversed(bids)
    )


def _levels_value(levels: tuple[OrderbookLevel, ...]) -> list[dict[str, Decimal]]:
    return [{"price": level.price, "count": level.count} for level in levels]


def _frozen_levels_value(
    levels: tuple[OrderbookLevel, ...],
) -> tuple[dict[str, Decimal], ...]:
    return tuple({"price": level.price, "count": level.count} for level in levels)
