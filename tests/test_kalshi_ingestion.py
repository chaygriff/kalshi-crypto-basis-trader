import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest

from kalshi_crypto_basis.kalshi_ingestion import (
    KalshiIngestionError,
    KalshiMarket,
    KalshiReadOnlyIngestor,
    OrderbookLevel,
    PriceRange,
    ReadOnlyResponse,
    ReviewedSeries,
    build_gap_report,
)
from kalshi_crypto_basis.snapshots import InMemorySnapshotStore


class FixtureTransport:
    def __init__(self, responses: list[ReadOnlyResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, path: str, parameters: dict[str, object]) -> ReadOnlyResponse:
        self.calls.append((path, parameters))
        return self.responses.pop(0)


class CredentialPresentTransport(FixtureTransport):
    def __init__(self, responses: list[ReadOnlyResponse]) -> None:
        super().__init__(responses)
        self.credential_present = True
        self.mutation_requests = 0


class MalformedTransport:
    def __init__(self, response: object) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, path: str, parameters: dict[str, object]) -> ReadOnlyResponse:
        self.calls.append((path, parameters))
        return self.response  # type: ignore[return-value]


class NonCallableGetTransport:
    get = 1


class FailingGetTransport:
    def get(self, path: str, parameters: dict[str, object]) -> ReadOnlyResponse:
        raise AttributeError("transport implementation failed")


class DomainFailingGetTransport:
    def get(self, path: str, parameters: dict[str, object]) -> ReadOnlyResponse:
        raise KalshiIngestionError("transport domain failure")


def _response(payload: dict[str, Any]) -> ReadOnlyResponse:
    return ReadOnlyResponse(
        body=json.dumps(payload, separators=(",", ":")).encode(),
        observed_at=datetime(2026, 8, 13, 19, tzinfo=UTC),
        received_at=datetime(2026, 8, 13, 19, 0, 1, tzinfo=UTC),
    )


def _live_detail_responses(
    *,
    series_updates: dict[str, Any] | None = None,
    event_updates: dict[str, Any] | None = None,
    market_updates: dict[str, Any] | None = None,
) -> list[ReadOnlyResponse]:
    series = {
        "ticker": "EXACT-REVIEWED",
        "category": "Crypto",
        "settlement_sources": [{"name": "CF Benchmarks", "url": "https://example.test"}],
        "contract_url": "https://example.test/contract",
        "contract_terms_url": "https://example.test/terms",
        "fee_type": "quadratic",
        "fee_multiplier": 1,
    }
    event = {
        "event_ticker": "EXACT-EVENT",
        "series_ticker": "EXACT-REVIEWED",
        "settlement_sources": [{"name": "CF Benchmarks", "url": "https://example.test"}],
        "fee_type_override": None,
        "fee_multiplier_override": None,
    }
    market = {
        "ticker": "EXACT-MARKET",
        "event_ticker": "EXACT-EVENT",
        "series_ticker": "EXACT-REVIEWED",
        "market_type": "binary",
        "status": "active",
        "strike_type": "greater",
        "floor_strike": 100000,
        "cap_strike": None,
        "rules_primary": "Resolves Yes above the stated level.",
        "rules_secondary": "Uses the documented settlement source.",
        "can_close_early": False,
        "close_time": "2026-08-14T16:00:00Z",
        "expected_expiration_time": "2026-08-14T16:00:00Z",
        "price_level_structure": "linear_cent",
        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
    }
    series.update(series_updates or {})
    event.update(event_updates or {})
    market.update(market_updates or {})
    return [
        _response({"series": series}),
        _response({"event": event}),
        _response({"market": market}),
    ]


def test_ingests_one_allowlisted_market_page_as_immutable_snapshot() -> None:
    transport = FixtureTransport(
        [
            _response(
                {
                    "markets": [
                        {
                            "ticker": "EXACT-NATIVE-TICKER",
                            "event_ticker": "EXACT-EVENT",
                            "series_ticker": "REVIEWED-BTC-SERIES",
                            "status": "open",
                            "market_type": "binary",
                            "yes_bid_dollars": "0.4100",
                            "no_bid_dollars": "0.5800",
                        }
                    ],
                    "cursor": "",
                }
            )
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="REVIEWED-BTC-SERIES", underlying="BTC"),),
    )

    batch = ingestor.discover_markets(series_ticker="REVIEWED-BTC-SERIES")

    assert transport.calls == [
        (
            "/trade-api/v2/markets",
            {"series_ticker": "REVIEWED-BTC-SERIES", "status": "open", "limit": 1000},
        )
    ]
    assert batch.complete is True
    assert len(batch.pages) == 1
    assert batch.pages[0].source == "kalshi"
    assert batch.pages[0].observed_at == datetime(2026, 8, 13, 19, tzinfo=UTC)
    assert batch.pages[0].ingested_at == datetime(2026, 8, 13, 19, 0, 1, tzinfo=UTC)
    assert batch.markets[0].ticker == "EXACT-NATIVE-TICKER"
    assert batch.markets[0].series_ticker == "REVIEWED-BTC-SERIES"
    assert batch.markets[0].underlying == "BTC"


def test_exhausts_opaque_cursor_with_frozen_filters() -> None:
    first = _response(
        {
            "markets": [
                {
                    "ticker": "FIRST",
                    "event_ticker": "EVENT-1",
                    "series_ticker": "REVIEWED-ETH-SERIES",
                    "status": "open",
                    "market_type": "binary",
                    "yes_bid_dollars": "0.4000",
                    "no_bid_dollars": "0.5900",
                }
            ],
            "cursor": "opaque+/=cursor",
        }
    )
    second = _response(
        {
            "markets": [
                {
                    "ticker": "SECOND",
                    "event_ticker": "EVENT-2",
                    "series_ticker": "REVIEWED-ETH-SERIES",
                    "status": "open",
                    "market_type": "binary",
                    "yes_bid_dollars": "0.5100",
                    "no_bid_dollars": "0.4800",
                }
            ],
            "cursor": "",
        }
    )
    transport = FixtureTransport([first, second])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="REVIEWED-ETH-SERIES", underlying="ETH"),),
    )

    batch = ingestor.discover_markets(series_ticker="REVIEWED-ETH-SERIES")

    assert transport.calls == [
        (
            "/trade-api/v2/markets",
            {"series_ticker": "REVIEWED-ETH-SERIES", "status": "open", "limit": 1000},
        ),
        (
            "/trade-api/v2/markets",
            {
                "series_ticker": "REVIEWED-ETH-SERIES",
                "status": "open",
                "limit": 1000,
                "cursor": "opaque+/=cursor",
            },
        ),
    ]
    assert [market.ticker for market in batch.markets] == ["FIRST", "SECOND"]
    assert len(batch.pages) == 2
    assert batch.pages[0].raw_sha256 != batch.pages[1].raw_sha256
    assert batch.complete is True


def test_repeated_cursor_fails_closed() -> None:
    repeated_page = {
        "markets": [],
        "cursor": "same-cursor",
    }
    transport = FixtureTransport([_response(repeated_page), _response(repeated_page)])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="REVIEWED-BTC-SERIES", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="cursor loop"):
        ingestor.discover_markets(series_ticker="REVIEWED-BTC-SERIES")


def test_conflicting_duplicate_ticker_across_pages_fails_closed() -> None:
    first_market = {
        "ticker": "DUPLICATE",
        "event_ticker": "EVENT",
        "series_ticker": "REVIEWED-BTC-SERIES",
        "status": "open",
        "market_type": "binary",
        "yes_bid_dollars": "0.4000",
        "no_bid_dollars": "0.5900",
    }
    conflicting_market = dict(first_market, yes_bid_dollars="0.4100")
    transport = FixtureTransport(
        [
            _response({"markets": [first_market], "cursor": "next"}),
            _response({"markets": [conflicting_market], "cursor": ""}),
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="REVIEWED-BTC-SERIES", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="conflicting duplicate ticker"):
        ingestor.discover_markets(series_ticker="REVIEWED-BTC-SERIES")


def test_page_budget_exhaustion_fails_before_an_unbounded_request() -> None:
    transport = FixtureTransport(
        [
            _response({"markets": [], "cursor": "cursor-1"}),
            _response({"markets": [], "cursor": "cursor-2"}),
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="REVIEWED-BTC-SERIES", underlying="BTC"),),
        max_pages=2,
    )

    with pytest.raises(KalshiIngestionError, match="page budget exhausted"):
        ingestor.discover_markets(series_ticker="REVIEWED-BTC-SERIES")
    assert len(transport.calls) == 2


def test_unreviewed_series_fails_before_transport_access() -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="not reviewed"):
        ingestor.discover_markets(series_ticker="BTC-LOOKS-PLAUSIBLE")
    assert transport.calls == []


@pytest.mark.parametrize(
    "bad_price",
    ["not-a-decimal", "NaN", "Infinity", "-0.01", "1.01"],
)
def test_invalid_fixed_point_quotes_fail_as_ingestion_errors(bad_price: str) -> None:
    transport = FixtureTransport(
        [
            _response(
                {
                    "markets": [
                        {
                            "ticker": "MARKET",
                            "event_ticker": "EVENT",
                            "series_ticker": "EXACT-REVIEWED",
                            "status": "open",
                            "market_type": "binary",
                            "yes_bid_dollars": bad_price,
                            "no_bid_dollars": "0.5000",
                        }
                    ],
                    "cursor": "",
                }
            )
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="yes_bid_dollars"):
        ingestor.discover_markets(series_ticker="EXACT-REVIEWED")


def test_ingests_full_binary_orderbook_and_derives_complementary_asks() -> None:
    transport = FixtureTransport(
        [
            _response(
                {
                    "orderbook_fp": {
                        "yes_dollars": [["0.4000", "2.00"], ["0.4500", "3.00"]],
                        "no_dollars": [["0.5200", "4.00"], ["0.5500", "5.00"]],
                    }
                }
            )
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    book = ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")

    assert transport.calls == [("/trade-api/v2/markets/EXACT-MARKET/orderbook", {"depth": 0})]
    assert [(level.price, level.count) for level in book.yes_bids] == [
        (Decimal("0.4000"), Decimal("2.00")),
        (Decimal("0.4500"), Decimal("3.00")),
    ]
    assert [(level.price, level.count) for level in book.yes_asks] == [
        (Decimal("0.4500"), Decimal("5.00")),
        (Decimal("0.4800"), Decimal("4.00")),
    ]
    assert [(level.price, level.count) for level in book.no_asks] == [
        (Decimal("0.5500"), Decimal("3.00")),
        (Decimal("0.6000"), Decimal("2.00")),
    ]
    assert book.snapshot.source == "kalshi"
    assert book.snapshot.parser_version == "kalshi-orderbook-v1"


def test_nonbinary_orderbook_rejects_before_transport_access() -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="market_type must be binary"):
        ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="scalar")
    assert transport.calls == []


class StringSubclass(str):
    pass


@pytest.mark.parametrize("market_type", [StringSubclass("binary"), 1, True, None, [], {}])
def test_orderbook_market_type_requires_exact_builtin_string(
    market_type: object,
) -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="market_type must be binary"):
        ingestor.get_orderbook(
            ticker="EXACT-MARKET",
            market_type=market_type,  # type: ignore[arg-type]
        )
    assert transport.calls == []


def test_empty_orderbook_sides_are_valid_absent_liquidity() -> None:
    transport = FixtureTransport(
        [_response({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}})]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    book = ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")

    assert book.yes_bids == ()
    assert book.no_bids == ()
    assert book.yes_asks == ()
    assert book.no_asks == ()


@pytest.mark.parametrize(
    ("levels", "message"),
    [
        ([["0.40"]], "two-string tuples"),
        ([["0.40", "0"]], "positive"),
        ([["0.40", "1.00"], ["0.40", "2.00"]], "strictly ascending"),
        ([["0.50", "1.00"], ["0.40", "2.00"]], "strictly ascending"),
    ],
)
def test_malformed_orderbook_levels_fail_closed(levels: list[list[str]], message: str) -> None:
    transport = FixtureTransport(
        [_response({"orderbook_fp": {"yes_dollars": levels, "no_dollars": []}})]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match=message):
        ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")


def test_crossed_binary_orderbook_fails_closed() -> None:
    transport = FixtureTransport(
        [
            _response(
                {
                    "orderbook_fp": {
                        "yes_dollars": [["0.6000", "1.00"]],
                        "no_dollars": [["0.4100", "1.00"]],
                    }
                }
            )
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="crossed"):
        ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")


def test_captures_exact_linked_live_series_event_and_market_details() -> None:
    transport = FixtureTransport(
        [
            _response(
                {
                    "series": {
                        "ticker": "EXACT-REVIEWED",
                        "category": "Crypto",
                        "settlement_sources": [
                            {"name": "CF Benchmarks", "url": "https://example.test"}
                        ],
                        "contract_url": "https://example.test/contract",
                        "contract_terms_url": "https://example.test/terms",
                        "fee_type": "quadratic",
                        "fee_multiplier": 1.0,
                    }
                }
            ),
            _response(
                {
                    "event": {
                        "event_ticker": "EXACT-EVENT",
                        "series_ticker": "EXACT-REVIEWED",
                        "settlement_sources": [
                            {"name": "CF Benchmarks", "url": "https://example.test"}
                        ],
                        "fee_type_override": None,
                        "fee_multiplier_override": None,
                    }
                }
            ),
            _response(
                {
                    "market": {
                        "ticker": "EXACT-MARKET",
                        "event_ticker": "EXACT-EVENT",
                        "series_ticker": "EXACT-REVIEWED",
                        "market_type": "binary",
                        "status": "active",
                        "strike_type": "greater",
                        "floor_strike": 100000,
                        "cap_strike": None,
                        "rules_primary": "Resolves Yes above the stated level.",
                        "rules_secondary": "Uses the documented settlement source.",
                        "can_close_early": False,
                        "close_time": "2026-08-14T16:00:00Z",
                        "expected_expiration_time": "2026-08-14T16:00:00Z",
                        "price_level_structure": "linear_cent",
                        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
                    }
                }
            ),
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    evidence = ingestor.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    )

    assert transport.calls == [
        ("/trade-api/v2/series/EXACT-REVIEWED", {}),
        ("/trade-api/v2/events/EXACT-EVENT", {"with_nested_markets": False}),
        ("/trade-api/v2/markets/EXACT-MARKET", {}),
    ]
    assert evidence.series.ticker == "EXACT-REVIEWED"
    assert evidence.event.series_ticker == evidence.series.ticker
    assert evidence.market.event_ticker == evidence.event.event_ticker
    assert evidence.market.underlying == "BTC"
    assert evidence.market.floor_strike == Decimal("100000")
    assert evidence.market.lifecycle_status == "active"
    assert evidence.market.price_ranges[0].step == Decimal("0.0100")
    assert len(evidence.snapshots) == 3
    assert {snapshot.parser_version for snapshot in evidence.snapshots} == {
        "kalshi-series-v1",
        "kalshi-event-v1",
        "kalshi-market-detail-v1",
    }


@pytest.mark.parametrize(
    ("series_updates", "event_updates", "market_updates", "message"),
    [
        ({"ticker": "OTHER"}, {}, {}, "series response identity mismatch"),
        ({"settlement_sources": []}, {}, {}, "settlement sources"),
        ({}, {"series_ticker": "OTHER"}, {}, "event response identity mismatch"),
        ({}, {"fee_type_override": "flat"}, {}, "jointly populated"),
        ({}, {}, {"event_ticker": "OTHER"}, "market response identity mismatch"),
        ({}, {}, {"status": "mystery"}, "lifecycle status"),
        ({}, {}, {"strike_type": "custom"}, "strike_type"),
        ({}, {}, {"cap_strike": 120000}, "greater strike"),
        ({}, {}, {"rules_primary": ""}, "rules_primary"),
        ({}, {}, {"close_time": "2026-08-14T12:00:00-04:00"}, "canonical UTC"),
    ],
)
def test_live_detail_identity_lifecycle_and_rules_fail_closed(
    series_updates: dict[str, Any],
    event_updates: dict[str, Any],
    market_updates: dict[str, Any],
    message: str,
) -> None:
    transport = FixtureTransport(
        _live_detail_responses(
            series_updates=series_updates,
            event_updates=event_updates,
            market_updates=market_updates,
        )
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match=message):
        ingestor.get_live_market_evidence(
            series_ticker="EXACT-REVIEWED",
            event_ticker="EXACT-EVENT",
            market_ticker="EXACT-MARKET",
        )


def test_pinned_cutoff_routes_pre_cutoff_settlement_to_historical_detail() -> None:
    transport = FixtureTransport(
        [
            _response(
                {
                    "market_settled_ts": "2026-08-01T00:00:00Z",
                    "trades_created_ts": "2026-08-01T00:00:00Z",
                }
            ),
            _response(
                {
                    "market": {
                        "ticker": "SETTLED-MARKET",
                        "event_ticker": "EXACT-EVENT",
                        "series_ticker": "EXACT-REVIEWED",
                        "market_type": "binary",
                        "status": "finalized",
                        "strike_type": "less_or_equal",
                        "floor_strike": None,
                        "cap_strike": 90000,
                        "rules_primary": "Resolves Yes at or below the stated level.",
                        "rules_secondary": "Uses the documented settlement source.",
                        "can_close_early": False,
                        "close_time": "2026-07-31T16:00:00Z",
                        "expected_expiration_time": "2026-07-31T16:00:00Z",
                        "price_level_structure": "linear_cent",
                        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
                    }
                }
            ),
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    routed = ingestor.get_routed_market_detail(
        ticker="SETTLED-MARKET",
        settlement_time=datetime(2026, 7, 31, 16, 0, tzinfo=UTC),
        underlying="BTC",
    )

    assert transport.calls == [
        ("/trade-api/v2/historical/cutoff", {}),
        ("/trade-api/v2/historical/markets/SETTLED-MARKET", {}),
    ]
    assert routed.source_tier == "historical"
    assert routed.cutoff.market_settled_at == datetime(2026, 8, 1, tzinfo=UTC)
    assert routed.market.ticker == "SETTLED-MARKET"
    assert [snapshot.parser_version for snapshot in routed.snapshots] == [
        "kalshi-historical-cutoff-v1",
        "kalshi-market-detail-v1",
    ]


def test_settlement_at_cutoff_routes_live() -> None:
    responses = [
        _response({"market_settled_ts": "2026-08-01T00:00:00Z"}),
        _live_detail_responses()[2],
    ]
    transport = FixtureTransport(responses)
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    routed = ingestor.get_routed_market_detail(
        ticker="EXACT-MARKET",
        settlement_time=datetime(2026, 8, 1, tzinfo=UTC),
        underlying="BTC",
    )

    assert routed.source_tier == "live"
    assert transport.calls[-1] == ("/trade-api/v2/markets/EXACT-MARKET", {})


def test_unknown_partition_rejects_before_any_transport_access() -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="PARTITION_UNKNOWN"):
        ingestor.get_routed_market_detail(
            ticker="PLAUSIBLE-DATED-TICKER",
            settlement_time=None,
            underlying="BTC",
        )
    assert transport.calls == []


class DatetimeSubclass(datetime):
    pass


@pytest.mark.parametrize(
    "settlement_time",
    [
        [],
        {},
        1,
        True,
        "2026-01-01T00:00:00Z",
        "",
        object(),
        DatetimeSubclass(2026, 1, 1, tzinfo=UTC),
    ],
)
def test_invalid_settlement_time_type_is_domain_error_before_transport(
    settlement_time: object,
) -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="settlement_time must be a datetime"):
        ingestor.get_routed_market_detail(
            ticker="EXACT-MARKET",
            settlement_time=settlement_time,  # type: ignore[arg-type]
            underlying="BTC",
        )
    assert transport.calls == []


def test_gap_report_is_stable_and_distinguishes_missing_from_empty_book() -> None:
    markets = tuple(
        KalshiMarket(
            ticker=ticker,
            event_ticker=f"EVENT-{ticker}",
            series_ticker="EXACT-REVIEWED",
            underlying="BTC",
            status="open",
            market_type="binary",
            yes_bid=Decimal("0.40"),
            no_bid=Decimal("0.59"),
        )
        for ticker in ("B-MISSING-BOOK", "A-MISSING-DETAIL", "C-EMPTY-BOOK")
    )
    transport = FixtureTransport(
        [_response({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}})]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    empty_book = ingestor.get_orderbook(ticker="C-EMPTY-BOOK", market_type="binary")

    report = build_gap_report(
        markets=markets,
        detail_tickers={"B-MISSING-BOOK", "C-EMPTY-BOOK"},
        orderbooks={"C-EMPTY-BOOK": empty_book},
    )

    assert [(gap.ticker, gap.code) for gap in report.gaps] == [
        ("A-MISSING-DETAIL", "DETAIL_MISSING"),
        ("B-MISSING-BOOK", "ORDERBOOK_MISSING"),
        ("C-EMPTY-BOOK", "NO_EXECUTABLE_LIQUIDITY"),
    ]
    assert report.complete is False


def test_orderbook_prices_must_match_returned_market_grid_exactly() -> None:
    transport = FixtureTransport(
        [
            *_live_detail_responses(),
            _response(
                {
                    "orderbook_fp": {
                        "yes_dollars": [["0.4050", "1.00"]],
                        "no_dollars": [],
                    }
                }
            ),
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    evidence = ingestor.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    )
    book = ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")

    with pytest.raises(KalshiIngestionError, match="price grid"):
        ingestor.validate_orderbook_for_market(
            orderbook_snapshot_id=book.snapshot.snapshot_id,
            market_snapshot_id=evidence.market.snapshot.snapshot_id,
        )


def test_credentials_present_still_produce_zero_mutation_requests() -> None:
    transport = CredentialPresentTransport(
        [
            _response({"markets": [], "cursor": ""}),
            *_live_detail_responses(),
            _response({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}),
            _response({"market_settled_ts": "2026-08-01T00:00:00Z"}),
            _live_detail_responses()[2],
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    ingestor.discover_markets(series_ticker="EXACT-REVIEWED")
    ingestor.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    )
    ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")
    ingestor.get_routed_market_detail(
        ticker="EXACT-MARKET",
        settlement_time=datetime(2026, 8, 1, tzinfo=UTC),
        underlying="BTC",
    )

    assert transport.credential_present is True
    assert transport.mutation_requests == 0
    assert not hasattr(ingestor, "post")
    assert not hasattr(ingestor, "submit_order")


@pytest.mark.parametrize(
    ("market_update", "message"),
    [
        ({"series_ticker": "OTHER-SERIES"}, "series identity"),
        ({"status": "closed"}, "discovery status"),
        ({"market_type": "scalar"}, "binary"),
    ],
)
def test_discovery_rejects_wrong_native_identity_or_scope(
    market_update: dict[str, object], message: str
) -> None:
    market: dict[str, object] = {
        "ticker": "EXACT-MARKET",
        "event_ticker": "EXACT-EVENT",
        "series_ticker": "EXACT-REVIEWED",
        "status": "open",
        "market_type": "binary",
        "yes_bid_dollars": "0.40",
        "no_bid_dollars": "0.59",
    }
    market.update(market_update)
    transport = FixtureTransport([_response({"markets": [market], "cursor": ""})])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match=message):
        ingestor.discover_markets(series_ticker="EXACT-REVIEWED")


@pytest.mark.parametrize(
    "unsafe_ticker",
    [
        "SAFE/../../historical/cutoff",
        "SAFE?depth=999",
        "SAFE#fragment",
        ".",
        "..",
    ],
)
def test_unsafe_native_identifier_rejects_before_transport_access(
    unsafe_ticker: str,
) -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="path segment"):
        ingestor.get_orderbook(ticker=unsafe_ticker, market_type="binary")
    assert transport.calls == []


def test_duplicate_json_object_keys_fail_closed() -> None:
    response = ReadOnlyResponse(
        body=(
            b'{"markets":[{"ticker":"EXACT-MARKET","ticker":"OTHER-MARKET",'
            b'"event_ticker":"EXACT-EVENT","series_ticker":"EXACT-REVIEWED",'
            b'"status":"open","market_type":"binary",'
            b'"yes_bid_dollars":"0.40","no_bid_dollars":"0.59"}],"cursor":""}'
        ),
        observed_at=datetime(2026, 8, 13, 19, tzinfo=UTC),
        received_at=datetime(2026, 8, 13, 19, 0, 1, tzinfo=UTC),
    )
    transport = FixtureTransport([response])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="duplicate JSON object key"):
        ingestor.discover_markets(series_ticker="EXACT-REVIEWED")


def test_unsafe_detail_identifiers_reject_before_transport_access() -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="path segment"):
        ingestor.get_live_market_evidence(
            series_ticker="EXACT-REVIEWED",
            event_ticker="EVENT?with_nested_markets=true",
            market_ticker="EXACT-MARKET",
        )
    with pytest.raises(KalshiIngestionError, match="path segment"):
        ingestor.get_routed_market_detail(
            ticker="MARKET/../OTHER",
            settlement_time=datetime(2026, 8, 1, tzinfo=UTC),
            underlying="BTC",
        )
    assert transport.calls == []


def test_unsafe_reviewed_series_identifier_rejects_at_configuration_boundary() -> None:
    transport = FixtureTransport([])

    with pytest.raises(KalshiIngestionError, match="path segment"):
        KalshiReadOnlyIngestor(
            transport=transport,
            reviewed_series=(ReviewedSeries(ticker="BTC/OTHER", underlying="BTC"),),
        )
    assert transport.calls == []


@pytest.mark.parametrize("series_ticker", [[], {}, 1, True, None])
@pytest.mark.parametrize("entry_point", ["discover", "live_detail"])
def test_invalid_series_input_is_domain_error_before_lookup_or_transport(
    series_ticker: object, entry_point: str
) -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="series_ticker must be a non-empty string"):
        if entry_point == "discover":
            ingestor.discover_markets(series_ticker=series_ticker)  # type: ignore[arg-type]
        else:
            ingestor.get_live_market_evidence(
                series_ticker=series_ticker,  # type: ignore[arg-type]
                event_ticker="EXACT-EVENT",
                market_ticker="EXACT-MARKET",
            )
    assert transport.calls == []


@pytest.mark.parametrize(
    "unsafe_ticker",
    ["A\\B", "A%2FB", " A", "A ", "A\nB", "A\x00B", "A\u2028B", "A\u2029B"],
)
def test_native_identifier_requires_strict_ascii_grammar_before_transport(
    unsafe_ticker: str,
) -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="path segment"):
        ingestor.get_orderbook(ticker=unsafe_ticker, market_type="binary")
    assert transport.calls == []


@pytest.mark.parametrize(
    "entry_point",
    ["discover", "orderbook", "live_series", "live_event", "live_market", "routed"],
)
def test_public_identity_arguments_reject_string_subclasses_before_transport(
    entry_point: str,
) -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="SAFE", underlying="BTC"),),
    )
    string_subclass_value = StringSubclass("SAFE")

    with pytest.raises(KalshiIngestionError, match="non-empty string"):
        if entry_point == "discover":
            ingestor.discover_markets(series_ticker=string_subclass_value)
        elif entry_point == "orderbook":
            ingestor.get_orderbook(ticker=string_subclass_value, market_type="binary")
        elif entry_point == "live_series":
            ingestor.get_live_market_evidence(
                series_ticker=string_subclass_value, event_ticker="SAFE", market_ticker="SAFE"
            )
        elif entry_point == "live_event":
            ingestor.get_live_market_evidence(
                series_ticker="SAFE", event_ticker=string_subclass_value, market_ticker="SAFE"
            )
        elif entry_point == "live_market":
            ingestor.get_live_market_evidence(
                series_ticker="SAFE", event_ticker="SAFE", market_ticker=string_subclass_value
            )
        else:
            ingestor.get_routed_market_detail(
                ticker=string_subclass_value,
                settlement_time=datetime(2026, 1, 1, tzinfo=UTC),
                underlying="BTC",
            )
    assert transport.calls == []


@pytest.mark.parametrize("member", [object(), 1, True, None, [], {}])
def test_reviewed_series_collection_rejects_malformed_members(member: object) -> None:
    with pytest.raises(KalshiIngestionError, match="ReviewedSeries"):
        KalshiReadOnlyIngestor(
            transport=FixtureTransport([]),
            reviewed_series=(member,),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("include_ticker", [False, True])
def test_incomplete_reviewed_series_is_domain_error(include_ticker: bool) -> None:
    incomplete = object.__new__(ReviewedSeries)
    if include_ticker:
        object.__setattr__(incomplete, "ticker", "SAFE")

    with pytest.raises(KalshiIngestionError, match="reviewed series"):
        KalshiReadOnlyIngestor(
            transport=FixtureTransport([]),
            reviewed_series=(incomplete,),
        )


@pytest.mark.parametrize(
    "reviewed_series",
    [[], [ReviewedSeries("SAFE", "BTC")], {}, 1, None],
)
def test_reviewed_series_collection_requires_exact_tuple(
    reviewed_series: object,
) -> None:
    with pytest.raises(KalshiIngestionError, match="reviewed_series must be a tuple"):
        KalshiReadOnlyIngestor(
            transport=FixtureTransport([]),
            reviewed_series=reviewed_series,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("snapshot_store", [object(), 1, True, [], {}, "store"])
def test_snapshot_store_requires_exact_supported_type(snapshot_store: object) -> None:
    with pytest.raises(KalshiIngestionError, match="snapshot_store"):
        KalshiReadOnlyIngestor(
            transport=FixtureTransport([]),
            reviewed_series=(ReviewedSeries("SAFE", "BTC"),),
            snapshot_store=snapshot_store,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("body", [[], {}, "{}", bytearray(b"{}"), memoryview(b"{}")])
def test_read_only_response_requires_exact_bytes(body: object) -> None:
    with pytest.raises(KalshiIngestionError, match="body must be bytes"):
        ReadOnlyResponse(
            body=body,  # type: ignore[arg-type]
            observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            received_at=datetime(2026, 1, 1, tzinfo=UTC),
        )


@pytest.mark.parametrize("field", ["observed_at", "received_at"])
def test_read_only_response_rejects_datetime_subclasses(field: str) -> None:
    values = {
        "body": b"{}",
        "observed_at": datetime(2026, 1, 1, tzinfo=UTC),
        "received_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    values[field] = DatetimeSubclass(2026, 1, 1, tzinfo=UTC)

    with pytest.raises(KalshiIngestionError, match=f"{field} must be a datetime"):
        ReadOnlyResponse(**values)  # type: ignore[arg-type]


def test_read_only_response_rejects_received_before_observed() -> None:
    with pytest.raises(KalshiIngestionError, match="received_at cannot precede observed_at"):
        ReadOnlyResponse(
            body=b"{}",
            observed_at=datetime(2026, 1, 2, tzinfo=UTC),
            received_at=datetime(2026, 1, 1, tzinfo=UTC),
        )


@pytest.mark.parametrize("workflow", ["discover", "orderbook", "live_detail", "routed"])
@pytest.mark.parametrize("response", [object(), 1, None, [], {}, "response"])
def test_malformed_transport_response_is_domain_error(workflow: str, response: object) -> None:
    transport = MalformedTransport(response)
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries("SAFE", "BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="transport must return ReadOnlyResponse"):
        if workflow == "discover":
            ingestor.discover_markets(series_ticker="SAFE")
        elif workflow == "orderbook":
            ingestor.get_orderbook(ticker="SAFE", market_type="binary")
        elif workflow == "live_detail":
            ingestor.get_live_market_evidence(
                series_ticker="SAFE", event_ticker="SAFE", market_ticker="SAFE"
            )
        else:
            ingestor.get_routed_market_detail(
                ticker="SAFE",
                settlement_time=datetime(2026, 1, 1, tzinfo=UTC),
                underlying="BTC",
            )
    assert len(transport.calls) == 1


class BytesSubclass(bytes):
    pass


@pytest.mark.parametrize("body", [[], BytesSubclass(b'{"markets":[],"cursor":""}')])
def test_unvalidated_response_is_revalidated_at_consumption(body: object) -> None:
    unvalidated_response = object.__new__(ReadOnlyResponse)
    object.__setattr__(unvalidated_response, "body", body)
    object.__setattr__(unvalidated_response, "observed_at", datetime(2026, 1, 1, tzinfo=UTC))
    object.__setattr__(unvalidated_response, "received_at", datetime(2026, 1, 1, tzinfo=UTC))
    ingestor = KalshiReadOnlyIngestor(
        transport=MalformedTransport(unvalidated_response),
        reviewed_series=(ReviewedSeries("SAFE", "BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="response body must be bytes"):
        ingestor.discover_markets(series_ticker="SAFE")


def test_unvalidated_response_clock_order_is_revalidated_at_consumption() -> None:
    unvalidated_response = object.__new__(ReadOnlyResponse)
    object.__setattr__(unvalidated_response, "body", b'{"markets":[],"cursor":""}')
    object.__setattr__(unvalidated_response, "observed_at", datetime(2026, 1, 2, tzinfo=UTC))
    object.__setattr__(unvalidated_response, "received_at", datetime(2026, 1, 1, tzinfo=UTC))
    ingestor = KalshiReadOnlyIngestor(
        transport=MalformedTransport(unvalidated_response),
        reviewed_series=(ReviewedSeries("SAFE", "BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="received_at cannot precede observed_at"):
        ingestor.discover_markets(series_ticker="SAFE")


@pytest.mark.parametrize(
    "transport",
    [object(), None, 1, [], {}, "transport", NonCallableGetTransport(), FailingGetTransport()],
)
def test_invalid_transport_get_capability_is_domain_error(transport: object) -> None:
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,  # type: ignore[arg-type]
        reviewed_series=(ReviewedSeries("SAFE", "BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="transport"):
        ingestor.discover_markets(series_ticker="SAFE")


def test_explicit_ingestion_error_from_transport_is_preserved() -> None:
    ingestor = KalshiReadOnlyIngestor(
        transport=DomainFailingGetTransport(),
        reviewed_series=(ReviewedSeries("SAFE", "BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match=r"^transport domain failure$"):
        ingestor.discover_markets(series_ticker="SAFE")


@pytest.mark.parametrize(
    "snapshot_id", [StringSubclass("sha256:" + "0" * 64), 1, True, None, [], {}]
)
@pytest.mark.parametrize("field", ["orderbook", "market"])
def test_validator_snapshot_ids_require_exact_builtin_strings(
    snapshot_id: object, field: str
) -> None:
    ingestor = KalshiReadOnlyIngestor(
        transport=FixtureTransport([]),
        reviewed_series=(ReviewedSeries("SAFE", "BTC"),),
    )
    values: dict[str, object] = {
        "orderbook_snapshot_id": "sha256:" + "1" * 64,
        "market_snapshot_id": "sha256:" + "2" * 64,
    }
    values[f"{field}_snapshot_id"] = snapshot_id

    with pytest.raises(KalshiIngestionError, match="snapshot_id must be a non-empty string"):
        ingestor.validate_orderbook_for_market(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize("constant", [b"NaN", b"Infinity", b"-Infinity"])
def test_nonstandard_json_constants_fail_globally(constant: bytes) -> None:
    transport = FixtureTransport(
        [
            ReadOnlyResponse(
                body=(b'{"markets":[],"cursor":"","unused":{"extra":' + constant + b"}}"),
                observed_at=datetime(2026, 8, 13, 19, tzinfo=UTC),
                received_at=datetime(2026, 8, 13, 19, 0, 1, tzinfo=UTC),
            )
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="non-standard JSON constant"):
        ingestor.discover_markets(series_ticker="EXACT-REVIEWED")


@pytest.mark.parametrize(
    "price_ranges",
    [
        [{"start": "0", "end": "0.95", "step": "0.1"}],
        [
            {"start": "0", "end": "0.4", "step": "0.1"},
            {"start": "0.6", "end": "1", "step": "0.1"},
        ],
        [{"start": "0e0", "end": "1e0", "step": "1e-2"}],
        [{"start": "0.1", "end": "1", "step": "0.1"}],
        [{"start": "0", "end": "0.9", "step": "0.1"}],
    ],
)
def test_malformed_price_grid_fails_closed(
    price_ranges: list[dict[str, str]],
) -> None:
    transport = FixtureTransport(
        _live_detail_responses(market_updates={"price_ranges": price_ranges})
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="price_ranges"):
        ingestor.get_live_market_evidence(
            series_ticker="EXACT-REVIEWED",
            event_ticker="EXACT-EVENT",
            market_ticker="EXACT-MARKET",
        )


@pytest.mark.parametrize("underlying", [True, 1, "DOGE"])
def test_reviewed_underlying_is_runtime_validated(underlying: object) -> None:
    transport = FixtureTransport([])

    with pytest.raises(KalshiIngestionError, match="underlying"):
        KalshiReadOnlyIngestor(
            transport=transport,
            reviewed_series=(ReviewedSeries(ticker="SAFE", underlying=underlying),),  # type: ignore[arg-type]
        )
    assert transport.calls == []


@pytest.mark.parametrize("ticker", [1, True, object(), None, [], {}, StringSubclass("SAFE")])
def test_reviewed_series_ticker_requires_exact_builtin_string(ticker: object) -> None:
    with pytest.raises(KalshiIngestionError, match="reviewed series ticker"):
        ReviewedSeries(ticker=ticker, underlying="BTC")  # type: ignore[arg-type]


@pytest.mark.parametrize("underlying", [StringSubclass("BTC"), None, [], {}, object()])
def test_reviewed_series_underlying_requires_exact_builtin_string(
    underlying: object,
) -> None:
    with pytest.raises(KalshiIngestionError, match="reviewed series underlying"):
        ReviewedSeries(ticker="SAFE", underlying=underlying)  # type: ignore[arg-type]


def test_orderbook_validator_rejects_fields_altered_after_snapshot() -> None:
    ingestor = KalshiReadOnlyIngestor(
        transport=FixtureTransport(
            [
                *_live_detail_responses(),
                _response({"orderbook_fp": {"yes_dollars": [["0.40", "1"]], "no_dollars": []}}),
            ]
        ),
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    evidence = ingestor.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    )
    book = ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")
    altered_book = replace(
        book,
        yes_bids=(OrderbookLevel(price=Decimal("0.41"), count=Decimal("999")),),
    )

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, ingestor.validate_orderbook_for_market)(book=altered_book, market=evidence.market)


def test_routed_underlying_must_match_returned_reviewed_series() -> None:
    transport = FixtureTransport(
        [
            _response({"market_settled_ts": "2026-08-01T00:00:00Z"}),
            _live_detail_responses()[2],
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(
            ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),
            ReviewedSeries(ticker="OTHER-ETH-SERIES", underlying="ETH"),
        ),
    )

    with pytest.raises(KalshiIngestionError, match="underlying identity mismatch"):
        ingestor.get_routed_market_detail(
            ticker="EXACT-MARKET",
            settlement_time=datetime(2026, 8, 1, tzinfo=UTC),
            underlying="ETH",
        )


@pytest.mark.parametrize("altered_field", ["ticker", "yes_asks"])
def test_orderbook_validator_rejects_all_provenance_alterations(
    altered_field: str,
) -> None:
    transport = FixtureTransport(
        [
            *_live_detail_responses(),
            _response({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}),
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    evidence = ingestor.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    )
    book = ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")
    if altered_field == "ticker":
        altered_book = replace(book, ticker="OTHER-MARKET")
    else:
        altered_book = replace(
            book,
            yes_asks=(OrderbookLevel(price=Decimal("0.50"), count=Decimal("1")),),
        )

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, ingestor.validate_orderbook_for_market)(book=altered_book, market=evidence.market)


def test_routed_market_rejects_unreviewed_returned_series() -> None:
    market_response = _live_detail_responses(market_updates={"series_ticker": "UNREVIEWED-SERIES"})[
        2
    ]
    transport = FixtureTransport(
        [
            _response({"market_settled_ts": "2026-08-01T00:00:00Z"}),
            market_response,
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="series is not reviewed"):
        ingestor.get_routed_market_detail(
            ticker="EXACT-MARKET",
            settlement_time=datetime(2026, 8, 1, tzinfo=UTC),
            underlying="BTC",
        )


def test_orderbook_validator_reconstructs_complete_snapshot_against_raw_evidence() -> None:
    transport = FixtureTransport(
        [
            *_live_detail_responses(),
            _response({"orderbook_fp": {"yes_dollars": [["0.40", "1"]], "no_dollars": []}}),
        ]
    )
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    evidence = ingestor.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    )
    book = ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")
    unvalidated_snapshot = object.__new__(type(book.snapshot))
    for field in (
        "source",
        "request_fingerprint",
        "observed_at",
        "ingested_at",
        "parser_version",
        "raw_sha256",
        "normalized",
        "snapshot_id",
        "idempotency_key",
    ):
        object.__setattr__(unvalidated_snapshot, field, getattr(book.snapshot, field))
    object.__setattr__(unvalidated_snapshot, "snapshot_id", "sha256:" + "0" * 64)
    altered_book = replace(book, snapshot=unvalidated_snapshot)

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, ingestor.validate_orderbook_for_market)(book=altered_book, market=evidence.market)


def test_market_grid_cannot_be_replaced_away_from_detail_snapshot() -> None:
    ingestor = KalshiReadOnlyIngestor(
        transport=FixtureTransport(
            [
                *_live_detail_responses(),
                _response({"orderbook_fp": {"yes_dollars": [["0.405", "1"]], "no_dollars": []}}),
            ]
        ),
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    evidence = ingestor.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    )
    book = ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")
    altered_market = replace(
        evidence.market,
        price_ranges=(PriceRange(Decimal("0"), Decimal("1"), Decimal("0.005")),),
    )

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, ingestor.validate_orderbook_for_market)(book=book, market=altered_market)


def test_unhashable_routed_underlying_fails_as_domain_error_before_get() -> None:
    transport = FixtureTransport([])
    ingestor = KalshiReadOnlyIngestor(
        transport=transport,
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )

    with pytest.raises(KalshiIngestionError, match="underlying"):
        ingestor.get_routed_market_detail(
            ticker="EXACT-MARKET",
            settlement_time=datetime(2026, 8, 1, tzinfo=UTC),
            underlying=[],  # type: ignore[arg-type]
        )
    assert transport.calls == []


def test_coherent_raw_and_snapshot_substitution_is_not_persisted_evidence() -> None:
    ingestor = KalshiReadOnlyIngestor(
        transport=FixtureTransport(
            [
                *_live_detail_responses(),
                _response({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}),
            ]
        ),
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    market = ingestor.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    ).market
    book = ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")
    substituted_raw = b'{ "orderbook_fp": { "yes_dollars": [], "no_dollars": [] } }'
    substituted_snapshot = type(book.snapshot).create(
        source="kalshi",
        request_fingerprint=book.snapshot.request_fingerprint,
        observed_at=book.snapshot.observed_at,
        ingested_at=book.snapshot.ingested_at,
        parser_version=book.snapshot.parser_version,
        raw_payload=substituted_raw,
        normalized=book.snapshot.normalized,
    )

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, ingestor.validate_orderbook_for_market)(
            book=replace(book, raw_payload=substituted_raw, snapshot=substituted_snapshot),
            market=market,
        )


def test_market_underlying_and_coherent_evidence_substitution_reject() -> None:
    ingestor = KalshiReadOnlyIngestor(
        transport=FixtureTransport(
            [
                *_live_detail_responses(),
                _response({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}),
            ]
        ),
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    market = ingestor.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    ).market
    book = ingestor.get_orderbook(ticker="EXACT-MARKET", market_type="binary")

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, ingestor.validate_orderbook_for_market)(
            book=book, market=replace(market, underlying="ETH")
        )

    substituted_raw = market.raw_payload.replace(b'"market":', b' "market" : ')
    substituted_snapshot = type(market.snapshot).create(
        source="kalshi",
        request_fingerprint=market.snapshot.request_fingerprint,
        observed_at=market.snapshot.observed_at,
        ingested_at=market.snapshot.ingested_at,
        parser_version=market.snapshot.parser_version,
        raw_payload=substituted_raw,
        normalized=market.snapshot.normalized,
    )
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, ingestor.validate_orderbook_for_market)(
            book=book,
            market=replace(
                market,
                raw_payload=substituted_raw,
                snapshot=substituted_snapshot,
            ),
        )


def test_evidence_from_another_store_is_not_persisted() -> None:
    first = KalshiReadOnlyIngestor(
        transport=FixtureTransport(_live_detail_responses()),
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    market = first.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    ).market
    second = KalshiReadOnlyIngestor(
        transport=FixtureTransport(
            [_response({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}})]
        ),
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
    )
    book = second.get_orderbook(ticker="EXACT-MARKET", market_type="binary")

    with pytest.raises(KalshiIngestionError, match="persisted evidence"):
        first.validate_orderbook_for_market(
            orderbook_snapshot_id=book.snapshot.snapshot_id,
            market_snapshot_id=market.snapshot.snapshot_id,
        )


def test_fresh_ingestor_can_validate_records_from_same_trusted_store() -> None:
    store = InMemorySnapshotStore()
    capture = KalshiReadOnlyIngestor(
        transport=FixtureTransport(
            [
                *_live_detail_responses(),
                _response({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}),
            ]
        ),
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
        snapshot_store=store,
    )
    market = capture.get_live_market_evidence(
        series_ticker="EXACT-REVIEWED",
        event_ticker="EXACT-EVENT",
        market_ticker="EXACT-MARKET",
    ).market
    book = capture.get_orderbook(ticker="EXACT-MARKET", market_type="binary")
    replay = KalshiReadOnlyIngestor(
        transport=FixtureTransport([]),
        reviewed_series=(ReviewedSeries(ticker="EXACT-REVIEWED", underlying="BTC"),),
        snapshot_store=store,
    )

    validated = replay.validate_orderbook_for_market(
        orderbook_snapshot_id=book.snapshot.snapshot_id,
        market_snapshot_id=market.snapshot.snapshot_id,
    )

    assert validated.book == book
    assert validated.market == market
