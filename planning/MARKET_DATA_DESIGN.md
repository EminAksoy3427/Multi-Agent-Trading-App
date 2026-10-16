# Market Data Backend — Detailed Design (v2)

Implementation-ready design for the FinAlly market data subsystem: the data model, price cache, source interface, GBM simulator, Massive (Polygon.io) poller, SSE stream, ticker tracking and FastAPI wiring. Every section includes the code to write.

**Scope:** `backend/app/market/` and how the rest of the backend uses it.
**Inputs:** `planning/PLAN.md` §6, `planning/MARKET_DATA_SUMMARY.md`, and the archived docs in `planning/archive/` (`MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md`, `MASSIVE_API.md`, `MARKET_DATA_DESIGN.md`, `MARKET_DATA_REVIEW.md`).

This document replaces `planning/archive/MARKET_DATA_DESIGN.md`. It keeps the v1 architecture, which is built and has 73 passing tests, and adds what v1 lacks. v1 has one real bug (§0.1), the watchlist's "daily change %" column has no data source, and nothing yet keeps the tracked tickers in step with the watchlist and positions.

---

## Table of Contents

0. [Status & What Changes in v2](#0-status--what-changes-in-v2)
1. [Architecture](#1-architecture)
2. [File Structure](#2-file-structure)
3. [Ticker Symbols — `tickers.py` (new)](#3-ticker-symbols--tickerspy-new)
4. [Data Model — `models.py`](#4-data-model--modelspy)
5. [Price Cache — `cache.py`](#5-price-cache--cachepy)
6. [Source Interface — `interface.py`](#6-source-interface--interfacepy)
7. [Seed Data — `seed_prices.py`](#7-seed-data--seed_pricespy)
8. [GBM Simulator — `simulator.py`](#8-gbm-simulator--simulatorpy)
9. [Massive Poller — `massive_client.py`](#9-massive-poller--massive_clientpy)
10. [Factory & Configuration — `factory.py`](#10-factory--configuration--factorypy)
11. [SSE Stream — `stream.py`](#11-sse-stream--streampy)
12. [Ticker Tracking — `tracking.py` (new)](#12-ticker-tracking--trackingpy-new)
13. [FastAPI Integration](#13-fastapi-integration)
14. [Frontend Contract](#14-frontend-contract)
15. [Testing](#15-testing)
16. [Error Handling & Edge Cases](#16-error-handling--edge-cases)
17. [Implementation Checklist](#17-implementation-checklist)

---

## 0. Status & What Changes in v2

### 0.1 Bug: the Massive poller drops every real snapshot

`massive_client.py` reads `snap.last_trade.timestamp`, but the `massive` package's `LastTrade` model has **no `timestamp` attribute**. The trade time is `sip_timestamp`, in **nanoseconds**. Each snapshot raises `AttributeError`, the per-snapshot `except` logs "Skipping snapshot", and the cache never gets a price.

Reproduced with the installed package:

```python
from massive.rest.models import TickerSnapshot
snap = TickerSnapshot.from_dict({"ticker": "AAPL", "lastTrade": {"p": 190.5, "t": 1707580800123456789}})
hasattr(snap.last_trade, "timestamp")   # False
snap.last_trade.sip_timestamp           # 1707580800123456789  (ns)
# MassiveDataSource._poll_once() → "Skipping snapshot for AAPL: 'LastTrade' object has no attribute 'timestamp'"
```

The unit tests miss this because they build snapshots from `MagicMock`, which answers any attribute name. v2 parses snapshots in one pure function (§9.2) and tests it against real `TickerSnapshot.from_dict(...)` objects (§15.3).

`archive/MASSIVE_API.md` has the same kind of mistake: it names `day.previous_close` and `day.change_percent`. Those fields don't exist on the stocks snapshot. The real ones are `prev_day.close`, `todays_change` and `todays_change_percent`.

### 0.2 Change list

| # | Area | v1 behaviour | v2 change |
|---|------|--------------|-----------|
| 1 | Massive parsing | Reads a nonexistent `last_trade.timestamp`, so no prices arrive | `parse_snapshot()` reads `sip_timestamp` (ns), falls back across price fields, and extracts the previous close |
| 2 | Daily change % | `change_percent` is tick-to-tick only. PLAN §10 wants "daily change %" | `PriceUpdate.reference_price` plus `day_change` and `day_change_percent` |
| 3 | Ghost tickers | A Massive poll that is in flight when a ticker is removed writes it back into the cache | Results are filtered against the active ticker set before writing |
| 4 | Removal visibility | `cache.remove()` doesn't bump `version`, so SSE clients keep showing the removed ticker | `remove()` bumps `version`, and SSE sends empty snapshots too |
| 5 | Router factory | A module-level `router` registers `/prices` again on every `create_stream_router()` call | The router is created inside the factory |
| 6 | `version` read | Read without the lock | Read under the lock (safe on free-threaded Python) |
| 7 | `timestamp or time.time()` | A timestamp of `0` is treated as missing | `time.time() if timestamp is None else timestamp` |
| 8 | Ticker casing | Massive upper-cases tickers. The simulator doesn't | One shared `normalize_ticker()` |
| 9 | Massive add latency | A new ticker waits up to 15 s for a price | `add_ticker()` wakes the poller early, within the rate limit |
| 10 | Massive failures | Retries at a fixed interval forever, including on 401/429 | Exponential backoff, capped |
| 11 | Simulator pre-start add | `add_ticker()` before `start()` is silently dropped | Queued and picked up by `start()` |
| 12 | What gets tracked | Nothing defined | Tracked set = watchlist ∪ open positions, applied by `sync_tracked_tickers()` |
| 13 | SSE keep-alive | Nothing is sent when prices don't change (Massive off-hours) | `: keep-alive` comment every 15 s |
| 14 | Poll interval | Hard-coded at 15 s | `MASSIVE_POLL_INTERVAL` env var |

Everything else in v1 stays: the strategy pattern, the cache as the single source of truth, the GBM math, Cholesky correlation, shock events, the SSE wire shape, and the public imports from `app.market`.

---

## 1. Architecture

```
                 ┌───────────────────────── app.state ─────────────────────────┐
                 │                                                              │
  MASSIVE_API_KEY│   create_market_data_source(cache)                           │
  ───────────────┼──►  ├─ SimulatorDataSource  (GBM, 500 ms ticks)  [default]   │
                 │     └─ MassiveDataSource    (REST snapshot poll, 15 s)       │
                 │                 │ writes                                     │
                 │                 ▼                                            │
                 │          PriceCache  (latest PriceUpdate per ticker,         │
                 │                       version counter, thread-safe)          │
                 │        ┌────────┼──────────────┬───────────────┐             │
                 │        ▼        ▼              ▼               ▼             │
                 │   SSE stream  /api/watchlist  /api/portfolio  /api/chat      │
                 │   (500 ms)    (price join)    (valuation,     (LLM context,  │
                 │                               trade fill)     trade fill)    │
                 │                                                              │
                 │   sync_tracked_tickers(source, watchlist ∪ positions)        │
                 │      ▲ called after every watchlist change or trade          │
                 └──────┼───────────────────────────────────────────────────────┘
                        │
                 SQLite (watchlist, positions)
```

Rules:

1. **Producers write; everyone else reads.** Exactly one `MarketDataSource` writes to the `PriceCache`. Routes never call the source for a price.
2. **The cache holds only tracked tickers.** The source decides what's tracked, and removing a ticker from the source removes it from the cache.
3. **Tracked = watchlist ∪ open positions.** A held ticker keeps getting priced after it leaves the watchlist, so valuation and sells keep working.
4. **One shape leaves the subsystem:** `PriceUpdate`, serialized with `to_dict()`.

---

## 2. File Structure

```
backend/app/market/
├── __init__.py          # Public API re-exports
├── tickers.py           # NEW  normalize_ticker(), is_valid_ticker()
├── models.py            # PriceUpdate (+ reference_price, day_change*)
├── cache.py             # PriceCache
├── interface.py         # MarketDataSource ABC
├── seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants (unchanged)
├── simulator.py         # GBMSimulator + SimulatorDataSource
├── massive_client.py    # parse_snapshot() + MassiveDataSource
├── factory.py           # create_market_data_source() + env config
├── stream.py            # create_stream_router() — SSE
└── tracking.py          # NEW  sync_tracked_tickers()

backend/tests/market/
├── test_tickers.py          # NEW
├── test_models.py
├── test_cache.py
├── test_simulator.py
├── test_simulator_source.py
├── test_massive.py          # + real TickerSnapshot parsing tests
├── test_factory.py
├── test_stream.py           # NEW
└── test_tracking.py         # NEW
```

`__init__.py`:

```python
"""Market data subsystem for FinAlly."""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router
from .tickers import InvalidTickerError, is_valid_ticker, normalize_ticker
from .tracking import sync_tracked_tickers

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
    "normalize_ticker",
    "is_valid_ticker",
    "InvalidTickerError",
    "sync_tracked_tickers",
]
```

---

## 3. Ticker Symbols — `tickers.py` (new)

Tickers come from the trade bar, the watchlist input, and LLM output. All three go through one normalizer, so `"aapl "`, `"AAPL"` and `"Aapl"` refer to the same cache key, watchlist row and position.

```python
"""Ticker symbol normalization and validation."""

from __future__ import annotations

import re

# US equity symbols: 1-10 chars, letters first; allow digits, '.' and '-' (BRK.B, BF-B).
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


class InvalidTickerError(ValueError):
    """Raised when a string cannot be a ticker symbol."""


def normalize_ticker(raw: str) -> str:
    """Upper-case and strip a ticker. Raises InvalidTickerError if malformed."""
    ticker = (raw or "").strip().upper()
    if not _TICKER_RE.fullmatch(ticker):
        raise InvalidTickerError(f"Invalid ticker symbol: {raw!r}")
    return ticker


def is_valid_ticker(raw: str) -> bool:
    try:
        normalize_ticker(raw)
    except InvalidTickerError:
        return False
    return True
```

Routes call `normalize_ticker()` at the edge and turn `InvalidTickerError` into HTTP 400. Data sources also call it, so a direct caller can't add a lower-case duplicate.

---

## 4. Data Model — `models.py`

`PriceUpdate` gains **`reference_price`**, the price that "daily change" is measured from:

- **Massive:** the previous session's close (`prev_day.close`).
- **Simulator:** the price when the ticker started being simulated (its seed price). The simulator has no trading days, so "since session start" is the honest equivalent.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float                      # price at the previous update (tick-to-tick)
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    reference_price: float | None = None       # previous close / session open (daily change)

    # --- tick-to-tick (drives the green/red flash) ---

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' relative to the previous tick."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    # --- daily (drives the watchlist "Chg %" column) ---

    @property
    def day_change(self) -> float | None:
        if self.reference_price is None:
            return None
        return round(self.price - self.reference_price, 4)

    @property
    def day_change_percent(self) -> float | None:
        if not self.reference_price:  # None or 0
            return None
        return round((self.price - self.reference_price) / self.reference_price * 100, 4)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "reference_price": self.reference_price,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
        }
```

The new field comes last and has a default, so existing positional and keyword construction keeps working.

Example `to_dict()` output:

```json
{
  "ticker": "AAPL",
  "price": 191.37,
  "previous_price": 191.33,
  "timestamp": 1760600000.512,
  "change": 0.04,
  "change_percent": 0.0209,
  "direction": "up",
  "reference_price": 190.0,
  "day_change": 1.37,
  "day_change_percent": 0.7211
}
```

---

## 5. Price Cache — `cache.py`

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Latest PriceUpdate per ticker plus a monotonically increasing version.

    Writers: exactly one MarketDataSource.
    Readers: SSE stream, watchlist/portfolio routes, trade execution, LLM context.

    A threading.Lock (not asyncio.Lock) is used because MassiveDataSource
    does network I/O via asyncio.to_thread; every critical section is tiny.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,
    ) -> PriceUpdate:
        """Record a new price and return the resulting PriceUpdate.

        - previous_price: last cached price, or `price` on first sight (direction 'flat').
        - reference_price: explicit value if given, else the previously cached
          reference, else `price` (first sight ⇒ daily change starts at 0).
        """
        with self._lock:
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if reference_price is None:
                reference_price = prev.reference_price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=time.time() if timestamp is None else timestamp,
                reference_price=None if reference_price is None else round(reference_price, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy; PriceUpdate is immutable so this is a safe snapshot."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        """Forget a ticker. Bumps version so SSE clients see the removal."""
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: object) -> bool:
        with self._lock:
            return ticker in self._prices
```

Why a version counter: the SSE loop compares one integer every 500 ms instead of diffing dicts. If the version hasn't moved, it sends nothing. If it has, it sends the full snapshot. Any change counts, including a removal.

Usage:

```python
cache = PriceCache()
cache.update("AAPL", 190.00)                         # flat, reference 190.00
u = cache.update("AAPL", 191.90)                     # up, change 1.90, day_change 1.90 (1.0%)
cache.update("MSFT", 421.0, reference_price=415.0)   # Massive-style explicit previous close
cache.get_price("AAPL")                              # 191.9
cache.remove("AAPL"); "AAPL" in cache                # False
```

---

## 6. Source Interface — `interface.py`

The contract doesn't change from v1. v2 only tightens the docstrings. Both implementations must:

- normalize tickers (`normalize_ticker`)
- make `add_ticker` and `remove_ticker` idempotent
- remove the ticker from the cache in `remove_ticker`
- make `stop()` safe to call more than once and stop all writes once it returns
- never let one bad tick or poll kill the background task

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push PriceUpdates into a shared PriceCache on their own
    schedule; consumers read from the cache, never from the source.

        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL"])
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Start the background task. Call once. Tickers added before start() are kept."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task. Idempotent. No cache writes after it returns."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Track a ticker (normalized). No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and remove it from the PriceCache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers (normalized), in insertion order."""
```

---

## 7. Seed Data — `seed_prices.py`

No change from v1. Summary for reference:

| Ticker | Seed $ | σ (vol) | μ (drift) | Group |
|--------|-------:|--------:|----------:|-------|
| AAPL | 190 | 0.22 | 0.05 | tech |
| GOOGL | 175 | 0.25 | 0.05 | tech |
| MSFT | 420 | 0.20 | 0.05 | tech |
| AMZN | 185 | 0.28 | 0.05 | tech |
| TSLA | 250 | 0.50 | 0.03 | — (0.3 with all) |
| NVDA | 800 | 0.40 | 0.08 | tech |
| META | 500 | 0.30 | 0.05 | tech |
| JPM | 195 | 0.18 | 0.04 | finance |
| V | 280 | 0.17 | 0.04 | finance |
| NFLX | 600 | 0.35 | 0.05 | tech |
| *other* | U(50, 300) | 0.25 | 0.05 | — |

Correlations: tech–tech 0.6, finance–finance 0.5, everything else (including TSLA and unknown tickers) 0.3. A matrix with a constant off-diagonal ρ ≥ 0 inside each block and 0.3 between blocks stays positive-definite for any n, so `np.linalg.cholesky` can't fail on these values.

---

## 8. GBM Simulator — `simulator.py`

### 8.1 The math

Each tick of `dt` years:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )        Z ~ N(0,1), correlated across tickers
dt      = 0.5 s / (252 · 6.5 · 3600 s)  ≈ 8.48e-8
Z_corr  = L · Z_indep,   L = cholesky(C)
```

Per-tick standard deviation is σ·√dt. For AAPL that's 0.22 · 2.9e-4 ≈ 0.0064%, about 1.2¢ on $190, so roughly every tick changes the displayed price and flashes. On top of that, each ticker has a 0.1% chance per tick of a ±2–5% shock. With 10 tickers at 2 ticks/s, that's one shock somewhere about every 50 s.

### 8.2 `GBMSimulator`

The v1 class stays as it is: `step()`, `add_ticker()`, `remove_ticker()`, `get_price()`, `get_tickers()`, `_rebuild_cholesky()`, `_pairwise_correlation()`. The only change is normalizing in the add and remove paths:

```python
from .tickers import normalize_ticker

    def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()
```

`__init__` passes each starting ticker through `normalize_ticker` before `_add_ticker_internal`.

`step()` is the hot path and is unchanged:

```python
    def step(self) -> dict[str, float]:
        n = len(self._tickers)
        if n == 0:
            return {}
        z = np.random.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]
            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:          # shock event
                self._prices[ticker] *= 1 + random.uniform(0.02, 0.05) * random.choice([-1, 1])

            result[ticker] = round(self._prices[ticker], 2)
        return result
```

### 8.3 `SimulatorDataSource`

Changes: a pre-start queue, normalization, and an explicit `reference_price` on the first write. Once the cache has seen a ticker, it carries the reference forward.

```python
class SimulatorDataSource(MarketDataSource):
    """Runs GBMSimulator.step() every `update_interval` seconds into the PriceCache."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None
        self._pending: list[str] = []  # add_ticker() calls before start()

    async def start(self, tickers: list[str]) -> None:
        initial = list(dict.fromkeys(normalize_ticker(t) for t in [*tickers, *self._pending]))
        self._pending.clear()
        self._sim = GBMSimulator(tickers=initial, event_probability=self._event_prob)
        for ticker in initial:
            self._seed_cache(ticker)  # SSE has data on the very first frame
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(initial))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim is None:
            if ticker not in self._pending:
                self._pending.append(ticker)
            return
        if ticker in self._sim.get_tickers():
            return
        self._sim.add_ticker(ticker)
        self._seed_cache(ticker)
        logger.info("Simulator: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim is not None:
            self._sim.remove_ticker(ticker)
        if ticker in self._pending:
            self._pending.remove(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else list(self._pending)

    def _seed_cache(self, ticker: str) -> None:
        price = self._sim.get_price(ticker) if self._sim else None
        if price is not None:
            self._cache.update(ticker=ticker, price=price, reference_price=price)

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

There is no `await` between `step()` and the cache writes, and `add_ticker`/`remove_ticker` run on the same event loop. A removal therefore can't interleave with a step, so the simulator can't produce ghost tickers.

---

## 9. Massive Poller — `massive_client.py`

### 9.1 API facts (verified against the installed `massive` package)

- Endpoint: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT`, one call for all tickers.
- Client call: `RESTClient(api_key=...).get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=[...])` returns `list[TickerSnapshot]`. It is **synchronous**, so we run it in `asyncio.to_thread`.
- `TickerSnapshot` fields: `ticker`, `day: Agg`, `prev_day: Agg`, `min: MinuteSnapshot`, `last_trade: LastTrade`, `last_quote`, `todays_change`, `todays_change_percent`, `updated`. **Any of them can be `None`.**
- `LastTrade` has `price`, `size`, and `sip_timestamp` (**ns**). There is **no `timestamp` attribute.**
- `Agg` has `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp` (ms).
- Snapshots are cleared at 12am ET and refill from about 4am ET. Pre-market, `day.*` can be `0`.
- Errors: `massive.exceptions.AuthError` (no key), `massive.exceptions.BadResponse` (any non-200, including 401, 403 and 429). urllib3 retries 5xx internally.
- Free tier: 5 requests/min. Polling every 15 s uses 4/min, leaving one call per minute for early wakes (§9.3).

Example response element (JSON, before the client maps it to models):

```json
{
  "ticker": "AAPL",
  "todaysChange": 1.37, "todaysChangePerc": 0.72,
  "updated": 1760600000512000000,
  "day":     {"o": 190.2, "h": 191.9, "l": 189.8, "c": 191.35, "v": 41234567, "vw": 190.9},
  "prevDay": {"o": 188.1, "h": 190.4, "l": 187.9, "c": 190.0,  "v": 52345678, "vw": 189.3},
  "min":     {"o": 191.3, "h": 191.4, "l": 191.2, "c": 191.37, "v": 12000, "t": 1760600000000},
  "lastTrade": {"p": 191.37, "s": 100, "x": 4, "t": 1760600000512000000}
}
```

### 9.2 `parse_snapshot()`: one pure, testable function

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource
from .tickers import normalize_ticker

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ParsedSnapshot:
    ticker: str
    price: float
    timestamp: float              # Unix seconds
    reference_price: float | None  # previous close


def _get(obj: Any, *path: str) -> Any:
    """Safe nested getattr: _get(snap, 'last_trade', 'price') → value or None."""
    for name in path:
        if obj is None:
            return None
        obj = getattr(obj, name, None)
    return obj


def _first_positive(*values: Any) -> float | None:
    for v in values:
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    return None


def _to_unix_seconds(ts: Any) -> float | None:
    """Massive mixes ns (trades, `updated`), ms (aggs) — infer unit by magnitude."""
    if not isinstance(ts, (int, float)) or ts <= 0:
        return None
    if ts > 1e17:   # nanoseconds
        return ts / 1e9
    if ts > 1e14:   # microseconds
        return ts / 1e6
    if ts > 1e11:   # milliseconds
        return ts / 1e3
    return float(ts)  # seconds


def parse_snapshot(snap: Any) -> ParsedSnapshot | None:
    """Turn a massive TickerSnapshot into a ParsedSnapshot, or None if unusable.

    Price preference: last trade → current minute bar close → today's close → previous close.
    """
    raw_ticker = getattr(snap, "ticker", None)
    if not raw_ticker:
        return None

    price = _first_positive(
        _get(snap, "last_trade", "price"),
        _get(snap, "min", "close"),
        _get(snap, "day", "close"),
        _get(snap, "prev_day", "close"),
    )
    if price is None:
        return None

    timestamp = (
        _to_unix_seconds(_get(snap, "last_trade", "sip_timestamp"))
        or _to_unix_seconds(getattr(snap, "updated", None))
        or time.time()
    )
    reference = _first_positive(_get(snap, "prev_day", "close"))

    return ParsedSnapshot(
        ticker=str(raw_ticker).upper(),
        price=price,
        timestamp=timestamp,
        reference_price=reference,
    )
```

### 9.3 `MassiveDataSource`

Design points:

- **Immediate first poll** in `start()`, so the first SSE frame has data, as in v1.
- **Early wake on add.** `add_ticker()` sets an `asyncio.Event`. The poll loop wakes, but never calls the API sooner than `min_request_gap` after the previous call (default `0.8 × poll_interval`, which is 12 s on the free tier and keeps us at ≤ 5 req/min). Adding several tickers at once results in **one** batched call.
- **Ghost filter.** Only snapshots for tickers still tracked when the response arrives get written.
- **Backoff.** After consecutive failed polls the delay doubles, capped at 5 min. One success resets it.
- The tickers list is **copied** before it's handed to the worker thread.

```python
class MassiveDataSource(MarketDataSource):
    """Polls the Massive snapshot endpoint for all tracked tickers into the PriceCache."""

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
        min_request_gap: float | None = None,
        max_backoff: float = 300.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._min_gap = 0.8 * poll_interval if min_request_gap is None else min_request_gap
        self._max_backoff = max_backoff

        self._tickers: list[str] = []
        self._client: RESTClient | None = None
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._failures = 0
        self._last_call = 0.0  # time.monotonic() of last API call

    # --- lifecycle ---

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        merged = [*self._tickers, *(normalize_ticker(t) for t in tickers)]
        self._tickers = list(dict.fromkeys(merged))  # dedupe, keep order
        await self._poll_once()                       # cache has data before first SSE frame
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval",
                    len(self._tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    # --- ticker set ---

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker in self._tickers:
            return
        self._tickers.append(ticker)
        self._wake.set()  # fetch soon (rate-limit permitting) instead of waiting a full interval
        logger.info("Massive: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- polling ---

    def _next_delay(self) -> float:
        if self._failures == 0:
            return self._interval
        return min(self._interval * 2 ** self._failures, self._max_backoff)

    async def _poll_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._next_delay())
            except TimeoutError:
                pass
            self._wake.clear()

            wait = self._min_gap - (time.monotonic() - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)

            ok = await self._poll_once()
            self._failures = 0 if ok else self._failures + 1

    async def _poll_once(self) -> bool:
        """One fetch + cache write. Returns False only if the API call failed."""
        requested = list(self._tickers)
        if not requested or self._client is None:
            return True

        self._last_call = time.monotonic()
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots, requested)
        except Exception as e:  # AuthError, BadResponse (401/403/429), network
            logger.error("Massive poll failed (%d consecutive): %s", self._failures + 1, e)
            return False

        active = set(self._tickers)  # re-read: tickers may have been removed mid-flight
        seen: set[str] = set()
        for snap in snapshots or []:
            parsed = parse_snapshot(snap)
            if parsed is None:
                logger.warning("Skipping unusable snapshot for %s", getattr(snap, "ticker", "?"))
                continue
            if parsed.ticker not in active:
                continue
            seen.add(parsed.ticker)
            self._cache.update(
                ticker=parsed.ticker,
                price=parsed.price,
                timestamp=parsed.timestamp,
                reference_price=parsed.reference_price,
            )

        missing = active.intersection(requested) - seen
        if missing:
            logger.warning("Massive returned no data for: %s", ", ".join(sorted(missing)))
        logger.debug("Massive poll: %d/%d tickers updated", len(seen), len(requested))
        return True

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Blocking HTTP call — runs in a worker thread."""
        assert self._client is not None
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )
```

`asyncio.Event()` is created in `__init__`. That's fine on Python ≥ 3.10, because events bind to a loop lazily on first use. Construct the source inside the lifespan, which runs on the serving loop.

Timeline on the free tier (interval 15 s, min gap 12 s):

```
t=0    start(): poll #1 (10 tickers)
t=15   poll #2
t=20   add_ticker("PYPL") → wake; last call 5 s ago → sleep 7 s
t=27   poll #3 (11 tickers, PYPL priced)
t=42   poll #4 …                      → never more than 5 calls in any 60 s window
```

---

## 10. Factory & Configuration — `factory.py`

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r, using %s", name, raw, default)
        return default
    return value if value > 0 else default


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set → MassiveDataSource; otherwise SimulatorDataSource. Unstarted."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        interval = _env_float("MASSIVE_POLL_INTERVAL", 15.0)
        logger.info("Market data source: Massive API (poll every %.1fs)", interval)
        return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=interval)

    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

| Env var | Default | Effect |
|---------|---------|--------|
| `MASSIVE_API_KEY` | *(empty)* | Non-empty selects the Massive poller; empty selects the simulator |
| `MASSIVE_POLL_INTERVAL` | `15` | Seconds between polls. Free tier: ≥ 12. Paid: 2–5 |

Add `MASSIVE_POLL_INTERVAL=` (commented) to `.env.example` next to `MASSIVE_API_KEY`.

---

## 11. SSE Stream — `stream.py`

### 11.1 Wire format

```
retry: 1000

id: 42
data: {"AAPL":{"ticker":"AAPL","price":191.37,...},"MSFT":{...}}

: keep-alive

id: 43
data: {...}
```

- One event holds the **full snapshot** of every tracked ticker, keyed by ticker. A ticker missing from an event has been removed, and the client should drop it.
- An event is sent only when `cache.version` changes, checked every 500 ms. The simulator changes it every tick. Massive changes it every 15 s.
- `id:` is the cache version, which helps when debugging in the browser's network tab.
- `: keep-alive` comments go out after 15 s of silence, so proxies and load balancers (App Runner, Render) don't close idle connections overnight.
- `retry: 1000` tells `EventSource` to reconnect after 1 s.

### 11.2 Code

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(
    price_cache: PriceCache,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> APIRouter:
    """Build a fresh router (safe to call more than once, e.g. in tests)."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request, interval, heartbeat),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"

    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    last_version = -1
    last_sent = time.monotonic()

    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                data = {t: u.to_dict() for t, u in price_cache.get_all().items()}
                yield f"id: {version}\ndata: {json.dumps(data)}\n\n"
                last_sent = time.monotonic()
            elif time.monotonic() - last_sent >= heartbeat:
                yield ": keep-alive\n\n"
                last_sent = time.monotonic()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client)
```

Compared with v1: empty snapshots are now sent (`{}` after the last ticker is removed), the router is created per call, and heartbeats and `id:` lines are new. The payload shape is a superset of v1's, so v1 clients keep working.

Cost: 10 tickers × ~260 bytes ≈ 2.6 KB per frame × 2 frames/s ≈ 5 KB/s per client, which doesn't matter for a single-user app.

---

## 12. Ticker Tracking — `tracking.py` (new)

The set of tickers the source tracks is **derived**:

```
tracked = { watchlist tickers } ∪ { position tickers with quantity > 0 }
```

Routes don't call `add_ticker`/`remove_ticker` by hand. After any DB change that can alter that set (watchlist add or remove, a trade that opens or closes a position, LLM actions), they recompute it from the DB and let `sync_tracked_tickers()` apply the difference. That one rule covers several edge cases:

- Removing a watchlist ticker you still hold keeps it priced.
- Fully selling a ticker that isn't on the watchlist stops tracking it.
- The LLM buying a ticker that isn't on the watchlist starts tracking it. See §13.4 for getting a price *before* the fill.

```python
"""Keep the market data source's ticker set in sync with DB state."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from .interface import MarketDataSource
from .tickers import normalize_ticker

_sync_lock = asyncio.Lock()


async def sync_tracked_tickers(
    source: MarketDataSource,
    desired: Iterable[str],
) -> tuple[list[str], list[str]]:
    """Add/remove tickers so source.get_tickers() == desired. Returns (added, removed).

    Serialized with a lock so concurrent requests (e.g. manual trade + chat
    trade) can't interleave half-applied diffs.
    """
    async with _sync_lock:
        want = {normalize_ticker(t) for t in desired}
        have = set(source.get_tickers())
        added = sorted(want - have)
        removed = sorted(have - want)
        for t in added:
            await source.add_ticker(t)
        for t in removed:
            await source.remove_ticker(t)
        return added, removed
```

The DB side belongs to the backend agent. It needs one query:

```sql
-- backend/db: tracked tickers for a user
SELECT ticker FROM watchlist WHERE user_id = :uid
UNION
SELECT ticker FROM positions WHERE user_id = :uid AND quantity > 0;
```

---

## 13. FastAPI Integration

### 13.1 Lifespan (`backend/app/main.py`)

`create_stream_router()` captures the cache instance when the route is registered, which happens at import time. So the cache is created at module scope, and the lifespan starts and stops the source that writes to it:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db import get_tracked_tickers, init_db  # backend agent provides these
from app.market import PriceCache, create_market_data_source, create_stream_router

DEFAULT_TICKERS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"]

price_cache = PriceCache()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()                                   # lazy schema + seed (PLAN §7)
    source = create_market_data_source(price_cache)
    await source.start(get_tracked_tickers("default") or DEFAULT_TICKERS)
    app.state.price_cache = price_cache
    app.state.market_source = source
    try:
        yield
    finally:
        await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
# ... other /api routers ...
# Static files LAST so /api/* wins:
# app.mount("/", StaticFiles(directory="static", html=True), name="static")
```

### 13.2 Dependencies (`backend/app/deps.py`)

```python
from fastapi import Request

from app.market import MarketDataSource, PriceCache


def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

### 13.3 Watchlist routes

```python
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.deps import get_market_source, get_price_cache
from app.market import InvalidTickerError, normalize_ticker, sync_tracked_tickers

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


class WatchlistAdd(BaseModel):
    ticker: str


@router.get("")
async def list_watchlist(cache=Depends(get_price_cache)):
    rows = db.get_watchlist("default")
    out = []
    for row in rows:
        u = cache.get(row.ticker)
        out.append({"ticker": row.ticker, "added_at": row.added_at,
                    **(u.to_dict() if u else {"price": None})})
    return out


@router.post("", status_code=201)
async def add_to_watchlist(body: WatchlistAdd, source=Depends(get_market_source),
                           cache=Depends(get_price_cache)):
    try:
        ticker = normalize_ticker(body.ticker)
    except InvalidTickerError as e:
        raise HTTPException(400, str(e))
    db.add_watchlist("default", ticker)                     # ON CONFLICT DO NOTHING
    await sync_tracked_tickers(source, db.get_tracked_tickers("default"))
    u = cache.get(ticker)                                   # simulator: present now; Massive: next poll
    return {"ticker": ticker, "price": u.price if u else None}


@router.delete("/{ticker}", status_code=204)
async def remove_from_watchlist(ticker: str, source=Depends(get_market_source)):
    try:
        ticker = normalize_ticker(ticker)
    except InvalidTickerError as e:
        raise HTTPException(400, str(e))
    db.remove_watchlist("default", ticker)
    await sync_tracked_tickers(source, db.get_tracked_tickers("default"))  # keeps held tickers
```

### 13.4 Trade execution: getting the fill price

```python
async def resolve_fill_price(ticker: str, source, cache, timeout: float = 0.0) -> float:
    """Current price for a trade. Starts tracking unknown tickers.

    Simulator: add_ticker() seeds the cache synchronously → price is immediate.
    Massive: price arrives on the next (early-woken) poll; with timeout=0 we
    fail fast and the user/LLM retries.
    """
    price = cache.get_price(ticker)
    if price is not None:
        return price
    await source.add_ticker(ticker)            # tracking will be reconciled after the trade
    deadline = asyncio.get_running_loop().time() + timeout
    while (price := cache.get_price(ticker)) is None:
        if asyncio.get_running_loop().time() >= deadline:
            raise HTTPException(409, f"No price available yet for {ticker}; try again shortly")
        await asyncio.sleep(0.25)
    return price
```

Order inside `POST /api/portfolio/trade`:

1. `ticker = normalize_ticker(...)`, then `price = await resolve_fill_price(...)`
2. Validate cash or shares, then write `trades` and `positions` and update cash in one DB transaction
3. Record a portfolio snapshot, valued from `cache.get_price()` for every position
4. `await sync_tracked_tickers(source, db.get_tracked_tickers("default"))`

The chat route uses the same helper for each LLM trade. A 409 becomes an error string in the chat response (PLAN §9: "the error is included in the chat response").

### 13.5 Portfolio valuation

```python
def value_portfolio(cash: float, positions, cache) -> dict:
    rows, total = [], cash
    for p in positions:
        price = cache.get_price(p.ticker) or p.avg_cost   # fall back to cost if not priced yet
        mv = price * p.quantity
        total += mv
        rows.append({
            "ticker": p.ticker, "quantity": p.quantity, "avg_cost": p.avg_cost,
            "current_price": price, "market_value": mv,
            "unrealized_pnl": (price - p.avg_cost) * p.quantity,
            "pnl_percent": (price / p.avg_cost - 1) * 100 if p.avg_cost else 0.0,
        })
    return {"cash": cash, "total_value": total, "positions": rows}
```

The 30 s snapshot task (PLAN §7) calls the same function. Write it as an `asyncio` task started and cancelled in the lifespan next to the market source.

---

## 14. Frontend Contract

The frontend agent can code against this without reading Python.

```ts
// frontend/src/lib/market.ts
export type Direction = "up" | "down" | "flat";

export interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;                 // Unix seconds
  change: number;                    // vs previous tick
  change_percent: number;
  direction: Direction;              // drives the flash
  reference_price: number | null;    // previous close (Massive) / session start (simulator)
  day_change: number | null;
  day_change_percent: number | null; // watchlist "Chg %" column
}

export type PriceSnapshot = Record<string, PriceUpdate>;

export function connectPrices(
  onSnapshot: (s: PriceSnapshot) => void,
  onStatus: (s: "connected" | "reconnecting" | "disconnected") => void,
): () => void {
  const es = new EventSource("/api/stream/prices");
  es.onopen = () => onStatus("connected");
  es.onmessage = (e) => onSnapshot(JSON.parse(e.data) as PriceSnapshot);
  es.onerror = () =>
    onStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
  return () => es.close();
}
```

Client rules:

- **Replace, don't merge.** Each snapshot is the complete tracked set. Tickers missing from it were removed.
- **Flash on `direction`** when a ticker's `timestamp` changes. Massive re-sends unchanged tickers with the same timestamp, and those must not flash.
- **Sparklines:** append `{t: timestamp, v: price}` per ticker when `timestamp` changes. Cap at about 500 points per ticker.
- **Held but not watched:** the stream can include tickers that aren't on the watchlist (open positions). Filter the watchlist panel by `GET /api/watchlist`, and use the stream for prices only.
- Keep-alive comments never reach `onmessage`.

---

## 15. Testing

Run with `cd backend && uv run --extra dev pytest -v`. Checked against this design: 72 of the 73 v1 tests pass unchanged, and every test in this section passes. The one failure is `test_massive.py::test_timestamp_conversion`. It builds a `MagicMock` snapshot with `last_trade.timestamp` in ms, which is exactly the bug in §0.1, so replace it with the real-model tests in §15.3.

### 15.1 `test_tickers.py`

```python
import pytest

from app.market.tickers import InvalidTickerError, is_valid_ticker, normalize_ticker


@pytest.mark.parametrize("raw,expected", [("aapl", "AAPL"), (" msft ", "MSFT"), ("brk.b", "BRK.B")])
def test_normalize(raw, expected):
    assert normalize_ticker(raw) == expected


@pytest.mark.parametrize("raw", ["", "  ", "1ABC", "TOOLONGTICKER", "AA PL", "$AAPL", None])
def test_invalid(raw):
    with pytest.raises(InvalidTickerError):
        normalize_ticker(raw)
    assert not is_valid_ticker(raw)
```

### 15.2 Model and cache additions

```python
from app.market.cache import PriceCache
from app.market.models import PriceUpdate


def test_day_change_from_reference():
    u = PriceUpdate("AAPL", price=199.5, previous_price=199.0, reference_price=190.0)
    assert u.day_change == 9.5
    assert u.day_change_percent == 5.0


def test_day_change_none_without_reference():
    u = PriceUpdate("AAPL", 100.0, 100.0)
    assert u.day_change is None and u.day_change_percent is None
    assert u.to_dict()["day_change_percent"] is None


def test_cache_carries_reference_forward():
    c = PriceCache()
    c.update("AAPL", 190.0)
    u = c.update("AAPL", 192.0)
    assert u.reference_price == 190.0 and u.day_change == 2.0


def test_cache_explicit_reference_wins():
    c = PriceCache()
    u = c.update("AAPL", 192.0, reference_price=188.0)
    assert u.reference_price == 188.0


def test_zero_timestamp_is_kept():
    assert PriceCache().update("AAPL", 1.0, timestamp=0.0).timestamp == 0.0


def test_remove_bumps_version_only_if_present():
    c = PriceCache()
    c.update("AAPL", 1.0)
    v = c.version
    c.remove("AAPL")
    assert c.version == v + 1
    c.remove("AAPL")
    assert c.version == v + 1
```

### 15.3 Massive: parse **real** model objects (the regression for §0.1)

```python
import asyncio

import pytest
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource, parse_snapshot

NS = 1_760_600_000_512_000_000


def snap(**d) -> TickerSnapshot:
    return TickerSnapshot.from_dict(d)


def test_parse_full_snapshot():
    p = parse_snapshot(snap(ticker="AAPL", lastTrade={"p": 191.37, "t": NS}, prevDay={"c": 190.0}))
    assert p.ticker == "AAPL" and p.price == 191.37 and p.reference_price == 190.0
    assert p.timestamp == pytest.approx(NS / 1e9)


def test_parse_falls_back_to_minute_then_day_then_prev_close():
    assert parse_snapshot(snap(ticker="A", min={"c": 10.0}, day={"c": 11.0})).price == 10.0
    assert parse_snapshot(snap(ticker="A", day={"c": 11.0})).price == 11.0
    assert parse_snapshot(snap(ticker="A", day={"c": 0}, prevDay={"c": 9.0})).price == 9.0


def test_parse_unusable():
    assert parse_snapshot(snap(ticker="A")) is None
    assert parse_snapshot(snap(lastTrade={"p": 1.0})) is None


async def test_poll_writes_real_snapshots():
    cache = PriceCache()
    src = MassiveDataSource("k", cache, poll_interval=60)
    src._client = object()
    src._tickers = ["AAPL"]
    src._fetch_snapshots = lambda tickers: [
        snap(ticker="AAPL", lastTrade={"p": 191.37, "t": NS}, prevDay={"c": 190.0})
    ]
    assert await src._poll_once() is True
    u = cache.get("AAPL")
    assert u.price == 191.37 and u.day_change_percent == pytest.approx(0.7211, abs=1e-3)


async def test_ticker_removed_mid_flight_is_not_written_back():
    cache = PriceCache()
    src = MassiveDataSource("k", cache, poll_interval=60)
    src._client = object()
    src._tickers = ["AAPL", "TSLA"]
    gate = asyncio.Event()

    def slow_fetch(tickers):
        asyncio.run_coroutine_threadsafe(gate.wait(), loop).result()
        return [snap(ticker="AAPL", lastTrade={"p": 1.0}), snap(ticker="TSLA", lastTrade={"p": 2.0})]

    loop = asyncio.get_running_loop()
    src._fetch_snapshots = slow_fetch
    poll = asyncio.create_task(src._poll_once())
    await asyncio.sleep(0.05)
    await src.remove_ticker("TSLA")
    gate.set()
    await poll
    assert "AAPL" in cache and "TSLA" not in cache


async def test_failed_poll_returns_false():
    src = MassiveDataSource("k", PriceCache(), poll_interval=60)
    src._client = object()
    src._tickers = ["AAPL"]

    def boom(tickers):
        raise RuntimeError("429")

    src._fetch_snapshots = boom
    assert await src._poll_once() is False


def test_backoff_delay():
    src = MassiveDataSource("k", PriceCache(), poll_interval=15, max_backoff=300)
    assert src._next_delay() == 15
    src._failures = 2
    assert src._next_delay() == 60
    src._failures = 10
    assert src._next_delay() == 300
```

### 15.4 Simulator source additions

```python
async def test_add_before_start_is_kept():
    cache = PriceCache()
    src = SimulatorDataSource(cache, update_interval=10)
    await src.add_ticker("pypl")
    await src.start(["AAPL"])
    assert set(src.get_tickers()) == {"AAPL", "PYPL"}
    assert cache.get("PYPL").reference_price == cache.get("PYPL").price
    await src.stop()


async def test_lowercase_add_is_deduped():
    src = SimulatorDataSource(PriceCache(), update_interval=10)
    await src.start(["AAPL"])
    await src.add_ticker("aapl")
    assert src.get_tickers() == ["AAPL"]
    await src.stop()


def test_cholesky_full_default_set():
    from app.market.seed_prices import SEED_PRICES
    sim = GBMSimulator(list(SEED_PRICES))
    assert sim._cholesky.shape == (10, 10)
```

### 15.5 `test_stream.py`: drive the generator directly

An infinite SSE response is awkward to test through an HTTP client, so test the generator with a fake request and test the route registration separately.

```python
import json

from fastapi import FastAPI

from app.market.cache import PriceCache
from app.market.stream import _generate_events, create_stream_router


class FakeRequest:
    def __init__(self, polls_before_disconnect: int):
        self._left = polls_before_disconnect
        self.client = None

    async def is_disconnected(self) -> bool:
        self._left -= 1
        return self._left < 0


async def collect(cache, polls, **kw):
    return [e async for e in _generate_events(cache, FakeRequest(polls), interval=0, **kw)]


async def test_first_event_is_retry_then_snapshot():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    events = await collect(cache, polls=1)
    assert events[0] == "retry: 1000\n\n"
    data = json.loads(events[1].split("data: ", 1)[1])
    assert data["AAPL"]["price"] == 190.0


async def test_no_duplicate_when_version_unchanged():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    events = await collect(cache, polls=3, heartbeat=999)
    assert sum(e.startswith("id:") for e in events) == 1


async def test_heartbeat_when_idle():
    events = await collect(PriceCache(), polls=3, heartbeat=0)
    assert any(e.startswith(": keep-alive") for e in events)


async def test_empty_snapshot_is_sent():
    events = await collect(PriceCache(), polls=1)
    assert events[1].endswith("data: {}\n\n")


def test_router_factory_is_idempotent():
    cache = PriceCache()
    app = FastAPI()
    app.include_router(create_stream_router(cache))
    r2 = create_stream_router(cache)
    assert len([r for r in r2.routes if r.path == "/api/stream/prices"]) == 1
```

### 15.6 `test_tracking.py`

```python
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource
from app.market.tracking import sync_tracked_tickers


async def test_sync_adds_and_removes():
    cache = PriceCache()
    src = SimulatorDataSource(cache, update_interval=10)
    await src.start(["AAPL", "GOOGL"])
    added, removed = await sync_tracked_tickers(src, ["aapl", "TSLA"])
    assert added == ["TSLA"] and removed == ["GOOGL"]
    assert set(src.get_tickers()) == {"AAPL", "TSLA"}
    assert "GOOGL" not in cache
    await src.stop()


async def test_sync_is_noop_when_equal():
    src = SimulatorDataSource(PriceCache(), update_interval=10)
    await src.start(["AAPL"])
    assert await sync_tracked_tickers(src, ["AAPL"]) == ([], [])
    await src.stop()
```

### 15.7 Optional live smoke test (not in CI)

```bash
MASSIVE_API_KEY=... uv run python -c "
import asyncio
from app.market import PriceCache, create_market_data_source
async def main():
    c = PriceCache(); s = create_market_data_source(c)
    await s.start(['AAPL','MSFT']); print({t: u.to_dict() for t, u in c.get_all().items()})
    await s.stop()
asyncio.run(main())"
```

You should see two tickers with non-null `reference_price`. An empty dict means parsing or auth failed, and the log says which.

---

## 16. Error Handling & Edge Cases

| Situation | Behaviour |
|-----------|-----------|
| Empty watchlist and no positions at startup | Source starts with `[]`. SSE sends `{}`. Adding a ticker later works normally |
| Malformed ticker from UI or LLM | `normalize_ticker` raises, giving HTTP 400 or an error string in the chat response. Never reaches the source |
| Massive key invalid (401) or plan lacks endpoint (403) | `BadResponse` is logged and backoff grows to 5 min. The cache stays empty, so trades get 409 "no price". The app stays up |
| Massive 429 | Same backoff. The min-gap rule makes this rare |
| Unknown or delisted symbol on Massive | Not in the response, so a warning is logged and the ticker shows `price: null`. Trades get 409. The watchlist can still hold it, and the user can remove it |
| Market closed or weekend | Last trade is stale but valid. `timestamp` stops advancing, so the frontend stops flashing. Heartbeats keep the connection open |
| Pre-market (snapshot reset) | `day.close == 0` is skipped by `_first_positive` and falls back to `prev_day.close` |
| Ticker removed while a Massive request is in flight | Filtered against the active set, so no ghost entry |
| Watchlist removal of a held ticker | `sync_tracked_tickers` keeps it, because positions are part of the desired set |
| Simulator exception in `step()` | Logged. The loop continues on the next tick |
| Client disconnects | `is_disconnected()` ends the generator, and a `CancelledError` on shutdown is swallowed |
| Shutdown | Lifespan `finally` awaits `source.stop()`, which cancels and awaits the task. No writes after it returns |
| Simulator price drift over long sessions | GBM is multiplicative, so prices stay positive. Drift of μ ≈ 5%/yr is invisible within a session |

---

## 17. Implementation Checklist

In order. Each step should leave `uv run --extra dev pytest` green and `ruff check app/ tests/` clean.

1. [ ] Add `tickers.py` and `test_tickers.py`.
2. [ ] `models.py`: add `reference_price`, `day_change`, `day_change_percent` and extend `to_dict()`. Add the tests.
3. [ ] `cache.py`: add the `reference_price` param, the `is None` timestamp check, `remove()` bumping version, and a locked `version`. Update or add tests.
4. [ ] `massive_client.py`: add `parse_snapshot()` and rewrite `MassiveDataSource` (§9.3). Rewrite `test_massive.py` around real `TickerSnapshot.from_dict` objects and drop the MagicMock snapshots.
5. [ ] `simulator.py`: add normalization, the pre-start queue, and `_seed_cache` with an explicit reference. Add the tests.
6. [ ] `factory.py`: read `MASSIVE_POLL_INTERVAL`. Add a factory test for it. Add the var to `.env.example`.
7. [ ] `stream.py`: create the router per call, add heartbeat and `id:`, and send empty snapshots. Add `test_stream.py`.
8. [ ] Add `tracking.py`, `test_tracking.py` and the `__init__.py` exports.
9. [ ] Update `backend/CLAUDE.md` (Market Data API section) and `planning/MARKET_DATA_SUMMARY.md` with the new fields and functions.
10. [ ] Hand off §12–§14 to the backend and frontend agents: the tracked-tickers query, the lifespan wiring, `resolve_fill_price`, and the TS contract.
