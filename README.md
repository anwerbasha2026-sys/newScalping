# newScalping — Fixed Build 2.2

This package is a corrected working copy based on the `main` branch snapshot reviewed on 2026-10-06.

## Main fixes
- Signal decisions use **closed 5-minute candles only**.
- Higher-timeframe trend is checked before the more expensive 5m confirmation.
- Entry logic is changed from an over-constrained all-at-once filter to a pullback/reclaim structure with EMA/RSI/MACD/VWAP/PSAR confirmation.
- Scanner no longer waits 0.25s after every symbol; the API limiter remains centralized.
- Live orders are never blindly retried after an ambiguous network response.
- BUY/SELL execution is verified by querying the exchange order.
- Executed quantity and order ID are persisted in the local active-position record.
- PnL/TP/SL use a conservative two-sided fee estimate.
- SQLite migration is non-destructive for existing databases.
- Existing positions are restored on app restart.
- Paper Trading defaults to ON for a fresh install as a safety measure. Disable it explicitly for live trading.
- No API keys are hard-coded in source.

## Important
This is a hardened code build, not a guarantee of profitability. Test in Paper Trading first. Exchange fees and execution behavior can differ by account and market.
