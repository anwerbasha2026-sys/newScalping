import time
import sqlite3
import requests
import hmac
import hashlib
import threading
import math
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import datetime
from urllib.parse import urlencode

BASE_URL = "https://api.mexc.com/api/v3"
DB_NAME = "trading_bot.db"

# --- Shared API state ---
_api_rate_lock = threading.Lock()
_api_last_request = 0.0
API_MIN_INTERVAL = 0.15

_server_time_offset_ms = 0
_server_time_sync_at = 0.0

_symbol_rules_lock = threading.Lock()
_symbol_rules_cache = {}
_symbol_rules_cache_at = 0.0
SYMBOL_RULES_CACHE_TTL = 900.0

_supported_symbols_lock = threading.Lock()
_supported_symbols_cache = set()
_supported_symbols_cache_at = 0.0
SUPPORTED_SYMBOLS_CACHE_TTL = 900.0

_db_lock = threading.RLock()


# =========================
# Time / signing
# =========================
def sync_mexc_server_time(force=False):
    global _server_time_offset_ms, _server_time_sync_at
    now_mono = time.monotonic()
    if not force and (now_mono - _server_time_sync_at) < 30:
        return _server_time_offset_ms

    try:
        t0 = int(time.time() * 1000)
        r = requests.get(f"{BASE_URL}/time", timeout=3)
        t1 = int(time.time() * 1000)
        if r.status_code == 200:
            payload = r.json()
            server_ms = int(payload.get("serverTime"))
            local_mid = (t0 + t1) // 2
            _server_time_offset_ms = server_ms - local_mid
            _server_time_sync_at = now_mono
    except Exception as e:
        print(f"[TIME SYNC] Failed: {e}")
    return _server_time_offset_ms


def mexc_timestamp(force_sync=False):
    sync_mexc_server_time(force=force_sync)
    return int(time.time() * 1000) + _server_time_offset_ms


# =========================
# Database
# =========================
def _get_db():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with _db_lock:
        conn = _get_db()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS active_position (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT,
                    entry_price REAL,
                    amount REAL,
                    tp_percent REAL,
                    sl_percent REAL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS closed_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    amount REAL,
                    pnl_usd REAL,
                    pnl_percent REAL,
                    reason TEXT,
                    timestamp TEXT
                )
                """
            )
            conn.commit()
        finally:
            conn.close()


def save_setting(key, value):
    with _db_lock:
        conn = _get_db()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, str(value)),
            )
            conn.commit()
        finally:
            conn.close()


def get_setting(key, default=""):
    with _db_lock:
        conn = _get_db()
        try:
            row = conn.execute(
                "SELECT value FROM settings WHERE key=?", (key,)
            ).fetchone()
            return row[0] if row else default
        finally:
            conn.close()


def save_api_credentials(api_key, secret_key):
    save_setting("mexc_api_key", api_key.strip())
    save_setting("mexc_secret_key", secret_key.strip())


def get_api_credentials():
    return (
        get_setting("mexc_api_key", ""),
        get_setting("mexc_secret_key", ""),
    )


def save_active_position(symbol, entry_price, amount, tp_percent, sl_percent):
    with _db_lock:
        conn = _get_db()
        try:
            conn.execute("DELETE FROM active_position")
            conn.execute(
                """
                INSERT INTO active_position
                    (id, symbol, entry_price, amount, tp_percent, sl_percent)
                VALUES (1, ?, ?, ?, ?, ?)
                """,
                (symbol, float(entry_price), float(amount), float(tp_percent), float(sl_percent)),
            )
            conn.commit()
        finally:
            conn.close()


def clear_active_position():
    with _db_lock:
        conn = _get_db()
        try:
            conn.execute("DELETE FROM active_position")
            conn.commit()
        finally:
            conn.close()


def get_active_position():
    with _db_lock:
        conn = _get_db()
        try:
            row = conn.execute(
                "SELECT symbol, entry_price, amount, tp_percent, sl_percent "
                "FROM active_position WHERE id=1"
            ).fetchone()
            if row:
                return {
                    "symbol": row[0],
                    "entry_price": row[1],
                    "amount": row[2],
                    "tp_percent": row[3],
                    "sl_percent": row[4],
                }
            return None
        finally:
            conn.close()


def record_closed_trade(
    symbol, entry_price, exit_price, amount, pnl_usd, pnl_percent, reason
):
    with _db_lock:
        conn = _get_db()
        try:
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                """
                INSERT INTO closed_trades
                    (symbol, entry_price, exit_price, amount, pnl_usd, pnl_percent, reason, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    float(entry_price),
                    float(exit_price),
                    float(amount),
                    float(pnl_usd),
                    float(pnl_percent),
                    reason,
                    now_str,
                ),
            )
            conn.commit()
        finally:
            conn.close()


def get_all_closed_trades():
    with _db_lock:
        conn = _get_db()
        try:
            return conn.execute(
                "SELECT symbol, entry_price, exit_price, amount, pnl_usd, pnl_percent, reason, timestamp "
                "FROM closed_trades ORDER BY id DESC"
            ).fetchall()
        finally:
            conn.close()


# =========================
# REST API helpers
# =========================
def _api_wait():
    global _api_last_request
    with _api_rate_lock:
        now = time.monotonic()
        wait = API_MIN_INTERVAL - (now - _api_last_request)
        if wait > 0:
            time.sleep(wait)
        _api_last_request = time.monotonic()


def _request_json(method, url, retry_connection=True, **kwargs):
    kwargs.setdefault("timeout", 5)
    last_response = None
    method = method.upper()

    for attempt in range(3):
        _api_wait()
        try:
            response = requests.request(method, url, **kwargs)
            last_response = response

            if response.status_code in (429, 418, 500, 502, 503, 504):
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = min(float(retry_after), 8.0) if retry_after else (1.0 * (2 ** attempt))
                except (TypeError, ValueError):
                    delay = 1.0 * (2 ** attempt)
                time.sleep(delay)
                continue

            return response
        except requests.RequestException:
            if retry_connection and attempt < 2:
                time.sleep(0.5 * (2 ** attempt))
                continue
            raise
    return last_response


def _safe_json(response):
    try:
        return response.json() if response is not None else {}
    except Exception:
        return {}


def _api_error_message(response):
    payload = _safe_json(response)
    return payload.get("msg") or payload.get("message") or str(payload) or f"HTTP {getattr(response, 'status_code', 'unknown')}"


def _signed_params(params, secret_key):
    query_string = urlencode(params)
    signature = hmac.new(
        secret_key.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    signed = dict(params)
    signed["signature"] = signature
    return signed


def _auth_headers(api_key):
    return {"X-MEXC-APIKEY": api_key, "Content-Type": "application/json"}


def _signed_request(method, path, api_key, secret_key, params=None, timeout=5, retries_on_network=False, force_time_sync=False):
    params = dict(params or {})
    params["recvWindow"] = 10000
    params["timestamp"] = mexc_timestamp(force_sync=force_time_sync)
    signed = _signed_params(params, secret_key)
    return _request_json(
        method,
        f"{BASE_URL}{path}",
        headers=_auth_headers(api_key),
        params=signed,
        timeout=timeout,
        retry_connection=retries_on_network,
    )


def _is_timestamp_error(response):
    payload = _safe_json(response)
    text = f"{payload.get('code', '')} {payload.get('errorCode', '')} {payload.get('msg', '')} {payload.get('message', '')}".lower()
    return (
        "timestamp" in text
        or "outside" in text and "window" in text
        or "recvwindow" in text
    )


# =========================
# Symbol metadata / rules
# =========================
def _decimal_places(step):
    d = Decimal(str(step))
    return max(0, -d.as_tuple().exponent)


def _extract_step(info):
    if not isinstance(info, dict):
        return None
    direct_candidates = [
        info.get("baseAssetPrecision"),
        info.get("quantityPrecision"),
        info.get("baseCommissionPrecision"),
    ]
    for value in direct_candidates:
        if value is not None:
            try:
                iv = int(value)
                if 0 <= iv <= 18:
                    return Decimal(1).scaleb(-iv)
            except Exception:
                pass

    for key in ("baseSizePrecision", "quotePrecision", "quantityScale"):
        value = info.get(key)
        if value is not None:
            try:
                if float(value) > 0 and float(value) < 1:
                    return Decimal(str(value))
            except Exception:
                pass

    filters = info.get("filters") or []
    for flt in filters:
        if not isinstance(flt, dict):
            continue
        ftype = str(flt.get("filterType", flt.get("filterTypeName", ""))).upper()
        if ftype in {"LOT_SIZE", "MARKET_LOT_SIZE"}:
            step = flt.get("stepSize") or flt.get("step") or flt.get("minQty")
            if step:
                try:
                    return Decimal(str(step))
                except Exception:
                    pass
    return None


def _extract_min_qty(info):
    filters = info.get("filters") or []
    for flt in filters:
        if not isinstance(flt, dict):
            continue
        ftype = str(flt.get("filterType", "")).upper()
        if ftype in {"LOT_SIZE", "MARKET_LOT_SIZE"}:
            value = flt.get("minQty") or flt.get("minQuantity")
            if value:
                try:
                    return Decimal(str(value))
                except Exception:
                    pass
    for key in ("baseMinAmount", "minQty", "minQuantity", "minTradeAmount"):
        value = info.get(key)
        if value is not None:
            try:
                return Decimal(str(value))
            except Exception:
                pass
    return Decimal("0")


def _extract_min_notional(info):
    filters = info.get("filters") or []
    for flt in filters:
        if not isinstance(flt, dict):
            continue
        ftype = str(flt.get("filterType", "")).upper()
        if ftype in {"MIN_NOTIONAL", "NOTIONAL"}:
            value = flt.get("minNotional") or flt.get("notional")
            if value:
                try:
                    return Decimal(str(value))
                except Exception:
                    pass
    for key in ("minNotional", "minQuoteAmount", "quoteMinAmount", "minTradeUSDT"):
        value = info.get(key)
        if value is not None:
            try:
                return Decimal(str(value))
            except Exception:
                pass
    return Decimal("0")


def get_symbol_rules(symbol, force=False):
    global _symbol_rules_cache_at
    formatted = symbol.replace("/", "").upper()
    now = time.monotonic()
    with _symbol_rules_lock:
        if not force and formatted in _symbol_rules_cache and (now - _symbol_rules_cache_at) < SYMBOL_RULES_CACHE_TTL:
            return _symbol_rules_cache[formatted]

    try:
        response = _request_json("GET", f"{BASE_URL}/exchangeInfo", timeout=8)
        if response is None or response.status_code != 200:
            return None
        payload = _safe_json(response)
        symbols = payload.get("symbols") if isinstance(payload, dict) else payload
        if not isinstance(symbols, list):
            return None
        local = {}
        for info in symbols:
            if not isinstance(info, dict):
                continue
            name = str(info.get("symbol", "")).replace("/", "").upper()
            if not name:
                continue
            step = _extract_step(info)
            min_qty = _extract_min_qty(info)
            min_notional = _extract_min_notional(info)
            status = str(info.get("status", info.get("state", ""))).upper()
            local[name] = {
                "symbol": name,
                "step": step or Decimal("0"),
                "min_qty": min_qty,
                "min_notional": min_notional,
                "status": status,
                "raw": info,
            }

        with _symbol_rules_lock:
            _symbol_rules_cache.update(local)
            _symbol_rules_cache_at = now
        return local.get(formatted)
    except Exception as e:
        print(f"[RULES] Failed to load exchangeInfo: {e}")
        return None


def get_supported_spot_symbols(force=False):
    global _supported_symbols_cache_at, _supported_symbols_cache
    now = time.monotonic()
    with _supported_symbols_lock:
        if _supported_symbols_cache and not force and (now - _supported_symbols_cache_at) < SUPPORTED_SYMBOLS_CACHE_TTL:
            return set(_supported_symbols_cache)

    try:
        response = _request_json("GET", f"{BASE_URL}/defaultSymbols", timeout=6)
        if response is not None and response.status_code == 200:
            payload = _safe_json(response)
            candidates = []
            if isinstance(payload, list):
                candidates = payload
            elif isinstance(payload, dict):
                for key in ("data", "symbols", "defaultSymbols"):
                    if isinstance(payload.get(key), list):
                        candidates = payload[key]
                        break
            parsed = set()
            for item in candidates:
                if isinstance(item, str):
                    name = item
                elif isinstance(item, dict):
                    name = item.get("symbol") or item.get("symbolName") or item.get("pair")
                else:
                    name = None
                if name:
                    parsed.add(str(name).replace("/", "").upper())
            if parsed:
                with _supported_symbols_lock:
                    _supported_symbols_cache = parsed
                    _supported_symbols_cache_at = now
                return set(parsed)
    except Exception as e:
        print(f"[SYMBOLS] Failed to load defaultSymbols: {e}")

    return set()


def _round_down_decimal(value, step):
    value = Decimal(str(value))
    step = Decimal(str(step))
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _decimal_to_string(value):
    d = Decimal(str(value))
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def normalize_order_quantity(symbol, quantity, price=None):
    rules = get_symbol_rules(symbol)
    q = Decimal(str(quantity))
    if not rules:
        return q, None

    step = rules.get("step") or Decimal("0")
    if step > 0:
        q = _round_down_decimal(q, step)

    min_qty = rules.get("min_qty") or Decimal("0")
    if q < min_qty:
        return q, f"Quantity {q} is below minimum quantity {min_qty} for {symbol}."

    min_notional = rules.get("min_notional") or Decimal("0")
    if price is not None and min_notional > 0 and q * Decimal(str(price)) < min_notional:
        return q, f"Order value {q * Decimal(str(price))} is below minimum notional {min_notional} for {symbol}."

    return q, None


# =========================
# Market data
# =========================
def get_top_200_symbols():
    try:
        supported = get_supported_spot_symbols()
        response = _request_json("GET", f"{BASE_URL}/ticker/24hr", timeout=8)
        if response is not None and response.status_code == 200:
            data = _safe_json(response)
            if isinstance(data, dict):
                data = data.get("data", data.get("ticker", []))
            if not isinstance(data, list):
                return []

            usdt_pairs = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol", "")).replace("/", "").upper()
                if not symbol.endswith("USDT"):
                    continue
                if supported and symbol not in supported:
                    continue
                status = str(item.get("status", "")).upper()
                if status and status not in {"1", "TRADING", "ENABLED", "ONLINE"}:
                    if status in {"0", "DISABLED", "OFFLINE", "CLOSED", "HALT", "BREAK"}:
                        continue
                try:
                    quote_volume = float(item.get("quoteVolume", 0) or 0)
                except (TypeError, ValueError):
                    quote_volume = 0.0
                usdt_pairs.append((symbol, quote_volume))

            usdt_pairs.sort(key=lambda x: x[1], reverse=True)
            symbols = [symbol for symbol, _ in usdt_pairs[:200]]
            if symbols:
                return symbols

        if response is not None:
            print(f"[API WARN] get_top_200_symbols returned HTTP {response.status_code}")
    except Exception as e:
        print(f"Top-200 error: {e}")
    return []


def get_mexc_real_price(symbol):
    try:
        formatted_symbol = symbol.replace("/", "").upper()
        response = _request_json(
            "GET",
            f"{BASE_URL}/ticker/price",
            params={"symbol": formatted_symbol},
            timeout=4,
        )
        if response is not None and response.status_code == 200:
            payload = _safe_json(response)
            return float(payload["price"])
    except Exception as e:
        print(f"Price error: {e}")
    return None


# =========================
# Account / orders
# =========================
def get_account_balances(api_key, secret_key):
    try:
        response = _signed_request("GET", "/account", api_key, secret_key, timeout=6, retries_on_network=True)
        if response is None or response.status_code != 200:
            if response is not None and _is_timestamp_error(response):
                response = _signed_request(
                    "GET", "/account", api_key, secret_key,
                    timeout=6, retries_on_network=True, force_time_sync=True
                )
        if response is not None and response.status_code == 200:
            payload = _safe_json(response)
            balances = payload.get("balances", []) if isinstance(payload, dict) else []
            return balances if isinstance(balances, list) else []
        print(f"[ACCOUNT] {_api_error_message(response)}")
    except Exception as e:
        print(f"Balance error: {e}")
    return []


def get_symbol_free_balance(symbol, api_key, secret_key):
    try:
        asset_name = symbol.replace("USDT", "").replace("/", "").upper()
        balances = get_account_balances(api_key, secret_key)
        for item in balances:
            if str(item.get("asset", "")).upper() == asset_name:
                return float(item.get("free", 0) or 0)
    except Exception as e:
        print(f"Balance error: {e}")
    return 0.0


def query_mexc_order(symbol, order_id, api_key, secret_key):
    try:
        params = {"symbol": symbol.replace("/", "").upper(), "orderId": order_id}
        response = _signed_request(
            "GET", "/order", api_key, secret_key, params=params, timeout=6, retries_on_network=True
        )
        if response is not None and _is_timestamp_error(response):
            response = _signed_request(
                "GET", "/order", api_key, secret_key,
                params=params, timeout=6, retries_on_network=True, force_time_sync=True
            )
        if response is not None and response.status_code == 200:
            return _safe_json(response)
        print(f"[ORDER QUERY] {_api_error_message(response)}")
    except Exception as e:
        print(f"[ORDER QUERY] Failed: {e}")
    return None


def _average_fill_price(order_data):
    if not isinstance(order_data, dict):
        return None

    for key in ("avgPrice", "averagePrice", "dealAvgPrice"):
        value = order_data.get(key)
        try:
            if value is not None and float(value) > 0:
                return float(value)
        except (TypeError, ValueError):
            pass

    executed_qty = order_data.get("executedQty") or order_data.get("executedQuantity")
    quote_qty = (
        order_data.get("cummulativeQuoteQty")
        or order_data.get("cumQuoteQty")
        or order_data.get("executedQuoteQty")
        or order_data.get("cumulativeAmount")
    )
    try:
        if executed_qty and quote_qty and float(executed_qty) > 0:
            return float(quote_qty) / float(executed_qty)
    except (TypeError, ValueError, ZeroDivisionError):
        pass

    fills = order_data.get("fills")
    if isinstance(fills, list) and fills:
        total_qty = Decimal("0")
        total_quote = Decimal("0")
        for fill in fills:
            try:
                price = Decimal(str(fill.get("price")))
                qty = Decimal(str(fill.get("qty") or fill.get("quantity")))
                if price > 0 and qty > 0:
                    total_qty += qty
                    total_quote += price * qty
            except Exception:
                continue
        if total_qty > 0:
            return float(total_quote / total_qty)

    return None


def _order_status(order_data):
    if not isinstance(order_data, dict):
        return ""
    return str(order_data.get("status") or order_data.get("state") or "").upper()


def _executed_quantity(order_data):
    if not isinstance(order_data, dict):
        return Decimal("0")
    for key in ("executedQty", "executedQuantity", "dealQuantity"):
        try:
            value = order_data.get(key)
            if value is not None:
                return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            pass
    return Decimal("0")


def _wait_for_order_result(symbol, order_id, api_key, secret_key, timeout_seconds=4.0):
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        last = query_mexc_order(symbol, order_id, api_key, secret_key)
        if isinstance(last, dict):
            status = _order_status(last)
            executed = _executed_quantity(last)
            if executed > 0:
                return last
            if status in {"CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}:
                return last
            if status in {"FILLED", "PARTIALLY_FILLED"} and executed >= 0:
                return last
        time.sleep(0.35)
    return last


def _extract_order_id(payload):
    if not isinstance(payload, dict):
        return None
    return payload.get("orderId") or payload.get("orderID") or payload.get("id")


def place_mexc_buy_order(symbol, amount_usd, api_key, secret_key):
    if not api_key or not secret_key:
        return False, "API Key / Secret Key missing", 0.0

    try:
        amount = Decimal(str(amount_usd))
        if amount <= 0:
            return False, "Trade amount must be greater than zero", 0.0
        rules = get_symbol_rules(symbol)
        if rules:
            min_notional = rules.get("min_notional") or Decimal("0")
            if min_notional > 0 and amount < min_notional:
                return False, f"Trade amount {amount} is below minimum notional {min_notional} for {symbol}", 0.0

        params = {
            "symbol": symbol.replace("/", "").upper(),
            "side": "BUY",
            "type": "MARKET",
            "quoteOrderQty": _decimal_to_string(amount),
        }

        response = _signed_request(
            "POST", "/order", api_key, secret_key,
            params=params, timeout=8, retries_on_network=False
        )

        if response is not None and _is_timestamp_error(response):
            response = _signed_request(
                "POST", "/order", api_key, secret_key,
                params=params, timeout=8, retries_on_network=False, force_time_sync=True
            )

        if response is None:
            return False, "No response from exchange. Verify the order on MEXC before retrying.", 0.0

        res_data = _safe_json(response)
        if response.status_code != 200 or not _extract_order_id(res_data):
            return False, _api_error_message(response), 0.0

        order_id = _extract_order_id(res_data)
        order_details = _wait_for_order_result(symbol, order_id, api_key, secret_key, timeout_seconds=4.0)
        if not isinstance(order_details, dict):
            return False, f"BUY order {order_id} was accepted but its execution could not be verified. Check MEXC Order History before retrying.", 0.0

        status = _order_status(order_details)
        executed_qty = _executed_quantity(order_details)
        fill_price = _average_fill_price(order_details)

        if executed_qty <= 0 or not fill_price or fill_price <= 0:
            return False, (
                f"BUY order {order_id} was not verified as executed (status={status or 'UNKNOWN'}, "
                f"executedQty={executed_qty}). Check MEXC before retrying."
            ), 0.0

        return True, (
            f"Order ID: {order_id} | Status: {status or 'UNKNOWN'} | "
            f"Executed Qty: {executed_qty} | Actual Avg Fill: {fill_price:.12f}"
        ), float(fill_price)

    except requests.RequestException as e:
        return False, f"Network error while placing BUY. Verify the order on MEXC before retrying: {e}", 0.0
    except Exception as e:
        return False, str(e), 0.0


def place_mexc_sell_order_market(symbol, api_key, secret_key):
    try:
        free_qty = get_symbol_free_balance(symbol, api_key, secret_key)
        if free_qty <= 0:
            return True, "Position already closed on exchange (0 balance)", get_mexc_real_price(symbol) or 0.0

        market_price = get_mexc_real_price(symbol)
        quantity, qty_error = normalize_order_quantity(symbol, free_qty, price=market_price)
        if qty_error:
            return False, qty_error, 0.0
        if quantity <= 0:
            return False, "Tradable balance is below the symbol's minimum quantity", 0.0

        params = {
            "symbol": symbol.replace("/", "").upper(),
            "side": "SELL",
            "type": "MARKET",
            "quantity": _decimal_to_string(quantity),
        }

        response = _signed_request(
            "POST", "/order", api_key, secret_key,
            params=params, timeout=8, retries_on_network=False
        )
        if response is not None and _is_timestamp_error(response):
            response = _signed_request(
                "POST", "/order", api_key, secret_key,
                params=params, timeout=8, retries_on_network=False, force_time_sync=True
            )

        if response is None:
            return False, "No response from exchange on sell. Verify order status on MEXC before retrying.", 0.0

        res_data = _safe_json(response)
        order_id = _extract_order_id(res_data)
        if response.status_code != 200 or not order_id:
            return False, f"Exchange rejected: {_api_error_message(response)}", 0.0

        order_details = _wait_for_order_result(symbol, order_id, api_key, secret_key, timeout_seconds=4.0)
        if not isinstance(order_details, dict):
            return False, f"SELL order {order_id} accepted but execution could not be verified. Check MEXC before retrying.", 0.0

        status = _order_status(order_details)
        executed_qty = _executed_quantity(order_details)
        exit_price = _average_fill_price(order_details)
        if executed_qty <= 0 or not exit_price or exit_price <= 0:
            return False, (
                f"SELL order {order_id} was not verified as executed (status={status or 'UNKNOWN'}, "
                f"executedQty={executed_qty}). Check MEXC before retrying."
            ), 0.0

        return True, (
            f"Sell order executed successfully | Order ID: {order_id} | Status: {status or 'UNKNOWN'} | "
            f"Executed Qty: {executed_qty} | Actual Avg Fill: {exit_price:.12f}"
        ), float(exit_price)

    except requests.RequestException as e:
        return False, f"Network error while placing SELL. Verify the order on MEXC before retrying: {e}", 0.0
    except Exception as e:
        return False, f"Connection error: {str(e)}", 0.0


# =========================
# Strategy / indicators
# =========================
def _get_klines(symbol, interval, limit=500):
    response = _request_json(
        "GET",
        f"{BASE_URL}/klines",
        params={
            "symbol": symbol.replace("/", "").upper(),
            "interval": interval,
            "limit": limit,
        },
        timeout=5,
    )
    if response is None or response.status_code != 200:
        return None
    payload = _safe_json(response)
    return payload if isinstance(payload, list) else None


def calculate_ema_series(data, period):
    if len(data) < period:
        return []
    ema = []
    multiplier = 2 / (period + 1)
    sma = sum(data[:period]) / period
    ema.append(sma)
    for price in data[period:]:
        ema.append((price - ema[-1]) * multiplier + ema[-1])
    return ema


def calculate_rsi_series(closes, period=14):
    """حساب مؤشر القوة النسبية RSI"""
    if len(closes) < period + 1:
        return []
    
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        if diff >= 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(diff))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    rsi_series = []
    if avg_loss == 0:
        rsi_series.append(100.0)
    else:
        rs = avg_gain / avg_loss
        rsi_series.append(100.0 - (100.0 / (1.0 + rs)))

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi_series.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi_series.append(100.0 - (100.0 / (1.0 + rs)))

    return rsi_series


def calculate_vwap_latest(klines):
    """حساب مؤشر متوسط السعر المرجح بالحجم VWAP للشمعة الأخيرة"""
    try:
        total_pv = 0.0
        total_vol = 0.0
        for k in klines:
            high = float(k[2])
            low = float(k[3])
            close = float(k[4])
            vol = float(k[5])
            typical_price = (high + low + close) / 3.0
            total_pv += typical_price * vol
            total_vol += vol
        if total_vol > 0:
            return total_pv / total_vol
    except Exception:
        pass
    return None


def calculate_psar_latest(klines, step=0.02, max_step=0.2):
    """حساب مؤشر البارابوليك سار Parabolic SAR للشمعة الأخيرة"""
    if len(klines) < 5:
        return None
    try:
        highs = [float(k[2]) for k in klines]
        lows = [float(k[3]) for k in klines]
        closes = [float(k[4]) for k in klines]

        is_long = True
        sar = lows[0]
        ep = highs[0]
        af = step

        for i in range(1, len(klines)):
            prev_sar = sar
            if is_long:
                sar = prev_sar + af * (ep - prev_sar)
                sar = min(sar, lows[i - 1], lows[max(0, i - 2)])
                if lows[i] < sar:
                    is_long = False
                    sar = ep
                    ep = lows[i]
                    af = step
                else:
                    if highs[i] > ep:
                        ep = highs[i]
                        af = min(af + step, max_step)
            else:
                sar = prev_sar + af * (ep - prev_sar)
                sar = max(sar, highs[i - 1], highs[max(0, i - 2)])
                if highs[i] > sar:
                    is_long = True
                    sar = ep
                    ep = highs[i]
                    af = step
                else:
                    if lows[i] < ep:
                        ep = lows[i]
                        af = min(af + step, max_step)
        return sar
    except Exception:
        return None


def calculate_macd_series(closes, fast_period=12, slow_period=26, signal_period=9):
    """حساب مؤشر MACD (Line, Signal Line, Histogram)"""
    if len(closes) < slow_period + signal_period:
        return [], [], []

    ema_fast = calculate_ema_series(closes, fast_period)
    ema_slow = calculate_ema_series(closes, slow_period)

    # توحيد أطوال السلاسل الزمنية للـ MACD Line
    diff_len = len(ema_fast) - len(ema_slow)
    ema_fast_trimmed = ema_fast[diff_len:]

    macd_line = [f - s for f, s in zip(ema_fast_trimmed, ema_slow)]
    signal_line = calculate_ema_series(macd_line, signal_period)

    # توحيد الأطوال للـ Histogram
    diff_sig = len(macd_line) - len(signal_line)
    macd_line_trimmed = macd_line[diff_sig:]

    histogram = [m - s for m, s in zip(macd_line_trimmed, signal_line)]

    return macd_line_trimmed, signal_line, histogram


def check_ema200_trend(formatted_symbol, interval):
    try:
        klines = _get_klines(formatted_symbol, interval, 500)
        if not klines or len(klines) < 201:
            return False
        closes = [float(k[4]) for k in klines[:-1]]
        ema200 = calculate_ema_series(closes, 200)
        return bool(ema200 and closes[-1] > ema200[-1])
    except Exception:
        return False


def _ema_value_at_candle(ema_series, period, candle_index):
    ema_index = candle_index - (period - 1)
    if 0 <= ema_index < len(ema_series):
        return ema_series[ema_index]
    return None


def check_trade_conditions_from_main(symbol):
    try:
        formatted_symbol = symbol.replace("/", "").upper()

        # الاتجاه العام عبر الأطر الزمنية المختلفة
        if not check_ema200_trend(formatted_symbol, "5m"):
            return False, 0.0, "5m trend not bullish"

        if not check_ema200_trend(formatted_symbol, "15m"):
            return False, 0.0, "15m trend not bullish"

        if not check_ema200_trend(formatted_symbol, "60m"):
            return False, 0.0, "60m trend not bullish"

        klines = _get_klines(formatted_symbol, "5m", 500)
        if not klines or len(klines) < 201:
            return False, 0.0, "Insufficient kline data"

        closed_klines = klines[:-1]

        # 1. الشمعة الحالية
        last_kline = klines[-1]
        lastopen  = float(last_kline[1])
        lastclose = float(last_kline[4])

        # 2. الشمعة السابقة
        close_kline = klines[-2]
        closeopen  = float(close_kline[1])
        closelow   = float(close_kline[3])
        closeclose = float(close_kline[4])

        closes  = [float(k[4]) for k in closed_klines]
        volumes = [float(k[5]) for k in closed_klines]

        if len(closes) < 200 or len(volumes) < 100:
            return False, 0.0, "Insufficient data"

        last_closed_price = closes[-1]

        ema9_series   = calculate_ema_series(closes, 9)
        ema21_series  = calculate_ema_series(closes, 21)
        ema200_series = calculate_ema_series(closes, 200)

        ema9_now   = _ema_value_at_candle(ema9_series, 9, len(closes) - 1)
        ema21_now  = _ema_value_at_candle(ema21_series, 21, len(closes) - 1)
        ema200_now = _ema_value_at_candle(ema200_series, 200, len(closes) - 1)

        if None in (ema9_now, ema21_now, ema200_now):
            return False, last_closed_price, "EMA data unavailable"

        # --- المؤشرات التوكيدية (RSI, VWAP, PSAR) ---
        rsi_series = calculate_rsi_series(closes, 14)
        rsi_now = rsi_series[-1] if rsi_series else None

        vwap_now = calculate_vwap_latest(closed_klines)
        psar_now = calculate_psar_latest(closed_klines)

        # --- حساب مؤشر MACD ---
        macd_line, signal_line, histogram = calculate_macd_series(closes)
        macd_now = macd_line[-1] if macd_line else None
        signal_now = signal_line[-1] if signal_line else None
        hist_now = histogram[-1] if histogram else None

        # التحقق من شروط المؤشرات
        rsi_ok  = rsi_now is not None and (45 < rsi_now < 68)
        vwap_ok = vwap_now is not None and (lastclose > vwap_now)
        psar_ok = psar_now is not None and (psar_now < lastclose)
        macd_ok = (
            macd_now is not None 
            and signal_now is not None 
            and hist_now is not None 
            and (macd_now > signal_now) # تقاطع صعودي للـ MACD
            and (hist_now > 0)           # الأعمدة الملونة موجبة
        )

        # الشرط المصحح المكتمل مع المؤشرات التوكيدية بما فيها MACD
        if (
            ema9_now > ema21_now
            and ema21_now > ema200_now
            and lastclose > lastopen        # الشمعة الحالية خضراء
            and closeclose > closeopen      # الشمعة المغلقة خضراء
            and closelow <= ema21_now       # أدنى سعر للشمعة المغلقة لامس/تجاوز EMA21
            and lastclose > ema21_now       # إغلاق الشمعة الحالية أعلى من إغلاق الشمعة المغلقة
            and rsi_ok                      # RSI في المدى المناسب للزخم
                          
            and psar_ok                     # نقاط SAR أسفل السعر الحالي
            and macd_ok                     # مؤشر MACD صعودي
        ):
            return True, lastclose, "Signal conditions confirmed"

        return False, lastclose, "Conditions not complete"

    except Exception as e:
        return False, 0.0, f"Error: {e}"


init_db()
