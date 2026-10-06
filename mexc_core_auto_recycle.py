import time
import sqlite3
import requests
import hmac
import hashlib
import threading
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import datetime
from urllib.parse import urlencode

BASE_URL = "https://api.mexc.com/api/v3"
DB_NAME = "trading_bot.db"
API_MIN_INTERVAL = 0.12
FEE_RATE = 0.001  # conservative default for net-PnL estimation; exchange/account fee may differ

_api_lock = threading.Lock()
_api_last_request = 0.0
_server_offset_ms = 0
_server_sync_at = 0.0

_rules_lock = threading.Lock()
_rules_cache = {}
_rules_cache_at = 0.0
RULES_TTL = 900.0

_db_lock = threading.RLock()


def _get_db():
    conn = sqlite3.connect(DB_NAME, timeout=15)
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with _db_lock:
        conn = _get_db()
        try:
            conn.execute("""CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY, value TEXT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS active_position(
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                entry_price REAL NOT NULL,
                amount REAL NOT NULL,
                tp_percent REAL NOT NULL,
                sl_percent REAL NOT NULL,
                executed_qty REAL DEFAULT 0,
                entry_order_id TEXT DEFAULT '',
                opened_at TEXT DEFAULT '')""")
            conn.execute("""CREATE TABLE IF NOT EXISTS closed_trades(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, entry_price REAL, exit_price REAL, amount REAL,
                pnl_usd REAL, pnl_percent REAL, reason TEXT, timestamp TEXT)""")
            # Non-destructive migration for databases created by older versions.
            cols = {row[1] for row in conn.execute("PRAGMA table_info(active_position)")}
            for name, ddl in [
                ("executed_qty", "ALTER TABLE active_position ADD COLUMN executed_qty REAL DEFAULT 0"),
                ("entry_order_id", "ALTER TABLE active_position ADD COLUMN entry_order_id TEXT DEFAULT ''"),
                ("opened_at", "ALTER TABLE active_position ADD COLUMN opened_at TEXT DEFAULT ''"),
            ]:
                if name not in cols:
                    conn.execute(ddl)
            conn.commit()
        finally:
            conn.close()


def save_setting(key, value):
    with _db_lock:
        conn = _get_db()
        try:
            conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, str(value)))
            conn.commit()
        finally:
            conn.close()


def get_setting(key, default=""):
    with _db_lock:
        conn = _get_db()
        try:
            row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            return row[0] if row else default
        finally:
            conn.close()


def save_api_credentials(api_key, secret_key):
    save_setting("mexc_api_key", api_key.strip())
    save_setting("mexc_secret_key", secret_key.strip())


def get_api_credentials():
    return get_setting("mexc_api_key", ""), get_setting("mexc_secret_key", "")


def save_active_position(symbol, entry_price, amount, tp_percent, sl_percent,
                         executed_qty=0, entry_order_id="", opened_at=None):
    opened_at = opened_at or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with _db_lock:
        conn = _get_db()
        try:
            # Single-position invariant.
            conn.execute("DELETE FROM active_position")
            conn.execute("""INSERT INTO active_position
                (id,symbol,entry_price,amount,tp_percent,sl_percent,executed_qty,entry_order_id,opened_at)
                VALUES(1,?,?,?,?,?,?,?,?)""",
                (symbol, float(entry_price), float(amount), float(tp_percent), float(sl_percent),
                 float(executed_qty or 0), str(entry_order_id or ""), opened_at))
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
            row = conn.execute("""SELECT symbol,entry_price,amount,tp_percent,sl_percent,
                executed_qty,entry_order_id,opened_at FROM active_position WHERE id=1""").fetchone()
            if not row:
                return None
            return {
                "symbol": row[0], "entry_price": row[1], "amount": row[2],
                "tp_percent": row[3], "sl_percent": row[4],
                "executed_qty": row[5] or 0, "entry_order_id": row[6] or "",
                "opened_at": row[7] or "",
            }
        finally:
            conn.close()


def record_closed_trade(symbol, entry_price, exit_price, amount, pnl_usd, pnl_percent, reason):
    with _db_lock:
        conn = _get_db()
        try:
            conn.execute("""INSERT INTO closed_trades
                (symbol,entry_price,exit_price,amount,pnl_usd,pnl_percent,reason,timestamp)
                VALUES(?,?,?,?,?,?,?,?)""",
                (symbol,float(entry_price),float(exit_price),float(amount),
                 float(pnl_usd),float(pnl_percent),reason,
                 datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit()
        finally:
            conn.close()


def _wait():
    global _api_last_request
    with _api_lock:
        now = time.monotonic()
        delay = API_MIN_INTERVAL - (now - _api_last_request)
        if delay > 0:
            time.sleep(delay)
        _api_last_request = time.monotonic()


def _request(method, url, retry=True, **kwargs):
    kwargs.setdefault("timeout", 6)
    last = None
    for attempt in range(3):
        _wait()
        try:
            response = requests.request(method, url, **kwargs)
            last = response
            if response.status_code in (418,429,500,502,503,504):
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = min(float(retry_after), 8.0) if retry_after else min(0.75 * (2 ** attempt), 8.0)
                except (TypeError, ValueError):
                    delay = min(0.75 * (2 ** attempt), 8.0)
                time.sleep(delay)
                continue
            return response
        except requests.RequestException:
            if not retry or attempt >= 2:
                raise
            time.sleep(0.5 * (2 ** attempt))
    return last


def _json(response):
    try:
        return response.json() if response is not None else {}
    except Exception:
        return {}


def _err(response):
    data = _json(response)
    return str(data.get("msg") or data.get("message") or data or f"HTTP {getattr(response,'status_code','unknown')}")


def sync_mexc_server_time(force=False):
    global _server_offset_ms, _server_sync_at
    now = time.monotonic()
    if not force and now - _server_sync_at < 30:
        return _server_offset_ms
    try:
        t0 = int(time.time() * 1000)
        r = requests.get(f"{BASE_URL}/time", timeout=3)
        t1 = int(time.time() * 1000)
        if r.status_code == 200:
            server_ms = int(_json(r).get("serverTime"))
            _server_offset_ms = server_ms - ((t0 + t1) // 2)
            _server_sync_at = now
    except Exception:
        pass
    return _server_offset_ms


def _timestamp(force=False):
    sync_mexc_server_time(force)
    return int(time.time() * 1000) + _server_offset_ms


def _signed(method, path, api_key, secret_key, params=None, force_time=False):
    params = dict(params or {})
    params["recvWindow"] = 10000
    params["timestamp"] = _timestamp(force_time)
    query = urlencode(params)
    sig = hmac.new(secret_key.encode(), query.encode(), hashlib.sha256).hexdigest()
    params["signature"] = sig
    return _request(method, BASE_URL + path, headers={
        "X-MEXC-APIKEY": api_key, "Content-Type": "application/json"
    }, params=params, retry=False, timeout=8)


def _timestamp_error(response):
    data = _json(response)
    text = f"{data.get('code','')} {data.get('msg','')} {data.get('message','')}".lower()
    return "timestamp" in text or ("recvwindow" in text) or ("outside" in text and "window" in text)


def _dec(v):
    return Decimal(str(v))


def _fmt(v):
    s = format(_dec(v), "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


def _round_down(v, step):
    v, step = _dec(v), _dec(step)
    if step <= 0:
        return v
    return (v / step).to_integral_value(rounding=ROUND_DOWN) * step


def _extract_rules(info):
    filters = info.get("filters") or []
    step = None
    min_qty = Decimal("0")
    min_notional = Decimal("0")
    for f in filters:
        if not isinstance(f, dict):
            continue
        typ = str(f.get("filterType","")).upper()
        if typ in ("LOT_SIZE","MARKET_LOT_SIZE"):
            step = f.get("stepSize") or f.get("step") or step
            min_qty = f.get("minQty") or f.get("minQuantity") or min_qty
        if typ in ("MIN_NOTIONAL","NOTIONAL"):
            min_notional = f.get("minNotional") or f.get("notional") or min_notional
    if step is None:
        qp = info.get("quantityPrecision")
        if qp is not None:
            try: step = str(Decimal("1").scaleb(-int(qp)))
            except Exception: pass
    return {
        "step": _dec(step or "0"),
        "min_qty": _dec(min_qty or "0"),
        "min_notional": _dec(min_notional or "0"),
        "status": str(info.get("status", info.get("state",""))).upper(),
    }


def get_symbol_rules(symbol, force=False):
    global _rules_cache_at
    symbol = symbol.replace("/","").upper()
    now = time.monotonic()
    with _rules_lock:
        if symbol in _rules_cache and not force and now - _rules_cache_at < RULES_TTL:
            return _rules_cache[symbol]
    try:
        r = _request("GET", f"{BASE_URL}/exchangeInfo", timeout=8)
        if not r or r.status_code != 200:
            return None
        data = _json(r)
        items = data.get("symbols", []) if isinstance(data, dict) else data
        local = {}
        for item in items or []:
            if isinstance(item, dict) and item.get("symbol"):
                name = str(item["symbol"]).replace("/","").upper()
                local[name] = _extract_rules(item)
        with _rules_lock:
            _rules_cache.update(local)
            _rules_cache_at = now
        return local.get(symbol)
    except Exception:
        return None


def normalize_order_quantity(symbol, quantity, price=None):
    rules = get_symbol_rules(symbol)
    q = _dec(quantity)
    if not rules:
        return q, None
    q = _round_down(q, rules["step"])
    if q < rules["min_qty"]:
        return q, f"Quantity {q} is below minimum {rules['min_qty']}."
    if price is not None and rules["min_notional"] > 0 and q * _dec(price) < rules["min_notional"]:
        return q, f"Order value {q * _dec(price)} is below minimum notional {rules['min_notional']}."
    return q, None


def get_top_200_symbols():
    try:
        r = _request("GET", f"{BASE_URL}/ticker/24hr", timeout=8)
        if not r or r.status_code != 200:
            return []
        data = _json(r)
        if isinstance(data, dict):
            data = data.get("data", data.get("ticker", []))
        pairs = []
        for item in data or []:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol","")).replace("/","").upper()
            if not symbol.endswith("USDT"):
                continue
            try:
                volume = float(item.get("quoteVolume", 0) or 0)
            except (TypeError, ValueError):
                volume = 0
            if volume <= 0:
                continue
            pairs.append((symbol, volume))
        pairs.sort(key=lambda x:x[1], reverse=True)
        return [s for s,_ in pairs[:200]]
    except Exception as e:
        print(f"[TOP200] {e}")
        return []


def get_mexc_real_price(symbol):
    try:
        r = _request("GET", f"{BASE_URL}/ticker/price",
                     params={"symbol":symbol.replace("/","").upper()}, timeout=4)
        if r and r.status_code == 200:
            return float(_json(r).get("price"))
    except Exception:
        pass
    return None


def get_account_balances(api_key, secret_key):
    try:
        r = _signed("GET","/account",api_key,secret_key)
        if r and _timestamp_error(r):
            r = _signed("GET","/account",api_key,secret_key,force_time=True)
        if r and r.status_code == 200:
            return _json(r).get("balances", [])
    except Exception as e:
        print(f"[ACCOUNT] {e}")
    return []


def get_symbol_free_balance(symbol, api_key, secret_key):
    asset = symbol.replace("USDT","").replace("/","").upper()
    for b in get_account_balances(api_key,secret_key):
        if str(b.get("asset","")).upper() == asset:
            try: return float(b.get("free",0) or 0)
            except Exception: return 0.0
    return 0.0


def _order_id(data):
    return data.get("orderId") or data.get("orderID") or data.get("id") if isinstance(data,dict) else None


def query_mexc_order(symbol, order_id, api_key, secret_key):
    try:
        params={"symbol":symbol.replace("/","").upper(),"orderId":order_id}
        r=_signed("GET","/order",api_key,secret_key,params)
        if r and _timestamp_error(r):
            r=_signed("GET","/order",api_key,secret_key,params,force_time=True)
        return _json(r) if r and r.status_code == 200 else None
    except Exception:
        return None


def _executed_qty(order):
    for key in ("executedQty","executedQuantity","dealQuantity"):
        try:
            if order.get(key) is not None:
                return _dec(order[key])
        except Exception:
            pass
    return Decimal("0")


def _avg_fill(order):
    for key in ("avgPrice","averagePrice","dealAvgPrice"):
        try:
            v=float(order.get(key))
            if v>0: return v
        except Exception: pass
    qty=_executed_qty(order)
    quote=order.get("cummulativeQuoteQty") or order.get("cumQuoteQty") or order.get("executedQuoteQty")
    try:
        if qty>0 and quote is not None:
            return float(_dec(quote)/qty)
    except Exception: pass
    return None


def _wait_order(symbol, oid, api_key, secret_key, timeout=6):
    deadline=time.monotonic()+timeout
    last=None
    while time.monotonic()<deadline:
        last=query_mexc_order(symbol,oid,api_key,secret_key)
        if last:
            status=str(last.get("status",last.get("state",""))).upper()
            qty=_executed_qty(last)
            if qty>0 or status in {"CANCELED","CANCELLED","REJECTED","EXPIRED"}:
                return last
        time.sleep(0.35)
    return last


def place_mexc_buy_order(symbol, amount_usd, api_key, secret_key):
    try:
        amount=_dec(amount_usd)
        if amount<=0: return False,"Trade amount must be > 0",0.0,0,""
        rules=get_symbol_rules(symbol)
        if rules and rules["min_notional"]>0 and amount<rules["min_notional"]:
            return False,f"Amount {amount} is below minimum notional {rules['min_notional']}",0.0,0,""
        params={"symbol":symbol.replace("/","").upper(),"side":"BUY","type":"MARKET",
                "quoteOrderQty":_fmt(amount)}
        r=_signed("POST","/order",api_key,secret_key,params)
        if r and _timestamp_error(r):
            r=_signed("POST","/order",api_key,secret_key,params,force_time=True)
        data=_json(r)
        oid=_order_id(data)
        if not r or r.status_code != 200 or not oid:
            return False,_err(r),0.0,0,""
        order=_wait_order(symbol,oid,api_key,secret_key)
        qty=_executed_qty(order or {})
        price=_avg_fill(order or {})
        if qty<=0 or not price:
            return False,f"BUY {oid} accepted but execution is unverified. Check MEXC before retrying.",0.0,0,oid
        return True,f"BUY verified | order={oid} | qty={qty} | avg={price:.12f}",float(price),float(qty),str(oid)
    except requests.RequestException as e:
        return False,f"Network error on BUY. Verify MEXC before retrying: {e}",0.0,0,""
    except Exception as e:
        return False,str(e),0.0,0,""


def place_mexc_sell_order_market(symbol, api_key, secret_key):
    try:
        free=get_symbol_free_balance(symbol,api_key,secret_key)
        if free<=0:
            return True,"No free asset balance; position appears closed on exchange.",0.0,0.0,""
        price=get_mexc_real_price(symbol)
        qty,error=normalize_order_quantity(symbol,free,price)
        if error or qty<=0:
            return False,error or "Quantity below minimum.",0.0,0.0,""
        params={"symbol":symbol.replace("/","").upper(),"side":"SELL","type":"MARKET","quantity":_fmt(qty)}
        r=_signed("POST","/order",api_key,secret_key,params)
        if r and _timestamp_error(r):
            r=_signed("POST","/order",api_key,secret_key,params,force_time=True)
        data=_json(r); oid=_order_id(data)
        if not r or r.status_code != 200 or not oid:
            return False,_err(r),0.0,0.0,""
        order=_wait_order(symbol,oid,api_key,secret_key)
        executed=_executed_qty(order or {})
        exit_price=_avg_fill(order or {})
        if executed<=0 or not exit_price:
            return False,f"SELL {oid} accepted but execution is unverified. Check MEXC before retrying.",0.0,0.0,str(oid)
        return True,f"SELL verified | order={oid} | qty={executed} | avg={exit_price:.12f}",float(exit_price),float(executed),str(oid)
    except requests.RequestException as e:
        return False,f"Network error on SELL. Verify MEXC before retrying: {e}",0.0,0.0,""
    except Exception as e:
        return False,str(e),0.0,0.0,""


def _get_klines(symbol, interval, limit=250):
    r=_request("GET",f"{BASE_URL}/klines",params={
        "symbol":symbol.replace("/","").upper(),"interval":interval,"limit":limit},timeout=6)
    if not r or r.status_code!=200: return None
    data=_json(r)
    return data if isinstance(data,list) else None


def _closed(klines):
    return klines[:-1] if klines and len(klines)>1 else []


def ema(values, period):
    if len(values)<period: return []
    k=2/(period+1)
    out=[sum(values[:period])/period]
    for x in values[period:]:
        out.append((x-out[-1])*k+out[-1])
    return out


def rsi(values, period=14):
    if len(values)<period+1: return []
    gains=[]; losses=[]
    for a,b in zip(values,values[1:]):
        d=b-a; gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains[:period])/period; al=sum(losses[:period])/period
    out=[]
    def calc():
        return 100.0 if al==0 else 100-(100/(1+ag/al))
    out.append(calc())
    for i in range(period,len(gains)):
        ag=(ag*(period-1)+gains[i])/period
        al=(al*(period-1)+losses[i])/period
        out.append(100.0 if al==0 else 100-(100/(1+ag/al)))
    return out


def macd(values, fast=12, slow=26, signal=9):
    ef=ema(values,fast); es=ema(values,slow)
    if not ef or not es: return [],[],[]
    ef=ef[len(ef)-len(es):]
    line=[a-b for a,b in zip(ef,es)]
    sig=ema(line,signal)
    if not sig: return [],[],[]
    line2=line[len(line)-len(sig):]
    hist=[a-b for a,b in zip(line2,sig)]
    return line2,sig,hist


def vwap(klines, window=96):
    data=klines[-window:]
    pv=0.0; vol=0.0
    for k in data:
        tp=(float(k[2])+float(k[3])+float(k[4]))/3
        q=float(k[5])
        pv += tp*q; vol += q
    return pv/vol if vol>0 else None


def psar(klines, step=0.02, max_step=0.2):
    if len(klines)<5: return None
    highs=[float(k[2]) for k in klines]; lows=[float(k[3]) for k in klines]
    long=True; sar=lows[0]; ep=highs[0]; af=step
    for i in range(1,len(klines)):
        sar=sar+af*(ep-sar)
        if long:
            sar=min(sar,lows[i-1],lows[max(0,i-2)])
            if lows[i]<sar:
                long=False; sar=ep; ep=lows[i]; af=step
            elif highs[i]>ep:
                ep=highs[i]; af=min(max_step,af+step)
        else:
            sar=max(sar,highs[i-1],highs[max(0,i-2)])
            if highs[i]>sar:
                long=True; sar=ep; ep=highs[i]; af=step
            elif lows[i]<ep:
                ep=lows[i]; af=min(max_step,af+step)
    return sar


def _trend_from_closed(klines):
    c=[float(k[4]) for k in _closed(klines)]
    if len(c)<205: return False
    e=ema(c,200)
    if len(e)<2: return False
    return c[-1]>e[-1] and e[-1]>=e[-2]


def check_trade_conditions_from_main(symbol):
    """
    Safer scalping signal:
    - never uses the still-forming candle for the decision
    - checks 15m/60m trend first
    - uses 5m pullback/reclaim + momentum confirmation
    """
    try:
        s=symbol.replace("/","").upper()
        k5=_get_klines(s,"5m",250)
        if not k5 or len(k5)<210: return False,0.0,"Insufficient 5m data"
        c5=_closed(k5)
        if len(c5)<205: return False,0.0,"Insufficient closed 5m candles"

        # Cheap first-stage trend gate.
        k15=_get_klines(s,"15m",210)
        if not k15 or not _trend_from_closed(k15): return False,float(c5[-1][4]),"15m trend not bullish"
        k60=_get_klines(s,"60m",210)
        if not k60 or not _trend_from_closed(k60): return False,float(c5[-1][4]),"60m trend not bullish"

        closes=[float(k[4]) for k in c5]
        opens=[float(k[1]) for k in c5]
        lows=[float(k[3]) for k in c5]

        e9=ema(closes,9); e21=ema(closes,21); e200=ema(closes,200)
        if not e9 or not e21 or not e200: return False,closes[-1],"EMA unavailable"
        e9_now,e21_now,e200_now=e9[-1],e21[-1],e200[-1]
        price=closes[-1]
        prev=closes[-2]

        rr=rsi(closes,14); mm,ss,hh=macd(closes)
        r=rr[-1] if rr else None
        v=vwap(c5,96); p=psar(c5)

        # Pullback/reclaim: previous closed candle touched EMA21, latest closed
        # candle reclaimed EMA21 and remains above EMA200.
        touched=lows[-2] <= e21_now * 1.003
        reclaim=price > e21_now and prev <= e21_now * 1.003
        structure=e9_now > e21_now > e200_now and price > e200_now
        candle=closes[-1] > opens[-1]
        momentum=(r is not None and 45 <= r <= 68 and
                  bool(mm and ss and hh) and mm[-1] > ss[-1] and hh[-1] > 0)
        confirmations=(v is not None and price>v and p is not None and p<price)

        if structure and touched and reclaim and candle and momentum and confirmations:
            return True,price,"Closed-candle pullback/reclaim signal confirmed"
        return False,price,"Conditions not complete"
    except Exception as e:
        return False,0.0,f"Strategy error: {e}"


def estimate_net_pnl(amount, entry_price, exit_price):
    if not entry_price or not exit_price:
        return 0.0,0.0
    gross_pct=(exit_price-entry_price)/entry_price*100.0
    # Approximate both-side fees; actual fee may differ by account/VIP/discount.
    net_pct=((1+gross_pct/100.0)*(1-FEE_RATE) - (1+FEE_RATE)) * 100.0
    net_usd=float(amount)*net_pct/100.0
    return net_usd,net_pct


init_db()
