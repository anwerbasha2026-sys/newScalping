import math
import tempfile
import os

# Use an isolated DB before importing the module.
tmp = tempfile.NamedTemporaryFile(delete=False)
tmp.close()
os.environ["NEWSCALPING_TEST_DB"] = tmp.name

# The module uses DB_NAME directly, so patch it after import and initialize.
import mexc_core_auto_recycle as core
core.DB_NAME = tmp.name
core.init_db()

def test_ema_and_rsi():
    vals = [100 + i for i in range(40)]
    assert core.ema(vals, 9)
    r = core.rsi(vals, 14)
    assert r and r[-1] > 90

def test_macd():
    vals = [100 + math.sin(i/3) + i*0.1 for i in range(80)]
    line, sig, hist = core.macd(vals)
    assert line and sig and hist

def test_net_pnl_is_less_than_gross_for_positive_move():
    gross = (105-100)/100*100
    net_usd, net_pct = core.estimate_net_pnl(100,100,105)
    assert net_pct < gross
    assert net_usd > 0

def test_position_roundtrip():
    core.save_active_position("TESTUSDT",100,79,1.5,2,0.7,"123")
    p = core.get_active_position()
    assert p["symbol"] == "TESTUSDT"
    assert p["executed_qty"] == 0.7
    assert p["entry_order_id"] == "123"
    core.clear_active_position()
    assert core.get_active_position() is None
