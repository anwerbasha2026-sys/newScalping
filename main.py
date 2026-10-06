__version__ = "2.2.0-fixed"

import threading
import traceback
from datetime import datetime

from kivy.app import App
from kivy.clock import Clock
from kivy.core.window import Window
from kivy.metrics import dp
from kivy.properties import StringProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.checkbox import CheckBox
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView
from kivy.uix.textinput import TextInput
from kivy.uix.popup import Popup

from mexc_core_auto_recycle import (
    get_top_200_symbols, check_trade_conditions_from_main,
    place_mexc_buy_order, place_mexc_sell_order_market,
    get_mexc_real_price, save_active_position, clear_active_position,
    get_active_position, record_closed_trade, save_setting, get_setting,
    save_api_credentials, get_api_credentials, get_symbol_rules,
    estimate_net_pnl,
)


class ScannerThread(threading.Thread):
    def __init__(self, stop_event, on_log, on_signal, on_finished):
        super().__init__(daemon=True)
        self.stop_event=stop_event; self.on_log=on_log
        self.on_signal=on_signal; self.on_finished=on_finished

    def run(self):
        try:
            while not self.stop_event.is_set():
                symbols=get_top_200_symbols()
                if not symbols:
                    self.on_log("[SCAN] Top 200 unavailable; retrying in 5s.")
                    self.stop_event.wait(5); continue
                self.on_log(f"[SCAN] Scanning {len(symbols)} USDT pairs.")
                for i,symbol in enumerate(symbols,1):
                    if self.stop_event.is_set(): break
                    try:
                        valid,price,msg=check_trade_conditions_from_main(symbol)
                        if valid:
                            self.on_log(f"[SIGNAL] {symbol} @ {price:.8f} | {msg}")
                            self.on_signal(symbol,price)
                            self.stop_event.set()
                            return
                    except Exception as exc:
                        self.on_log(f"[WORKER] {symbol}: {type(exc).__name__}: {exc}")
                    # Small cooperative pause; strategy now rejects forming candles.
                    self.stop_event.wait(0.05)
                if not self.stop_event.is_set():
                    self.on_log("[SCAN] Cycle complete; refreshing Top 200.")
        except Exception as exc:
            self.on_log(f"[SCANNER CRASH PREVENTED] {type(exc).__name__}: {exc}")
            self.on_log(traceback.format_exc())
        finally:
            self.on_finished()


class MonitorThread(threading.Thread):
    def __init__(self, stop_event, symbol, entry, amount, tp, sl, on_price, on_close, on_log, on_finished):
        super().__init__(daemon=True)
        self.stop_event=stop_event; self.symbol=symbol
        self.entry=float(entry); self.amount=float(amount)
        self.tp=float(tp); self.sl=float(sl)
        self.on_price=on_price; self.on_close=on_close
        self.on_log=on_log; self.on_finished=on_finished

    def run(self):
        try:
            while not self.stop_event.is_set():
                try:
                    price=get_mexc_real_price(self.symbol)
                    if price and self.entry>0:
                        gross=((price-self.entry)/self.entry)*100.0
                        # Exit trigger uses conservative net estimate, not gross display PnL.
                        _, net_pct=estimate_net_pnl(self.amount,self.entry,price)
                        net_usd,_=estimate_net_pnl(self.amount,self.entry,price)
                        self.on_price(price,net_usd,net_pct)
                        if net_pct >= self.tp:
                            self.on_log(f"[TP] Net target reached: {net_pct:.3f}%")
                            self.on_close("TP"); return
                        if net_pct <= -self.sl:
                            self.on_log(f"[SL] Net stop reached: {net_pct:.3f}%")
                            self.on_close("SL"); return
                except Exception as exc:
                    self.on_log(f"[MONITOR] {type(exc).__name__}: {exc}")
                self.stop_event.wait(1.2)
        except Exception as exc:
            self.on_log(f"[MONITOR CRASH PREVENTED] {type(exc).__name__}: {exc}")
            self.on_log(traceback.format_exc())
        finally:
            self.on_finished()


class MEXCScalperMobile(App):
    status_text=StringProperty("Status: Ready")
    position_text=StringProperty("Position: None")
    pnl_text=StringProperty("PnL: --")

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.scanner=None; self.monitor=None
        self.stop_event=threading.Event()
        self.closing=False; self.closing_position=False
        self.last_price=0.0; self.root_layout=None; self.log_view=None

    def build(self):
        Window.clearcolor=(0.06,0.07,0.09,1)
        Window.softinput_mode="below_target"
        self.root_layout=BoxLayout(orientation="vertical",padding=dp(10),spacing=dp(8))
        self.root_layout.add_widget(Label(text="[b]MEXC Mobile Scalper 2.2[/b]",
            markup=True,size_hint_y=None,height=dp(42),font_size="20sp"))

        scroll=ScrollView()
        content=BoxLayout(orientation="vertical",spacing=dp(8),size_hint_y=None)
        content.bind(minimum_height=content.setter("height"))

        grid=GridLayout(cols=2,spacing=dp(6),size_hint_y=None,height=dp(110))
        grid.add_widget(Label(text="API Key:"))
        self.api_key=TextInput(password=False,multiline=False,size_hint_y=None,height=dp(44)); grid.add_widget(self.api_key)
        grid.add_widget(Label(text="Secret Key:"))
        self.secret_key=TextInput(password=True,multiline=False,size_hint_y=None,height=dp(44)); grid.add_widget(self.secret_key)
        content.add_widget(grid)

        cfg=GridLayout(cols=2,spacing=dp(6),size_hint_y=None,height=dp(170))
        cfg.add_widget(Label(text="Trade Amount ($):"))
        self.amount=TextInput(text="79",multiline=False,input_filter="float",size_hint_y=None,height=dp(44)); cfg.add_widget(self.amount)
        cfg.add_widget(Label(text="TP (% net):"))
        self.tp=TextInput(text="1.5",multiline=False,input_filter="float",size_hint_y=None,height=dp(44)); cfg.add_widget(self.tp)
        cfg.add_widget(Label(text="SL (% net):"))
        self.sl=TextInput(text="2.0",multiline=False,input_filter="float",size_hint_y=None,height=dp(44)); cfg.add_widget(self.sl)
        cfg.add_widget(Label(text="Paper Trading:"))
        box=BoxLayout(); self.paper=CheckBox(size_hint=(None,None),size=(dp(44),dp(44)))
        box.add_widget(self.paper); box.add_widget(Label(text="Enabled")); cfg.add_widget(box)
        content.add_widget(cfg)

        buttons=BoxLayout(size_hint_y=None,height=dp(52),spacing=dp(6))
        self.start_btn=Button(text="Start Scan"); self.stop_btn=Button(text="Stop Scan",disabled=True)
        self.close_btn=Button(text="Close Position",disabled=True)
        self.start_btn.bind(on_release=lambda *_:self.start_scan())
        self.stop_btn.bind(on_release=lambda *_:self.stop_scan())
        self.close_btn.bind(on_release=lambda *_:self.close_position("MANUAL"))
        buttons.add_widget(self.start_btn); buttons.add_widget(self.stop_btn); buttons.add_widget(self.close_btn)
        content.add_widget(buttons)

        self.status=Label(text=self.status_text,size_hint_y=None,height=dp(34))
        self.position=Label(text=self.position_text,size_hint_y=None,height=dp(34))
        self.pnl=Label(text=self.pnl_text,size_hint_y=None,height=dp(34))
        content.add_widget(self.status); content.add_widget(self.position); content.add_widget(self.pnl)
        content.add_widget(Label(text="Activity Log",size_hint_y=None,height=dp(32)))
        self.log_view=TextInput(readonly=True,multiline=True,size_hint_y=None,height=dp(360),font_size="11sp")
        content.add_widget(self.log_view)
        scroll.add_widget(content); self.root_layout.add_widget(scroll)

        self.load_settings(); self.load_position()
        self.write_log("[SYSTEM] Fixed build ready. Paper Trading defaults to ON for safety.")
        return self.root_layout

    def ui(self,fn,*args): Clock.schedule_once(lambda _dt:fn(*args),0)

    def write_log(self,text):
        ts=datetime.now().strftime("%H:%M:%S"); self.ui(self._log,f"[{ts}] {text}")

    def _log(self,text):
        if self.log_view:
            self.log_view.text=(self.log_view.text+"\n" if self.log_view.text else "")+text
            self.log_view.cursor=(0,len(self.log_view.text))

    def set_status(self,text): self.ui(self._status,text)
    def _status(self,text): self.status_text=text; self.status.text=text
    def set_position(self,text): self.ui(self._position,text)
    def _position(self,text): self.position_text=text; self.position.text=text
    def set_pnl(self,text): self.ui(self._pnl,text)
    def _pnl(self,text): self.pnl_text=text; self.pnl.text=text

    def set_buttons(self,start=None,stop=None,close=None):
        self.ui(self._buttons,start,stop,close)
    def _buttons(self,start,stop,close):
        if start is not None:self.start_btn.disabled=not start
        if stop is not None:self.stop_btn.disabled=not stop
        if close is not None:self.close_btn.disabled=not close

    def popup(self,title,msg):
        def show(_dt):
            box=BoxLayout(orientation="vertical",padding=dp(12),spacing=dp(12))
            box.add_widget(Label(text=msg))
            b=Button(text="OK",size_hint_y=None,height=dp(44)); box.add_widget(b)
            p=Popup(title=title,content=box,size_hint=(.9,.45)); b.bind(on_release=p.dismiss); p.open()
        self.ui(show)

    def safe_float(self,w,default):
        try:
            v=float(w.text.strip()); return v if v>0 else default
        except Exception:return default

    def save_ui_settings(self,*_):
        try:
            save_setting("amount",self.amount.text.strip()); save_setting("tp",self.tp.text.strip())
            save_setting("sl",self.sl.text.strip()); save_setting("paper","1" if self.paper.active else "0")
            save_api_credentials(self.api_key.text.strip(),self.secret_key.text.strip())
        except Exception as exc:self.write_log(f"[SETTINGS] Save failed: {exc}")

    def load_settings(self):
        try:
            a,s=get_api_credentials(); self.api_key.text=a; self.secret_key.text=s
            self.amount.text=get_setting("amount","79"); self.tp.text=get_setting("tp","1.5")
            self.sl.text=get_setting("sl","2.0")
            # Safety: old DB value is honored; fresh installs default to paper mode.
            self.paper.active=get_setting("paper","1")=="1"
        except Exception as exc:self.write_log(f"[SETTINGS] Load failed: {exc}")

    def start_scan(self):
        if self.closing:return
        if self.get_position_safe():
            self.popup("Position Open","Close the active position before starting another scan."); return
        if not self.paper.active and (not self.api_key.text.strip() or not self.secret_key.text.strip()):
            self.popup("API Required","Enter MEXC credentials or enable Paper Trading."); return
        self.save_ui_settings(); self.stop_event=threading.Event()
        self.set_buttons(start=False,stop=True,close=False); self.set_status("Status: Scanning...")
        self.write_log("[SYSTEM] Scanner started.")
        self.scanner=ScannerThread(self.stop_event,self.write_log,self.on_signal_threadsafe,self.on_scanner_finished)
        self.scanner.start()

    def stop_scan(self):
        self.stop_event.set(); self.set_status("Status: Stopping..."); self.set_buttons(stop=False)

    def on_signal_threadsafe(self,symbol,price): self.ui(self.on_signal,symbol,price)
    def on_scanner_finished(self): self.ui(self._scanner_finished)
    def _scanner_finished(self):
        self.stop_btn.disabled=True
        if not self.monitor or not self.monitor.is_alive():
            if not self.closing and not self.get_position_safe(): self.start_btn.disabled=False; self.status.text="Status: Ready"

    def on_signal(self,symbol,price):
        if self.closing or self.closing_position or self.get_position_safe(): return
        amount=self.safe_float(self.amount,79)
        try:
            rules=get_symbol_rules(symbol)
            if rules and rules.get("min_notional",0)>0 and amount<float(rules["min_notional"]):
                self.write_log(f"[BUY BLOCKED] {symbol} minimum notional exceeds configured amount.")
                self.stop_event.clear(); Clock.schedule_once(lambda _dt:self.start_scan(),.5); return
        except Exception as exc:self.write_log(f"[RULE] {exc}")

        self.write_log(f"[BUY] {symbol} for ${amount:.2f}")
        try:
            if self.paper.active:
                ok,msg,fill,qty,oid=True,"Paper order",price,0,"PAPER"
            else:
                ak,sk=get_api_credentials()
                if not ak or not sk: ak=self.api_key.text.strip(); sk=self.secret_key.text.strip()
                ok,msg,fill,qty,oid=place_mexc_buy_order(symbol,amount,ak,sk)
            if not ok:
                self.write_log(f"[BUY FAILED] {msg}")
                # Never blindly retry an ambiguous live order.
                if "verify" in msg.lower() or "unverified" in msg.lower():
                    self.stop_event.set(); self.set_status("Status: Verify order on MEXC"); self.set_buttons(start=True,stop=False); return
                self.set_status("Status: Ready"); self.set_buttons(start=True,stop=False); return

            entry=float(fill or price); tp=self.safe_float(self.tp,1.5); sl=self.safe_float(self.sl,2)
            save_active_position(symbol,entry,amount,tp,sl,qty,oid)
            self.set_position(f"Position: {symbol} @ {entry:.8f}")
            self.set_pnl("PnL: --"); self.set_buttons(start=False,stop=False,close=True)
            self.set_status("Status: Position Open"); self.write_log(f"[BUY OK] {msg}")
            self.start_monitor(symbol,entry,amount,tp,sl)
        except Exception as exc:
            self.write_log(f"[BUY ERROR] {type(exc).__name__}: {exc}")
            self.write_log(traceback.format_exc()); self.set_buttons(start=True,stop=False)

    def start_monitor(self,symbol,entry,amount,tp,sl):
        self.stop_event=threading.Event()
        self.monitor=MonitorThread(self.stop_event,symbol,entry,amount,tp,sl,
                                   self.on_price_threadsafe,self.close_position_threadsafe,
                                   self.write_log,self.on_monitor_finished)
        self.monitor.start()

    def on_price_threadsafe(self,p,usd,pct): self.ui(self._price,p,usd,pct)
    def _price(self,p,usd,pct):
        self.last_price=float(p); self.pnl.text=f"PnL (net est.): ${usd:.4f} ({pct:.3f}%)"

    def close_position_threadsafe(self,reason): self.ui(self.close_position,reason)

    def close_position(self,reason="MANUAL"):
        if self.closing_position or self.closing:return
        pos=self.get_position_safe()
        if not pos:
            self.set_buttons(close=False); return
        self.closing_position=True
        try:
            if self.monitor and self.monitor.is_alive(): self.monitor.stop_event.set()
            if self.paper.active:
                exit_price=self.last_price or get_mexc_real_price(pos["symbol"]) or float(pos["entry_price"])
                ok,msg=True,"Paper position closed"; sell_qty=pos.get("executed_qty",0)
            else:
                ak,sk=get_api_credentials()
                if not ak or not sk: ak=self.api_key.text.strip(); sk=self.secret_key.text.strip()
                ok,msg,exit_price,sell_qty,oid=place_mexc_sell_order_market(pos["symbol"],ak,sk)
            if not ok:
                self.write_log(f"[SELL FAILED] {msg}")
                self.set_status("Status: Position may still be open - verify MEXC")
                return
            exit_price=float(exit_price or self.last_price or pos["entry_price"])
            pnl_usd,pnl_pct=estimate_net_pnl(float(pos["amount"]),float(pos["entry_price"]),exit_price)
            record_closed_trade(pos["symbol"],pos["entry_price"],exit_price,pos["amount"],pnl_usd,pnl_pct,reason)
            clear_active_position()
            self.set_position("Position: None"); self.set_pnl(f"Last net est.: ${pnl_usd:.4f} ({pnl_pct:.3f}%)")
            self.set_buttons(start=False,stop=False,close=False)
            self.set_status("Status: Closed - searching again")
            self.write_log(f"[SELL OK] {msg} | net est. PnL ${pnl_usd:.4f} ({pnl_pct:.3f}%)")
            Clock.schedule_once(lambda _dt:self.start_scan(),.8)
        except Exception as exc:
            self.write_log(f"[SELL ERROR] {type(exc).__name__}: {exc}")
            self.set_status("Status: Close error - verify MEXC")
        finally:self.closing_position=False

    def get_position_safe(self):
        try:return get_active_position()
        except Exception as exc:
            self.write_log(f"[RECOVERY] {exc}"); return None

    def load_position(self):
        pos=self.get_position_safe()
        if pos:
            self.set_position(f"Position: {pos['symbol']} @ {float(pos['entry_price']):.8f}")
            self.set_buttons(start=False,stop=False,close=True); self.set_status("Status: Position recovered")
            self.write_log("[RECOVERY] Local position recovered; monitor restarted.")
            Clock.schedule_once(lambda _dt:self.start_monitor(
                pos["symbol"],float(pos["entry_price"]),float(pos["amount"]),
                float(pos["tp_percent"]),float(pos["sl_percent"])),.5)

    def on_monitor_finished(self): self.ui(self._monitor_finished)
    def _monitor_finished(self):
        if self.closing:return
        if not self.get_position_safe(): self.close_btn.disabled=True; self.start_btn.disabled=False

    def on_stop(self):
        self.closing=True; self.stop_event.set()
        if self.scanner and self.scanner.is_alive(): self.scanner.join(timeout=3)
        if self.monitor and self.monitor.is_alive(): self.monitor.stop_event.set(); self.monitor.join(timeout=3)
        return True


if __name__=="__main__":
    MEXCScalperMobile().run()
