__version__ = "2.1.0-mobile"

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
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView
from kivy.uix.popup import Popup

from mexc_core import (
    get_top_200_symbols,
    check_trade_conditions_from_main,
    place_mexc_buy_order,
    place_mexc_sell_order_market,
    get_mexc_real_price,
    save_active_position,
    clear_active_position,
    get_active_position,
    record_closed_trade,
    save_setting,
    get_setting,
    save_api_credentials,
    get_api_credentials,
    get_symbol_rules,
)


class ScannerThread(threading.Thread):
    def __init__(self, stop_event, on_log, on_signal, on_finished):
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.on_log = on_log
        self.on_signal = on_signal
        self.on_finished = on_finished

    def run(self):
        try:
            while not self.stop_event.is_set():
                symbols = get_top_200_symbols()
                if not symbols:
                    self.on_log("[SCAN] Could not load Top 200 from MEXC. Retrying in 5 seconds...")
                    self.stop_event.wait(5.0)
                    continue

                self.on_log(f"[SCAN] Starting scan of {len(symbols)} symbols.")

                for index, symbol in enumerate(symbols, 1):
                    if self.stop_event.is_set():
                        break
                    try:
                        valid, price, msg = check_trade_conditions_from_main(symbol)
                        if valid:
                            self.on_log(f"[SIGNAL] {symbol} | price ${price:.8f} | {msg}")
                            self.on_signal(symbol, price)
                            self.stop_event.set()
                            return
                        self.on_log(f"[{index}/{len(symbols)}] {symbol} | {msg}")
                    except Exception as exc:
                        self.on_log(f"[WORKER ERROR] {symbol}: {type(exc).__name__}: {exc}")
                    self.stop_event.wait(0.25)

                if not self.stop_event.is_set():
                    self.on_log("[SCAN] Cycle completed. Refreshing Top 200.")
        except Exception as exc:
            self.on_log(f"[SCANNER CRASH PREVENTED] {type(exc).__name__}: {exc}")
            self.on_log(traceback.format_exc())
        finally:
            self.on_finished()


class MonitorThread(threading.Thread):
    def __init__(self, stop_event, symbol, entry_price, amount, tp, sl, on_price, on_close, on_log, on_finished):
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.symbol = symbol
        self.entry_price = float(entry_price)
        self.amount = float(amount)
        self.tp = float(tp)
        self.sl = float(sl)
        self.on_price = on_price
        self.on_close = on_close
        self.on_log = on_log
        self.on_finished = on_finished

    def run(self):
        try:
            while not self.stop_event.is_set():
                try:
                    price = get_mexc_real_price(self.symbol)
                    if price and self.entry_price > 0:
                        pnl_pct = ((price - self.entry_price) / self.entry_price) * 100.0
                        pnl_usd = self.amount * pnl_pct / 100.0
                        self.on_price(price, pnl_usd, pnl_pct)
                        if pnl_pct >= self.tp:
                            self.on_log(f"[TP] Target reached: {pnl_pct:.3f}%")
                            self.on_close("TP")
                            return
                        if pnl_pct <= -self.sl:
                            self.on_log(f"[SL] Stop reached: {pnl_pct:.3f}%")
                            self.on_close("SL")
                            return
                except Exception as exc:
                    self.on_log(f"[MONITOR ERROR] {type(exc).__name__}: {exc}")
                self.stop_event.wait(1.5)
        except Exception as exc:
            self.on_log(f"[MONITOR CRASH PREVENTED] {type(exc).__name__}: {exc}")
            self.on_log(traceback.format_exc())
        finally:
            self.on_finished()


class MEXCScalperMobile(App):
    status_text = StringProperty("Status: Ready")
    position_text = StringProperty("Position: None")
    pnl_text = StringProperty("PnL: --")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scanner = None
        self.monitor = None
        self.stop_event = threading.Event()
        self.closing = False
        self.closing_position = False
        self.last_price = 0.0
        self.root_layout = None
        self.log_view = None

    def build(self):
        Window.clearcolor = (0.06, 0.07, 0.09, 1)
        Window.softinput_mode = "below_target"

        self.root_layout = BoxLayout(orientation="vertical", padding=dp(10), spacing=dp(8))

        title = Label(text="[b]MEXC Mobile Scalper[/b]", markup=True, size_hint_y=None, height=dp(42), font_size="20sp")
        self.root_layout.add_widget(title)

        scroll = ScrollView(size_hint_y=1)
        content = BoxLayout(orientation="vertical", spacing=dp(8), size_hint_y=None)
        content.bind(minimum_height=content.setter("height"))

        api_grid = GridLayout(cols=2, spacing=dp(6), size_hint_y=None, height=dp(110))
        api_grid.add_widget(Label(text="API Key:", halign="left"))
        self.api_key = TextInput(password=False, multiline=False, size_hint_y=None, height=dp(44))
        api_grid.add_widget(self.api_key)
        api_grid.add_widget(Label(text="Secret Key:", halign="left"))
        self.secret_key = TextInput(password=True, multiline=False, size_hint_y=None, height=dp(44))
        api_grid.add_widget(self.secret_key)
        content.add_widget(api_grid)

        cfg_grid = GridLayout(cols=2, spacing=dp(6), size_hint_y=None, height=dp(170))
        cfg_grid.add_widget(Label(text="Trade Amount ($):"))
        self.amount = TextInput(text="79", multiline=False, input_filter="float", size_hint_y=None, height=dp(44))
        cfg_grid.add_widget(self.amount)
        cfg_grid.add_widget(Label(text="TP (%):"))
        self.tp = TextInput(text="1.5", multiline=False, input_filter="float", size_hint_y=None, height=dp(44))
        cfg_grid.add_widget(self.tp)
        cfg_grid.add_widget(Label(text="SL (%):"))
        self.sl = TextInput(text="2.0", multiline=False, input_filter="float", size_hint_y=None, height=dp(44))
        cfg_grid.add_widget(self.sl)
        cfg_grid.add_widget(Label(text="Paper Trading:"))
        paper_box = BoxLayout()
        self.paper = CheckBox(size_hint=(None, None), size=(dp(44), dp(44)))
        paper_box.add_widget(self.paper)
        paper_box.add_widget(Label(text="Enabled"))
        cfg_grid.add_widget(paper_box)
        content.add_widget(cfg_grid)

        buttons = BoxLayout(size_hint_y=None, height=dp(52), spacing=dp(6))
        self.start_btn = Button(text="Start Scan")
        self.stop_btn = Button(text="Stop Scan", disabled=True)
        self.close_btn = Button(text="Close Position", disabled=True)
        self.start_btn.bind(on_release=lambda *_: self.start_scan())
        self.stop_btn.bind(on_release=lambda *_: self.stop_scan())
        self.close_btn.bind(on_release=lambda *_: self.close_position("MANUAL"))
        buttons.add_widget(self.start_btn)
        buttons.add_widget(self.stop_btn)
        buttons.add_widget(self.close_btn)
        content.add_widget(buttons)

        self.status = Label(text=self.status_text, size_hint_y=None, height=dp(34))
        self.position = Label(text=self.position_text, size_hint_y=None, height=dp(34))
        self.pnl = Label(text=self.pnl_text, size_hint_y=None, height=dp(34))
        content.add_widget(self.status)
        content.add_widget(self.position)
        content.add_widget(self.pnl)

        content.add_widget(Label(text="Activity Log", size_hint_y=None, height=dp(32)))
        self.log_view = TextInput(readonly=True, multiline=True, size_hint_y=None, height=dp(360), font_size="11sp")
        content.add_widget(self.log_view)

        scroll.add_widget(content)
        self.root_layout.add_widget(scroll)

        self.load_settings()
        self.load_position()
        self.write_log("[SYSTEM] Mobile app ready.")
        return self.root_layout

    # ---------- UI helpers ----------
    def ui(self, fn, *args):
        Clock.schedule_once(lambda _dt: fn(*args), 0)

    def write_log(self, text):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.ui(self._write_log_ui, f"[{timestamp}] {text}")

    def _write_log_ui(self, text):
        if not self.log_view:
            return
        current = self.log_view.text
        self.log_view.text = (current + "\n" if current else "") + text
        self.log_view.cursor = (0, len(self.log_view.text))

    def set_status(self, text):
        self.ui(self._set_status_ui, text)

    def _set_status_ui(self, text):
        self.status_text = text
        self.status.text = text

    def set_position(self, text):
        self.ui(self._set_position_ui, text)

    def _set_position_ui(self, text):
        self.position_text = text
        self.position.text = text

    def set_pnl(self, text):
        self.ui(self._set_pnl_ui, text)

    def _set_pnl_ui(self, text):
        self.pnl_text = text
        self.pnl.text = text

    def set_buttons(self, start=None, stop=None, close=None):
        self.ui(self._set_buttons_ui, start, stop, close)

    def _set_buttons_ui(self, start, stop, close):
        if start is not None:
            self.start_btn.disabled = not start
        if stop is not None:
            self.stop_btn.disabled = not stop
        if close is not None:
            self.close_btn.disabled = not close

    def popup(self, title, message):
        self.ui(self._popup_ui, title, message)

    def _popup_ui(self, title, message):
        box = BoxLayout(orientation="vertical", padding=dp(12), spacing=dp(12))
        box.add_widget(Label(text=message))
        btn = Button(text="OK", size_hint_y=None, height=dp(44))
        box.add_widget(btn)
        pop = Popup(title=title, content=box, size_hint=(0.9, 0.45))
        btn.bind(on_release=pop.dismiss)
        pop.open()

    # ---------- Settings ----------
    def safe_float(self, widget, default):
        try:
            value = float(widget.text.strip())
            return value if value > 0 else default
        except Exception:
            return default

    def save_ui_settings(self, *_args):
        try:
            save_setting("amount", self.amount.text.strip())
            save_setting("tp", self.tp.text.strip())
            save_setting("sl", self.sl.text.strip())
            save_setting("paper", "1" if self.paper.active else "0")
            save_api_credentials(
                self.api_key.text.strip(),
                self.secret_key.text.strip(),
            )
            self.write_log("[SETTINGS] API credentials and trading settings saved to database.")
        except Exception as exc:
            self.write_log(f"[SETTINGS] Save failed: {exc}")

    def load_settings(self):
        try:
            saved_api, saved_secret = get_api_credentials()
            self.api_key.text = saved_api
            self.secret_key.text = saved_secret
            self.amount.text = get_setting("amount", "79")
            self.tp.text = get_setting("tp", "1.5")
            self.sl.text = get_setting("sl", "2.0")
            self.paper.active = get_setting("paper", "0") == "1"
        except Exception as exc:
            self.write_log(f"[SETTINGS] Load failed: {exc}")

    # ---------- Scanner ----------
    def start_scan(self):
        if self.closing:
            return
        if self.get_position_safe():
            self.popup("Position Open", "An active position already exists. Close it before starting a new scan.")
            return
        if not self.paper.active and (not self.api_key.text.strip() or not self.secret_key.text.strip()):
            self.popup("API Required", "Enter MEXC API Key and Secret Key, or enable Paper Trading.")
            return

        self.save_ui_settings()
        self.stop_event = threading.Event()
        self.set_buttons(start=False, stop=True, close=False)
        self.set_status("Status: Scanning...")
        self.write_log("[SYSTEM] Scanner started.")

        self.scanner = ScannerThread(
            self.stop_event,
            self.write_log,
            self.on_signal_threadsafe,
            self.on_scanner_finished,
        )
        self.scanner.start()

    def stop_scan(self):
        if self.scanner and self.scanner.is_alive():
            self.stop_event.set()
            self.set_status("Status: Stopping...")
            self.write_log("[SYSTEM] Stop requested.")
        self.set_buttons(stop=False)

    def on_signal_threadsafe(self, symbol, price):
        self.ui(self.on_signal, symbol, price)

    def on_scanner_finished(self):
        self.ui(self._on_scanner_finished_ui)

    def _on_scanner_finished_ui(self):
        self.stop_btn.disabled = True
        if not self.monitor or not self.monitor.is_alive():
            if not self.closing and not self.get_position_safe():
                self.start_btn.disabled = False
                self.status.text = "Status: Ready"

    # ---------- Trading ----------
    def on_signal(self, symbol, price):
        if self.closing or self.closing_position:
            return
        if self.get_position_safe():
            self.write_log("[BUY BLOCKED] An active position already exists.")
            return

        amount = self.safe_float(self.amount, 79.0)
        self.write_log(f"[BUY] Sending buy order {symbol} for ${amount:.2f}...")

        try:
            rules = get_symbol_rules(symbol)
            if rules:
                min_notional = rules.get("min_notional")
                if min_notional is not None and float(min_notional) > amount:
                    self.write_log(f"[BUY BLOCKED] {symbol} minimum notional is {min_notional}; amount is {amount:.2f}.")
                    self.stop_event.clear()
                    Clock.schedule_once(lambda _dt: self.start_scan(), 0.5)
                    return
        except Exception as exc:
            self.write_log(f"[RULE CHECK] Failed for {symbol}: {exc}")

        try:
            if self.paper.active:
                ok, msg, fill = True, "Paper order", price
            else:
                api_key, secret_key = get_api_credentials()
                if not api_key or not secret_key:
                    api_key = self.api_key.text.strip()
                    secret_key = self.secret_key.text.strip()
                    save_api_credentials(api_key, secret_key)
                ok, msg, fill = place_mexc_buy_order(
                    symbol, amount, api_key, secret_key
                )

            if not ok:
                self.write_log(f"[BUY FAILED] {msg}")
                if "verify the order on MEXC" in msg.lower() and not self.paper.active:
                    self.stop_event.set()
                    self.set_status("Status: Order verification required")
                    self.set_buttons(start=True, stop=False)
                    return
                self.stop_event.clear()
                self.set_status("Status: Resuming scan...")
                self.set_buttons(start=True, stop=False)
                Clock.schedule_once(lambda _dt: self.start_scan(), 0.5)
                return

            entry = float(fill or price)
            tp = self.safe_float(self.tp, 1.5)
            sl = self.safe_float(self.sl, 2.0)
            save_active_position(symbol, entry, amount, tp, sl)

            self.set_position(f"Position: {symbol} @ {entry:.8f}")
            self.set_pnl("PnL: --")
            self.set_buttons(start=False, stop=False, close=True)
            self.set_status("Status: Position Open")
            self.write_log(f"[BUY OK] {msg}")
            self.write_log(
                f"[BUY VERIFIED] {symbol} actual average fill = {entry:.12f} | "
                f"signal price = {float(price):.12f} | deviation = {((entry - float(price)) / float(price) * 100.0) if price else 0.0:.3f}%"
            )
            self.start_monitor(symbol, entry, amount, tp, sl)
        except Exception as exc:
            self.write_log(f"[BUY ERROR] {type(exc).__name__}: {exc}")
            self.write_log(traceback.format_exc())
            self.stop_event.clear()
            self.set_buttons(start=True, stop=False)

    def start_monitor(self, symbol, entry, amount, tp, sl):
        self.stop_event = threading.Event()
        self.monitor = MonitorThread(
            self.stop_event,
            symbol,
            entry,
            amount,
            tp,
            sl,
            self.on_price_threadsafe,
            self.close_position_threadsafe,
            self.write_log,
            self.on_monitor_finished,
        )
        self.monitor.start()

    def on_price_threadsafe(self, price, pnl_usd, pnl_pct):
        self.ui(self._on_price_ui, price, pnl_usd, pnl_pct)

    def _on_price_ui(self, price, pnl_usd, pnl_pct):
        self.last_price = float(price)
        self.pnl.text = f"PnL: ${pnl_usd:.4f} ({pnl_pct:.3f}%)"

    def close_position_threadsafe(self, reason):
        self.ui(self.close_position, reason)

    def close_position(self, reason="MANUAL"):
        if self.closing_position or self.closing:
            return
        self.closing_position = True
        try:
            pos = self.get_position_safe()
            if not pos:
                self.write_log("[SELL] No active position.")
                self.set_buttons(close=False)
                return

            if self.monitor and self.monitor.is_alive():
                self.monitor.stop_event.set()

            self.write_log(f"[SELL] Closing {pos['symbol']} | reason: {reason}")

            if self.paper.active:
                exit_price = self.last_price or get_mexc_real_price(pos["symbol"]) or float(pos["entry_price"])
                ok, msg = True, "Paper position closed"
            else:
                api_key, secret_key = get_api_credentials()
                if not api_key or not secret_key:
                    api_key = self.api_key.text.strip()
                    secret_key = self.secret_key.text.strip()
                    save_api_credentials(api_key, secret_key)
                ok, msg, exit_price = place_mexc_sell_order_market(
                    pos["symbol"], api_key, secret_key
                )

            if ok:
                exit_price = float(exit_price or self.last_price or pos["entry_price"])
                entry_price = float(pos["entry_price"])
                amount = float(pos["amount"])
                pnl_pct = ((exit_price - entry_price) / entry_price) * 100.0 if entry_price else 0.0
                pnl_usd = amount * pnl_pct / 100.0
                record_closed_trade(
                    pos["symbol"], entry_price, exit_price, amount,
                    pnl_usd, pnl_pct, reason,
                )
                clear_active_position()
                self.set_position("Position: None")
                self.set_pnl(f"PnL: ${pnl_usd:.4f} ({pnl_pct:.3f}%)")
                self.set_buttons(start=False, stop=False, close=False)
                self.set_status("Status: Position Closed - Searching for next trade...")
                self.write_log(f"[SELL OK] {msg} | Exit: {exit_price:.8f} | PnL: ${pnl_usd:.4f} ({pnl_pct:.3f}%)")
                # Automatically start a fresh Top-200 scan after the position is fully closed.
                self.stop_event = threading.Event()
                Clock.schedule_once(lambda _dt: self.start_scan(), 0.8)
            else:
                self.write_log(f"[SELL FAILED] {msg}")
                self.set_status("Status: Position still open - retry required")
        except Exception as exc:
            self.write_log(f"[SELL ERROR] {type(exc).__name__}: {exc}")
            self.write_log(traceback.format_exc())
            self.set_status("Status: Close error - position may still be open")
        finally:
            self.closing_position = False

    # ---------- Recovery ----------
    def get_position_safe(self):
        try:
            return get_active_position()
        except Exception as exc:
            self.write_log(f"[RECOVERY] Database read failed: {exc}")
            return None

    def load_position(self):
        pos = self.get_position_safe()
        if pos:
            self.set_position(f"Position: {pos['symbol']} @ {float(pos['entry_price']):.8f}")
            self.set_buttons(start=False, stop=False, close=True)
            self.set_status("Status: Position Open - Recovery")
            self.write_log("[RECOVERY] Active position recovered from database.")
            Clock.schedule_once(
                lambda _dt: self.start_monitor(
                    pos["symbol"], float(pos["entry_price"]), float(pos["amount"]),
                    float(pos["tp_percent"]), float(pos["sl_percent"])
                ), 0.2
            )

    def on_monitor_finished(self):
        self.ui(self._on_monitor_finished_ui)

    def _on_monitor_finished_ui(self):
        if self.closing:
            return
        if not self.get_position_safe():
            self.close_btn.disabled = True
            # A successful close schedules the next scan from close_position().
            # Do not start a competing scanner here.
            if not (self.scanner and self.scanner.is_alive()):
                self.start_btn.disabled = False

    def on_stop(self):
        self.closing = True
        self.stop_event.set()
        if self.scanner and self.scanner.is_alive():
            self.scanner.join(timeout=3.0)
        if self.monitor and self.monitor.is_alive():
            self.monitor.stop_event.set()
            self.monitor.join(timeout=3.0)
        return True


if __name__ == "__main__":
    MEXCScalperMobile().run()
