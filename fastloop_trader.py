
#!/usr/bin/env python3
"""
Simmer FastLoop Trading Skill - live-ready version

What this version adds:
- safer defaults for March 12 launch
- continuous scanning loop
- daily budget / max trades / cooldown
- daily P&L guardrails (best-effort, using current fast-market P&L from positions)
- price filters to avoid low-edge near-50c trades
- Coinbase candles instead of Binance for better cloud reliability
- momentum + acceleration + order-book imbalance filters
- best-effort stop-loss / take-profit monitor placement via Simmer SDK
- auto-redeem on every cycle

Notes:
- Real Polymarket trading requires:
  * SIMMER_API_KEY
  * TRADING_VENUE=polymarket
  * WALLET_PRIVATE_KEY for self-custody wallets
- Keep paper mode until you are comfortable with fills and logs.
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import quote

sys.stdout.reconfigure(line_buffering=True)

try:
    from tradejournal import log_trade
    JOURNAL_AVAILABLE = True
except ImportError:
    try:
        from skills.tradejournal import log_trade
        JOURNAL_AVAILABLE = True
    except ImportError:
        JOURNAL_AVAILABLE = False
        def log_trade(*args, **kwargs):
            pass

CONFIG_SCHEMA = {
    "entry_threshold": {"default": 0.03, "env": "SIMMER_SPRINT_ENTRY", "type": float,
                        "help": "Min divergence from fair price to trigger a trade"},
    "min_momentum_pct": {"default": 0.05, "env": "SIMMER_SPRINT_MOMENTUM", "type": float,
                         "help": "Min slow momentum pct required"},
    "max_position": {"default": 5.0, "env": "SIMMER_SPRINT_MAX_POSITION", "type": float,
                     "help": "Max dollars per trade"},
    "signal_source": {"default": "coinbase", "env": "SIMMER_SPRINT_SIGNAL", "type": str,
                      "help": "Price feed source (coinbase)"},
    "lookback_minutes": {"default": 5, "env": "SIMMER_SPRINT_LOOKBACK", "type": int,
                         "help": "Slow momentum lookback"},
    "min_time_remaining": {"default": 30, "env": "SIMMER_SPRINT_MIN_TIME", "type": int,
                           "help": "Skip fast markets with less than this many seconds remaining"},
    "asset": {"default": "BTC", "env": "SIMMER_SPRINT_ASSET", "type": str,
              "help": "Asset to trade (BTC, ETH, SOL)"},
    "window": {"default": "5m", "env": "SIMMER_SPRINT_WINDOW", "type": str,
               "help": "Market window duration"},
    "volume_confidence": {"default": True, "env": "SIMMER_SPRINT_VOL_CONF", "type": bool,
                          "help": "Use volume as a confidence filter"},
    "daily_budget": {"default": 20.0, "env": "SIMMER_SPRINT_DAILY_BUDGET", "type": float,
                     "help": "Max total spend per UTC day"},
    "daily_loss_limit": {"default": 20.0, "env": "SIMMER_SPRINT_DAILY_LOSS", "type": float,
                         "help": "Stop for the day once fast-market P&L <= -limit"},
    "daily_profit_target": {"default": 50.0, "env": "SIMMER_SPRINT_DAILY_PROFIT", "type": float,
                            "help": "Stop for the day once fast-market P&L >= target"},
    "take_profit_pct": {"default": 0.20, "env": "SIMMER_SPRINT_TP", "type": float,
                        "help": "Take-profit percentage for monitors"},
    "stop_loss_pct": {"default": 0.10, "env": "SIMMER_SPRINT_SL", "type": float,
                      "help": "Stop-loss percentage for monitors"},
    "min_entry_price": {"default": 0.10, "env": "SIMMER_SPRINT_MIN_PRICE", "type": float,
                        "help": "Minimum buy price"},
    "max_entry_price": {"default": 0.49, "env": "SIMMER_SPRINT_MAX_PRICE", "type": float,
                        "help": "Maximum buy price"},
    "skip_mid_low": {"default": 0.35, "env": "SIMMER_SPRINT_SKIP_LOW", "type": float,
                     "help": "Skip buy prices above this lower bound if below skip_mid_high"},
    "skip_mid_high": {"default": 0.65, "env": "SIMMER_SPRINT_SKIP_HIGH", "type": float,
                      "help": "Skip buy prices below this upper bound if above skip_mid_low"},
    "max_trades_per_day": {"default": 8, "env": "SIMMER_SPRINT_MAX_TRADES", "type": int,
                           "help": "Max number of trades per UTC day"},
    "cooldown_after_loss_sec": {"default": 300, "env": "SIMMER_SPRINT_LOSS_COOLDOWN", "type": int,
                                "help": "Cooldown after a loss in seconds"},
    "imbalance_bull": {"default": 0.60, "env": "SIMMER_SPRINT_IMBAL_BULL", "type": float,
                       "help": "Bullish order book imbalance threshold"},
    "imbalance_bear": {"default": 0.40, "env": "SIMMER_SPRINT_IMBAL_BEAR", "type": float,
                       "help": "Bearish order book imbalance threshold"},
    "max_spread_pct": {"default": 0.06, "env": "SIMMER_SPRINT_MAX_SPREAD", "type": float,
                       "help": "Skip if spread exceeds this pct of midpoint"},
    "extreme_low": {"default": 0.10, "env": "SIMMER_SPRINT_EXTREME_LOW", "type": float,
                    "help": "Mean-reversion long threshold"},
    "extreme_high": {"default": 0.90, "env": "SIMMER_SPRINT_EXTREME_HIGH", "type": float,
                     "help": "Mean-reversion short threshold"},
    "scan_interval_sec": {"default": 30, "env": "SIMMER_SPRINT_SCAN_INTERVAL", "type": int,
                          "help": "Seconds between scans"},
}

TRADE_SOURCE = "sdk:fastloop"
SKILL_SLUG = "polymarket-fast-loop"
_client = None
ASSET_SYMBOLS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}
ASSET_PATTERNS = {
    "BTC": ["bitcoin up or down"],
    "ETH": ["ethereum up or down"],
    "SOL": ["solana up or down"],
}
CLOB_API = "https://clob.polymarket.com"

from simmer_sdk.skill import load_config, update_config, get_config_path

cfg = load_config(CONFIG_SCHEMA, __file__, slug=SKILL_SLUG)

ENTRY_THRESHOLD = cfg["entry_threshold"]
MIN_MOMENTUM_PCT = cfg["min_momentum_pct"]
MAX_POSITION_USD = cfg["max_position"]
SIGNAL_SOURCE = cfg["signal_source"]
LOOKBACK_MINUTES = cfg["lookback_minutes"]
MIN_TIME_REMAINING = cfg["min_time_remaining"]
ASSET = cfg["asset"].upper()
WINDOW = cfg["window"]
VOLUME_CONFIDENCE = cfg["volume_confidence"]
DAILY_BUDGET = cfg["daily_budget"]
DAILY_LOSS_LIMIT = cfg["daily_loss_limit"]
DAILY_PROFIT_TARGET = cfg["daily_profit_target"]
TAKE_PROFIT_PCT = cfg["take_profit_pct"]
STOP_LOSS_PCT = cfg["stop_loss_pct"]
MIN_ENTRY_PRICE = cfg["min_entry_price"]
MAX_ENTRY_PRICE = cfg["max_entry_price"]
SKIP_MID_LOW = cfg["skip_mid_low"]
SKIP_MID_HIGH = cfg["skip_mid_high"]
MAX_TRADES_PER_DAY = cfg["max_trades_per_day"]
COOLDOWN_AFTER_LOSS_SEC = cfg["cooldown_after_loss_sec"]
IMBALANCE_BULL = cfg["imbalance_bull"]
IMBALANCE_BEAR = cfg["imbalance_bear"]
MAX_SPREAD_PCT = cfg["max_spread_pct"]
EXTREME_LOW = cfg["extreme_low"]
EXTREME_HIGH = cfg["extreme_high"]
SCAN_INTERVAL_SEC = cfg["scan_interval_sec"]

def _state_path():
    from pathlib import Path
    return Path(__file__).parent / "fastloop_state.json"

def load_state():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = _state_path()
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            if data.get("date") == today:
                return data
        except Exception:
            pass
    return {
        "date": today,
        "spent": 0.0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "last_loss_ts": 0,
        "last_trade_ts": 0,
        "notes": []
    }

def save_state(state):
    with open(_state_path(), "w") as f:
        json.dump(state, f, indent=2)

def append_note(state, note):
    note = f"{datetime.now(timezone.utc).isoformat()} | {note}"
    state.setdefault("notes", []).append(note)
    state["notes"] = state["notes"][-200:]
    save_state(state)

def get_client():
    global _client
    if _client is None:
        from simmer_sdk import SimmerClient
        api_key = os.environ.get("SIMMER_API_KEY")
        if not api_key:
            print("Error: SIMMER_API_KEY environment variable not set")
            sys.exit(1)
        venue = os.environ.get("TRADING_VENUE", "sim")
        _client = SimmerClient(api_key=api_key, venue=venue)
    return _client

def _api_request(url, method="GET", data=None, headers=None, timeout=15):
    try:
        req_headers = headers.copy() if headers else {}
        req_headers.setdefault("User-Agent", "simmer-fastloop/2.0")
        body = None
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        req = Request(url, data=body, headers=req_headers, method=method)
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        try:
            err = e.read().decode("utf-8")
        except Exception:
            err = str(e)
        return {"error": f"HTTP {e.code}: {err}"}
    except URLError as e:
        return {"error": f"Connection error: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}

def fetch_live_midpoint(token_id):
    result = _api_request(f"{CLOB_API}/midpoint?token_id={quote(str(token_id))}", timeout=5)
    if not result or isinstance(result, dict) and result.get("error"):
        return None
    try:
        return float(result["mid"])
    except Exception:
        return None

def fetch_orderbook_summary(clob_token_ids):
    if not clob_token_ids:
        return None
    yes_token = clob_token_ids[0]
    result = _api_request(f"{CLOB_API}/book?token_id={quote(str(yes_token))}", timeout=5)
    if not result or isinstance(result, dict) and result.get("error"):
        return None
    bids = result.get("bids", []) or []
    asks = result.get("asks", []) or []
    if not bids or not asks:
        return None
    try:
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        mid = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid if mid > 0 else 1.0
        bid_depth = sum(float(b.get("price", 0)) * float(b.get("size", 0)) for b in bids[:5])
        ask_depth = sum(float(a.get("price", 0)) * float(a.get("size", 0)) for a in asks[:5])
        imbalance = bid_depth / (bid_depth + ask_depth) if (bid_depth + ask_depth) > 0 else 0.5
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread_pct": spread_pct,
            "bid_depth_usd": bid_depth,
            "ask_depth_usd": ask_depth,
            "imbalance": imbalance,
        }
    except Exception:
        return None

def discover_fast_markets(asset="BTC", window="5m"):
    try:
        client = get_client()
        sdk_markets = client.get_fast_markets(asset=asset, window=window, limit=50)
        if sdk_markets:
            markets = []
            for m in sdk_markets:
                end_time = _parse_resolves_at(getattr(m, "resolves_at", None)) if getattr(m, "resolves_at", None) else None
                clob_tokens = []
                if getattr(m, "polymarket_token_id", None):
                    clob_tokens.append(m.polymarket_token_id)
                if getattr(m, "polymarket_no_token_id", None):
                    clob_tokens.append(m.polymarket_no_token_id)
                markets.append({
                    "question": m.question,
                    "market_id": m.id,
                    "end_time": end_time,
                    "clob_token_ids": clob_tokens,
                    "is_live_now": getattr(m, "is_live_now", None),
                    "spread_cents": getattr(m, "spread_cents", None),
                    "liquidity_tier": getattr(m, "liquidity_tier", None),
                    "source": "simmer",
                })
            return markets
    except Exception as e:
        print(f"  ⚠️ Simmer fast-markets API failed ({e}), using Gamma fallback")
    return _discover_via_gamma(asset, window)

def _discover_via_gamma(asset="BTC", window="5m"):
    patterns = ASSET_PATTERNS.get(asset, ASSET_PATTERNS["BTC"])
    url = "https://gamma-api.polymarket.com/markets?limit=100&closed=false&tag=crypto&order=endDate&ascending=true"
    result = _api_request(url)
    if not result or isinstance(result, dict) and result.get("error"):
        return []
    markets = []
    for m in result:
        q = (m.get("question") or "").lower()
        slug = m.get("slug", "")
        if not any(p in q for p in patterns):
            continue
        if f"-{window}-" not in slug:
            continue
        raw = m.get("clobTokenIds", "[]")
        try:
            clob_tokens = json.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception:
            clob_tokens = []
        markets.append({
            "question": m.get("question", ""),
            "slug": slug,
            "market_id": None,
            "end_time": _parse_fast_market_end_time(m.get("question", "")),
            "clob_token_ids": clob_tokens,
            "is_live_now": None,
            "source": "gamma",
        })
    return markets

def _parse_resolves_at(s):
    try:
        s = s.replace("Z", "+00:00").replace(" ", "T")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _parse_fast_market_end_time(question):
    import re
    from zoneinfo import ZoneInfo
    pattern = r'(\w+ \d+),.*?-\s*(\d{1,2}:\d{2}(?:AM|PM))\s*ET'
    m = re.search(pattern, question)
    if not m:
        return None
    try:
        year = datetime.now(timezone.utc).year
        dt = datetime.strptime(f"{m.group(1)} {year} {m.group(2)}", "%B %d %Y %I:%M%p")
        return dt.replace(tzinfo=ZoneInfo("America/New_York")).astimezone(timezone.utc)
    except Exception:
        return None

def find_best_fast_market(markets):
    now = datetime.now(timezone.utc)
    candidates = []
    for m in markets:
        end_time = m.get("end_time")
        if not end_time:
            continue
        remaining = (end_time - now).total_seconds()
        if m.get("is_live_now") is False:
            continue
        if remaining <= MIN_TIME_REMAINING:
            continue
        candidates.append((remaining, m))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]

def get_coinbase_candles(product, granularity=60):
    url = f"https://api.exchange.coinbase.com/products/{product}/candles?granularity={granularity}"
    result = _api_request(url)
    if not result or isinstance(result, dict):
        return None
    return result

def get_market_signal(asset="BTC", lookback_minutes=5):
    product = ASSET_SYMBOLS.get(asset, "BTC-USD")
    candles = get_coinbase_candles(product, granularity=60)
    if not candles or len(candles) < max(lookback_minutes, 2):
        return None
    try:
        slow = candles[:lookback_minutes]
        fast = candles[:2]
        slow_price_then = float(slow[-1][4])
        slow_price_now = float(slow[0][4])
        slow_momentum = ((slow_price_now - slow_price_then) / slow_price_then) * 100
        fast_price_then = float(fast[-1][4])
        fast_price_now = float(fast[0][4])
        fast_momentum = ((fast_price_now - fast_price_then) / fast_price_then) * 100
        acceleration = fast_momentum - slow_momentum
        direction = "up" if slow_momentum > 0 else "down"
        volumes = [float(c[5]) for c in slow]
        avg_volume = sum(volumes) / len(volumes)
        latest_volume = float(slow[0][5])
        volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0
        return {
            "price_now": slow_price_now,
            "price_then": slow_price_then,
            "momentum_pct": slow_momentum,
            "fast_momentum_pct": fast_momentum,
            "acceleration_pct": acceleration,
            "direction": direction,
            "avg_volume": avg_volume,
            "latest_volume": latest_volume,
            "volume_ratio": volume_ratio,
            "candles": len(slow),
        }
    except Exception:
        return None

def get_positions():
    try:
        positions = get_client().get_positions()
        from dataclasses import asdict, is_dataclass
        out = []
        for p in positions:
            out.append(asdict(p) if is_dataclass(p) else dict(p))
        return out
    except Exception:
        return []

def import_fast_market(slug):
    try:
        res = get_client().import_market(f"https://polymarket.com/event/{slug}")
        if res.get("status") in ("imported", "already_exists"):
            return res.get("market_id"), None
        return None, res.get("error") or res.get("status")
    except Exception as e:
        return None, str(e)

def execute_trade(market_id, side, amount_usd, live=False, reasoning=""):
    try:
        result = get_client().trade(
            market_id=market_id,
            side=side,
            amount=amount_usd,
            dry_run=not live,
            source=TRADE_SOURCE,
            skill_slug=SKILL_SLUG,
            reasoning=reasoning or f"FastLoop {side.upper()} signal"
        )
        return {
            "success": getattr(result, "success", False),
            "trade_id": getattr(result, "trade_id", None),
            "shares_bought": getattr(result, "shares_bought", 0),
            "cost": getattr(result, "cost", amount_usd),
            "error": getattr(result, "error", None),
            "simulated": getattr(result, "simulated", (not live)),
        }
    except Exception as e:
        return {"success": False, "error": str(e), "simulated": not live}

def best_effort_set_monitor(market_id, side, take_profit_pct, stop_loss_pct):
    client = get_client()
    attempts = [
        {"market_id": market_id, "side": side, "take_profit_pct": take_profit_pct, "stop_loss_pct": stop_loss_pct},
        {"market_id": market_id, "side": side, "tp_pct": take_profit_pct, "sl_pct": stop_loss_pct},
        {"market_id": market_id, "take_profit_pct": take_profit_pct, "stop_loss_pct": stop_loss_pct},
        {"market_id": market_id, "take_profit": take_profit_pct, "stop_loss": stop_loss_pct},
    ]
    last_err = None
    for kwargs in attempts:
        try:
            return client.set_monitor(**kwargs)
        except Exception as e:
            last_err = str(e)
    return {"error": f"set_monitor failed: {last_err}"}

def auto_redeem_if_possible():
    try:
        return get_client().auto_redeem()
    except Exception:
        return []

def fast_market_positions_pnl():
    pnl = 0.0
    count = 0
    for p in get_positions():
        q = (p.get("question") or "").lower()
        if "up or down" not in q:
            continue
        try:
            pnl += float(p.get("pnl", 0) or 0)
            count += 1
        except Exception:
            pass
    return pnl, count

def in_loss_cooldown(state):
    last_loss = state.get("last_loss_ts", 0)
    if not last_loss:
        return False, 0
    elapsed = time.time() - last_loss
    if elapsed >= COOLDOWN_AFTER_LOSS_SEC:
        return False, 0
    return True, int(COOLDOWN_AFTER_LOSS_SEC - elapsed)

def should_skip_price(buy_price):
    if buy_price < MIN_ENTRY_PRICE:
        return True, f"buy price {buy_price:.3f} below min {MIN_ENTRY_PRICE:.2f}"
    if buy_price > MAX_ENTRY_PRICE:
        return True, f"buy price {buy_price:.3f} above max {MAX_ENTRY_PRICE:.2f}"
    if SKIP_MID_LOW < buy_price < SKIP_MID_HIGH:
        return True, f"buy price {buy_price:.3f} in no-edge middle zone"
    return False, ""

def compute_signal_and_side(yes_price, signal, imbalance, remaining_s):
    no_price = 1 - yes_price
    slow = signal["momentum_pct"]
    fast = signal["fast_momentum_pct"]
    accel = signal["acceleration_pct"]
    if remaining_s >= 120:
        if yes_price >= EXTREME_HIGH and accel <= 0:
            return "no", f"YES extreme {yes_price:.3f} with stalling momentum/accel {accel:+.3f}%", yes_price - 0.50
        if yes_price <= EXTREME_LOW and accel >= 0:
            return "yes", f"YES depressed {yes_price:.3f} with stalling downside/accel {accel:+.3f}%", 0.50 - yes_price
    if abs(slow) < MIN_MOMENTUM_PCT:
        return None, f"momentum {abs(slow):.3f}% below minimum", 0.0
    if VOLUME_CONFIDENCE and signal["volume_ratio"] < 0.50:
        return None, f"volume ratio {signal['volume_ratio']:.2f}x too low", 0.0
    if slow > 0:
        if fast <= 0 or accel < -0.02:
            return None, f"up move lacks fresh acceleration ({fast:+.3f}% / {accel:+.3f}%)", 0.0
        if imbalance < IMBALANCE_BULL:
            return None, f"imbalance {imbalance:.2f} below bullish threshold {IMBALANCE_BULL:.2f}", 0.0
        fair_yes = min(0.95, 0.50 + ENTRY_THRESHOLD + min(0.20, abs(slow) * 1.5))
        divergence = fair_yes - yes_price
        if divergence <= 0:
            return None, f"market already priced in (fair {fair_yes:.3f}, yes {yes_price:.3f})", divergence
        return "yes", f"BTC up {slow:+.3f}% / accel {accel:+.3f}% / imbalance {imbalance:.2f}", divergence
    if fast >= 0 or accel > 0.02:
        return None, f"down move lacks fresh acceleration ({fast:+.3f}% / {accel:+.3f}%)", 0.0
    if imbalance > IMBALANCE_BEAR:
        return None, f"imbalance {imbalance:.2f} above bearish threshold {IMBALANCE_BEAR:.2f}", 0.0
    fair_no = min(0.95, 0.50 + ENTRY_THRESHOLD + min(0.20, abs(slow) * 1.5))
    divergence = fair_no - no_price
    if divergence <= 0:
        return None, f"market already priced in (fair NO {fair_no:.3f}, no {no_price:.3f})", divergence
    return "no", f"BTC down {slow:+.3f}% / accel {accel:+.3f}% / imbalance {imbalance:.2f}", divergence

def run_fast_market_strategy(live=False, positions_only=False, show_config=False, quiet=False):
    def log(msg, force=False):
        if (not quiet) or force:
            print(msg)
    state = load_state()
    log("⚡ Simmer FastLoop Trading Skill")
    log("=" * 50)
    log(f"\n  [{'LIVE' if live else 'PAPER MODE'}] {'Real trades enabled.' if live else 'Trades simulated. Use --live for real trades.'}")
    log(f"\n⚙️  Configuration:")
    log(f"  Asset:            {ASSET}")
    log(f"  Window:           {WINDOW}")
    log(f"  Entry threshold:  {ENTRY_THRESHOLD}")
    log(f"  Min momentum:     {MIN_MOMENTUM_PCT}%")
    log(f"  Max position:     ${MAX_POSITION_USD:.2f}")
    log(f"  Signal source:    {SIGNAL_SOURCE}")
    log(f"  Lookback:         {LOOKBACK_MINUTES} minutes")
    log(f"  Min time left:    {MIN_TIME_REMAINING}s")
    log(f"  Volume weighting: {'✓' if VOLUME_CONFIDENCE else '✗'}")
    log(f"  Daily budget:     ${DAILY_BUDGET:.2f} (${state['spent']:.2f} spent today, {state['trades']} trades)")
    log(f"  Daily stops:      -${DAILY_LOSS_LIMIT:.2f} / +${DAILY_PROFIT_TARGET:.2f}")
    log(f"  Entry prices:     {MIN_ENTRY_PRICE:.2f} to {MAX_ENTRY_PRICE:.2f}, skip middle {SKIP_MID_LOW:.2f}-{SKIP_MID_HIGH:.2f}")
    log(f"  TP/SL:            +{TAKE_PROFIT_PCT:.0%} / -{STOP_LOSS_PCT:.0%}")
    if show_config:
        log(f"\n  Config file: {get_config_path(__file__)}")
        return
    if live:
        if os.environ.get("TRADING_VENUE", "sim") != "polymarket":
            log("🛑 Live mode requested but TRADING_VENUE is not 'polymarket'.", force=True)
            return
        if not os.environ.get("WALLET_PRIVATE_KEY"):
            log("🛑 Live mode requested but WALLET_PRIVATE_KEY is not set.", force=True)
            return
    redeemed = auto_redeem_if_possible()
    if redeemed:
        ok = [r for r in redeemed if isinstance(r, dict) and r.get("success")]
        if ok:
            log(f"💸 Auto-redeemed {len(ok)} winning position(s)")
    current_pnl, open_fast_positions = fast_market_positions_pnl()
    log(f"  Current fast-market P&L: ${current_pnl:.2f} across {open_fast_positions} open positions")
    if current_pnl <= -abs(DAILY_LOSS_LIMIT):
        log(f"🛑 Daily loss limit hit (${current_pnl:.2f}) — no more trades today", force=True)
        append_note(state, f"STOP loss limit hit at {current_pnl:.2f}")
        return
    if current_pnl >= DAILY_PROFIT_TARGET:
        log(f"🎯 Daily profit target hit (${current_pnl:.2f}) — no more trades today", force=True)
        append_note(state, f"STOP profit target hit at {current_pnl:.2f}")
        return
    cooldown, sec_left = in_loss_cooldown(state)
    if cooldown:
        log(f"⏸️  Cooldown after loss: {sec_left}s remaining")
        return
    if state["spent"] >= DAILY_BUDGET:
        log(f"⏸️  Daily budget exhausted (${state['spent']:.2f}/${DAILY_BUDGET:.2f})")
        return
    if state["trades"] >= MAX_TRADES_PER_DAY:
        log(f"⏸️  Max trades per day reached ({state['trades']}/{MAX_TRADES_PER_DAY})")
        return
    if positions_only:
        positions = get_positions()
        fast_positions = [p for p in positions if "up or down" in (p.get("question", "") or "").lower()]
        if not fast_positions:
            log("No open fast-market positions")
            return
        for p in fast_positions:
            log(f"{p.get('question','?')[:80]} | pnl ${float(p.get('pnl',0) or 0):.2f}")
        return
    log(f"\n🔍 Discovering {ASSET} fast markets...")
    markets = discover_fast_markets(ASSET, WINDOW)
    log(f"  Found {len(markets)} active fast markets")
    if not markets:
        return
    best = find_best_fast_market(markets)
    if not best:
        for m in markets[:50]:
            if m.get("is_live_now") is False:
                log(f"  Skipped: {m['question'][:60]}... (not live yet)")
        log("  No live tradeable markets among 50 found — waiting for next window")
        print("📊 Summary: No tradeable markets (0/50 live with enough time)")
        return
    end_time = best.get("end_time")
    remaining = int((end_time - datetime.now(timezone.utc)).total_seconds()) if end_time else 0
    log(f"\n🎯 Selected: {best['question']}")
    log(f"  Expires in: {remaining}s")
    clob_tokens = best.get("clob_token_ids", [])
    if not clob_tokens:
        log("  ⏸️  No CLOB tokens available")
        return
    yes_price = fetch_live_midpoint(clob_tokens[0])
    if yes_price is None:
        log("  ⏸️  Could not fetch live midpoint")
        return
    log(f"  Current YES price: ${yes_price:.3f} (live CLOB)")
    book = fetch_orderbook_summary(clob_tokens)
    if not book:
        log("  ⏸️  Could not fetch orderbook summary")
        return
    log(f"  Spread:           {book['spread_pct']:.1%} (bid ${book['best_bid']:.3f} / ask ${book['best_ask']:.3f})")
    log(f"  Imbalance:        {book['imbalance']:.2f} (YES-book bid depth share)")
    if book["spread_pct"] > MAX_SPREAD_PCT:
        log(f"  ⏸️  Spread {book['spread_pct']:.1%} > max {MAX_SPREAD_PCT:.1%}")
        print("📊 Summary: No trade (wide spread)")
        return
    log(f"\n📈 Fetching {ASSET} price signal ({SIGNAL_SOURCE})...")
    signal = get_market_signal(ASSET, LOOKBACK_MINUTES)
    if not signal:
        log("  ❌ Failed to fetch price data", force=True)
        return
    log(f"  Price:            ${signal['price_now']:,.2f} (was ${signal['price_then']:,.2f})")
    log(f"  Momentum:         {signal['momentum_pct']:+.3f}%")
    log(f"  Fast momentum:    {signal['fast_momentum_pct']:+.3f}%")
    log(f"  Acceleration:     {signal['acceleration_pct']:+.3f}%")
    log(f"  Direction:        {signal['direction']}")
    log(f"  Volume ratio:     {signal['volume_ratio']:.2f}x avg")
    log(f"\n🧠 Analyzing...")
    side, rationale, divergence = compute_signal_and_side(yes_price, signal, book["imbalance"], remaining)
    if not side:
        log(f"  ⏸️  {rationale}")
        print(f"📊 Summary: No trade ({rationale})")
        return
    buy_price = yes_price if side == "yes" else (1 - yes_price)
    skip_price, reason = should_skip_price(buy_price)
    if skip_price:
        log(f"  ⏸️  {reason}")
        print(f"📊 Summary: No trade ({reason})")
        return
    for p in get_positions():
        q = (p.get("question") or "").lower()
        if q == (best.get("question") or "").lower():
            shares_yes = float(p.get("shares_yes", 0) or 0)
            shares_no = float(p.get("shares_no", 0) or 0)
            if shares_yes > 0 or shares_no > 0:
                log("  ⏸️  Already holding this market")
                return
    position_size = min(MAX_POSITION_USD, DAILY_BUDGET - state["spent"])
    if position_size <= 0:
        log("  ⏸️  No budget remaining")
        return
    est_shares = position_size / buy_price if buy_price > 0 else 0
    if est_shares < 5:
        log(f"  ⏸️  Position too small for 5+ shares (est {est_shares:.1f})")
        return
    if not best.get("market_id"):
        market_id, err = import_fast_market(best["slug"])
        if not market_id:
            log(f"  ❌ Import failed: {err}", force=True)
            return
        best["market_id"] = market_id
    market_id = best["market_id"]
    log(f"  ✅ Signal:        {side.upper()} — {rationale}", force=True)
    log(f"  Divergence:      {divergence:.3f}", force=True)
    log(f"  Buy price:       ${buy_price:.3f}", force=True)
    log(f"  Order size:      ${position_size:.2f} (~{est_shares:.1f} shares)", force=True)
    result = execute_trade(
        market_id=market_id,
        side=side,
        amount_usd=position_size,
        live=live,
        reasoning=f"{rationale}; yes={yes_price:.3f}; buy={buy_price:.3f}; rem={remaining}s"
    )
    if not result.get("success"):
        log(f"  ❌ Trade failed: {result.get('error')}", force=True)
        return
    state["last_trade_ts"] = int(time.time())
    if live:
        state["spent"] += float(result.get("cost") or position_size)
        state["trades"] += 1
    save_state(state)
    shares = float(result.get("shares_bought") or est_shares)
    tag = "PAPER" if result.get("simulated") else "LIVE"
    log(f"  ✅ [{tag}] Bought {shares:.1f} {side.upper()} shares @ ${buy_price:.3f}", force=True)
    monitor_res = best_effort_set_monitor(market_id, side, TAKE_PROFIT_PCT, STOP_LOSS_PCT)
    if isinstance(monitor_res, dict) and monitor_res.get("error"):
        log(f"  ⚠️  Monitor not set: {monitor_res['error']}")
    else:
        log(f"  🛡️  Monitor set: TP +{TAKE_PROFIT_PCT:.0%} / SL -{STOP_LOSS_PCT:.0%}")
    if JOURNAL_AVAILABLE and not result.get("simulated"):
        confidence = min(0.95, 0.5 + abs(divergence) + min(0.20, abs(signal["momentum_pct"]) / 100))
        log_trade(
            trade_id=result.get("trade_id"),
            source=TRADE_SOURCE,
            skill_slug=SKILL_SLUG,
            thesis=rationale,
            confidence=round(confidence, 2),
            asset=ASSET,
            momentum_pct=round(signal["momentum_pct"], 3),
            fast_momentum_pct=round(signal["fast_momentum_pct"], 3),
            acceleration_pct=round(signal["acceleration_pct"], 3),
            imbalance=round(book["imbalance"], 3),
            signal_source=SIGNAL_SOURCE,
        )
    print("\n📊 Summary:")
    print(f"  Sprint: {best['question'][:70]}")
    print(f"  Signal: {signal['direction']} {signal['momentum_pct']:.3f}% | YES ${yes_price:.3f}")
    print(f"  Action: {'PAPER' if result.get('simulated') else 'LIVE'}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simmer FastLoop Trading Skill (live-ready)")
    parser.add_argument("--live", action="store_true", help="Execute real trades")
    parser.add_argument("--positions", action="store_true", help="Show current fast-market positions")
    parser.add_argument("--config", action="store_true", help="Show current config path")
    parser.add_argument("--set", action="append", metavar="KEY=VALUE",
                        help="Update config (e.g. --set min_momentum_pct=0.05)")
    parser.add_argument("--quiet", "-q", action="store_true", help="Only print important lines")
    args = parser.parse_args()
    if args.set:
        updates = {}
        for item in args.set:
            if "=" not in item:
                print(f"Invalid --set format: {item}. Use KEY=VALUE")
                sys.exit(1)
            key, val = item.split("=", 1)
            if key not in CONFIG_SCHEMA:
                print(f"Unknown config key: {key}")
                print(f"Valid keys: {', '.join(CONFIG_SCHEMA.keys())}")
                sys.exit(1)
            type_fn = CONFIG_SCHEMA[key].get("type", str)
            try:
                updates[key] = val.lower() in ("1", "true", "yes", "on") if type_fn == bool else type_fn(val)
            except Exception:
                print(f"Invalid value for {key}: {val}")
                sys.exit(1)
        update_config(updates, __file__)
        print(f"✅ Config updated: {json.dumps(updates)}")
        sys.exit(0)
    while True:
        try:
            run_fast_market_strategy(
                live=args.live,
                positions_only=args.positions,
                show_config=args.config,
                quiet=args.quiet,
            )
        except Exception as e:
            print(f"Loop error: {e}")
        print(f"\n⏳ Waiting {SCAN_INTERVAL_SEC} seconds before next scan...\n")
        time.sleep(SCAN_INTERVAL_SEC)
