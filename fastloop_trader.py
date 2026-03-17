#!/usr/bin/env python3
"""
Simmer FastLoop Trading Skill

Trades Polymarket BTC 5-minute fast markets using CEX price momentum.
Default signal: Binance BTCUSDT candles. Agents can customize signal source.

Usage:
    python fast_trader.py              # Dry run (show opportunities, no trades)
    python fast_trader.py --live       # Execute real trades
    python fast_trader.py --positions  # Show current fast market positions
    python fast_trader.py --quiet      # Only output on trades/errors

Requires:
    SIMMER_API_KEY environment variable (get from simmer.markets/dashboard)
"""

import os
import sys
import json
import math
import argparse
import time
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote

# Force line-buffered stdout for non-TTY environments (cron, Docker, OpenClaw)
sys.stdout.reconfigure(line_buffering=True)

# Optional: Trade Journal integration
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

# =============================================================================
# Configuration (config.json > env vars > defaults)
# =============================================================================

CONFIG_SCHEMA = {
    "entry_threshold": {"default": 0.05, "env": "SIMMER_SPRINT_ENTRY", "type": float,
                        "help": "Min price divergence from 50¢ to trigger trade"},
    "min_momentum_pct": {"default": 0.05, "env": "SIMMER_SPRINT_MOMENTUM", "type": float,
                         "help": "Min BTC % move in lookback window to trigger"},
    "max_position": {"default": 2.5, "env": "SIMMER_SPRINT_MAX_POSITION", "type": float,
                     "help": "Max $ per trade"},
    "signal_source": {"default": "binance", "env": "SIMMER_SPRINT_SIGNAL", "type": str,
                      "help": "Price feed source (binance)"},
    "lookback_minutes": {"default": 5, "env": "SIMMER_SPRINT_LOOKBACK", "type": int,
                         "help": "Minutes of price history for momentum calc"},
    "min_time_remaining": {"default": 30, "env": "SIMMER_SPRINT_MIN_TIME", "type": int,
                           "help": "Skip fast_markets with less than this many seconds remaining (0 = auto: 10%% of window)"},
    "asset": {"default": "BTC", "env": "SIMMER_SPRINT_ASSET", "type": str,
              "help": "Asset to trade (BTC, ETH, SOL)"},
    "window": {"default": "5m", "env": "SIMMER_SPRINT_WINDOW", "type": str,
               "help": "Market window duration (5m or 15m)"},
    "volume_confidence": {"default": True, "env": "SIMMER_SPRINT_VOL_CONF", "type": bool,
                          "help": "Weight signal by volume (higher volume = more confident)"},
    "daily_budget": {"default": 0.0, "env": "SIMMER_SPRINT_DAILY_BUDGET", "type": float,
                     "help": "Legacy budget cap (unused)"},
    "max_open_exposure": {"default": 2.5, "env": "SIMMER_SPRINT_MAX_EXPOSURE", "type": float,
                            "help": "Maximum simultaneous open exposure across active positions"},
    "take_profit_pct": {"default": 0.15, "env": "SIMMER_SPRINT_TP", "type": float,
                        "help": "Take profit percentage for position exits"},
    "stop_loss_pct": {"default": 0.08, "env": "SIMMER_SPRINT_SL", "type": float,
                      "help": "Stop loss percentage for position exits"},
    "daily_loss_limit": {"default": 20.0, "env": "SIMMER_SPRINT_DAILY_LOSS", "type": float,
                         "help": "Stop trading after this much realized loss in a UTC day"},
    "daily_profit_target": {"default": 0.0, "env": "SIMMER_SPRINT_DAILY_PROFIT", "type": float,
                            "help": "Legacy profit target (unused)"},
    "max_trades_per_day": {"default": 0, "env": "SIMMER_SPRINT_MAX_TRADES", "type": int,
                           "help": "Legacy trade cap (unused)"},
    "pause_hours_after_loss": {"default": 24, "env": "SIMMER_SPRINT_PAUSE_HOURS", "type": int,
                           "help": "Pause new entries for this many hours after loss stop is hit"},
    "resolution_exit_seconds": {"default": 60, "env": "SIMMER_SPRINT_RESOLVE_EXIT", "type": int,
                                "help": "Exit paper positions this many seconds before market expiry if still open"},
}

TRADE_SOURCE = "sdk:fastloop"
SKILL_SLUG = "polymarket-fast-loop"
_automaton_reported = False
SMART_SIZING_PCT = 0.05  # 5% of balance per trade
MIN_SHARES_PER_ORDER = 5  # Polymarket minimum
MAX_SPREAD_PCT = 0.06     # Skip if CLOB bid-ask spread exceeds this
MIN_ENTRY_PRICE = 0.05
MAX_ENTRY_PRICE = 0.99
SKIP_MIDDLE_LOW = 0.35
SKIP_MIDDLE_HIGH = 0.65
MOMENTUM_MAX_ENTRY = 0.35
CONTRARIAN_LOW = 0.15
CONTRARIAN_HIGH = 0.85
BAD_MARKET_COOLDOWN_CYCLES = 3
SCAN_INTERVAL_SECONDS = 30
SINGLE_POSITION_LIVE_MODE = True
ENABLE_CONTRARIAN = False
LIVE_TIME_STOP_SECONDS = 60
LIVE_MAX_HOLD_SECONDS = 90

# Asset → Binance symbol mapping
ASSET_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
}

# Asset → Gamma API search patterns
ASSET_PATTERNS = {
    "BTC": ["bitcoin up or down"],
    "ETH": ["ethereum up or down"],
    "SOL": ["solana up or down"],
}


from simmer_sdk.skill import load_config, update_config, get_config_path

# Load config
cfg = load_config(CONFIG_SCHEMA, __file__, slug="polymarket-fast-loop")
ENTRY_THRESHOLD = cfg["entry_threshold"]
MIN_MOMENTUM_PCT = cfg["min_momentum_pct"]
MAX_POSITION_USD = cfg["max_position"]
_automaton_max = os.environ.get("AUTOMATON_MAX_BET")
if _automaton_max:
    MAX_POSITION_USD = min(MAX_POSITION_USD, float(_automaton_max))
SIGNAL_SOURCE = cfg["signal_source"]
LOOKBACK_MINUTES = cfg["lookback_minutes"]
ASSET = cfg["asset"].upper()
WINDOW = cfg["window"]  # "5m" or "15m"

# Dynamic min_time_remaining: 0 = auto (10% of window duration)
_window_seconds = {"5m": 300, "15m": 900, "1h": 3600}
_configured_min_time = cfg["min_time_remaining"]
if _configured_min_time > 0:
    MIN_TIME_REMAINING = _configured_min_time
else:
    MIN_TIME_REMAINING = max(30, _window_seconds.get(WINDOW, 300) // 10)
VOLUME_CONFIDENCE = cfg["volume_confidence"]
DAILY_BUDGET = cfg["daily_budget"]
MAX_OPEN_EXPOSURE = cfg["max_open_exposure"]
TAKE_PROFIT_PCT = cfg["take_profit_pct"]
STOP_LOSS_PCT = cfg["stop_loss_pct"]
DAILY_LOSS_LIMIT = cfg["daily_loss_limit"]
DAILY_PROFIT_TARGET = cfg["daily_profit_target"]
MAX_TRADES_PER_DAY = cfg["max_trades_per_day"]
PAUSE_HOURS_AFTER_LOSS = cfg["pause_hours_after_loss"]
RESOLUTION_EXIT_SECONDS = cfg["resolution_exit_seconds"]

# Polymarket crypto fee formula constants (from docs.polymarket.com/trading/fees)
# fee = C × p × POLY_FEE_RATE × (p × (1-p))^POLY_FEE_EXPONENT
POLY_FEE_RATE = 0.25       # Crypto markets
POLY_FEE_EXPONENT = 2      # Crypto markets


# =============================================================================
# Daily Budget Tracking
# =============================================================================

def _get_spend_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "daily_spend.json"


def _load_daily_spend(skill_file):
    """Load today's spend. Resets if date != today (UTC)."""
    spend_path = _get_spend_path(skill_file)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if spend_path.exists():
        try:
            with open(spend_path) as f:
                data = json.load(f)
            if data.get("date") == today:
                return data
        except (json.JSONDecodeError, IOError):
            pass
    return {"date": today, "spent": 0.0, "trades": 0}


def _save_daily_spend(skill_file, spend_data):
    """Save daily spend to file."""
    spend_path = _get_spend_path(skill_file)
    with open(spend_path, "w") as f:
        json.dump(spend_data, f, indent=2)


def _get_bad_market_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "bad_markets.json"


def _load_bad_markets(skill_file):
    path = _get_bad_market_path(skill_file)
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def _save_bad_markets(skill_file, data):
    path = _get_bad_market_path(skill_file)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def _market_cache_key(market):
    return str(market.get("market_id") or market.get("slug") or market.get("question") or "")


def _cooldown_is_active(cooldowns, market):
    key = _market_cache_key(market)
    return cooldowns.get(key, 0) > 0


def _set_market_cooldown(skill_file, market, cycles=BAD_MARKET_COOLDOWN_CYCLES):
    cooldowns = _load_bad_markets(skill_file)
    key = _market_cache_key(market)
    if key:
        cooldowns[key] = max(cycles, int(cooldowns.get(key, 0)))
        _save_bad_markets(skill_file, cooldowns)


def _tick_market_cooldowns(skill_file):
    cooldowns = _load_bad_markets(skill_file)
    if not cooldowns:
        return {}
    updated = {}
    for key, value in cooldowns.items():
        try:
            remaining = int(value) - 1
        except Exception:
            remaining = 0
        if remaining > 0:
            updated[key] = remaining
    _save_bad_markets(skill_file, updated)
    return updated



# =============================================================================
# Paper State Tracking
# =============================================================================

def _get_paper_state_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "paper_state.json"


def _load_paper_state(skill_file):
    """Load today's paper state. Resets automatically at UTC date rollover."""
    path = _get_paper_state_path(skill_file)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            if data.get("date") == today and isinstance(data.get("open_positions", []), list):
                data.setdefault("spent", 0.0)
                data.setdefault("trades", 0)
                data.setdefault("realized_pnl", 0.0)
                data.setdefault("wins", 0)
                data.setdefault("losses", 0)
                return data
        except (json.JSONDecodeError, IOError):
            pass
    return {
        "date": today,
        "spent": 0.0,
        "trades": 0,
        "realized_pnl": 0.0,
        "wins": 0,
        "losses": 0,
        "open_positions": [],
    }


def _save_paper_state(skill_file, state):
    path = _get_paper_state_path(skill_file)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)

def _get_guard_state_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "guard_state.json"


def _load_guard_state(skill_file):
    path = _get_guard_state_path(skill_file)
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, dict):
                data.setdefault("pause_until", None)
                data.setdefault("reason", "")
                data.setdefault("trigger_pnl", 0.0)
                return data
        except (json.JSONDecodeError, IOError):
            pass
    return {"pause_until": None, "reason": "", "trigger_pnl": 0.0}


def _save_guard_state(skill_file, state):
    path = _get_guard_state_path(skill_file)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def _parse_iso_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _guard_pause_remaining(skill_file):
    state = _load_guard_state(skill_file)
    until = _parse_iso_dt(state.get("pause_until"))
    if until is None:
        return state, 0
    now = datetime.now(timezone.utc)
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    remaining = int((until - now).total_seconds())
    if remaining <= 0:
        state["pause_until"] = None
        state["reason"] = ""
        state["trigger_pnl"] = 0.0
        _save_guard_state(skill_file, state)
        return state, 0
    return state, remaining


def _activate_loss_pause(skill_file, realized_pnl, reason="daily_loss_stop"):
    state = _load_guard_state(skill_file)
    until = datetime.now(timezone.utc) + timedelta(hours=int(PAUSE_HOURS_AFTER_LOSS))
    state["pause_until"] = until.isoformat()
    state["reason"] = reason
    state["trigger_pnl"] = round(float(realized_pnl), 6)
    _save_guard_state(skill_file, state)
    return state


def _get_live_runtime_state_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "live_runtime_state.json"


def _load_live_runtime_state(skill_file):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = _get_live_runtime_state_path(skill_file)
    default = {"date": today, "baseline_total_pnl": None, "market_locks": []}
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, dict) and data.get("date") == today:
                data.setdefault("baseline_total_pnl", None)
                data.setdefault("market_locks", [])
                return data
        except (json.JSONDecodeError, IOError):
            pass
    return default


def _save_live_runtime_state(skill_file, state):
    path = _get_live_runtime_state_path(skill_file)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def _prune_live_runtime_state(skill_file, state=None):
    state = state or _load_live_runtime_state(skill_file)
    now = datetime.now(timezone.utc)
    cleaned = []
    for lock in state.get("market_locks", []):
        until = _parse_iso_dt(lock.get("until"))
        if until is None:
            continue
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        if until > now:
            cleaned.append(lock)
    state["market_locks"] = cleaned
    _save_live_runtime_state(skill_file, state)
    return state


def _market_lock_key(market_id=None, question=None):
    if market_id:
        return str(market_id)
    return (question or "").strip().lower()


def _live_market_lock_active(state, market_id=None, question=None):
    key = _market_lock_key(market_id=market_id, question=question)
    if not key:
        return False
    for lock in state.get("market_locks", []):
        if lock.get("key") == key:
            return True
    return False


def _current_live_locked_exposure(state):
    return round(sum(float(lock.get("entry_cost", 0.0) or 0.0) for lock in state.get("market_locks", [])), 6)


def _register_live_market_lock(skill_file, market_id, question, end_time, entry_cost, side, shares=None, entry_price=None, entry_time=None):
    state = _prune_live_runtime_state(skill_file)
    key = _market_lock_key(market_id=market_id, question=question)
    if not key:
        return state
    if end_time is None:
        end_time = datetime.now(timezone.utc) + timedelta(seconds=_window_seconds.get(WINDOW, 300))
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)
    state["market_locks"] = [lock for lock in state.get("market_locks", []) if lock.get("key") != key]
    state["market_locks"].append({
        "key": key,
        "market_id": market_id,
        "question": question,
        "entry_cost": round(float(entry_cost), 6),
        "side": side,
        "shares": round(float(shares), 6) if shares else None,
        "entry_price": round(float(entry_price), 6) if entry_price is not None else None,
        "entry_time": entry_time or datetime.now(timezone.utc).isoformat(),
        "closed": False,
        "until": end_time.isoformat(),
    })
    _save_live_runtime_state(skill_file, state)
    return state

def _get_live_trade_ledger_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "live_trade_ledger.jsonl"


def _append_live_trade_event(skill_file, event):
    path = _get_live_trade_ledger_path(skill_file)
    payload = dict(event)
    payload.setdefault("ts", datetime.now(timezone.utc).isoformat())
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _find_live_position(market_id=None, question=None, side=None):
    positions = get_positions()
    qnorm = (question or "").strip().lower()
    for pos in positions or []:
        pq = (pos.get("question") or "").strip().lower()
        if market_id and pos.get("market_id") != market_id and qnorm and pq != qnorm:
            continue
        if not market_id and qnorm and pq != qnorm:
            continue
        pside = _position_side_from_dict(pos)
        if side and pside != side:
            continue
        shares = _position_shares_for_side(pos, pside)
        if shares > 0:
            return pos
    return None


def _confirm_live_fill(skill_file, market_id, question, side, entry_cost, quoted_price, shares_hint=0.0, attempts=4, sleep_seconds=1.0):
    confirmed = None
    for _ in range(max(1, attempts)):
        pos = _find_live_position(market_id=market_id, question=question, side=side)
        if pos:
            confirmed = pos
            break
        time.sleep(max(0.0, sleep_seconds))
    shares = float(shares_hint or 0.0)
    avg_fill = float(quoted_price)
    if confirmed:
        shares = _position_shares_for_side(confirmed, side) or shares
        actual_cost = _best_live_entry_cost(confirmed) or float(entry_cost)
        if shares > 0 and actual_cost > 0:
            avg_fill = actual_cost / shares
        return confirmed, shares, actual_cost, avg_fill
    if shares > 0 and entry_cost > 0:
        avg_fill = float(entry_cost) / shares
    return None, shares, float(entry_cost), avg_fill


def _current_paper_open_exposure(state):
    return round(sum(float(p.get("entry_cost", 0.0)) for p in state.get("open_positions", [])), 6)


def _estimate_live_open_exposure(positions):
    exposure = 0.0
    count = 0
    for pos in positions or []:
        held = float(pos.get("shares_yes", 0) or 0) + float(pos.get("shares_no", 0) or 0)
        if held <= 0:
            continue
        if "up or down" not in (pos.get("question", "") or "").lower():
            continue
        count += 1
        exposure += float(pos.get("entry_cost", 0.0) or pos.get("cost_basis", 0.0) or pos.get("notional_usdc", 0.0) or MAX_POSITION_USD)
    return round(exposure, 6), count


def _infer_live_fill_price(position_size, shares, quoted_price):
    """Best-effort live average fill price.

    For live trades, Simmer/venue may move between quote and execution. If we know
    the notional sent and the filled shares, derive an average fill price from
    amount/shares instead of logging the stale pre-trade quote.
    """
    try:
        shares = float(shares or 0)
        if shares > 0 and position_size > 0:
            inferred = float(position_size) / shares
            if 0 < inferred < 1:
                return inferred
    except Exception:
        pass
    return float(quoted_price)


def _position_side_from_dict(pos):
    yes = float(pos.get("shares_yes", 0) or 0)
    no = float(pos.get("shares_no", 0) or 0)
    if yes > 0 and yes >= no:
        return "yes"
    if no > 0:
        return "no"
    return None


def _position_shares_for_side(pos, side):
    if side == "yes":
        return float(pos.get("shares_yes", 0) or 0)
    if side == "no":
        return float(pos.get("shares_no", 0) or 0)
    return 0.0


def _get_live_market_lock(state, market_id=None, question=None):
    key = _market_lock_key(market_id=market_id, question=question)
    if not key:
        return None
    for lock in state.get("market_locks", []):
        if lock.get("key") == key:
            return lock
    return None


def _mark_live_market_lock_closed(skill_file, market_id=None, question=None):
    state = _prune_live_runtime_state(skill_file)
    key = _market_lock_key(market_id=market_id, question=question)
    changed = False
    for lock in state.get("market_locks", []):
        if lock.get("key") == key:
            lock["entry_cost"] = 0.0
            lock["closed"] = True
            changed = True
    if changed:
        _save_live_runtime_state(skill_file, state)
    return state


def _best_live_entry_cost(pos, runtime_state=None):
    for key in ("entry_cost", "cost_basis", "notional_usdc"):
        try:
            value = float(pos.get(key, 0) or 0)
            if value > 0:
                return value
        except Exception:
            pass
    try:
        current_value = float(pos.get("current_value", 0) or 0)
        pnl = float(pos.get("pnl", 0) or 0)
        inferred = current_value - pnl
        if inferred > 0:
            return inferred
    except Exception:
        pass
    if runtime_state is not None:
        lock = _get_live_market_lock(runtime_state, market_id=pos.get("market_id"), question=pos.get("question"))
        if lock:
            try:
                value = float(lock.get("entry_cost", 0) or 0)
                if value > 0:
                    return value
            except Exception:
                pass
    return 0.0


def _position_end_time(pos):
    for key in ("resolves_at", "end_time", "until"):
        val = pos.get(key)
        dt = _parse_resolves_at(val) if isinstance(val, str) else None
        if dt is not None:
            return dt
    q = pos.get("question", "")
    return _parse_fast_market_end_time(q) if q else None


def _set_live_monitor(market_id, side, log):
    try:
        client = get_client()
        client.set_monitor(market_id, side=side, stop_loss_pct=STOP_LOSS_PCT, take_profit_pct=TAKE_PROFIT_PCT)
        log(f"  🛡️  Live monitor armed for {side.upper()} ({STOP_LOSS_PCT:.0%} stop / {TAKE_PROFIT_PCT:.0%} take profit)")
        return True
    except Exception as e:
        log(f"  ⚠️  Could not set live monitor: {e}")
        return False


def manage_live_positions(skill_file, log):
    """Actively exit live positions and never scan for fresh entries while one is open."""
    state = _prune_live_runtime_state(skill_file)
    positions = get_positions()
    if not positions:
        return state, []

    closed = []
    now = datetime.now(timezone.utc)

    for pos in positions:
        question = pos.get("question", "") or ""
        if "up or down" not in question.lower():
            continue
        side = _position_side_from_dict(pos)
        if side not in ("yes", "no"):
            continue
        shares = _position_shares_for_side(pos, side)
        if shares <= 0:
            continue

        lock = _get_live_market_lock(state, market_id=pos.get("market_id"), question=question)
        entry_cost = _best_live_entry_cost(pos, state)
        if entry_cost <= 0:
            continue
        pnl = float(pos.get("pnl", 0) or 0)
        pnl_pct = pnl / entry_cost if entry_cost > 0 else None
        end_time = _position_end_time(pos)
        seconds_left = (end_time - now).total_seconds() if end_time else None
        entry_time = _parse_iso_dt((lock or {}).get("entry_time"))
        hold_seconds = (now - entry_time).total_seconds() if entry_time else None

        reason = None
        if pnl_pct is not None and pnl_pct <= -abs(STOP_LOSS_PCT):
            reason = "stop_loss"
        elif pnl_pct is not None and pnl_pct >= abs(TAKE_PROFIT_PCT):
            reason = "take_profit"
        elif seconds_left is not None and seconds_left <= LIVE_TIME_STOP_SECONDS and pnl < 0:
            reason = "time_exit"
        elif hold_seconds is not None and hold_seconds >= LIVE_MAX_HOLD_SECONDS and pnl < 0:
            reason = "max_hold_exit"

        if not reason:
            continue

        result = execute_trade(pos.get("market_id"), side, shares=shares, action="sell")
        if result and result.get("success"):
            proceeds = float(result.get("cost") or float(pos.get("current_value", 0) or 0.0) or 0.0)
            avg_exit = (proceeds / shares) if shares > 0 and proceeds > 0 else None
            log(
                f"  ✅ Sold {shares:.2f} {side.upper()} shares"
                + (f" @ ${avg_exit:.3f}" if avg_exit else "")
                + f" ({reason}, est P&L ${pnl:.2f})",
                force=True,
            )
            _mark_live_market_lock_closed(skill_file, market_id=pos.get("market_id"), question=question)
            _append_live_trade_event(skill_file, {
                "type": "exit",
                "market_id": pos.get("market_id"),
                "question": question,
                "side": side,
                "shares": shares,
                "entry_cost": round(entry_cost, 6),
                "exit_value": round(proceeds, 6),
                "avg_exit": round(avg_exit, 6) if avg_exit else None,
                "reason": reason,
                "estimated_pnl": round(pnl, 6),
            })
            closed.append({
                "market_id": pos.get("market_id"),
                "question": question,
                "side": side,
                "shares": shares,
                "reason": reason,
                "estimated_pnl": round(pnl, 6),
            })
        else:
            err = result.get("error", "Unknown error") if result else "No response"
            log(f"  ❌ Live sell failed on {question[:45]}... ({reason}): {err}", force=True)

    return _prune_live_runtime_state(skill_file), closed


def _extract_live_pnl_fields():
    """Read live P&L fields from Simmer portfolio response.

    Simmer may return either a plain dict or an object/dataclass. This helper
    normalizes the response and extracts the P&L fields we care about.
    """
    try:
        portfolio = get_portfolio()
        if not portfolio:
            return None

        def _normalize(obj):
            if obj is None:
                return None
            if isinstance(obj, dict):
                return obj
            try:
                from dataclasses import asdict, is_dataclass
                if is_dataclass(obj):
                    return asdict(obj)
            except Exception:
                pass
            if hasattr(obj, "model_dump"):
                try:
                    dumped = obj.model_dump()
                    if isinstance(dumped, dict):
                        return dumped
                except Exception:
                    pass
            if hasattr(obj, "_asdict"):
                try:
                    dumped = obj._asdict()
                    if isinstance(dumped, dict):
                        return dumped
                except Exception:
                    pass
            if hasattr(obj, "__dict__"):
                try:
                    dumped = vars(obj)
                    if isinstance(dumped, dict):
                        return dumped
                except Exception:
                    pass
            return None

        portfolio = _normalize(portfolio)
        if not portfolio or portfolio.get("error"):
            return None

        def _to_float(value):
            if value is None:
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        def _get_path(obj, *path):
            cur = obj
            for key in path:
                if not isinstance(cur, dict):
                    return None
                cur = cur.get(key)
                cur = _normalize(cur) if not isinstance(cur, (str, int, float, bool, type(None))) else cur
            return cur

        pnl_24h = None
        pnl_total = None

        candidate_24h_paths = [
            ("pnl_24h",),
            ("stats", "pnl_24h"),
            ("summary", "pnl_24h"),
            ("portfolio", "pnl_24h"),
            ("metrics", "pnl_24h"),
        ]
        candidate_total_paths = [
            ("pnl_total",),
            ("stats", "pnl_total"),
            ("summary", "pnl_total"),
            ("portfolio", "pnl_total"),
            ("metrics", "pnl_total"),
            ("realized_pnl",),
            ("total_pnl",),
        ]

        for path in candidate_24h_paths:
            pnl_24h = _to_float(_get_path(portfolio, *path))
            if pnl_24h is not None:
                break

        for path in candidate_total_paths:
            pnl_total = _to_float(_get_path(portfolio, *path))
            if pnl_total is not None:
                break

        return {
            "pnl_24h": pnl_24h,
            "pnl_total": pnl_total,
            "portfolio": portfolio,
        }

    except Exception as e:
        print(f"  ⚠️  Live P&L extraction error: {e}")
        return None


def _get_live_pnl_snapshot(skill_file):
    """Return live total P&L and an effective 24h P&L.

    If Simmer does not provide pnl_24h, derive it from today's baseline pnl_total.
    """
    live_pnl = _extract_live_pnl_fields()
    if not live_pnl:
        return {"pnl_total": None, "pnl_24h_effective": None, "pnl_24h_raw": None, "portfolio": None}

    state = _prune_live_runtime_state(skill_file)
    pnl_total = live_pnl.get("pnl_total")
    pnl_24h_raw = live_pnl.get("pnl_24h")
    baseline = state.get("baseline_total_pnl")

    if pnl_total is not None and baseline is None:
        state["baseline_total_pnl"] = float(pnl_total)
        baseline = state["baseline_total_pnl"]
        _save_live_runtime_state(skill_file, state)

    pnl_24h_effective = pnl_24h_raw
    if pnl_24h_effective is None and pnl_total is not None and baseline is not None:
        pnl_24h_effective = float(pnl_total) - float(baseline)

    return {
        "pnl_total": pnl_total,
        "pnl_24h_raw": pnl_24h_raw,
        "pnl_24h_effective": pnl_24h_effective,
        "portfolio": live_pnl.get("portfolio"),
    }


def _paper_has_open_position(state, market_id=None, question=None):
    for pos in state.get("open_positions", []):
        if market_id and pos.get("market_id") == market_id:
            return True
        if question and pos.get("question", "").lower() == (question or "").lower():
            return True
    return False


def _estimate_fee_per_share(price):
    return price * (POLY_FEE_RATE * (price * (1 - price)) ** POLY_FEE_EXPONENT)


def _close_paper_position(state, pos, exit_price, reason):
    shares = float(pos.get("shares", 0.0))
    entry_price = float(pos.get("entry_price", 0.0))
    entry_fee_per_share = float(pos.get("entry_fee_per_share", _estimate_fee_per_share(entry_price)))
    exit_fee_per_share = float(_estimate_fee_per_share(exit_price))
    gross = shares * (exit_price - entry_price)
    fees = shares * (entry_fee_per_share + exit_fee_per_share)
    realized = gross - fees
    state["realized_pnl"] = round(float(state.get("realized_pnl", 0.0)) + realized, 6)
    if realized >= 0:
        state["wins"] = int(state.get("wins", 0)) + 1
    else:
        state["losses"] = int(state.get("losses", 0)) + 1
    pos["exit_price"] = exit_price
    pos["exit_reason"] = reason
    pos["realized_pnl"] = round(realized, 6)
    return realized, fees


def manage_paper_positions(skill_file, log):
    """Check open paper positions for TP/SL/time exits and update paper ledger."""
    state = _load_paper_state(skill_file)
    open_positions = list(state.get("open_positions", []))
    if not open_positions:
        return state, []

    remaining_positions = []
    closed = []

    for pos in open_positions:
        clob_tokens = pos.get("clob_token_ids") or []
        yes_price = fetch_live_prices(clob_tokens) if clob_tokens else None
        if yes_price is None:
            remaining_positions.append(pos)
            continue

        current_price = yes_price if pos.get("side") == "yes" else (1 - yes_price)
        target_price = float(pos.get("target_price", 0.0))
        stop_price = float(pos.get("stop_price", 0.0))
        end_time = _parse_resolves_at(pos.get("end_time")) if isinstance(pos.get("end_time"), str) else pos.get("end_time")
        seconds_left = None
        if end_time:
            seconds_left = (end_time - datetime.now(timezone.utc)).total_seconds()

        reason = None
        if current_price >= target_price > 0:
            reason = "take_profit"
        elif current_price <= stop_price < 1:
            reason = "stop_loss"
        elif seconds_left is not None and seconds_left <= RESOLUTION_EXIT_SECONDS:
            reason = "time_exit"

        if reason:
            realized, fees = _close_paper_position(state, pos, current_price, reason)
            closed.append({
                "question": pos.get("question", "Unknown"),
                "side": pos.get("side"),
                "shares": pos.get("shares", 0.0),
                "entry_price": pos.get("entry_price", 0.0),
                "exit_price": round(current_price, 6),
                "reason": reason,
                "realized_pnl": round(realized, 6),
                "fees": round(fees, 6),
            })
            log(
                f"  ✅ [PAPER] Sold {float(pos.get('shares', 0.0)):.1f} {str(pos.get('side', '')).upper()} shares @ ${current_price:.3f} "
                f"({reason}, P&L ${realized:.2f})",
                force=True,
            )
        else:
            pos["last_price"] = round(current_price, 6)
            remaining_positions.append(pos)

    state["open_positions"] = remaining_positions
    _save_paper_state(skill_file, state)
    return state, closed

# =============================================================================
# API Helpers
# =============================================================================

_client = None

def get_client(live=True):
    """Lazy-init SimmerClient singleton."""
    global _client
    if _client is None:
        try:
            from simmer_sdk import SimmerClient
        except ImportError:
            print("Error: simmer-sdk not installed. Run: pip install simmer-sdk")
            sys.exit(1)
        api_key = os.environ.get("SIMMER_API_KEY")
        if not api_key:
            print("Error: SIMMER_API_KEY environment variable not set")
            print("Get your API key from: simmer.markets/dashboard → SDK tab")
            sys.exit(1)
        venue = os.environ.get("TRADING_VENUE", "polymarket")
        _client = SimmerClient(api_key=api_key, venue=venue, live=live)
    return _client


def _api_request(url, method="GET", data=None, headers=None, timeout=15):
    """Make an HTTP request to external APIs (Binance, CoinGecko, Gamma). Returns parsed JSON or None on error."""
    try:
        req_headers = headers or {}
        if "User-Agent" not in req_headers:
            req_headers["User-Agent"] = "simmer-fastloop_market/1.0"
        body = None
        if data:
            body = json.dumps(data).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        req = Request(url, data=body, headers=req_headers, method=method)
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        try:
            error_body = json.loads(e.read().decode("utf-8"))
            return {"error": error_body.get("detail", str(e)), "status_code": e.code}
        except Exception:
            return {"error": str(e), "status_code": e.code}
    except URLError as e:
        return {"error": f"Connection error: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}


CLOB_API = "https://clob.polymarket.com"


def _lookup_fee_rate(token_id):
    """Fetch taker fee rate (bps) from Polymarket CLOB for a token. Returns 0 on failure."""
    result = _api_request(f"{CLOB_API}/fee-rate?token_id={quote(str(token_id))}", timeout=5)
    if not result or not isinstance(result, dict) or result.get("error"):
        return 0
    try:
        return int(float(result.get("base_fee") or 0))
    except (ValueError, TypeError):
        return 0


def fetch_live_midpoint(token_id):
    """Fetch live midpoint price from Polymarket CLOB for a single token."""
    result = _api_request(f"{CLOB_API}/midpoint?token_id={quote(str(token_id))}", timeout=5)
    if not result or not isinstance(result, dict) or result.get("error"):
        return None
    try:
        return float(result["mid"])
    except (KeyError, ValueError, TypeError):
        return None


def fetch_live_prices(clob_token_ids):
    """Fetch live YES midpoint from Polymarket CLOB.

    Args:
        clob_token_ids: List of [yes_token_id, no_token_id] from Gamma.

    Returns:
        float or None: Live YES price (0-1).
    """
    if not clob_token_ids or len(clob_token_ids) < 1:
        return None
    yes_token = clob_token_ids[0]
    return fetch_live_midpoint(yes_token)


def fetch_orderbook_summary(clob_token_ids):
    """Fetch order book for YES token and return spread + depth summary.

    Args:
        clob_token_ids: List of [yes_token_id, no_token_id] from Gamma.

    Returns:
        dict with spread_pct, best_bid, best_ask, bid_depth_usd, ask_depth_usd
        or None on failure.
    """
    if not clob_token_ids or len(clob_token_ids) < 1:
        return None
    yes_token = clob_token_ids[0]
    result = _api_request(f"{CLOB_API}/book?token_id={quote(str(yes_token))}", timeout=5)
    if not result or not isinstance(result, dict):
        return None

    bids = result.get("bids", [])
    asks = result.get("asks", [])
    if not bids or not asks:
        return None

    try:
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        spread = best_ask - best_bid
        mid = (best_ask + best_bid) / 2
        spread_pct = spread / mid if mid > 0 else 0

        # Sum depth (top 5 levels)
        bid_depth = sum(float(b.get("size", 0)) * float(b.get("price", 0)) for b in bids[:5])
        ask_depth = sum(float(a.get("size", 0)) * float(a.get("price", 0)) for a in asks[:5])

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread_pct": spread_pct,
            "bid_depth_usd": bid_depth,
            "ask_depth_usd": ask_depth,
        }
    except (KeyError, ValueError, IndexError, TypeError):
        return None


# =============================================================================
# Sprint Market Discovery
# =============================================================================

def discover_fast_market_markets(asset="BTC", window="5m"):
    """Find active fast markets via Simmer API (pre-imported, reliable).
    Falls back to Gamma API if Simmer returns no results."""
    # Primary: Simmer's /api/sdk/fast-markets (markets already imported, is_live_now computed)
    try:
        client = get_client()
        sdk_markets = client.get_fast_markets(asset=asset, window=window, limit=50)
        if sdk_markets:
            markets = []
            for m in sdk_markets:
                # Parse resolves_at string to datetime for time calculations
                end_time = _parse_resolves_at(m.resolves_at) if m.resolves_at else None
                clob_tokens = [m.polymarket_token_id] if m.polymarket_token_id else []
                if m.polymarket_no_token_id:
                    clob_tokens.append(m.polymarket_no_token_id)
                markets.append({
                    "question": m.question,
                    "market_id": m.id,  # Already imported — no import step needed
                    "end_time": end_time,
                    "clob_token_ids": clob_tokens,
                    "is_live_now": m.is_live_now,
                    "spread_cents": m.spread_cents,
                    "liquidity_tier": m.liquidity_tier,
                    "external_price_yes": m.external_price_yes,
                    "fee_rate_bps": getattr(m, 'fee_rate_bps', 0),  # Filled by dynamic lookup after discovery
                    "source": "simmer",
                })
            return markets
    except Exception as e:
        print(f"  ⚠️  Simmer fast-markets API failed ({e}), falling back to Gamma")

    # Fallback: Gamma API (may return stale data)
    return _discover_via_gamma(asset, window)


def _discover_via_gamma(asset="BTC", window="5m"):
    """Fallback: Find active fast markets on Polymarket via Gamma API."""
    patterns = ASSET_PATTERNS.get(asset, ASSET_PATTERNS["BTC"])
    url = (
        "https://gamma-api.polymarket.com/markets"
        "?limit=100&closed=false&tag=crypto&order=endDate&ascending=true"
    )
    result = _api_request(url)
    if not result or isinstance(result, dict) and result.get("error"):
        return []

    markets = []
    for m in result:
        q = (m.get("question") or "").lower()
        slug = m.get("slug", "")
        matches_window = f"-{window}-" in slug
        if any(p in q for p in patterns) and matches_window:
            condition_id = m.get("conditionId", "")
            closed = m.get("closed", False)
            if not closed and slug:
                end_time = _parse_fast_market_end_time(m.get("question", ""))
                clob_tokens_raw = m.get("clobTokenIds", "[]")
                if isinstance(clob_tokens_raw, str):
                    try:
                        clob_tokens = json.loads(clob_tokens_raw)
                    except (json.JSONDecodeError, ValueError):
                        clob_tokens = []
                else:
                    clob_tokens = clob_tokens_raw or []
                markets.append({
                    "question": m.get("question", ""),
                    "slug": slug,
                    "condition_id": condition_id,
                    "end_time": end_time,
                    "outcomes": m.get("outcomes", []),
                    "outcome_prices": m.get("outcomePrices", "[]"),
                    "clob_token_ids": clob_tokens,
                    "fee_rate_bps": int(m.get("fee_rate_bps") or m.get("feeRateBps") or 0),
                    "source": "gamma",
                })
    return markets


def _parse_resolves_at(resolves_at_str):
    """Parse a resolves_at string (ISO format) into a timezone-aware UTC datetime."""
    try:
        # Handle both "2026-03-02 05:10:00Z" and "2026-03-02T05:10:00Z" formats
        s = resolves_at_str.replace("Z", "+00:00").replace(" ", "T")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _parse_fast_market_end_time(question):
    """Parse end time from fast market question (Gamma fallback path).
    e.g., 'Bitcoin Up or Down - February 15, 5:30AM-5:35AM ET' → datetime
    """
    import re
    pattern = r'(\w+ \d+),.*?-\s*(\d{1,2}:\d{2}(?:AM|PM))\s*ET'
    match = re.search(pattern, question)
    if not match:
        return None
    try:
        from zoneinfo import ZoneInfo
        date_str = match.group(1)
        time_str = match.group(2)
        year = datetime.now(timezone.utc).year
        dt_str = f"{date_str} {year} {time_str}"
        dt = datetime.strptime(dt_str, "%B %d %Y %I:%M%p")
        et = ZoneInfo("America/New_York")
        dt = dt.replace(tzinfo=et).astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def find_best_fast_market(markets):
    """Pick the best fast_market to trade: live now, soonest expiring, enough time remaining."""
    now = datetime.now(timezone.utc)
    max_remaining = _window_seconds.get(WINDOW, 300) * 2
    candidates = []
    for m in markets:
        # Prefer is_live_now flag from Simmer API (reliable, server-computed)
        if m.get("is_live_now") is not None:
            if not m["is_live_now"]:
                continue  # Not live yet — skip
            end_time = m.get("end_time")
            if end_time:
                remaining = (end_time - now).total_seconds()
                if remaining > MIN_TIME_REMAINING:
                    candidates.append((remaining, m))
        else:
            # Gamma fallback: use time-based filtering
            end_time = m.get("end_time")
            if not end_time:
                continue
            remaining = (end_time - now).total_seconds()
            if remaining > MIN_TIME_REMAINING and remaining < max_remaining:
                candidates.append((remaining, m))

    if not candidates:
        return None
    # Sort by soonest expiring
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


# =============================================================================
# CEX Price Signal
# =============================================================================

def get_binance_momentum(symbol="BTCUSDT", lookback_minutes=5):
    """Get price momentum from Coinbase API (more reliable on cloud hosts)."""

    product_map = {
        "BTCUSDT": "BTC-USD",
        "ETHUSDT": "ETH-USD",
        "SOLUSDT": "SOL-USD",
    }
    product = product_map.get(symbol, "BTC-USD")

    url = f"https://api.exchange.coinbase.com/products/{product}/candles?granularity=60"

    result = _api_request(url)

    if not result or isinstance(result, dict):
        return None

    try:
        candles = result[:lookback_minutes]

        if len(candles) < 2:
            return None

        # Coinbase candle format:
        # [time, low, high, open, close, volume]
        price_then = float(candles[-1][4])
        price_now = float(candles[0][4])

        momentum_pct = ((price_now - price_then) / price_then) * 100
        direction = "up" if momentum_pct > 0 else "down"

        volumes = [float(c[5]) for c in candles]
        avg_volume = sum(volumes) / len(volumes)
        latest_volume = volumes[0]
        volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0

        return {
            "momentum_pct": momentum_pct,
            "direction": direction,
            "price_now": price_now,
            "price_then": price_then,
            "avg_volume": avg_volume,
            "latest_volume": latest_volume,
            "volume_ratio": volume_ratio,
            "candles": len(candles),
        }

    except Exception:
        return None


COINGECKO_ASSETS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}


def get_momentum(asset="BTC", source="binance", lookback=5):
    """Get price momentum from configured source."""
    if source == "binance":
        symbol = ASSET_SYMBOLS.get(asset, "BTCUSDT")
        return get_binance_momentum(symbol, lookback)
    elif source == "coingecko":
        print("  ⚠️  CoinGecko free tier doesn't provide candle data — switch to binance")
        print("  Run: python fastloop_trader.py --set signal_source=binance")
        return None
    else:
        return None


# =============================================================================
# Import & Trade
# =============================================================================

def import_fast_market_market(slug):
    """Import a fast market to Simmer. Returns market_id or None."""
    url = f"https://polymarket.com/event/{slug}"
    try:
        result = get_client().import_market(url)
    except Exception as e:
        return None, str(e)

    if not result:
        return None, "No response from import endpoint"

    if result.get("error"):
        return None, result.get("error", "Unknown error")

    status = result.get("status")
    market_id = result.get("market_id")

    if status == "resolved":
        alternatives = result.get("active_alternatives", [])
        if alternatives:
            return None, f"Market resolved. Try alternative: {alternatives[0].get('id')}"
        return None, "Market resolved, no alternatives found"

    if status in ("imported", "already_exists"):
        return market_id, None

    return None, f"Unexpected status: {status}"


def get_market_details(market_id):
    """Fetch market details by ID."""
    try:
        market = get_client().get_market_by_id(market_id)
        if not market:
            return None
        from dataclasses import asdict
        return asdict(market)
    except Exception:
        return None


def get_portfolio():
    """Get portfolio summary."""
    try:
        return get_client().get_portfolio()
    except Exception as e:
        return {"error": str(e)}


def get_positions():
    """Get current positions as list of dicts."""
    try:
        positions = get_client().get_positions()
        from dataclasses import asdict
        return [asdict(p) for p in positions]
    except Exception:
        return []


def execute_trade(market_id, side, amount=None, shares=None, action="buy"):
    """Execute a trade on Simmer.

    For buys, pass `amount` in USDC.e. For sells, pass `shares` to close.
    This wrapper is intentionally flexible because the strategy uses both
    buy-notional and sell-shares flows.
    """
    try:
        kwargs = {
            "market_id": market_id,
            "side": side,
            "source": TRADE_SOURCE,
            "skill_slug": SKILL_SLUG,
        }
        if action:
            kwargs["action"] = action
        if amount is not None:
            kwargs["amount"] = amount
        if shares is not None:
            kwargs["shares"] = shares

        result = get_client().trade(**kwargs)
        return {
            "success": getattr(result, "success", False),
            "trade_id": getattr(result, "trade_id", None),
            "shares_bought": getattr(result, "shares_bought", None),
            "shares": getattr(result, "shares_bought", None) or shares,
            "cost": getattr(result, "cost", None),
            "error": getattr(result, "error", None),
            "simulated": getattr(result, "simulated", False),
        }
    except Exception as e:
        return {"error": str(e)}


def calculate_position_size(max_size, smart_sizing=False):
    """Calculate position size, optionally based on portfolio."""
    if not smart_sizing:
        return max_size
    portfolio = get_portfolio()
    if not portfolio or portfolio.get("error"):
        return max_size
    balance = portfolio.get("balance_usdc", 0)
    if balance <= 0:
        return max_size
    smart_size = balance * SMART_SIZING_PCT
    return min(smart_size, max_size)


# =============================================================================
# Main Strategy Logic
# =============================================================================

def run_fast_market_strategy(dry_run=True, positions_only=False, show_config=False,
                        smart_sizing=False, quiet=False):
    """Run one cycle of the fast_market trading strategy."""

    def log(msg, force=False):
        """Print unless quiet mode is on. force=True always prints."""
        if not quiet or force:
            print(msg)

    log("⚡ Simmer FastLoop Trading Skill")
    log("=" * 50)

    if dry_run:
        log("\n  [PAPER MODE] Trades will be simulated with real prices. Use --live for real trades.")

    log(f"\n⚙️  Configuration:")
    log(f"  Asset:            {ASSET}")
    log(f"  Window:           {WINDOW}")
    log(f"  Entry threshold:  {ENTRY_THRESHOLD} (min divergence from 50¢)")
    log(f"  Min momentum:     {MIN_MOMENTUM_PCT}% (min price move)")
    log(f"  Max position:     ${MAX_POSITION_USD:.2f}")
    log(f"  Signal source:    {SIGNAL_SOURCE}")
    log(f"  Lookback:         {LOOKBACK_MINUTES} minutes")
    log(f"  Min time left:    {MIN_TIME_REMAINING}s")
    log(f"  Volume weighting: {'✓' if VOLUME_CONFIDENCE else '✗'}")
    log(f"  Entry rules:      momentum only below 0.35 | contrarian disabled")
    log(f"  TP/SL:            +{TAKE_PROFIT_PCT:.0%} / -{STOP_LOSS_PCT:.0%} | time-stop {LIVE_TIME_STOP_SECONDS}s | max-hold {LIVE_MAX_HOLD_SECONDS}s")
    log(f"  Daily stop:       -${DAILY_LOSS_LIMIT:.2f} then 24h pause")
    live_spend = _load_daily_spend(__file__)
    paper_state = _load_paper_state(__file__)
    live_runtime_state = _prune_live_runtime_state(__file__)

    if dry_run:
        budget_label = "Paper ledger"
        budget_spent = float(paper_state.get("spent", 0.0))
        budget_trades = int(paper_state.get("trades", 0))
        display_pnl = float(paper_state.get("realized_pnl", 0.0))
        display_open_positions = len(paper_state.get("open_positions", []))
        display_open_exposure = _current_paper_open_exposure(paper_state)
    else:
        budget_label = "Live entries"
        budget_spent = float(live_spend.get("spent", 0.0))
        budget_trades = int(live_spend.get("trades", 0))
        live_positions = get_positions()
        live_pnl = _get_live_pnl_snapshot(__file__)
        display_pnl = None if not live_pnl else live_pnl.get("pnl_total")
        positions_exposure, positions_count = _estimate_live_open_exposure(live_positions)
        locked_exposure = _current_live_locked_exposure(live_runtime_state)
        display_open_exposure = round(max(positions_exposure, locked_exposure), 6)
        display_open_positions = max(positions_count, len(live_runtime_state.get("market_locks", [])))

    log(f"  Exposure cap:     ${MAX_OPEN_EXPOSURE:.2f} total open exposure")
    log(f"  Loss stop:        -${DAILY_LOSS_LIMIT:.2f} then pause {PAUSE_HOURS_AFTER_LOSS}h")
    log(f"  {budget_label}:     ${budget_spent:.2f} cumulative, {budget_trades} trades")

    pnl_label = "Current paper P&L" if dry_run else "Current total live P&L"
    if display_pnl is None:
        log(
            f"  {pnl_label}: unavailable across "
            f"{display_open_positions} open positions (open exposure ${display_open_exposure:.2f})"
        )
    else:
        log(
            f"  {pnl_label}: ${display_pnl:.2f} across "
            f"{display_open_positions} open positions (open exposure ${display_open_exposure:.2f})"
        )

    if show_config:
        config_path = get_config_path(__file__)
        log(f"\n  Config file: {config_path}")
        log(f"\n  To change settings:")
        log(f'    python fast_trader.py --set entry_threshold=0.08')
        log(f'    python fast_trader.py --set asset=ETH')
        log(f'    Or edit config.json directly')
        return

    # Initialize client early to validate API key (paper mode when not live)
    get_client(live=not dry_run)

    if not dry_run:
        try:
            redeem_results = get_client().auto_redeem()
            redeemed = [r for r in (redeem_results or []) if isinstance(r, dict) and r.get("success")]
            if redeemed:
                log(f"  ✅ Auto-redeemed {len(redeemed)} resolved winning position(s).", force=True)
        except Exception as e:
            log(f"  ⚠️  Auto-redeem skipped: {e}")

    # Manage exits before looking for fresh entries
    paper_state, closed_paper_positions = manage_paper_positions(__file__, log)
    live_runtime_state, closed_live_positions = manage_live_positions(__file__, log) if not dry_run else (live_runtime_state, [])

    guard_state, pause_remaining = _guard_pause_remaining(__file__)
    if pause_remaining > 0:
        log(f"\n⏸️  Loss-stop pause active for another {pause_remaining}s (reason: {guard_state.get('reason','loss_stop')}).", force=True)
        return

    if dry_run:
        if paper_state["realized_pnl"] <= -abs(DAILY_LOSS_LIMIT):
            _activate_loss_pause(__file__, paper_state["realized_pnl"], reason="paper_daily_loss_stop")
            log(f"\n🛑 Daily paper loss limit reached (${paper_state['realized_pnl']:.2f}). Pausing new entries for {PAUSE_HOURS_AFTER_LOSS}h.", force=True)
            return
    else:
        live_pnl = _get_live_pnl_snapshot(__file__)
        live_pnl_24h = None if not live_pnl else live_pnl.get("pnl_24h_effective")

        if live_pnl_24h is None:
            log("  ⚠️  Could not read live 24h P&L from portfolio API.", force=True)
        elif live_pnl_24h <= -abs(DAILY_LOSS_LIMIT):
            _activate_loss_pause(__file__, live_pnl_24h, reason="live_daily_loss_stop")
            log(
                f"\n🛑 Live 24h loss limit reached (${live_pnl_24h:.2f}). "
                f"Pausing new entries for {PAUSE_HOURS_AFTER_LOSS}h.",
                force=True,
            )
            return

    # Show positions if requested
    if positions_only:
        log("\n📊 Sprint Positions:")
        positions = get_positions()
        fast_market_positions = [p for p in positions if "up or down" in (p.get("question", "") or "").lower()]
        if not fast_market_positions:
            log("  No open fast market positions")
        else:
            for pos in fast_market_positions:
                log(f"  • {pos.get('question', 'Unknown')[:60]}")
                log(f"    YES: {pos.get('shares_yes', 0):.1f} | NO: {pos.get('shares_no', 0):.1f} | P&L: ${pos.get('pnl', 0):.2f}")
        return

    # Show portfolio if smart sizing
    if smart_sizing:
        log("\n💰 Portfolio:")
        portfolio = get_portfolio()
        if portfolio and not portfolio.get("error"):
            log(f"  Balance: ${portfolio.get('balance_usdc', 0):.2f}")

    # Step 1: Discover fast markets
    log(f"\n🔍 Discovering {ASSET} fast markets...")
    markets = discover_fast_market_markets(ASSET, WINDOW)
    log(f"  Found {len(markets)} active fast markets")

    # Look up fee rate once per run from a sample token (same window = same fee tier)
    if markets:
        sample = next((m for m in markets if m.get("clob_token_ids")), None)
        if sample and sample.get("fee_rate_bps", 0) == 0:
            fee = _lookup_fee_rate(sample["clob_token_ids"][0])
            if fee > 0:
                log(f"  Fee rate for {WINDOW} markets: {fee} bps ({fee/100:.0f}%)")
                for m in markets:
                    m["fee_rate_bps"] = fee

    if not markets:
        log("  No active fast markets found — may be outside market hours or wrong asset/window")
        log(f"  Check: asset={ASSET}, window={WINDOW}")
        if not quiet:
            print("📊 Summary: No markets available")
        return

    # Step 2: Find best fast_market to trade
    best = find_best_fast_market(markets)
    if not best:
        # Show what we skipped so users understand the gap
        now = datetime.now(timezone.utc)
        for m in markets:
            end_time = m.get("end_time")
            if m.get("is_live_now") is False:
                log(f"  Skipped: {m['question'][:50]}... (not live yet)")
            elif end_time:
                secs_left = (end_time - now).total_seconds()
                log(f"  Skipped: {m['question'][:50]}... ({secs_left:.0f}s left < {MIN_TIME_REMAINING}s min)")
        log(f"  No live tradeable markets among {len(markets)} found — waiting for next window")
        if not quiet:
            print(f"📊 Summary: No tradeable markets (0/{len(markets)} live with enough time)")
        return

    end_time = best.get("end_time")
    remaining = (end_time - datetime.now(timezone.utc)).total_seconds() if end_time else 0
    log(f"\n🎯 Selected: {best['question']}")
    log(f"  Expires in: {remaining:.0f}s")

    # Dedup: skip if we already hold a live or paper position on this market
    _mid = best.get("market_id") or ""
    _q = best.get("question", "").lower()
    skip_reasons = []
    existing = [] if dry_run else get_positions()
    for pos in existing:
        held = (pos.get("shares_yes") or 0) + (pos.get("shares_no") or 0)
        if held <= 0:
            continue
        if (_mid and pos.get("market_id") == _mid) or (_q and pos.get("question", "").lower() == _q):
            log(f"  ⏸️  Already holding position on this market — skip (dedup)")
            if not quiet:
                print(f"📊 Summary: No trade (already holding this market)")
            skip_reasons.append("already holding")
            return

    if _paper_has_open_position(paper_state, market_id=_mid, question=best.get("question", "")):
        log(f"  ⏸️  Already holding PAPER position on this market — skip (dedup)")
        if not quiet:
            print(f"📊 Summary: No trade (already holding this market)")
        skip_reasons.append("already holding paper position")
        return

    if not dry_run and _live_market_lock_active(live_runtime_state, market_id=_mid, question=best.get("question", "")):
        log(f"  ⏸️  Live market lock active for this market — skip (dedup)")
        if not quiet:
            print(f"📊 Summary: No trade (market lock active)")
        skip_reasons.append("live market lock active")
        return

    # Fetch live CLOB price — required for fast markets (stale prices cause bad trades)
    clob_tokens = best.get("clob_token_ids", [])
    live_price = fetch_live_prices(clob_tokens) if clob_tokens else None
    if live_price is not None:
        market_yes_price = live_price
        log(f"  Current YES price: ${market_yes_price:.3f} (live CLOB)")
    else:
        log(f"  ⏸️  Could not fetch live CLOB price — skipping (stale prices are unsafe on fast markets)")
        _set_market_cooldown(__file__, best)
        if not quiet:
            print(f"📊 Summary: No trade (CLOB price unavailable)")
        return

    # Fee info: Polymarket crypto fee formula (docs.polymarket.com/trading/fees):
    # fee = C × p × POLY_FEE_RATE × (p × (1-p))^POLY_FEE_EXPONENT
    # Max effective rate: 1.56% at 50¢. fee_rate_bps from API is a contract param,
    # not a direct percentage — we use the documented formula constants instead.
    fee_rate_bps = best.get("fee_rate_bps", 0)
    if fee_rate_bps > 0:
        # Effective fee at current market price using Polymarket crypto formula
        _p = market_yes_price if market_yes_price <= 0.5 else (1 - market_yes_price)
        _eff = POLY_FEE_RATE * (_p * (1 - _p)) ** POLY_FEE_EXPONENT
        log(f"  Fee rate:         {_eff:.2%} effective at current price (feeRateBps={fee_rate_bps})")

    # Step 3: Get CEX price momentum
    log(f"\n📈 Fetching {ASSET} price signal ({SIGNAL_SOURCE})...")
    momentum = get_momentum(ASSET, SIGNAL_SOURCE, LOOKBACK_MINUTES)

    if not momentum:
        log("  ❌ Failed to fetch price data", force=True)
        return

    log(f"  Price: ${momentum['price_now']:,.2f} (was ${momentum['price_then']:,.2f})")
    log(f"  Momentum: {momentum['momentum_pct']:+.3f}%")
    log(f"  Direction: {momentum['direction']}")
    if VOLUME_CONFIDENCE:
        log(f"  Volume ratio: {momentum['volume_ratio']:.2f}x avg")

    # Step 4: Decision logic
    log(f"\n🧠 Analyzing...")

    momentum_pct = abs(momentum["momentum_pct"])
    direction = momentum["direction"]

    def _emit_skip_report(signals=1, attempted=0):
        """Emit automaton JSON with skip_reason before early return."""
        global _automaton_reported
        if os.environ.get("AUTOMATON_MANAGED") and skip_reasons:
            report = {"signals": signals, "trades_attempted": attempted, "trades_executed": 0,
                      "skip_reason": ", ".join(dict.fromkeys(skip_reasons))}
            print(json.dumps({"automaton": report}))
            _automaton_reported = True

    # Check order book spread and depth
    # Use pre-fetched spread from Simmer API if available, otherwise fetch from CLOB
    pre_spread = best.get("spread_cents")
    if pre_spread is not None:
        # spread_cents is raw cents (e.g. 2.5 = 2.5¢). Convert to fraction of midpoint
        # for comparison with MAX_SPREAD_PCT. Fast markets trade near 50¢ midpoint.
        mid_estimate = market_yes_price if market_yes_price > 0 else 0.5
        spread_pct = (pre_spread / 100.0) / mid_estimate
        log(f"  Spread: {pre_spread:.1f}¢ ({best.get('liquidity_tier', 'unknown')})")
        if spread_pct > MAX_SPREAD_PCT:
            log(f"  ⏸️  Spread {spread_pct:.1%} > max {MAX_SPREAD_PCT:.1%} — illiquid, skip")
            if not quiet:
                print(f"📊 Summary: No trade (wide spread: {spread_pct:.1%})")
            skip_reasons.append("wide spread")
            _emit_skip_report()
            return
    else:
        book = fetch_orderbook_summary(clob_tokens) if clob_tokens else None
        if book:
            log(f"  Spread: {book['spread_pct']:.1%} (bid ${book['best_bid']:.3f} / ask ${book['best_ask']:.3f})")
            log(f"  Depth: ${book['bid_depth_usd']:.0f} bid / ${book['ask_depth_usd']:.0f} ask (top 5)")
            if book["spread_pct"] > MAX_SPREAD_PCT:
                log(f"  ⏸️  Spread {book['spread_pct']:.1%} > max {MAX_SPREAD_PCT:.1%}")
                _set_market_cooldown(__file__, best)
                if not quiet:
                    print(f"📊 Summary: No trade (wide spread)")
                skip_reasons.append("wide spread")
                _emit_skip_report()
                return
        else:
            log("  ⏸️  Could not fetch usable order book — skip")
            _set_market_cooldown(__file__, best)
            if not quiet:
                print("📊 Summary: No trade (order book unavailable)")
            skip_reasons.append("order book unavailable")
            _emit_skip_report()
            return

    # Check minimum momentum
    if momentum_pct < MIN_MOMENTUM_PCT:
        log(f"  ⏸️  Momentum {momentum_pct:.3f}% < minimum {MIN_MOMENTUM_PCT}% — skip")
        if not quiet:
            print(f"📊 Summary: No trade (momentum too weak: {momentum_pct:.3f}%)")
        return

    # Calculate expected fair price based on momentum direction
    # Simple model: strong momentum → higher probability of continuation
    if direction == "up":
        side = "yes"
        divergence = 0.50 + ENTRY_THRESHOLD - market_yes_price
        trade_rationale = f"{ASSET} up {momentum['momentum_pct']:+.3f}% but YES only ${market_yes_price:.3f}"
    else:
        side = "no"
        divergence = market_yes_price - (0.50 - ENTRY_THRESHOLD)
        trade_rationale = f"{ASSET} down {momentum['momentum_pct']:+.3f}% but YES still ${market_yes_price:.3f}"

    # Volume confidence adjustment
    vol_note = ""
    if VOLUME_CONFIDENCE and momentum["volume_ratio"] < (0.75 if not dry_run else 0.5):
        log(f"  ⏸️  Low volume ({momentum['volume_ratio']:.2f}x avg) — weak signal, skip")
        if not quiet:
            print(f"📊 Summary: No trade (low volume)")
        skip_reasons.append("low volume")
        _emit_skip_report()
        return
    elif VOLUME_CONFIDENCE and momentum["volume_ratio"] > 2.0:
        vol_note = f" 📊 (high volume: {momentum['volume_ratio']:.1f}x avg)"

    # Check divergence threshold
    if divergence <= 0:
        log(f"  ⏸️  Market already priced in: divergence {divergence:.3f} ≤ 0 — skip")
        if not quiet:
            print(f"📊 Summary: No trade (market already priced in)")
        skip_reasons.append("market already priced in")
        _emit_skip_report()
        return

    # Fee-aware EV check: require enough divergence to cover fees
    # EV = win_prob * payout_after_fees - (1 - win_prob) * cost
    # At the buy price, win_prob ≈ buy_price (market-implied).
    # We need our edge (divergence) to overcome the fee drag.
    if fee_rate_bps > 0:
        buy_price = market_yes_price if side == "yes" else (1 - market_yes_price)
        # Polymarket crypto fee: fee = C × p × 0.25 × (p × (1-p))^2
        # Effective rate = 0.25 × (p × (1-p))^2. Fee per share = buy_price × eff_rate.
        effective_fee_rate = POLY_FEE_RATE * (buy_price * (1 - buy_price)) ** POLY_FEE_EXPONENT
        fee_per_share = buy_price * effective_fee_rate  # absolute fee in price terms
        # Divergence is in absolute price — compare to fee drag + buffer
        min_divergence = fee_per_share * 2 + 0.02  # round-trip fee + buffer
        log(f"  Fee:              ${fee_per_share:.4f}/share ({effective_fee_rate:.2%} effective, min divergence {min_divergence:.3f})")
        if divergence < min_divergence:
            log(f"  ⏸️  Divergence {divergence:.3f} < fee-adjusted minimum {min_divergence:.3f} — skip")
            if not quiet:
                print(f"📊 Summary: No trade (fees eat the edge)")
            skip_reasons.append("fees eat the edge")
            _emit_skip_report()
            return

    # We have a signal!
    position_size = calculate_position_size(MAX_POSITION_USD, smart_sizing)
    price = market_yes_price if side == "yes" else (1 - market_yes_price)

    # Entry price / strategy-mode filter
    strategy_mode = None
    if price < MIN_ENTRY_PRICE or price > MAX_ENTRY_PRICE:
        log(f"  ⏸️  Entry price ${price:.3f} outside global allowed range")
        skip_reasons.append("price filter")
        _emit_skip_report()
        return

    if price < MOMENTUM_MAX_ENTRY:
        strategy_mode = "momentum"
    else:
        log(f"  ⏸️  Entry price ${price:.3f} outside allowed range for momentum mode (< ${MOMENTUM_MAX_ENTRY:.2f})")
        skip_reasons.append("price filter")
        _emit_skip_report()
        return

    log(f"  Strategy mode:   {strategy_mode}")

    # Open exposure check (capital recycles after exits; no cumulative daily budget cap)
    if dry_run:
        current_open_exposure = _current_paper_open_exposure(paper_state)
    else:
        positions_open_exposure, _ = _estimate_live_open_exposure(existing)
        locked_open_exposure = _current_live_locked_exposure(live_runtime_state)
        current_open_exposure = max(positions_open_exposure, locked_open_exposure)

    remaining_exposure = MAX_OPEN_EXPOSURE - current_open_exposure
    if remaining_exposure <= 0:
        log(f"  ⏸️  Open exposure cap reached (${current_open_exposure:.2f}/${MAX_OPEN_EXPOSURE:.2f}) — skip")
        if not quiet:
            print(f"📊 Summary: No trade (open exposure cap reached)")
        skip_reasons.append("open exposure cap")
        _emit_skip_report()
        return
    if position_size > remaining_exposure:
        position_size = remaining_exposure
        log(f"  Exposure cap: trade capped at ${position_size:.2f} (${current_open_exposure:.2f}/${MAX_OPEN_EXPOSURE:.2f} currently open)")
    if position_size < 0.50:
        log(f"  ⏸️  Remaining exposure room ${position_size:.2f} < $0.50 — skip")
        if not quiet:
            print(f"📊 Summary: No trade (exposure room too small)")
        skip_reasons.append("exposure room too small")
        _emit_skip_report()
        return

    # Check minimum order size
    if price > 0:
        min_cost = MIN_SHARES_PER_ORDER * price
        if min_cost > position_size:
            log(f"  ⚠️  Position ${position_size:.2f} too small for {MIN_SHARES_PER_ORDER} shares at ${price:.2f}")
            skip_reasons.append("position too small")
            _emit_skip_report(attempted=1)
            return

    log(f"  ✅ Signal: {side.upper()} — {trade_rationale}{vol_note}", force=True)
    log(f"  Divergence: {divergence:.3f}", force=True)

    # Step 5: Get market ID (already have it from Simmer API, or import from Gamma)
    if best.get("market_id"):
        market_id = best["market_id"]
        log(f"\n🔗 Market ready: {market_id[:16]}...", force=True)
    else:
        log(f"\n🔗 Importing to Simmer...", force=True)
        market_id, import_error = import_fast_market_market(best["slug"])
        if not market_id:
            log(f"  ❌ Import failed: {import_error}", force=True)
            return
        log(f"  ✅ Market ID: {market_id[:16]}...", force=True)

    execution_error = None
    tag = "SIMULATED" if dry_run else "LIVE"
    log(f"  Executing {side.upper()} trade for ${position_size:.2f} ({tag})...", force=True)
    result = execute_trade(market_id, side, amount=position_size, action="buy")

    if result and result.get("success"):
        shares = float(result.get("shares_bought") or result.get("shares") or 0)
        trade_id = result.get("trade_id")
        display_fill_price = price if result.get("simulated") else _infer_live_fill_price(position_size, shares, price)
        if result.get("simulated"):
            log(
                f"  ✅ [PAPER] Bought {shares:.1f} {side.upper()} shares @ ${display_fill_price:.3f}",
                force=True,
            )

        if result.get("simulated"):
            target_price = round(price * (1 + TAKE_PROFIT_PCT), 6)
            stop_price = round(max(0.001, price * (1 - STOP_LOSS_PCT)), 6)
            paper_state["spent"] = round(float(paper_state.get("spent", 0.0)) + position_size, 6)
            paper_state["trades"] = int(paper_state.get("trades", 0)) + 1
            paper_state.setdefault("open_positions", []).append({
                "market_id": market_id,
                "question": best.get("question", ""),
                "side": side,
                "shares": round(shares, 6),
                "entry_price": round(price, 6),
                "entry_cost": round(position_size, 6),
                "entry_time": datetime.now(timezone.utc).isoformat(),
                "end_time": end_time.isoformat() if end_time else None,
                "clob_token_ids": clob_tokens,
                "target_price": target_price,
                "stop_price": stop_price,
                "entry_fee_per_share": round(_estimate_fee_per_share(price), 8),
            })
            _save_paper_state(__file__, paper_state)
            log(f"  📒 [PAPER] Tracking position with TP ${target_price:.3f} / SL ${stop_price:.3f}", force=True)
        else:
            confirmed_pos, confirmed_shares, confirmed_cost, confirmed_fill = _confirm_live_fill(
                __file__,
                market_id=market_id,
                question=best.get("question", ""),
                side=side,
                entry_cost=position_size,
                quoted_price=price,
                shares_hint=shares,
            )
            shares = float(confirmed_shares or shares or 0.0)
            display_fill_price = float(confirmed_fill or display_fill_price)
            actual_cost = float(confirmed_cost or position_size)
            log(
                f"  ✅ Bought {shares:.2f} {side.upper()} shares @ ${display_fill_price:.3f}"
                + (f" (confirmed fill; quote was ${price:.3f})" if abs(display_fill_price - price) > 1e-6 else ""),
                force=True,
            )
            live_spend["spent"] += actual_cost
            live_spend["trades"] += 1
            _save_daily_spend(__file__, live_spend)
            _register_live_market_lock(
                __file__,
                market_id=market_id,
                question=best.get("question", ""),
                end_time=end_time,
                entry_cost=actual_cost,
                side=side,
                shares=shares,
                entry_price=display_fill_price,
                entry_time=datetime.now(timezone.utc).isoformat(),
            )
            _append_live_trade_event(__file__, {
                "type": "entry",
                "market_id": market_id,
                "question": best.get("question", ""),
                "side": side,
                "quoted_price": round(price, 6),
                "avg_fill": round(display_fill_price, 6),
                "shares": round(shares, 6),
                "entry_cost": round(actual_cost, 6),
                "strategy_mode": strategy_mode,
                "momentum_pct": round(momentum["momentum_pct"], 6),
                "volume_ratio": round(momentum["volume_ratio"], 6),
                "divergence": round(divergence, 6),
            })
            _set_live_monitor(market_id, side, log)

        # Log to trade journal (skip for paper trades)
        if trade_id and JOURNAL_AVAILABLE and not result.get("simulated"):
            confidence = min(0.9, 0.5 + divergence + (momentum_pct / 100))
            log_trade(
                trade_id=trade_id,
                source=TRADE_SOURCE, skill_slug=SKILL_SLUG,
                thesis=trade_rationale,
                confidence=round(confidence, 2),
                asset=ASSET,
                momentum_pct=round(momentum["momentum_pct"], 3),
                volume_ratio=round(momentum["volume_ratio"], 2),
                signal_source=SIGNAL_SOURCE,
            )
    else:
        error = result.get("error", "Unknown error") if result else "No response"
        log(f"  ❌ Trade failed: {error}", force=True)
        execution_error = error[:120]

    # Summary
    total_trades = 1 if result and result.get("success") else 0
    show_summary = not quiet or total_trades > 0
    if show_summary:
        print(f"\n📊 Summary:")
        print(f"  Sprint: {best['question'][:50]}")
        print(f"  Signal: {direction} {momentum_pct:.3f}% | YES ${market_yes_price:.3f}")
        print(f"  Action: {'PAPER' if dry_run else ('TRADED' if total_trades else 'FAILED')}")

    # Structured report for automaton (takes priority over fallback in __main__)
    if os.environ.get("AUTOMATON_MANAGED"):
        amount = round(position_size, 2) if total_trades > 0 else 0
        report = {"signals": 1, "trades_attempted": 1, "trades_executed": total_trades, "amount_usd": amount}
        if execution_error:
            report["execution_errors"] = [execution_error]
        print(json.dumps({"automaton": report}))
        _automaton_reported = True


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simmer FastLoop Trading Skill")
    parser.add_argument("--live", action="store_true", help="Execute real trades (default is dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="(Default) Show opportunities without trading")
    parser.add_argument("--positions", action="store_true", help="Show current fast market positions")
    parser.add_argument("--config", action="store_true", help="Show current config")
    parser.add_argument("--set", action="append", metavar="KEY=VALUE",
                        help="Update config (e.g., --set entry_threshold=0.08)")
    parser.add_argument("--smart-sizing", action="store_true", help="Use portfolio-based position sizing")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Only output on trades/errors (ideal for high-frequency runs)")
    args = parser.parse_args()

    if args.set:
        updates = {}
        for item in args.set:
            if "=" not in item:
                print(f"Invalid --set format: {item}. Use KEY=VALUE")
                sys.exit(1)
            key, val = item.split("=", 1)
            if key in CONFIG_SCHEMA:
                type_fn = CONFIG_SCHEMA[key].get("type", str)
                try:
                    if type_fn == bool:
                        updates[key] = val.lower() in ("true", "1", "yes")
                    else:
                        updates[key] = type_fn(val)
                except ValueError:
                    print(f"Invalid value for {key}: {val}")
                    sys.exit(1)
            else:
                print(f"Unknown config key: {key}")
                print(f"Valid keys: {', '.join(CONFIG_SCHEMA.keys())}")
                sys.exit(1)
        result = update_config(updates, __file__)
        print(f"✅ Config updated: {json.dumps(updates)}")
        sys.exit(0)

    dry_run = not args.live

    while True:
        try:
            _tick_market_cooldowns(__file__)
            run_fast_market_strategy(
                dry_run=dry_run,
                positions_only=args.positions,
                show_config=args.config,
                smart_sizing=args.smart_sizing,
                quiet=args.quiet,
            )

            # Fallback report for automaton if the strategy returned early (no signal)
            # The function emits its own report when it reaches a trade; this covers early exits.
            if os.environ.get("AUTOMATON_MANAGED") and not _automaton_reported:
                print(json.dumps({
                    "automaton": {
                        "signals": 0,
                        "trades_attempted": 0,
                        "trades_executed": 0,
                        "skip_reason": "no_signal"
                    }
                }))

        except Exception as e:
            print(f"Loop error: {e}")

        print(f"\n⏳ Waiting {SCAN_INTERVAL_SECONDS} seconds before next scan...\n")
        time.sleep(SCAN_INTERVAL_SECONDS)
