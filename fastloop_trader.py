#!/usr/bin/env python3
"""
Simmer FastLoop Trading Skill - REVISED v2.0

Key fixes from v1.3.4:
- Local entry record storage (fixes TP/SL not firing)
- Skip incomplete Coinbase candle[0] (fixes false momentum signals)
- Mean-reversion entry logic (fixes chasing priced-in moves)
- Railway TZ fix (manual UTC offset fallback)
- 3-second exit loop when position is open
- Reliable share count from trade result
- Revised config parameters for profitability

Trades Polymarket BTC 5-minute fast markets using CEX price momentum.
Default signal: Coinbase BTC-USD candles.

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

# =============================================================================
# Timezone Helper - Railway compatible
# =============================================================================

def _safe_et_timestamp():
    """Get Eastern Time timestamp with Railway-compatible fallback."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime('%Y-%m-%d %I:%M:%S %p ET')
    except Exception:
        pass
    try:
        # Manual fallback: detect EST vs EDT
        # EDT: second Sunday March to first Sunday November = UTC-4
        # EST: otherwise = UTC-5
        now_utc = datetime.now(timezone.utc)
        month = now_utc.month
        if 3 < month < 11:
            offset = timedelta(hours=-4)  # EDT
        elif month == 3:
            # After second Sunday
            day = now_utc.day
            offset = timedelta(hours=-4) if day >= 8 else timedelta(hours=-5)
        elif month == 11:
            day = now_utc.day
            offset = timedelta(hours=-5) if day >= 1 else timedelta(hours=-4)
        else:
            offset = timedelta(hours=-5)  # EST
        et_time = now_utc + offset
        suffix = "EDT" if offset.total_seconds() == -14400 else "EST"
        return et_time.strftime(f'%Y-%m-%d %I:%M:%S %p {suffix}')
    except Exception:
        return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')


def _get_et_zone():
    """Get Eastern timezone object with fallback."""
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/New_York")
    except Exception:
        return timezone.utc


# Force line-buffered stdout for non-TTY environments (cron, Docker, Railway)
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

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
    "entry_threshold": {
        "default": 0.06, "env": "SIMMER_SPRINT_ENTRY", "type": float,
        "help": "Min price divergence from CEX-implied price to trigger trade"
    },
    "min_momentum_pct": {
        "default": 0.08, "env": "SIMMER_SPRINT_MOMENTUM", "type": float,
        "help": "Min BTC % move in lookback window to trigger"
    },
    "entry_score_threshold": {
        "default": 0.55, "env": "SIMMER_SPRINT_SCORE_THRESHOLD", "type": float,
        "help": "Minimum multi-factor entry score required to trade (0-1)"
    },
    "max_position": {
        "default": 2.5, "env": "SIMMER_SPRINT_MAX_POSITION", "type": float,
        "help": "Max $ per trade"
    },
    "signal_source": {
        "default": "coinbase", "env": "SIMMER_SPRINT_SIGNAL", "type": str,
        "help": "Price feed source (coinbase)"
    },
    "lookback_minutes": {
        "default": 3, "env": "SIMMER_SPRINT_LOOKBACK", "type": int,
        "help": "Minutes of price history for momentum calc"
    },
    "min_time_remaining": {
        "default": 150, "env": "SIMMER_SPRINT_MIN_TIME", "type": int,
        "help": "Skip fast markets with less than this many seconds remaining"
    },
    "asset": {
        "default": "BTC", "env": "SIMMER_SPRINT_ASSET", "type": str,
        "help": "Asset to trade (BTC, ETH, SOL)"
    },
    "window": {
        "default": "5m", "env": "SIMMER_SPRINT_WINDOW", "type": str,
        "help": "Market window duration (5m or 15m)"
    },
    "volume_confidence": {
        "default": True, "env": "SIMMER_SPRINT_VOL_CONF", "type": bool,
        "help": "Weight signal by volume (higher volume = more confident)"
    },
    "max_open_exposure": {
        "default": 2.5, "env": "SIMMER_SPRINT_MAX_EXPOSURE", "type": float,
        "help": "Maximum simultaneous open exposure across active positions"
    },
    "take_profit_pct": {
        "default": 0.12, "env": "SIMMER_SPRINT_TP", "type": float,
        "help": "Take profit percentage for position exits"
    },
    "stop_loss_pct": {
        "default": 0.07, "env": "SIMMER_SPRINT_SL", "type": float,
        "help": "Stop loss percentage for position exits"
    },
    "daily_loss_limit": {
        "default": 15.0, "env": "SIMMER_SPRINT_DAILY_LOSS", "type": float,
        "help": "Stop trading after this much realized loss in a UTC day"
    },
    "pause_hours_after_loss": {
        "default": 1, "env": "SIMMER_SPRINT_PAUSE_HOURS", "type": int,
        "help": "Pause new entries for this many hours after loss stop is hit"
    },
    "resolution_exit_seconds": {
        "default": 45, "env": "SIMMER_SPRINT_RESOLVE_EXIT", "type": int,
        "help": "Exit positions this many seconds before market expiry"
    },
    "daily_budget": {
        "default": 0.0, "env": "SIMMER_SPRINT_DAILY_BUDGET", "type": float,
        "help": "Legacy budget cap (unused)"
    },
    "daily_profit_target": {
        "default": 0.0, "env": "SIMMER_SPRINT_DAILY_PROFIT", "type": float,
        "help": "Legacy profit target (unused)"
    },
    "max_trades_per_day": {
        "default": 0, "env": "SIMMER_SPRINT_MAX_TRADES", "type": int,
        "help": "Legacy trade cap (unused)"
    },
}

TRADE_SOURCE = "sdk:fastloop"
SKILL_SLUG = "polymarket-fast-loop"
_automaton_reported = False

SMART_SIZING_PCT = 0.05        # 5% of balance per trade
MIN_SHARES_PER_ORDER = 5       # Polymarket minimum
MAX_SPREAD_PCT = 0.06          # Skip if CLOB bid-ask spread exceeds this
MIN_ENTRY_PRICE = 0.05
MIN_LIVE_ENTRY_PRICE = 0.10    # Lowered from 0.12 to allow more entries
MAX_ENTRY_PRICE = 0.95         # Avoid near-certain outcomes
MOMENTUM_MAX_ENTRY = 0.45      # Raised from 0.35 - allow entries up to 45¢

# Timing constants
SCAN_INTERVAL_SECONDS = 30
LIVE_SCAN_INTERVAL_SECONDS = 15
FOCUSED_LIVE_SCAN_INTERVAL_SECONDS = 3   # Faster: 3s when position is open
HEARTBEAT_SECONDS = 600
LIVE_TIME_STOP_SECONDS = 45              # Exit if losing with <45s left
LIVE_MAX_HOLD_SECONDS = 120             # Max hold before forced exit if losing

ACTION_ONLY_LOGS = True
_last_heartbeat_ts = 0
_last_auto_redeem_ts = 0

SINGLE_POSITION_LIVE_MODE = True
ENABLE_CONTRARIAN = False

BAD_MARKET_COOLDOWN_CYCLES = 3

# Polymarket crypto fee formula constants
POLY_FEE_RATE = 0.25
POLY_FEE_EXPONENT = 2

# Asset mappings
ASSET_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
}

ASSET_COINBASE = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD",
}

ASSET_PATTERNS = {
    "BTC": ["bitcoin up or down"],
    "ETH": ["ethereum up or down"],
    "SOL": ["solana up or down"],
}

CLOB_API = "https://clob.polymarket.com"

# =============================================================================
# Load Config
# =============================================================================

from simmer_sdk.skill import load_config, update_config, get_config_path

cfg = load_config(CONFIG_SCHEMA, __file__, slug="polymarket-fast-loop")

ENTRY_THRESHOLD = cfg["entry_threshold"]
MIN_MOMENTUM_PCT = cfg["min_momentum_pct"]
ENTRY_SCORE_THRESHOLD = cfg["entry_score_threshold"]
MAX_POSITION_USD = cfg["max_position"]

_automaton_max = os.environ.get("AUTOMATON_MAX_BET")
if _automaton_max:
    MAX_POSITION_USD = min(MAX_POSITION_USD, float(_automaton_max))

SIGNAL_SOURCE = cfg["signal_source"]
LOOKBACK_MINUTES = cfg["lookback_minutes"]
ASSET = cfg["asset"].upper()
WINDOW = cfg["window"]

_window_seconds = {"5m": 300, "15m": 900, "1h": 3600}
MIN_TIME_REMAINING = cfg["min_time_remaining"]

VOLUME_CONFIDENCE = cfg["volume_confidence"]
MAX_OPEN_EXPOSURE = cfg["max_open_exposure"]
TAKE_PROFIT_PCT = cfg["take_profit_pct"]
STOP_LOSS_PCT = cfg["stop_loss_pct"]
DAILY_LOSS_LIMIT = cfg["daily_loss_limit"]
PAUSE_HOURS_AFTER_LOSS = cfg["pause_hours_after_loss"]
RESOLUTION_EXIT_SECONDS = cfg["resolution_exit_seconds"]


# =============================================================================
# Local Entry Record - CRITICAL FIX
# Stores entry data locally so TP/SL always has reliable prices
# =============================================================================

def _get_entry_record_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "current_entry.json"


def _save_entry_record(skill_file, market_id, question, side, entry_price,
                       shares, entry_cost, end_time, clob_token_ids=None):
    """
    Save entry record locally immediately after a successful trade.
    This is the authoritative source for entry_price - do NOT rely on
    Simmer's position API which can be delayed or return 0.
    """
    path = _get_entry_record_path(skill_file)
    record = {
        "market_id": market_id,
        "question": question,
        "side": side,
        "entry_price": round(float(entry_price), 6),
        "shares": round(float(shares), 6),
        "entry_cost": round(float(entry_cost), 6),
        "end_time": end_time.isoformat() if end_time else None,
        "clob_token_ids": list(clob_token_ids) if clob_token_ids else [],
        "ts": datetime.now(timezone.utc).isoformat(),
        "target_price": round(float(entry_price) * (1 + TAKE_PROFIT_PCT), 6),
        "stop_price": round(max(0.001, float(entry_price) * (1 - STOP_LOSS_PCT)), 6),
    }
    with open(path, "w") as f:
        json.dump(record, f, indent=2)
    return record


def _load_entry_record(skill_file):
    """Load current entry record. Returns None if no active entry."""
    path = _get_entry_record_path(skill_file)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        # Validate it has required fields
        if not data.get("entry_price") or not data.get("side"):
            return None
        return data
    except Exception:
        return None


def _clear_entry_record(skill_file):
    """Clear entry record after position is closed."""
    from pathlib import Path
    path = _get_entry_record_path(skill_file)
    if path.exists():
        try:
            path.unlink()
        except Exception:
            pass


def _entry_record_is_expired(record):
    """Check if entry record is past its market end time."""
    if not record:
        return True
    end_str = record.get("end_time")
    if not end_str:
        return False
    try:
        end_time = datetime.fromisoformat(end_str)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) > end_time + timedelta(seconds=30)
    except Exception:
        return False


# =============================================================================
# Daily Spend Tracking
# =============================================================================

def _get_spend_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "daily_spend.json"


def _load_daily_spend(skill_file):
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
    spend_path = _get_spend_path(skill_file)
    with open(spend_path, "w") as f:
        json.dump(spend_data, f, indent=2)


# =============================================================================
# Paper State Tracking
# =============================================================================

def _get_paper_state_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "paper_state.json"


def _load_paper_state(skill_file):
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


# =============================================================================
# Guard State (loss pause)
# =============================================================================

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


# =============================================================================
# Bad Market Cooldown
# =============================================================================

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
# Live Trade Ledger
# =============================================================================

def _get_live_trade_ledger_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "live_trade_ledger.jsonl"


def _append_live_trade_event(skill_file, event):
    path = _get_live_trade_ledger_path(skill_file)
    payload = dict(event)
    payload.setdefault("ts", datetime.now(timezone.utc).isoformat())
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


# =============================================================================
# API Helpers
# =============================================================================

def _api_request(url, method="GET", data=None, headers=None, timeout=15):
    """Make an HTTP request. Returns parsed JSON or None on error."""
    try:
        req_headers = headers or {}
        if "User-Agent" not in req_headers:
            req_headers["User-Agent"] = "simmer-fastloop/2.0"
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


# =============================================================================
# Simmer Client
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
            sys.exit(1)
        venue = os.environ.get("TRADING_VENUE", "polymarket")
        _client = SimmerClient(api_key=api_key, venue=venue, live=live)
    return _client


def get_portfolio():
    try:
        return get_client().get_portfolio()
    except Exception as e:
        return {"error": str(e)}


def get_positions():
    try:
        positions = get_client().get_positions()
        from dataclasses import asdict
        return [asdict(p) for p in positions]
    except Exception:
        return []


def get_market_details(market_id):
    try:
        market = get_client().get_market_by_id(market_id)
        if not market:
            return None
        from dataclasses import asdict
        return asdict(market)
    except Exception:
        return None


def execute_trade(market_id, side, amount=None, shares=None, action="buy"):
    """Execute a trade on Simmer."""
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
# CLOB Price Fetching
# =============================================================================

def _lookup_fee_rate(token_id):
    result = _api_request(f"{CLOB_API}/fee-rate?token_id={quote(str(token_id))}", timeout=5)
    if not result or not isinstance(result, dict) or result.get("error"):
        return 0
    try:
        return int(float(result.get("base_fee") or 0))
    except (ValueError, TypeError):
        return 0


def fetch_live_midpoint(token_id):
    result = _api_request(f"{CLOB_API}/midpoint?token_id={quote(str(token_id))}", timeout=5)
    if not result or not isinstance(result, dict) or result.get("error"):
        return None
    try:
        return float(result["mid"])
    except (KeyError, ValueError, TypeError):
        return None


def fetch_live_prices(clob_token_ids):
    """Fetch live YES midpoint from Polymarket CLOB."""
    if not clob_token_ids or len(clob_token_ids) < 1:
        return None
    yes_token = clob_token_ids[0]
    return fetch_live_midpoint(yes_token)


def fetch_orderbook_summary(clob_token_ids):
    """Fetch order book for YES token and return spread + depth summary."""
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


def fetch_side_orderbook_summary(clob_token_ids, side="yes"):
    """Fetch order book summary for the requested side token."""
    if not clob_token_ids:
        return None
    idx = 0 if side == "yes" else 1
    if len(clob_token_ids) <= idx:
        return None
    token_id = clob_token_ids[idx]
    result = _api_request(f"{CLOB_API}/book?token_id={quote(str(token_id))}", timeout=5)
    if not result or not isinstance(result, dict):
        return None
    bids = result.get("bids", []) or []
    asks = result.get("asks", []) or []
    try:
        best_bid = float(bids[0]["price"]) if bids else None
        best_ask = float(asks[0]["price"]) if asks else None
        mid = None
        spread_pct = None
        if best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid
            mid = (best_ask + best_bid) / 2
            spread_pct = spread / mid if mid and mid > 0 else None
        bid_depth = sum(float(b.get("size", 0)) * float(b.get("price", 0)) for b in bids[:5]) if bids else 0.0
        ask_depth = sum(float(a.get("size", 0)) * float(a.get("price", 0)) for a in asks[:5]) if asks else 0.0
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": mid,
            "spread_pct": spread_pct,
            "bid_depth_usd": bid_depth,
            "ask_depth_usd": ask_depth,
        }
    except (KeyError, ValueError, IndexError, TypeError):
        return None


# =============================================================================
# CEX Price Signal - FIXED (skip incomplete candle[0])
# =============================================================================

def get_coinbase_momentum(asset="BTC", lookback_minutes=3):
    """
    Get price momentum from Coinbase API.

    CRITICAL FIX: Coinbase returns candles[0] as the CURRENT INCOMPLETE candle.
    We skip it and use candles[1] as our most recent COMPLETED price.
    Using an incomplete candle causes false momentum signals.
    """
    product = ASSET_COINBASE.get(asset, "BTC-USD")
    # Request more candles than needed to have buffer after skipping [0]
    url = f"https://api.exchange.coinbase.com/products/{product}/candles?granularity=60"

    result = _api_request(url, timeout=10)
    if not result or isinstance(result, dict):
        return None

    try:
        # SKIP candles[0] - it's the current incomplete candle
        # candles[1] = most recent COMPLETED 1-minute candle
        # Coinbase format: [time, low, high, open, close, volume]
        completed = result[1:]  # All completed candles, newest first

        if len(completed) < lookback_minutes + 1:
            return None

        # Most recent completed candle
        price_now = float(completed[0][4])
        # N minutes ago
        price_then = float(completed[lookback_minutes - 1][4])

        if price_then <= 0:
            return None

        momentum_pct = ((price_now - price_then) / price_then) * 100
        direction = "up" if momentum_pct > 0 else "down"

        # Recent (last completed candle vs one before it)
        recent_pct = 0.0
        prior_pct = 0.0
        if len(completed) >= 2:
            prev_close = float(completed[1][4])
            if prev_close > 0:
                recent_pct = ((price_now - prev_close) / prev_close) * 100

        if len(completed) >= 3:
            prior_close = float(completed[2][4])
            prev_close = float(completed[1][4])
            if prior_close > 0:
                prior_pct = ((prev_close - prior_close) / prior_close) * 100

        acceleration_pct = recent_pct - prior_pct

        # Volume from completed candles only
        volumes = [float(c[5]) for c in completed[:lookback_minutes]]
        avg_volume = sum(volumes) / len(volumes) if volumes else 1.0
        latest_volume = volumes[0] if volumes else 0.0
        volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0

        return {
            "momentum_pct": momentum_pct,
            "recent_momentum_pct": recent_pct,
            "acceleration_pct": acceleration_pct,
            "direction": direction,
            "price_now": price_now,
            "price_then": price_then,
            "avg_volume": avg_volume,
            "latest_volume": latest_volume,
            "volume_ratio": volume_ratio,
            "candles": len(completed),
        }
    except Exception as e:
        return None


def get_momentum(asset="BTC", source="coinbase", lookback=3):
    """Get price momentum from configured source."""
    return get_coinbase_momentum(asset, lookback)


# =============================================================================
# REVISED Entry Signal Logic - Mean Reversion / Divergence Fade
# =============================================================================

def _find_trade_signal(momentum, market_yes_price, remaining_seconds):
    """
    REVISED: Find trade signal based on CEX/CLOB divergence (mean reversion).

    Previous approach (WRONG): Buy YES when BTC is going UP.
    Problem: By the time momentum is visible, YES price has already moved.

    Correct approach: Find where CLOB price HASN'T caught up to CEX signal.

    Logic:
    - Calculate where YES SHOULD be priced based on BTC momentum
    - If actual YES price is far BELOW the implied price -> buy YES (underpriced)
    - If actual YES price is far ABOVE the implied price -> buy NO (overpriced)

    The edge is the gap between where Polymarket thinks BTC will go
    versus where Coinbase momentum says it's actually going.
    """
    momentum_pct = momentum["momentum_pct"]  # signed: positive = up
    volume_ratio = momentum["volume_ratio"]

    # Require minimum volume to trust signal
    if volume_ratio < 0.3:
        return None, None, "volume too low"

    # Calculate CEX-implied YES probability
    # Model: neutral market = 50¢
    # Strong up move (+1%) -> market should price YES at ~65¢
    # Strong down move (-1%) -> market should price YES at ~35¢
    # Scale factor: 1% BTC move = ~15¢ shift in fair value
    # This is conservative - real markets move more, giving us edge when CLOB lags
    SCALE_FACTOR = 15.0  # cents per 1% BTC move
    implied_yes = 0.50 + (momentum_pct / 100.0) * SCALE_FACTOR
    implied_yes = max(0.15, min(0.85, implied_yes))  # Clamp to realistic range

    # How far is CLOB from where it should be?
    clob_divergence = implied_yes - market_yes_price

    # Fee-aware minimum edge
    # Need divergence > fees to be profitable
    buy_price = market_yes_price if clob_divergence > 0 else (1.0 - market_yes_price)
    fee_estimate = POLY_FEE_RATE * (buy_price * (1 - buy_price)) ** POLY_FEE_EXPONENT
    min_edge = fee_estimate * 2 + ENTRY_THRESHOLD  # Need at least 2x fees + threshold

    if clob_divergence > min_edge:
        # CLOB underpricing YES relative to BTC momentum -> buy YES
        return "yes", clob_divergence, None
    elif clob_divergence < -min_edge:
        # CLOB overpricing YES relative to BTC momentum -> buy NO
        return "no", abs(clob_divergence), None

    return None, None, f"divergence {clob_divergence:.3f} < min edge {min_edge:.3f}"


def _clamp01(value):
    try:
        return max(0.0, min(1.0, float(value)))
    except Exception:
        return 0.0


def _score_entry_setup(side, momentum, divergence, min_divergence,
                       seconds_left, yes_book=None, side_book=None, side_price=None):
    """Multi-factor entry score for fast markets. Returns (score, details)."""
    momentum_pct = abs(float(momentum.get("momentum_pct", 0.0) or 0.0))
    recent_pct = abs(float(momentum.get("recent_momentum_pct", 0.0) or 0.0))
    acceleration_pct = float(momentum.get("acceleration_pct", 0.0) or 0.0)
    direction = str(momentum.get("direction") or "").lower()
    volume_ratio = float(momentum.get("volume_ratio", 1.0) or 1.0)

    spread_source = side_book or yes_book or {}
    spread_pct = float(spread_source.get("spread_pct") or 0.0) if spread_source else 0.0

    # Momentum strength
    momentum_score = _clamp01(
        (momentum_pct - MIN_MOMENTUM_PCT) / max(0.0001, 0.30 - MIN_MOMENTUM_PCT)
    )

    # Recent alignment with momentum direction
    recent_aligned = 1.0 if (
        (direction == "up" and float(momentum.get("recent_momentum_pct", 0.0) or 0.0) > 0) or
        (direction == "down" and float(momentum.get("recent_momentum_pct", 0.0) or 0.0) < 0)
    ) else 0.0

    aligned_accel = acceleration_pct if direction == "up" else -acceleration_pct
    acceleration_score = _clamp01(
        0.55 * _clamp01(recent_pct / 0.06) * recent_aligned +
        0.45 * _clamp01(aligned_accel / 0.05)
    )

    spread_score = _clamp01(1.0 - (spread_pct / max(MAX_SPREAD_PCT, 1e-6)))
    volume_score = _clamp01((volume_ratio - 0.30) / 1.70)

    # Order book imbalance
    imbalance_score = 0.45
    if side_book:
        bid_depth = float(side_book.get("bid_depth_usd") or 0.0)
        ask_depth = float(side_book.get("ask_depth_usd") or 0.0)
        total_depth = bid_depth + ask_depth
        if total_depth > 0:
            bid_share = bid_depth / total_depth
            imbalance_score = _clamp01((bid_share - 0.40) / 0.30)

    # Time remaining score - favor early in window
    window_secs = _window_seconds.get(WINDOW, 300)
    time_score = _clamp01(
        (float(seconds_left or 0.0) - float(MIN_TIME_REMAINING)) /
        max(1.0, window_secs - float(MIN_TIME_REMAINING))
    )

    # Edge (divergence above fee floor)
    edge_score = _clamp01((float(divergence) - float(min_divergence)) / 0.10)

    # Slippage estimate
    slippage_score = 0.50
    if side_book and side_price:
        best_ask = side_book.get("best_ask")
        try:
            if best_ask is not None and side_price > 0:
                slip_pct = max(0.0, (float(best_ask) - float(side_price)) / float(side_price))
                slippage_score = _clamp01(1.0 - (slip_pct / 0.05))
        except Exception:
            pass

    weights = {
        "momentum": 0.22,
        "acceleration": 0.10,
        "spread": 0.15,
        "volume": 0.10,
        "imbalance": 0.13,
        "time": 0.08,
        "edge": 0.17,
        "slippage": 0.05,
    }

    details = {
        "momentum": round(momentum_score, 3),
        "acceleration": round(acceleration_score, 3),
        "spread": round(spread_score, 3),
        "volume": round(volume_score, 3),
        "imbalance": round(imbalance_score, 3),
        "time": round(time_score, 3),
        "edge": round(edge_score, 3),
        "slippage": round(slippage_score, 3),
    }

    score = sum(weights[k] * details[k] for k in weights)
    details["score"] = round(score, 3)
    details["threshold"] = round(ENTRY_SCORE_THRESHOLD, 3)
    details["spread_pct"] = round(spread_pct, 4)
    details["volume_ratio"] = round(volume_ratio, 3)
    details["divergence"] = round(float(divergence), 4)
    details["min_divergence"] = round(float(min_divergence), 4)

    return score, details


# =============================================================================
# Fast Market Discovery
# =============================================================================

def _parse_resolves_at(resolves_at_str):
    """Parse a resolves_at string into a timezone-aware UTC datetime."""
    if not resolves_at_str:
        return None
    try:
        s = str(resolves_at_str).replace("Z", "+00:00").replace(" ", "T")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _parse_fast_market_end_time(question):
    """
    Parse end time from fast market question title.
    e.g., 'Bitcoin Up or Down - February 15, 5:30AM-5:35AM ET' -> datetime

    RAILWAY FIX: Uses manual UTC offset if ZoneInfo unavailable.
    """
    import re
    pattern = r'(\w+ \d+),.*?-\s*(\d{1,2}:\d{2}(?:AM|PM))\s*ET'
    match = re.search(pattern, question)
    if not match:
        return None
    try:
        date_str = match.group(1)
        time_str = match.group(2)
        year = datetime.now(timezone.utc).year
        dt_str = f"{date_str} {year} {time_str}"
        dt_naive = datetime.strptime(dt_str, "%B %d %Y %I:%M%p")

        # Try ZoneInfo first, fall back to manual offset
        et_zone = _get_et_zone()
        if et_zone != timezone.utc:
            dt = dt_naive.replace(tzinfo=et_zone).astimezone(timezone.utc)
        else:
            # Manual: assume EDT (UTC-4) during typical market hours
            # Most BTC fast markets run during US market hours
            now_utc = datetime.now(timezone.utc)
            month = now_utc.month
            if 3 < month < 11:
                offset = timedelta(hours=4)  # EDT
            else:
                offset = timedelta(hours=5)  # EST
            dt = dt_naive.replace(tzinfo=timezone.utc) + offset

        return dt
    except Exception:
        return None


def discover_fast_market_markets(asset="BTC", window="5m"):
    """Find active fast markets via Simmer API, falling back to Gamma."""
    try:
        client = get_client()
        sdk_markets = client.get_fast_markets(asset=asset, window=window, limit=50)
        if sdk_markets:
            markets = []
            for m in sdk_markets:
                end_time = _parse_resolves_at(m.resolves_at) if m.resolves_at else None
                clob_tokens = [m.polymarket_token_id] if m.polymarket_token_id else []
                if m.polymarket_no_token_id:
                    clob_tokens.append(m.polymarket_no_token_id)
                markets.append({
                    "question": m.question,
                    "market_id": m.id,
                    "end_time": end_time,
                    "clob_token_ids": clob_tokens,
                    "is_live_now": m.is_live_now,
                    "spread_cents": m.spread_cents,
                    "liquidity_tier": m.liquidity_tier,
                    "external_price_yes": m.external_price_yes,
                    "fee_rate_bps": getattr(m, 'fee_rate_bps', 0),
                    "source": "simmer",
                })
            return markets
    except Exception as e:
        print(f" ⚠️ Simmer fast-markets API failed ({e}), falling back to Gamma")

    return _discover_via_gamma(asset, window)


def _discover_via_gamma(asset="BTC", window="5m"):
    """Fallback: Find active fast markets on Polymarket via Gamma API."""
    patterns = ASSET_PATTERNS.get(asset, ASSET_PATTERNS["BTC"])
    url = (
        "https://gamma-api.polymarket.com/markets"
        "?limit=100&closed=false&tag=crypto&order=endDate&ascending=true"
    )
    result = _api_request(url)
    if not result or (isinstance(result, dict) and result.get("error")):
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
                    "clob_token_ids": clob_tokens,
                    "fee_rate_bps": int(m.get("fee_rate_bps") or m.get("feeRateBps") or 0),
                    "source": "gamma",
                })
    return markets


def find_best_fast_market(markets):
    """Pick the best fast market to trade: live now, enough time remaining."""
    now = datetime.now(timezone.utc)
    max_remaining = _window_seconds.get(WINDOW, 300) * 2
    candidates = []

    for m in markets:
        if m.get("is_live_now") is not None:
            if not m["is_live_now"]:
                continue
            end_time = m.get("end_time")
            if end_time:
                remaining = (end_time - now).total_seconds()
                if remaining > MIN_TIME_REMAINING:
                    candidates.append((remaining, m))
        else:
            end_time = m.get("end_time")
            if not end_time:
                continue
            remaining = (end_time - now).total_seconds()
            if MIN_TIME_REMAINING < remaining < max_remaining:
                candidates.append((remaining, m))

    if not candidates:
        return None

    # Sort by soonest expiring (most urgent)
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


# =============================================================================
# Import Market
# =============================================================================

def import_fast_market_market(slug):
    """Import a fast market to Simmer. Returns (market_id, error)."""
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


# =============================================================================
# Fee Estimation
# =============================================================================

def _estimate_fee_per_share(price):
    return price * (POLY_FEE_RATE * (price * (1 - price)) ** POLY_FEE_EXPONENT)


# =============================================================================
# Paper Position Management
# =============================================================================

def manage_paper_positions(skill_file, log):
    """Check open paper positions for TP/SL/time exits."""
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

        end_time_str = pos.get("end_time")
        end_time = _parse_resolves_at(end_time_str) if isinstance(end_time_str, str) else end_time_str
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
            shares = float(pos.get("shares", 0.0))
            entry_price = float(pos.get("entry_price", 0.0))
            entry_fee = float(pos.get("entry_fee_per_share", _estimate_fee_per_share(entry_price)))
            exit_fee = float(_estimate_fee_per_share(current_price))
            gross = shares * (current_price - entry_price)
            fees = shares * (entry_fee + exit_fee)
            realized = gross - fees

            state["realized_pnl"] = round(float(state.get("realized_pnl", 0.0)) + realized, 6)
            if realized >= 0:
                state["wins"] = int(state.get("wins", 0)) + 1
            else:
                state["losses"] = int(state.get("losses", 0)) + 1

            log(
                f" ✅ [PAPER] Sold {shares:.1f} {str(pos.get('side', '')).upper()} "
                f"shares @ ${current_price:.3f} ({reason}, P&L ${realized:.2f})",
                force=True,
            )
            closed.append({
                "question": pos.get("question", "Unknown"),
                "side": pos.get("side"),
                "shares": shares,
                "entry_price": entry_price,
                "exit_price": round(current_price, 6),
                "reason": reason,
                "realized_pnl": round(realized, 6),
            })
        else:
            pos["last_price"] = round(current_price, 6)
            remaining_positions.append(pos)

    state["open_positions"] = remaining_positions
    _save_paper_state(skill_file, state)
    return state, closed


# =============================================================================
# REVISED Live Position Management
# Uses local entry record for reliable entry_price
# =============================================================================

def manage_live_positions_v2(skill_file, log):
    """
    REVISED: Actively manage live positions using local entry record.

    KEY FIX: Uses locally stored entry_price instead of Simmer's position API
    which can return 0 or be delayed, causing TP/SL to never fire.

    Runs every 3 seconds when position is active.
    """
    entry_record = _load_entry_record(skill_file)

    # Check if entry record is stale (market expired)
    if entry_record and _entry_record_is_expired(entry_record):
        log(f" ⏰ Entry record expired (market ended), clearing.", force=True)
        _clear_entry_record(skill_file)
        return []

    if not entry_record:
        return []

    # Extract from LOCAL record - these are RELIABLE
    entry_price = float(entry_record["entry_price"])
    entry_cost = float(entry_record["entry_cost"])
    side = entry_record["side"]
    market_id = entry_record["market_id"]
    question = entry_record["question"]
    shares = float(entry_record.get("shares", 0))
    clob_tokens = entry_record.get("clob_token_ids", [])
    target_price = float(entry_record.get("target_price", entry_price * (1 + TAKE_PROFIT_PCT)))
    stop_price = float(entry_record.get("stop_price", entry_price * (1 - STOP_LOSS_PCT)))

    end_str = entry_record.get("end_time")
    end_time = _parse_resolves_at(end_str) if end_str else None

    if entry_price <= 0 or shares <= 0:
        log(f" ⚠️ Invalid entry record (price={entry_price}, shares={shares}), clearing.", force=True)
        _clear_entry_record(skill_file)
        return []

    # Get current CLOB price for our side
    current_price = None
    price_source = "unavailable"

    if clob_tokens:
        # Try side-specific book first for best bid (what we can actually sell at)
        side_book = fetch_side_orderbook_summary(clob_tokens, side=side)
        if side_book and side_book.get("best_bid") is not None:
            current_price = float(side_book["best_bid"])
            price_source = "clob_best_bid"
        elif side_book and side_book.get("mid") is not None:
            current_price = float(side_book["mid"])
            price_source = "clob_mid"

        # Fallback: derive from YES midpoint
        if current_price is None:
            yes_mid = fetch_live_prices(clob_tokens)
            if yes_mid is not None:
                current_price = yes_mid if side == "yes" else (1.0 - yes_mid)
                price_source = "yes_mid_derived"

    if current_price is None:
        log(f" ⚠️ Cannot get current price for {side.upper()} position, skipping exit check.")
        return []

    now = datetime.now(timezone.utc)
    seconds_left = (end_time - now).total_seconds() if end_time else None

    entry_time_str = entry_record.get("ts")
    entry_time = _parse_resolves_at(entry_time_str)
    hold_seconds = (now - entry_time).total_seconds() if entry_time else None

    est_pnl = shares * (current_price - entry_price)

    # Determine exit reason
    reason = None
    if current_price >= target_price:
        reason = "take_profit"
    elif current_price <= stop_price:
        reason = "stop_loss"
    elif seconds_left is not None and seconds_left <= LIVE_TIME_STOP_SECONDS and est_pnl < 0:
        reason = "time_exit_losing"
    elif seconds_left is not None and seconds_left <= RESOLUTION_EXIT_SECONDS:
        reason = "pre_expiry_exit"
    elif hold_seconds is not None and hold_seconds >= LIVE_MAX_HOLD_SECONDS and est_pnl < 0:
        reason = "max_hold_exit"

    if not reason:
        # Log status periodically (every ~30 seconds via heartbeat)
        pnl_pct = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
        log(
            f" 📊 Position: {side.upper()} @ entry ${entry_price:.3f} | "
            f"now ${current_price:.3f} ({pnl_pct:+.1f}%) | "
            f"TP ${target_price:.3f} SL ${stop_price:.3f} | "
            f"{seconds_left:.0f}s left" if seconds_left else ""
        )
        return []

    # Check minimum exit notional ($1 minimum on Polymarket)
    exit_notional = shares * current_price
    if exit_notional < 1.0:
        log(
            f" ⏸️ Exit deferred ({reason}) - notional ${exit_notional:.2f} < $1 minimum. "
            f"Holding {shares:.4f} {side.upper()} @ ${current_price:.3f}",
            force=True,
        )
        # If we can't sell and expiry is imminent, just clear the record
        if seconds_left is not None and seconds_left <= 10:
            log(f" ⏰ Market expiring, clearing entry record.", force=True)
            _clear_entry_record(skill_file)
        return []

    # Execute the exit
    log(
        f" 🔄 Exiting {side.upper()} position: {reason} | "
        f"entry ${entry_price:.3f} -> now ${current_price:.3f} ({price_source}) | "
        f"est P&L ${est_pnl:.2f}",
        force=True,
    )

    result = execute_trade(market_id, side, shares=shares, action="sell")

    if result and result.get("success"):
        proceeds = float(result.get("cost") or 0.0)
        if proceeds <= 0:
            proceeds = exit_notional  # Estimate if not returned
        avg_exit = (proceeds / shares) if shares > 0 else current_price
        realized = shares * (avg_exit - entry_price)

        log(
            f" ✅ Sold {shares:.2f} {side.upper()} shares @ ${avg_exit:.3f} "
            f"({reason}, P&L ${realized:.2f})",
            force=True,
        )

        _append_live_trade_event(skill_file, {
            "type": "exit",
            "market_id": market_id,
            "question": question,
            "side": side,
            "shares": shares,
            "entry_cost": round(entry_cost, 6),
            "entry_price": round(entry_price, 6),
            "target_price": round(target_price, 6),
            "stop_price": round(stop_price, 6),
            "trigger_price": round(current_price, 6),
            "trigger_source": price_source,
            "exit_value": round(proceeds, 6),
            "avg_exit": round(avg_exit, 6),
            "reason": reason,
            "estimated_pnl": round(realized, 6),
        })

        _clear_entry_record(skill_file)

        return [{
            "market_id": market_id,
            "question": question,
            "side": side,
            "shares": shares,
            "reason": reason,
            "estimated_pnl": round(realized, 6),
        }]

    else:
        err = result.get("error", "Unknown error") if result else "No response"

        # Handle insufficient shares - try refreshing count
        if "Insufficient shares" in str(err) or "insufficient" in str(err).lower():
            log(f" ⚠️ Insufficient shares error, attempting with smaller amount...", force=True)
            retry_shares = round(shares * 0.9, 4)  # Try 90% of expected shares
            if retry_shares * current_price >= 1.0:
                retry = execute_trade(market_id, side, shares=retry_shares, action="sell")
                if retry and retry.get("success"):
                    proceeds = float(retry.get("cost") or 0.0)
                    if proceeds <= 0:
                        proceeds = retry_shares * current_price
                    avg_exit = (proceeds / retry_shares) if retry_shares > 0 else current_price
                    realized = retry_shares * (avg_exit - entry_price)
                    log(
                        f" ✅ Sold {retry_shares:.2f} {side.upper()} shares @ ${avg_exit:.3f} "
                        f"(retry, {reason}, P&L ${realized:.2f})",
                        force=True,
                    )
                    _clear_entry_record(skill_file)
                    return [{
                        "market_id": market_id,
                        "question": question,
                        "side": side,
                        "shares": retry_shares,
                        "reason": reason,
                        "estimated_pnl": round(realized, 6),
                    }]

        log(
            f" ❌ Live sell failed ({reason}, {side.upper()} @ ${current_price:.3f}): {err}",
            force=True,
        )
        return []


def _has_active_entry_record(skill_file):
    """Check if there's an active (non-expired) entry record."""
    record = _load_entry_record(skill_file)
    if not record:
        return False
    if _entry_record_is_expired(record):
        _clear_entry_record(skill_file)
        return False
    return True


# =============================================================================
# Portfolio Helpers
# =============================================================================

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
        exposure += float(
            pos.get("entry_cost", 0.0) or
            pos.get("cost_basis", 0.0) or
            pos.get("notional_usdc", 0.0) or
            MAX_POSITION_USD
        )
    return round(exposure, 6), count


def _get_live_pnl_snapshot(skill_file):
    """Return live total P&L info from portfolio."""
    try:
        portfolio = get_portfolio()
        if not portfolio or (isinstance(portfolio, dict) and portfolio.get("error")):
            return {"pnl_total": None, "pnl_24h_effective": None}

        def _to_float(obj, *keys):
            for key in keys:
                try:
                    val = obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)
                    if val is not None:
                        return float(val)
                except Exception:
                    pass
            return None

        pnl_total = _to_float(portfolio, "pnl_total", "total_pnl", "realized_pnl")
        pnl_24h = _to_float(portfolio, "pnl_24h")

        return {
            "pnl_total": pnl_total,
            "pnl_24h_effective": pnl_24h,
        }
    except Exception:
        return {"pnl_total": None, "pnl_24h_effective": None}


# =============================================================================
# Main Strategy Logic
# =============================================================================

def run_fast_market_strategy(dry_run=True, positions_only=False, show_config=False,
                              smart_sizing=False, quiet=False):
    """Run one cycle of the fast market trading strategy."""

    def log(msg, force=False):
        if force or (not quiet and not ACTION_ONLY_LOGS):
            print(msg)

    log("⚡ Simmer FastLoop Trading Skill v2.0")
    log("=" * 50)

    if dry_run:
        log("\n  [PAPER MODE] Trades will be simulated. Use --live for real trades.")

    log(f"\n⚙️  Configuration:")
    log(f"  Asset:           {ASSET}")
    log(f"  Window:          {WINDOW}")
    log(f"  Entry threshold: {ENTRY_THRESHOLD} (min CEX/CLOB divergence)")
    log(f"  Entry score min: {ENTRY_SCORE_THRESHOLD:.2f}")
    log(f"  Min momentum:    {MIN_MOMENTUM_PCT}%")
    log(f"  Max position:    ${MAX_POSITION_USD:.2f}")
    log(f"  Signal source:   {SIGNAL_SOURCE}")
    log(f"  Lookback:        {LOOKBACK_MINUTES} minutes (completed candles)")
    log(f"  Min time left:   {MIN_TIME_REMAINING}s")
    log(f"  TP/SL:           +{TAKE_PROFIT_PCT:.0%} / -{STOP_LOSS_PCT:.0%}")
    log(f"  Pre-expiry exit: {RESOLUTION_EXIT_SECONDS}s before end")
    log(f"  Daily stop:      -${DAILY_LOSS_LIMIT:.2f}")
    log(f"  Time:            {_safe_et_timestamp()}")

    # Initialize client
    get_client(live=not dry_run)

    # Load state
    live_spend = _load_daily_spend(__file__)
    paper_state = _load_paper_state(__file__)

    if show_config:
        config_path = get_config_path(__file__)
        log(f"\n  Config file: {config_path}")
        log(f"\n  To change settings:")
        log(f"    python fast_trader.py --set entry_threshold=0.06")
        log(f"    python fast_trader.py --set asset=BTC")
        return

    # Auto-redeem resolved positions (live mode only)
    if not dry_run:
        global _last_auto_redeem_ts
        now_redeem_ts = time.time()
        if now_redeem_ts - _last_auto_redeem_ts >= 180:
            try:
                redeem_results = get_client().auto_redeem()
                _last_auto_redeem_ts = now_redeem_ts
                redeemed = [r for r in (redeem_results or []) if isinstance(r, dict) and r.get("success")]
                if redeemed:
                    log(f" ✅ Auto-redeemed {len(redeemed)} resolved winning position(s).", force=True)
            except Exception as e:
                _last_auto_redeem_ts = now_redeem_ts
                log(f" ⚠️ Auto-redeem skipped: {e}")

    # Manage exits FIRST before looking for new entries
    paper_state, closed_paper = manage_paper_positions(__file__, log)
    closed_live = []

    if not dry_run:
        closed_live = manage_live_positions_v2(__file__, log)

    # Show positions if requested
    if positions_only:
        log("\n📊 Fast Market Positions:")
        entry = _load_entry_record(__file__)
        if entry:
            log(f"  LIVE: {entry['question'][:60]}")
            log(f"    Side: {entry['side'].upper()} | Entry: ${entry['entry_price']:.3f} | "
                f"Shares: {entry['shares']:.2f}")
            log(f"    TP: ${entry['target_price']:.3f} | SL: ${entry['stop_price']:.3f}")
        else:
            positions = get_positions()
            fast_positions = [p for p in positions if "up or down" in (p.get("question", "") or "").lower()]
            if not fast_positions and not paper_state.get("open_positions"):
                log("  No open fast market positions")
            for pos in fast_positions:
                log(f"  • {pos.get('question', 'Unknown')[:60]}")
                log(f"    YES: {pos.get('shares_yes', 0):.1f} | NO: {pos.get('shares_no', 0):.1f} | "
                    f"P&L: ${pos.get('pnl', 0):.2f}")
            for pos in paper_state.get("open_positions", []):
                log(f"  [PAPER] • {pos.get('question', 'Unknown')[:60]}")
                log(f"    {pos.get('side', '').upper()}: {pos.get('shares', 0):.1f} shares | "
                    f"Entry: ${pos.get('entry_price', 0):.3f}")
        return

    # SINGLE POSITION MODE: If we have an active entry, don't scan for new ones
    if not dry_run and SINGLE_POSITION_LIVE_MODE:
        if _has_active_entry_record(__file__):
            log(f" 🎯 Position-focus mode: active position exists, skipping new entry scan.")
            return

        # Also check live positions in case entry record was cleared but position exists
        live_positions = get_positions()
        fast_positions = [p for p in live_positions
                         if "up or down" in (p.get("question", "") or "").lower()]
        if fast_positions:
            log(f" 🎯 Found {len(fast_positions)} open position(s), skipping new entry scan.")
            return

    # Check loss guard
    guard_state, pause_remaining = _guard_pause_remaining(__file__)
    if pause_remaining > 0:
        log(f"\n⏸️  Loss-stop pause active for {pause_remaining}s "
            f"(reason: {guard_state.get('reason', 'loss_stop')}).", force=True)
        return

    # Check daily loss limits
    if dry_run:
        if paper_state["realized_pnl"] <= -abs(DAILY_LOSS_LIMIT):
            _activate_loss_pause(__file__, paper_state["realized_pnl"], reason="paper_daily_loss_stop")
            log(f"\n🔴 Daily paper loss limit reached (${paper_state['realized_pnl']:.2f}). "
                f"Pausing {PAUSE_HOURS_AFTER_LOSS}h.", force=True)
            return
    else:
        live_pnl = _get_live_pnl_snapshot(__file__)
        live_pnl_24h = live_pnl.get("pnl_24h_effective")
        if live_pnl_24h is not None and live_pnl_24h <= -abs(DAILY_LOSS_LIMIT):
            _activate_loss_pause(__file__, live_pnl_24h, reason="live_daily_loss_stop")
            log(f"\n🔴 Live 24h loss limit reached (${live_pnl_24h:.2f}). "
                f"Pausing {PAUSE_HOURS_AFTER_LOSS}h.", force=True)
            return

    # Smart sizing portfolio info
    if smart_sizing:
        portfolio = get_portfolio()
        if portfolio and not portfolio.get("error"):
            log(f"\n💰 Portfolio Balance: ${portfolio.get('balance_usdc', 0):.2f}")

    # =========================================================================
    # STEP 1: Discover fast markets
    # =========================================================================
    log(f"\n🔍 Discovering {ASSET} fast markets...")
    markets = discover_fast_market_markets(ASSET, WINDOW)
    log(f"  Found {len(markets)} active fast markets")

    if not markets:
        log("  No active fast markets found — may be outside market hours")
        return

    # Look up fee rate
    sample = next((m for m in markets if m.get("clob_token_ids")), None)
    if sample and sample.get("fee_rate_bps", 0) == 0:
        fee = _lookup_fee_rate(sample["clob_token_ids"][0])
        if fee > 0:
            for m in markets:
                m["fee_rate_bps"] = fee

    # =========================================================================
    # STEP 2: Find best market to trade
    # =========================================================================
    best = find_best_fast_market(markets)
    if not best:
        now = datetime.now(timezone.utc)
        for m in markets:
            if m.get("is_live_now") is False:
                log(f"  Skipped: {m['question'][:50]}... (not live yet)")
            elif m.get("end_time"):
                secs = (m["end_time"] - now).total_seconds()
                log(f"  Skipped: {m['question'][:50]}... ({secs:.0f}s left < {MIN_TIME_REMAINING}s min)")
        log(f"  No tradeable markets — waiting for next window")
        return

    end_time = best.get("end_time")
    remaining = (end_time - datetime.now(timezone.utc)).total_seconds() if end_time else 0

    log(f"\n🎯 Selected: {best['question']}")
    log(f"  Expires in: {remaining:.0f}s | Time: {_safe_et_timestamp()}")

    # Check cooldown
    cooldowns = _load_bad_markets(__file__)
    if _cooldown_is_active(cooldowns, best):
        log(f"  Market on cooldown — skip")
        return

    # =========================================================================
    # STEP 3: Fetch live CLOB price
    # =========================================================================
    clob_tokens = best.get("clob_token_ids", [])
    live_price = fetch_live_prices(clob_tokens) if clob_tokens else None

    if live_price is None:
        log(f"  ⏸️ Cannot fetch live CLOB price — skipping (unsafe for fast markets)")
        _set_market_cooldown(__file__, best)
        return
