#!/usr/bin/env python3
"""
Simmer FastLoop Trading Skill - REVISED v2.1

Changes from v2.0:
- Added immediate startup confirmation prints (flush=True)
- ACTION_ONLY_LOGS = False (show all logs for debugging)
- Scan cycle timestamp printed at start of every loop
- All print statements use flush=True for Railway compatibility
- Fixed timezone handling with tzdata package
- Added comprehensive error logging throughout

Trades Polymarket BTC 5-minute fast markets using CEX price momentum.
Default signal: Coinbase BTC-USD candles (completed candles only).

Usage:
    python fast_trader.py              # Dry run
    python fast_trader.py --live       # Execute real trades
    python fast_trader.py --positions  # Show current positions
    python fast_trader.py --quiet      # Only output on trades/errors

Requires:
    SIMMER_API_KEY environment variable (get from simmer.markets/dashboard)
"""

# =============================================================================
# IMMEDIATE STARTUP - Must be before ALL other imports for Railway visibility
# =============================================================================
import sys
import os

# Force unbuffered output immediately
os.environ['PYTHONUNBUFFERED'] = '1'

# Flush startup confirmation before anything else loads
print("=" * 60, flush=True)
print("FASTLOOP v2.1 - CONTAINER STARTING", flush=True)
print(f"Python: {sys.version}", flush=True)
print(f"PID: {os.getpid()}", flush=True)
print("=" * 60, flush=True)
sys.stdout.flush()

# Now load remaining imports
import json
import math
import argparse
import time
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote

print("✅ Core imports loaded", flush=True)

# =============================================================================
# Timezone Helper - Railway/Docker compatible with tzdata package
# =============================================================================

def _safe_et_timestamp():
    """Get Eastern Time timestamp. Uses tzdata package if available."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime('%Y-%m-%d %I:%M:%S %p ET')
    except Exception:
        pass
    try:
        now_utc = datetime.now(timezone.utc)
        month = now_utc.month
        day = now_utc.day
        # Approximate EDT/EST detection
        if 3 < month < 11:
            offset = timedelta(hours=-4)  # EDT
            suffix = "EDT"
        elif month == 3 and day >= 8:
            offset = timedelta(hours=-4)  # EDT after second Sunday March
            suffix = "EDT"
        elif month == 11 and day < 7:
            offset = timedelta(hours=-4)  # EDT before first Sunday Nov
            suffix = "EDT"
        else:
            offset = timedelta(hours=-5)  # EST
            suffix = "EST"
        et_time = now_utc + offset
        return et_time.strftime(f'%Y-%m-%d %I:%M:%S %p {suffix}')
    except Exception:
        return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ (UTC)')


def _get_et_zone():
    """Get Eastern timezone object with fallback."""
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/New_York")
    except Exception:
        return timezone.utc


print(f"✅ Timezone initialized: {_safe_et_timestamp()}", flush=True)

# =============================================================================
# Force line-buffered stdout
# =============================================================================
try:
    sys.stdout.reconfigure(line_buffering=True)
    print("✅ stdout reconfigured to line-buffered", flush=True)
except Exception as e:
    print(f"⚠️ stdout reconfigure skipped: {e}", flush=True)

# =============================================================================
# Optional Trade Journal
# =============================================================================
try:
    from tradejournal import log_trade
    JOURNAL_AVAILABLE = True
    print("✅ Trade journal loaded", flush=True)
except ImportError:
    try:
        from skills.tradejournal import log_trade
        JOURNAL_AVAILABLE = True
        print("✅ Trade journal loaded (skills path)", flush=True)
    except ImportError:
        JOURNAL_AVAILABLE = False
        print("ℹ️ Trade journal not available (optional)", flush=True)

        def log_trade(*args, **kwargs):
            pass

# =============================================================================
# Configuration Schema
# =============================================================================

CONFIG_SCHEMA = {
    "entry_threshold": {
        "default": 0.06, "env": "SIMMER_SPRINT_ENTRY", "type": float,
        "help": "Min CEX/CLOB divergence to trigger trade"
    },
    "min_momentum_pct": {
        "default": 0.08, "env": "SIMMER_SPRINT_MOMENTUM", "type": float,
        "help": "Min BTC % move in lookback window to trigger"
    },
    "entry_score_threshold": {
        "default": 0.55, "env": "SIMMER_SPRINT_SCORE_THRESHOLD", "type": float,
        "help": "Minimum multi-factor entry score (0-1)"
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
        "help": "Minutes of completed candles for momentum calc"
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
        "help": "Weight signal by volume"
    },
    "max_open_exposure": {
        "default": 2.5, "env": "SIMMER_SPRINT_MAX_EXPOSURE", "type": float,
        "help": "Maximum simultaneous open exposure"
    },
    "take_profit_pct": {
        "default": 0.12, "env": "SIMMER_SPRINT_TP", "type": float,
        "help": "Take profit percentage"
    },
    "stop_loss_pct": {
        "default": 0.07, "env": "SIMMER_SPRINT_SL", "type": float,
        "help": "Stop loss percentage"
    },
    "daily_loss_limit": {
        "default": 15.0, "env": "SIMMER_SPRINT_DAILY_LOSS", "type": float,
        "help": "Stop trading after this much loss in a UTC day"
    },
    "pause_hours_after_loss": {
        "default": 1, "env": "SIMMER_SPRINT_PAUSE_HOURS", "type": int,
        "help": "Pause hours after daily loss limit hit"
    },
    "resolution_exit_seconds": {
        "default": 45, "env": "SIMMER_SPRINT_RESOLVE_EXIT", "type": int,
        "help": "Exit positions this many seconds before market expiry"
    },
    "daily_budget": {
        "default": 0.0, "env": "SIMMER_SPRINT_DAILY_BUDGET", "type": float,
        "help": "Legacy (unused)"
    },
    "daily_profit_target": {
        "default": 0.0, "env": "SIMMER_SPRINT_DAILY_PROFIT", "type": float,
        "help": "Legacy (unused)"
    },
    "max_trades_per_day": {
        "default": 0, "env": "SIMMER_SPRINT_MAX_TRADES", "type": int,
        "help": "Legacy (unused)"
    },
}

# =============================================================================
# Constants
# =============================================================================

TRADE_SOURCE = "sdk:fastloop"
SKILL_SLUG = "polymarket-fast-loop"
_automaton_reported = False

SMART_SIZING_PCT = 0.05
MIN_SHARES_PER_ORDER = 5
MAX_SPREAD_PCT = 0.06
MIN_ENTRY_PRICE = 0.05
MIN_LIVE_ENTRY_PRICE = 0.10
MAX_ENTRY_PRICE = 0.95
MOMENTUM_MAX_ENTRY = 0.45

SCAN_INTERVAL_SECONDS = 30
LIVE_SCAN_INTERVAL_SECONDS = 15
FOCUSED_LIVE_SCAN_INTERVAL_SECONDS = 3
HEARTBEAT_SECONDS = 300          # Every 5 minutes
LIVE_TIME_STOP_SECONDS = 45
LIVE_MAX_HOLD_SECONDS = 120

# Show ALL logs for debugging - change to True once confirmed working
ACTION_ONLY_LOGS = False

_last_heartbeat_ts = 0
_last_auto_redeem_ts = 0

SINGLE_POSITION_LIVE_MODE = True
BAD_MARKET_COOLDOWN_CYCLES = 3

POLY_FEE_RATE = 0.25
POLY_FEE_EXPONENT = 2

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
# Load Config from simmer_sdk
# =============================================================================

print("⏳ Loading simmer_sdk...", flush=True)

try:
    from simmer_sdk.skill import load_config, update_config, get_config_path
    print("✅ simmer_sdk loaded successfully", flush=True)
except ImportError as e:
    print(f"❌ FATAL: simmer_sdk not found: {e}", flush=True)
    print("   Run: pip install simmer-sdk", flush=True)
    sys.exit(1)
except Exception as e:
    print(f"❌ FATAL: simmer_sdk load error: {e}", flush=True)
    sys.exit(1)

try:
    cfg = load_config(CONFIG_SCHEMA, __file__, slug="polymarket-fast-loop")
    print("✅ Config loaded", flush=True)
except Exception as e:
    print(f"❌ FATAL: Config load failed: {e}", flush=True)
    sys.exit(1)

# Apply config values
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

print(f"✅ Config applied:", flush=True)
print(f"   Asset: {ASSET} | Window: {WINDOW}", flush=True)
print(f"   Max position: ${MAX_POSITION_USD:.2f}", flush=True)
print(f"   TP: {TAKE_PROFIT_PCT:.0%} | SL: {STOP_LOSS_PCT:.0%}", flush=True)
print(f"   Entry threshold: {ENTRY_THRESHOLD}", flush=True)
print(f"   Min momentum: {MIN_MOMENTUM_PCT}%", flush=True)

# =============================================================================
# Local Entry Record - Stores entry data so TP/SL always has reliable prices
# =============================================================================

def _get_entry_record_path(skill_file):
    from pathlib import Path
    return Path(skill_file).parent / "current_entry.json"


def _save_entry_record(skill_file, market_id, question, side, entry_price,
                       shares, entry_cost, end_time, clob_token_ids=None):
    """
    Save entry record locally immediately after a successful trade.
    CRITICAL: This is the authoritative source for entry_price.
    Do NOT rely on Simmer position API which can be delayed or return 0.
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
    print(f"✅ Entry record saved: {side.upper()} @ ${entry_price:.3f} "
          f"| TP ${record['target_price']:.3f} | SL ${record['stop_price']:.3f}", flush=True)
    return record


def _load_entry_record(skill_file):
    """Load current entry record. Returns None if no active entry."""
    path = _get_entry_record_path(skill_file)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        if not data.get("entry_price") or not data.get("side"):
            return None
        return data
    except Exception as e:
        print(f"⚠️ Could not load entry record: {e}", flush=True)
        return None


def _clear_entry_record(skill_file):
    """Clear entry record after position is closed."""
    from pathlib import Path
    path = Path(skill_file).parent / "current_entry.json"
    if path.exists():
        try:
            path.unlink()
            print("✅ Entry record cleared", flush=True)
        except Exception as e:
            print(f"⚠️ Could not clear entry record: {e}", flush=True)


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
        expired = datetime.now(timezone.utc) > end_time + timedelta(seconds=30)
        return expired
    except Exception:
        return False


def _has_active_entry_record(skill_file):
    """Check if there is an active non-expired entry record."""
    record = _load_entry_record(skill_file)
    if not record:
        return False
    if _entry_record_is_expired(record):
        print("⏰ Entry record expired, clearing.", flush=True)
        _clear_entry_record(skill_file)
        return False
    return True


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
    print(f"⏸️ Loss pause activated: {reason} | Resume at {until.isoformat()}", flush=True)
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
            req_headers["User-Agent"] = "simmer-fastloop/2.1"
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
            print("❌ FATAL: simmer-sdk not installed. Run: pip install simmer-sdk", flush=True)
            sys.exit(1)
        api_key = os.environ.get("SIMMER_API_KEY")
        if not api_key:
            print("❌ FATAL: SIMMER_API_KEY environment variable not set", flush=True)
            sys.exit(1)
        print(f"⏳ Connecting to Simmer (live={live})...", flush=True)
        venue = os.environ.get("TRADING_VENUE", "polymarket")
        try:
            _client = SimmerClient(api_key=api_key, venue=venue, live=live)
            print("✅ Simmer client connected", flush=True)
        except Exception as e:
            print(f"❌ FATAL: Simmer client connection failed: {e}", flush=True)
            sys.exit(1)
    return _client


def get_portfolio():
    try:
        return get_client().get_portfolio()
    except Exception as e:
        print(f"⚠️ get_portfolio error: {e}", flush=True)
        return {"error": str(e)}


def get_positions():
    try:
        positions = get_client().get_positions()
        from dataclasses import asdict
        return [asdict(p) for p in positions]
    except Exception as e:
        print(f"⚠️ get_positions error: {e}", flush=True)
        return []


def get_market_details(market_id):
    try:
        market = get_client().get_market_by_id(market_id)
        if not market:
            return None
        from dataclasses import asdict
        return asdict(market)
    except Exception as e:
        print(f"⚠️ get_market_details error: {e}", flush=True)
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
        print(f"⏳ Executing trade: {action} {side.upper()} "
              f"{'$' + str(round(amount, 2)) if amount else str(shares) + ' shares'}", flush=True)
        result = get_client().trade(**kwargs)
        trade_result = {
            "success": getattr(result, "success", False),
            "trade_id": getattr(result, "trade_id", None),
            "shares_bought": getattr(result, "shares_bought", None),
            "shares": getattr(result, "shares_bought", None) or shares,
            "cost": getattr(result, "cost", None),
            "error": getattr(result, "error", None),
            "simulated": getattr(result, "simulated", False),
        }
        print(f"   Trade result: success={trade_result['success']} | "
              f"shares={trade_result['shares_bought']} | "
              f"cost={trade_result['cost']} | "
              f"simulated={trade_result['simulated']} | "
              f"error={trade_result['error']}", flush=True)
        return trade_result
    except Exception as e:
        print(f"❌ execute_trade exception: {e}", flush=True)
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
    """Fetch YES token order book spread and depth."""
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
# CEX Price Signal - FIXED: Skip incomplete candle[0]
# =============================================================================

def get_coinbase_momentum(asset="BTC", lookback_minutes=3):
    """
    Get price momentum from Coinbase API.

    CRITICAL FIX: candles[0] is the CURRENT INCOMPLETE candle.
    We skip it and start from candles[1] (last completed candle).
    Using an incomplete candle causes false momentum signals.
    """
    product = ASSET_COINBASE.get(asset, "BTC-USD")
    url = f"https://api.exchange.coinbase.com/products/{product}/candles?granularity=60"

    print(f"  📡 Fetching Coinbase {product} candles...", flush=True)
    result = _api_request(url, timeout=10)

    if not result:
        print(f"  ❌ Coinbase API returned empty response", flush=True)
        return None
    if isinstance(result, dict):
        print(f"  ❌ Coinbase API error: {result.get('error', result)}", flush=True)
        return None

    try:
        print(f"  📊 Got {len(result)} candles from Coinbase", flush=True)

        # SKIP candles[0] - current incomplete candle
        # candles[1] = most recent COMPLETED 1-minute candle
        # Format: [timestamp, low, high, open, close, volume]
        completed = result[1:]

        if len(completed) < lookback_minutes + 1:
            print(f"  ⚠️ Not enough completed candles: {len(completed)} < {lookback_minutes + 1}", flush=True)
            return None

        price_now = float(completed[0][4])    # Most recent completed close
        price_then = float(completed[lookback_minutes - 1][4])  # N minutes ago

        if price_then <= 0:
            print(f"  ⚠️ Invalid price_then: {price_then}", flush=True)
            return None

        momentum_pct = ((price_now - price_then) / price_then) * 100
        direction = "up" if momentum_pct > 0 else "down"

        recent_pct = 0.0
        prior_pct = 0.0
        if len(completed) >= 2:
            prev_close = float(completed[1][4])
            if prev_close > 0:
                recent_pct = ((price_now - prev_close) / prev_close) * 100

        if len(completed) >= 3:
            prior_close = float(completed[2][4])
            prev_close_2 = float(completed[1][4])
            if prior_close > 0:
                prior_pct = ((prev_close_2 - prior_close) / prior_close) * 100

        acceleration_pct = recent_pct - prior_pct

        volumes = [float(c[5]) for c in completed[:lookback_minutes]]
        avg_volume = sum(volumes) / len(volumes) if volumes else 1.0
        latest_volume = volumes[0] if volumes else 0.0
        volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0

        print(f"  💹 BTC: ${price_now:,.2f} (was ${price_then:,.2f}) | "
              f"momentum: {momentum_pct:+.3f}% | "
              f"direction: {direction} | "
              f"volume ratio: {volume_ratio:.2f}x", flush=True)

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
        print(f"  ❌ Coinbase momentum calculation error: {e}", flush=True)
        return None


def get_momentum(asset="BTC", source="coinbase", lookback=3):
    """Get price momentum from configured source."""
    return get_coinbase_momentum(asset, lookback)


# =============================================================================
# REVISED Entry Signal - Mean Reversion / CEX-CLOB Divergence
# =============================================================================

def _find_trade_signal(momentum, market_yes_price, remaining_seconds):
    """
    REVISED: Find trade signal based on CEX/CLOB divergence.

    Previous (WRONG): Buy YES when BTC going UP - chases already-priced moves.

    Correct: Find where CLOB price HASN'T caught up to CEX signal yet.
    - Calculate where YES SHOULD be priced given BTC momentum
    - If actual YES < implied YES by enough to cover fees -> buy YES
    - If actual YES > implied YES by enough to cover fees -> buy NO

    The edge is the gap between Polymarket's current price and
    where Coinbase momentum says it should be.
    """
    momentum_pct = momentum["momentum_pct"]  # signed
    volume_ratio = momentum["volume_ratio"]

    if volume_ratio < 0.3:
        return None, None, f"volume too low ({volume_ratio:.2f}x)"

    # CEX-implied YES probability
    # 1% BTC move = ~15 cent shift in fair value (conservative)
    SCALE_FACTOR = 15.0
    implied_yes = 0.50 + (momentum_pct / 100.0) * SCALE_FACTOR
    implied_yes = max(0.15, min(0.85, implied_yes))

    clob_divergence = implied_yes - market_yes_price

    # Fee-aware minimum edge
    buy_price = market_yes_price if clob_divergence > 0 else (1.0 - market_yes_price)
    fee_estimate = POLY_FEE_RATE * (buy_price * (1 - buy_price)) ** POLY_FEE_EXPONENT
    min_edge = fee_estimate * 2 + ENTRY_THRESHOLD

    print(f"  🧮 Signal calc: BTC {momentum_pct:+.3f}% | "
          f"implied YES ${implied_yes:.3f} | "
          f"actual YES ${market_yes_price:.3f} | "
          f"divergence {clob_divergence:+.3f} | "
          f"min edge {min_edge:.3f}", flush=True)

    if clob_divergence > min_edge:
        return "yes", clob_divergence, None
    elif clob_divergence < -min_edge:
        return "no", abs(clob_divergence), None

    return None, None, f"divergence {clob_divergence:.3f} < min edge {min_edge:.3f}"


def _clamp01(value):
    try:
        return max(0.0, min(1.0, float(value)))
    except Exception:
        return 0.0


def _score_entry_setup(side, momentum, divergence, min_divergence,
                       seconds_left, yes_book=None, side_book=None, side_price=None):
    """Multi-factor entry score. Returns (score, details)."""
    momentum_pct = abs(float(momentum.get("momentum_pct", 0.0) or 0.0))
    recent_pct = abs(float(momentum.get("recent_momentum_pct", 0.0) or 0.0))
    acceleration_pct = float(momentum.get("acceleration_pct", 0.0) or 0.0)
    direction = str(momentum.get("direction") or "").lower()
    volume_ratio = float(momentum.get("volume_ratio", 1.0) or 1.0)

    spread_source = side_book or yes_book or {}
    spread_pct = float(spread_source.get("spread_pct") or 0.0) if spread_source else 0.0

    momentum_score = _clamp01(
        (momentum_pct - MIN_MOMENTUM_PCT) / max(0.0001, 0.30 - MIN_MOMENTUM_PCT)
    )

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

    imbalance_score = 0.45
    if side_book:
        bid_depth = float(side_book.get("bid_depth_usd") or 0.0)
        ask_depth = float(side_book.get("ask_depth_usd") or 0.0)
        total_depth = bid_depth + ask_depth
        if total_depth > 0:
            bid_share = bid_depth / total_depth
            imbalance_score = _clamp01((bid_share - 0.40) / 0.30)

    window_secs = _window_seconds.get(WINDOW, 300)
    time_score = _clamp01(
        (float(seconds_left or 0.0) - float(MIN_TIME_REMAINING)) /
        max(1.0, window_secs - float(MIN_TIME_REMAINING))
    )

    edge_score = _clamp01((float(divergence) - float(min_divergence)) / 0.10)

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
# Fee Estimation
# =============================================================================

def _estimate_fee_per_share(price):
    return price * (POLY_FEE_RATE * (price * (1 - price)) ** POLY_FEE_EXPONENT)


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
    """Parse end time from fast market question title."""
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

        et_zone = _get_et_zone()
        if et_zone != timezone.utc:
            from zoneinfo import ZoneInfo
            dt = dt_naive.replace(tzinfo=ZoneInfo("America/New_York")).astimezone(timezone.utc)
        else:
            now_utc = datetime.now(timezone.utc)
            month = now_utc.month
            if 3 < month < 11:
                offset_hours = 4
            else:
                offset_hours = 5
            dt = dt_naive.replace(tzinfo=timezone.utc) + timedelta(hours=offset_hours)
        return dt
    except Exception as e:
        print(f"  ⚠️ Could not parse market end time from '{question}': {e}", flush=True)
        return None


def discover_fast_market_markets(asset="BTC", window="5m"):
    """Find active fast markets via Simmer API, falling back to Gamma."""
    print(f"  🔍 Querying Simmer fast markets API...", flush=True)
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
            print(f"  ✅ Simmer API returned {len(markets)} fast markets", flush=True)
            return markets
    except Exception as e:
        print(f"  ⚠️ Simmer fast-markets API failed ({e}), trying Gamma fallback...", flush=True)

    return _discover_via_gamma(asset, window)


def _discover_via_gamma(asset="BTC", window="5m"):
    """Fallback: Find active fast markets via Gamma API."""
    print(f"  🔍 Querying Gamma API fallback...", flush=True)
    patterns = ASSET_PATTERNS.get(asset, ASSET_PATTERNS["BTC"])
    url = (
        "https://gamma-api.polymarket.com/markets"
        "?limit=100&closed=false&tag=crypto&order=endDate&ascending=true"
    )
    result = _api_request(url)
    if not result or (isinstance(result, dict) and result.get("error")):
        print(f"  ❌ Gamma API also failed", flush=True)
        return []

    markets = []
    for m in result:
        q = (m.get("question") or "").lower()
        slug = m.get("slug", "")
        matches_window = f"-{window}-" in slug
        if any(p in q for p in patterns) and matches_window:
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
                    "condition_id": m.get("conditionId", ""),
                    "end_time": end_time,
                    "clob_token_ids": clob_tokens,
                    "fee_rate_bps": int(m.get("fee_rate_bps") or m.get("feeRateBps") or 0),
                    "source": "gamma",
                })
    print(f"  ✅ Gamma API returned {len(markets)} matching markets", flush=True)
    return markets


def find_best_fast_market(markets):
    """Pick the best fast market: live now, enough time remaining."""
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
                    print(f"  ✅ Candidate: {m['question'][:50]}... ({remaining:.0f}s remaining)", flush=True)
        else:
            end_time = m.get("end_time")
            if not end_time:
                continue
            remaining = (end_time - now).total_seconds()
            if MIN_TIME_REMAINING < remaining < max_remaining:
                candidates.append((remaining, m))
                print(f"  ✅ Candidate: {m['question'][:50]}... ({remaining:.0f}s remaining)", flush=True)

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


# =============================================================================
# Import Market
# =============================================================================

def import_fast_market_market(slug):
    """Import a fast market to Simmer. Returns (market_id, error)."""
    url = f"https://polymarket.com/event/{slug}"
    print(f"  ⏳ Importing market: {slug[:40]}...", flush=True)
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
            return None, f"Market resolved. Try: {alternatives[0].get('id')}"
        return None, "Market resolved, no alternatives"

    if status in ("imported", "already_exists"):
        print(f"  ✅ Market imported: {market_id}", flush=True)
        return market_id, None

    return None, f"Unexpected status: {status}"


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

            print(f"✅ [PAPER] Sold {shares:.1f} {str(pos.get('side', '')).upper()} "
                  f"@ ${current_price:.3f} ({reason}, P&L ${realized:.2f})", flush=True)
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
# REVISED Live Position Management - Uses local entry record
# =============================================================================

def manage_live_positions_v2(skill_file, log):
    """
    REVISED: Manage live positions using local entry record.

    CRITICAL FIX: Uses locally stored entry_price (reliable) instead of
    Simmer's position API which can return 0 or be delayed.
    """
    entry_record = _load_entry_record(skill_file)

    if entry_record and _entry_record_is_expired(entry_record):
        print(f"⏰ Entry record expired (market ended), clearing.", flush=True)
        _clear_entry_record(skill_file)
        return []

    if not entry_record:
        return []

    # Extract from LOCAL record - RELIABLE values
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
        print(f"⚠️ Invalid entry record (price={entry_price}, shares={shares}), clearing.", flush=True)
        _clear_entry_record(skill_file)
        return []

    # Get current CLOB price for our side
    current_price = None
    price_source = "unavailable"

    if clob_tokens:
        side_book = fetch_side_orderbook_summary(clob_tokens, side=side)
        if side_book and side_book.get("best_bid") is not None:
            current_price = float(side_book["best_bid"])
            price_source = "clob_best_bid"
        elif side_book and side_book.get("mid") is not None:
            current_price = float(side_book["mid"])
            price_source = "clob_mid"

        if current_price is None:
            yes_mid = fetch_live_prices(clob_tokens)
            if yes_mid is not None:
                current_price = yes_mid if side == "yes" else (1.0 - yes_mid)
                price_source = "yes_mid_derived"

    if current_price is None:
        print(f"⚠️ Cannot get current price for {side.upper()} position, skipping exit check.", flush=True)
        return []

    now = datetime.now(timezone.utc)
    seconds_left = (end_time - now).total_seconds() if end_time else None
    entry_time_str = entry_record.get("ts")
    entry_time = _parse_resolves_at(entry_time_str)
    hold_seconds = (now - entry_time).total_seconds() if entry_time else None
    est_pnl = shares * (current_price - entry_price)
    pnl_pct = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0

    print(f"📊 POSITION STATUS: {side.upper()} | "
          f"entry ${entry_price:.3f} | now ${current_price:.3f} ({pnl_pct:+.1f}%) | "
          f"TP ${target_price:.3f} | SL ${stop_price:.3f} | "
          f"{f'{seconds_left:.0f}s left' if seconds_left else 'no expiry'} | "
          f"est P&L ${est_pnl:.3f}", flush=True)

    # Determine exit reason
    reason = None
    if current_price >= target_price:
        reason = "take_profit"
        print(f"🎯 TAKE PROFIT triggered! ${current_price:.3f} >= ${target_price:.3f}", flush=True)
    elif current_price <= stop_price:
        reason = "stop_loss"
        print(f"🛑 STOP LOSS triggered! ${current_price:.3f} <= ${stop_price:.3f}", flush=True)
    elif seconds_left is not None and seconds_left <= LIVE_TIME_STOP_SECONDS and est_pnl < 0:
        reason = "time_exit_losing"
        print(f"⏰ TIME EXIT (losing): {seconds_left:.0f}s left, P&L ${est_pnl:.3f}", flush=True)
    elif seconds_left is not None and seconds_left <= RESOLUTION_EXIT_SECONDS:
        reason = "pre_expiry_exit"
        print(f"⏰ PRE-EXPIRY EXIT: {seconds_left:.0f}s left", flush=True)
    elif hold_seconds is not None and hold_seconds >= LIVE_MAX_HOLD_SECONDS and est_pnl < 0:
        reason = "max_hold_exit"
        print(f"⏰ MAX HOLD EXIT: held {hold_seconds:.0f}s, P&L ${est_pnl:.3f}", flush=True)

    if not reason:
        return []

    # Check minimum exit notional
    exit_notional = shares * current_price
    if exit_notional < 1.0:
        print(f"⏸️ Exit deferred ({reason}): notional ${exit_notional:.2f} < $1 minimum. "
              f"Holding {shares:.4f} {side.upper()} @ ${current_price:.3f}", flush=True)
        if seconds_left is not None and seconds_left <= 10:
            print(f"⏰ Market expiring imminently, clearing entry record.", flush=True)
            _clear_entry_record(skill_file)
        return []

    # Execute exit
    print(f"🔄 EXECUTING EXIT: {reason} | {side.upper()} {shares:.4f} shares | "
          f"entry ${entry_price:.3f} -> now ${current_price:.3f} ({price_source}) | "
          f"est P&L ${est_pnl:.2f}", flush=True)

    result = execute_trade(market_id, side, shares=shares, action="sell")

    if result and result.get("success"):
        proceeds = float(result.get("cost") or 0.0)
        if proceeds <= 0:
            proceeds = exit_notional
        avg_exit = (proceeds / shares) if shares > 0 else current_price
        realized = shares * (avg_exit - entry_price)

        print(f"✅ EXIT SUCCESSFUL: Sold {shares:.2f} {side.upper()} @ ${avg_exit:.3f} | "
              f"P&L ${realized:.2f} | reason: {reason}", flush=True)

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

        # Retry with slightly fewer shares for insufficient shares error
        if "insufficient" in str(err).lower() or "Insufficient" in str(err):
            print(f"⚠️ Insufficient shares error, retrying with 90% of shares...", flush=True)
            retry_shares = round(shares * 0.9, 4)
            if retry_shares * current_price >= 1.0:
                retry = execute_trade(market_id, side, shares=retry_shares, action="sell")
                if retry and retry.get("success"):
                    proceeds = float(retry.get("cost") or retry_shares * current_price)
                    avg_exit = (proceeds / retry_shares) if retry_shares > 0 else current_price
                    realized = retry_shares * (avg_exit - entry_price)
                    print(f"✅ RETRY EXIT: Sold {retry_shares:.2f} {side.upper()} @ ${avg_exit:.3f} | "
                          f"P&L ${realized:.2f}", flush=True)
                    _clear_entry_record(skill_file)
                    return [{
                        "market_id": market_id,
                        "question": question,
                        "side": side,
                        "shares": retry_shares,
                        "reason": reason,
                        "estimated_pnl": round(realized, 6),
                    }]

        print(f"❌ EXIT FAILED ({reason}): {err}", flush=True)
        return []


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
    """Return live P&L info from portfolio."""
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
        return {"pnl_total": pnl_total, "pnl_24h_effective": pnl_24h}
    except Exception as e:
        print(f"⚠️ PnL snapshot error: {e}", flush=True)
        return {"pnl_total": None, "pnl_24h_effective": None}


# =============================================================================
# Main Strategy Logic
# =============================================================================

def run_fast_market_strategy(dry_run=True, positions_only=False, show_config=False,
                              smart_sizing=False, quiet=False):
    """Run one cycle of the fast market trading strategy."""

    def log(msg, force=False):
        print(msg, flush=True)

    log(f"\n{'='*60}")
    log(f"⚡ FastLoop Cycle | {_safe_et_timestamp()}")
    log(f"{'='*60}")

    if dry_run:
        log("  [PAPER MODE] Use --live for real trades.")

    log(f"\n⚙️  Config: {ASSET}/{WINDOW} | "
        f"max ${MAX_POSITION_USD:.2f} | "
        f"TP {TAKE_PROFIT_PCT:.0%} / SL {STOP_LOSS_PCT:.0%} | "
        f"threshold {ENTRY_THRESHOLD} | "
        f"momentum {MIN_MOMENTUM_PCT}% | "
        f"score {ENTRY_SCORE_THRESHOLD:.2f}")

    # Initialize client
    get_client(live=not dry_run)

    # Load state
    live_spend = _load_daily_spend(__file__)
    paper_state = _load_paper_state(__file__)

    if show_config:
        config_path = get_config_path(__file__)
        log(f"\n  Config file: {config_path}")
        return

    # Auto-redeem resolved positions (live only)
    if not dry_run:
        global _last_auto_redeem_ts
        now_redeem_ts = time.time()
        if now_redeem_ts - _last_auto_redeem_ts >= 180:
            try:
                redeem_results = get_client().auto_redeem()
                _last_auto_redeem_ts = now_redeem_ts
                redeemed = [r for r in (redeem_results or []) if isinstance(r, dict) and r.get("success")]
                if redeemed:
                    log(f"✅ Auto-redeemed {len(redeemed)} resolved position(s).")
            except Exception as e:
                _last_auto_redeem_ts = now_redeem_ts
                log(f"⚠️ Auto-redeem skipped: {e}")

    # Manage exits FIRST
    paper_state, closed_paper = manage_paper_positions(__file__, log)
    closed_live = []
    if not dry_run:
        closed_live = manage_live_positions_v2(__file__, log)

    # Show positions if requested
    if positions_only:
        log("\n📊 Current Positions:")
        entry = _load_entry_record(__file__)
        if entry:
            log(f"  LIVE: {entry['question'][:60]}")
            log(f"    {entry['side'].upper()} @ ${entry['entry_price']:.3f} | "
                f"shares: {entry['shares']:.2f} | "
                f"TP: ${entry['target_price']:.3f} | SL: ${entry['stop_price']:.3f}")
        else:
            positions = get_positions()
            fast_pos = [p for p in positions if "up or down" in (p.get("question", "") or "").lower()]
            if not fast_pos and not paper_state.get("open_positions"):
                log("  No open fast market positions")
            for pos in fast_pos:
                log(f"  • {pos.get('question', 'Unknown')[:60]}")
        return

    # SINGLE POSITION MODE: If position active, skip new entry scan
    if not dry_run and SINGLE_POSITION_LIVE_MODE:
        if _has_active_entry_record(__file__):
            log(f"🎯 Position-focus mode: active entry exists, skipping new scan.")
            return
        live_positions = get_positions()
        fast_positions = [p for p in live_positions
                         if "up or down" in (p.get("question", "") or "").lower()]
        if fast_positions:
            log(f"🎯 Found {len(fast_positions)} open position(s), skipping new scan.")
            return

    # Check loss guard pause
    guard_state, pause_remaining = _guard_pause_remaining(__file__)
    if pause_remaining > 0:
        log(f"⏸️  Loss-stop pause: {pause_remaining}s remaining "
            f"(reason: {guard_state.get('reason', 'loss_stop')})")
        return

    # Check daily loss limits
    if dry_run:
        if paper_state["realized_pnl"] <= -abs(DAILY_LOSS_LIMIT):
            _activate_loss_pause(__file__, paper_state["realized_pnl"], "paper_daily_loss")
            log(f"🔴 Paper loss limit reached (${paper_state['realized_pnl']:.2f}). Pausing.")
            return
    else:
        live_pnl = _get_live_pnl_snapshot(__file__)
        live_pnl_24h = live_pnl.get("pnl_24h_effective")
        if live_pnl_24h is not None and live_pnl_24h <= -abs(DAILY_LOSS_LIMIT):
            _activate_loss_pause(__file__, live_pnl_24h, "live_daily_loss")
            log(f"🔴 Live loss limit reached (${live_pnl_24h:.2f}). Pausing.")
            return

    # =========================================================================
    # STEP 1: Discover markets
    # =========================================================================
    log(f"\n🔍 Discovering
