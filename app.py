"""
=============================================================
  NIFTY & SENSEX Trading Intelligence System - Backend
  Real-time market analysis using Upstox API v2
=============================================================

SETUP INSTRUCTIONS:
  1. pip install flask flask-cors flask-socketio requests aiohttp numpy
  2. python app.py
  3. Open http://localhost:5000 in your browser
  4. Enter your Upstox Access Token in the UI

REQUIRED PIP INSTALLS:
  pip install flask flask-cors flask-socketio requests numpy

NOTE: Access token is entered via the web UI - no hardcoding needed.
=============================================================
"""

import json
import time
import math
import threading
import requests
import numpy as np
from datetime import datetime, timedelta
from flask import Flask, render_template, send_from_directory, jsonify, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from collections import deque

# ============================================================
# Flask App Setup
# ============================================================
app = Flask(__name__, static_folder='.', template_folder='.')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ============================================================
# Global State
# ============================================================
ACCESS_TOKEN = None
DATA_THREAD = None
THREAD_RUNNING = False

# Instrument keys
INSTRUMENTS = {
    'NIFTY': {
        'index_key': 'NSE_INDEX|Nifty 50',
        'option_prefix': 'NSE_FO',
        'name': 'NIFTY',
        'strike_gap': 50,
        'lot_size': 50
    },
    'SENSEX': {
        'index_key': 'BSE_INDEX|SENSEX',
        'option_prefix': 'BSE_FO',
        'name': 'SENSEX',
        'strike_gap': 100,
        'lot_size': 10
    }
}

# Data stores
market_data = {
    'NIFTY': {
        'ltp': 0, 'open': 0, 'high': 0, 'low': 0, 'close': 0,
        'prev_close': 0, 'prev_high': 0, 'prev_low': 0,
        'volume': 0, 'change': 0, 'change_pct': 0,
        'candles_3m': [], 'candles_5m': [],
        'vwap': 0, 'vwap_data': {'cum_vol': 0, 'cum_tp_vol': 0},
        'option_chain': [], 'supports': [], 'resistances': [],
        'alerts': [], 'trade_suggestion': {},
        'smc_zones': [], 'market_breadth': {},
        'oi_analysis': {}, 'vix': 0,
        'institutional': {}, 'dashboard': {},
        'chart_alerts': []
    },
    'SENSEX': {
        'ltp': 0, 'open': 0, 'high': 0, 'low': 0, 'close': 0,
        'prev_close': 0, 'prev_high': 0, 'prev_low': 0,
        'volume': 0, 'change': 0, 'change_pct': 0,
        'candles_3m': [], 'candles_5m': [],
        'vwap': 0, 'vwap_data': {'cum_vol': 0, 'cum_tp_vol': 0},
        'option_chain': [], 'supports': [], 'resistances': [],
        'alerts': [], 'trade_suggestion': {},
        'smc_zones': [], 'market_breadth': {},
        'oi_analysis': {}, 'vix': 0,
        'institutional': {}, 'dashboard': {},
        'chart_alerts': []
    }
}

alert_history = {'NIFTY': deque(maxlen=50), 'SENSEX': deque(maxlen=50)}


# ============================================================
# Upstox API Helper Functions
# ============================================================
def upstox_headers():
    """Get headers for Upstox API calls."""
    return {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }


def fetch_market_quote(instrument_key):
    """Fetch full market quote for an instrument."""
    try:
        url = f'https://api.upstox.com/v2/market-quote/quotes?instrument_key={instrument_key}'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data'):
                return data['data']
        return None
    except Exception as e:
        print(f"[ERROR] fetch_market_quote: {e}")
        return None


def fetch_ohlc_quote(instrument_key):
    """Fetch OHLC quote."""
    try:
        url = f'https://api.upstox.com/v2/market-quote/ohlc?instrument_key={instrument_key}&interval=1d'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success':
                return data.get('data')
        return None
    except Exception as e:
        print(f"[ERROR] fetch_ohlc_quote: {e}")
        return None


def fetch_historical_candles(instrument_key, interval='5minute', days_back=5):
    """Fetch historical candle data."""
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        url = f'https://api.upstox.com/v2/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}'
        resp = requests.get(url, headers={'Accept': 'application/json'}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data', {}).get('candles'):
                return data['data']['candles']
        return []
    except Exception as e:
        print(f"[ERROR] fetch_historical_candles: {e}")
        return []


def fetch_intraday_candles(instrument_key, interval='1minute'):
    """Fetch intraday candle data."""
    try:
        url = f'https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/{interval}'
        resp = requests.get(url, headers={'Accept': 'application/json'}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data', {}).get('candles'):
                return data['data']['candles']
        return []
    except Exception as e:
        print(f"[ERROR] fetch_intraday_candles: {e}")
        return []


def fetch_option_chain(instrument_key, expiry_date=None):
    """Fetch option chain data."""
    try:
        url = 'https://api.upstox.com/v2/option/chain'
        params = {'instrument_key': instrument_key}
        if expiry_date:
            params['expiry_date'] = expiry_date
        resp = requests.get(url, params=params, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success':
                return data.get('data', [])
        return []
    except Exception as e:
        print(f"[ERROR] fetch_option_chain: {e}")
        return []


def fetch_option_expiries(instrument_key):
    """Get nearest expiry date for option chain."""
    try:
        url = f'https://api.upstox.com/v2/option/contract?instrument_key={instrument_key}'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data'):
                expiries = set()
                for item in data['data']:
                    if item.get('expiry'):
                        expiries.add(item['expiry'])
                sorted_expiries = sorted(expiries)
                # Return nearest expiry
                today = datetime.now().strftime('%Y-%m-%d')
                for exp in sorted_expiries:
                    if exp >= today:
                        return exp
                return sorted_expiries[0] if sorted_expiries else None
        return None
    except Exception as e:
        print(f"[ERROR] fetch_option_expiries: {e}")
        return None


def fetch_india_vix():
    """Fetch India VIX data."""
    try:
        url = 'https://api.upstox.com/v2/market-quote/quotes?instrument_key=NSE_INDEX|India VIX'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'success' and data.get('data'):
                for key, val in data['data'].items():
                    if val and val.get('last_price'):
                        return val['last_price']
        return 0
    except Exception as e:
        print(f"[ERROR] fetch_india_vix: {e}")
        return 0


# ============================================================
# Analysis Engine Functions
# ============================================================

def aggregate_candles(minute_candles, period_minutes):
    """Aggregate 1-minute candles into N-minute candles."""
    if not minute_candles:
        return []
    
    # Candle format: [timestamp, open, high, low, close, volume, oi]
    # Sort by time ascending
    sorted_candles = sorted(minute_candles, key=lambda x: x[0])
    
    aggregated = []
    i = 0
    while i < len(sorted_candles):
        batch = sorted_candles[i:i + period_minutes]
        if not batch:
            break
        ts = batch[0][0]
        o = batch[0][1]
        h = max(c[2] for c in batch)
        l = min(c[3] for c in batch)
        c_val = batch[-1][4]
        v = sum(c[5] for c in batch)
        oi = batch[-1][6] if len(batch[-1]) > 6 else 0
        aggregated.append([ts, o, h, l, c_val, v, oi])
        i += period_minutes
    
    return aggregated


def calculate_vwap(candles):
    """Calculate VWAP from candle data."""
    cum_vol = 0
    cum_tp_vol = 0
    vwap_series = []
    
    for c in candles:
        tp = (c[2] + c[3] + c[4]) / 3  # (high + low + close) / 3
        vol = c[5]
        cum_vol += vol
        cum_tp_vol += tp * vol
        vwap = cum_tp_vol / cum_vol if cum_vol > 0 else tp
        vwap_series.append(vwap)
    
    return vwap_series[-1] if vwap_series else 0, vwap_series


def detect_support_resistance(candles, ltp, num_levels=2, oi_analysis=None):
    """
    Advanced Support/Resistance Detection Engine
    
    Detection based on:
    1. Pivot Points (swing highs/lows)
    2. Volume Behavior - High volume at level = stronger
    3. Price Rejection Strength - Long wicks = stronger rejection
    4. Repeated Testing - More tests = stronger level
    5. Breakout Attempts - Failed breakouts strengthen level
    6. Liquidity Zones - Areas of high trading activity
    7. OI-based levels from option chain (max pain, max OI strikes)
    8. Dynamic Weakening - Recent breaks weaken levels
    """
    if not candles or len(candles) < 5:
        return [], []
    
    # Extract OHLCV data
    opens = [c[1] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    volumes = [c[5] if len(c) > 5 else 0 for c in candles]
    
    # Calculate average volume for comparison
    avg_volume = sum(volumes) / len(volumes) if volumes else 1
    
    # ============================================================
    # 1. PIVOT POINT DETECTION (Swing Highs/Lows)
    # ============================================================
    pivot_highs = []
    pivot_lows = []
    
    # Use different lookback periods for multi-timeframe pivots
    for lookback in [2, 3, 5]:
        if len(candles) < lookback * 2 + 1:
            continue
            
        for i in range(lookback, len(candles) - lookback):
            # Check pivot high
            is_pivot_high = True
            for j in range(1, lookback + 1):
                if highs[i] <= highs[i-j] or highs[i] <= highs[i+j]:
                    is_pivot_high = False
                    break
            
            if is_pivot_high:
                # Calculate rejection strength (upper wick ratio)
                body = abs(closes[i] - opens[i])
                upper_wick = highs[i] - max(opens[i], closes[i])
                wick_ratio = upper_wick / body if body > 0 else 0
                
                # Volume strength
                vol_ratio = volumes[i] / avg_volume if avg_volume > 0 else 1
                
                pivot_highs.append({
                    'price': highs[i],
                    'volume': volumes[i],
                    'vol_ratio': vol_ratio,
                    'wick_ratio': wick_ratio,
                    'idx': i,
                    'lookback': lookback,
                    'recency': len(candles) - i  # How recent (lower = more recent)
                })
            
            # Check pivot low
            is_pivot_low = True
            for j in range(1, lookback + 1):
                if lows[i] >= lows[i-j] or lows[i] >= lows[i+j]:
                    is_pivot_low = False
                    break
            
            if is_pivot_low:
                # Calculate rejection strength (lower wick ratio)
                body = abs(closes[i] - opens[i])
                lower_wick = min(opens[i], closes[i]) - lows[i]
                wick_ratio = lower_wick / body if body > 0 else 0
                
                # Volume strength
                vol_ratio = volumes[i] / avg_volume if avg_volume > 0 else 1
                
                pivot_lows.append({
                    'price': lows[i],
                    'volume': volumes[i],
                    'vol_ratio': vol_ratio,
                    'wick_ratio': wick_ratio,
                    'idx': i,
                    'lookback': lookback,
                    'recency': len(candles) - i
                })
    
    # ============================================================
    # 2. CLUSTER LEVELS & CALCULATE STRENGTH
    # ============================================================
    def cluster_and_score_levels(levels, level_type='resistance'):
        """Cluster nearby levels and calculate comprehensive strength score."""
        if not levels:
            return []
        
        # Sort by price
        sorted_levels = sorted(levels, key=lambda x: x['price'])
        clusters = []
        threshold_pct = 0.002  # 0.2% clustering threshold
        
        current_cluster = [sorted_levels[0]]
        
        for i in range(1, len(sorted_levels)):
            price_diff = abs(sorted_levels[i]['price'] - current_cluster[-1]['price'])
            if price_diff / current_cluster[-1]['price'] < threshold_pct:
                current_cluster.append(sorted_levels[i])
            else:
                # Process current cluster
                clusters.append(process_cluster(current_cluster, level_type))
                current_cluster = [sorted_levels[i]]
        
        # Process last cluster
        if current_cluster:
            clusters.append(process_cluster(current_cluster, level_type))
        
        return clusters
    
    def process_cluster(cluster, level_type):
        """Process a cluster of levels and calculate strength score."""
        avg_price = sum(l['price'] for l in cluster) / len(cluster)
        total_vol = sum(l['volume'] for l in cluster)
        
        # ============================================================
        # STRENGTH SCORING SYSTEM (0-100)
        # ============================================================
        score = 0
        factors = []
        
        # Factor 1: Number of tests (max 25 points)
        tests = len(cluster)
        test_score = min(25, tests * 8)
        score += test_score
        if tests >= 3:
            factors.append(f'{tests}x tested')
        
        # Factor 2: Volume at level (max 20 points)
        avg_vol_ratio = sum(l['vol_ratio'] for l in cluster) / len(cluster)
        if avg_vol_ratio > 2:
            score += 20
            factors.append('High volume')
        elif avg_vol_ratio > 1.5:
            score += 15
            factors.append('Above avg volume')
        elif avg_vol_ratio > 1:
            score += 10
        
        # Factor 3: Rejection wicks (max 20 points)
        avg_wick_ratio = sum(l['wick_ratio'] for l in cluster) / len(cluster)
        if avg_wick_ratio > 1.5:
            score += 20
            factors.append('Strong rejection')
        elif avg_wick_ratio > 1:
            score += 15
            factors.append('Good rejection')
        elif avg_wick_ratio > 0.5:
            score += 10
        
        # Factor 4: Multi-timeframe confluence (max 15 points)
        lookbacks = set(l['lookback'] for l in cluster)
        if len(lookbacks) >= 3:
            score += 15
            factors.append('Multi-TF')
        elif len(lookbacks) >= 2:
            score += 10
        
        # Factor 5: Recency - recent levels more relevant (max 10 points)
        avg_recency = sum(l['recency'] for l in cluster) / len(cluster)
        if avg_recency < 10:
            score += 10
            factors.append('Recent')
        elif avg_recency < 20:
            score += 7
        elif avg_recency < 40:
            score += 4
        
        # Factor 6: Round number bonus (max 10 points)
        if avg_price % 100 < 5 or avg_price % 100 > 95:
            score += 10
            factors.append('Round number')
        elif avg_price % 50 < 3 or avg_price % 50 > 47:
            score += 5
        
        # Determine strength label
        if score >= 70:
            strength = 'strong'
        elif score >= 45:
            strength = 'moderate'
        else:
            strength = 'weak'
        
        return {
            'price': round(avg_price, 2),
            'volume': total_vol,
            'tests': tests,
            'score': score,
            'strength': strength,
            'factors': factors,
            'vol_ratio': avg_vol_ratio,
            'wick_ratio': avg_wick_ratio
        }
    
    # ============================================================
    # 3. DETECT BREAKOUT ATTEMPTS & DYNAMIC WEAKENING
    # ============================================================
    def check_breakout_weakness(levels, candles, is_resistance=True):
        """Check if levels have been recently broken or tested weakly."""
        updated_levels = []
        
        for level in levels:
            price = level['price']
            weakness_score = 0
            weakness_reasons = []
            
            # Check recent candles for breakout attempts
            for i in range(-min(10, len(candles)), 0):
                idx = len(candles) + i
                if idx < 0:
                    continue
                
                h, l, c, o = highs[idx], lows[idx], closes[idx], opens[idx]
                
                if is_resistance:
                    # Check if price went above resistance
                    if h > price:
                        # Did it close above? (breakout) or below? (rejection)
                        if c > price:
                            weakness_score += 15  # Broken
                            weakness_reasons.append('Recently broken')
                        elif h > price and c < price:
                            # Wick above but closed below - still holding but tested
                            weakness_score += 5
                else:
                    # Support
                    if l < price:
                        if c < price:
                            weakness_score += 15  # Broken
                            weakness_reasons.append('Recently broken')
                        elif l < price and c > price:
                            weakness_score += 5
            
            # Reduce score based on weakness
            adjusted_score = max(0, level['score'] - weakness_score)
            
            # Update strength
            if weakness_score >= 15:
                new_strength = 'weak'
                weakness_reasons.append('Level weakening')
            elif adjusted_score >= 70:
                new_strength = 'strong'
            elif adjusted_score >= 45:
                new_strength = 'moderate'
            else:
                new_strength = 'weak'
            
            updated_level = level.copy()
            updated_level['score'] = adjusted_score
            updated_level['strength'] = new_strength
            updated_level['weakness_reasons'] = weakness_reasons
            updated_levels.append(updated_level)
        
        return updated_levels
    
    # ============================================================
    # 4. PROCESS RESISTANCE LEVELS
    # ============================================================
    resistance_clusters = cluster_and_score_levels(pivot_highs, 'resistance')
    resistance_clusters = check_breakout_weakness(resistance_clusters, candles, is_resistance=True)
    
    # Filter: only levels above LTP
    resistances = sorted(
        [r for r in resistance_clusters if r['price'] > ltp],
        key=lambda x: (-x['score'], x['price'])  # Sort by score desc, then price asc
    )
    
    # ============================================================
    # 5. PROCESS SUPPORT LEVELS
    # ============================================================
    support_clusters = cluster_and_score_levels(pivot_lows, 'support')
    support_clusters = check_breakout_weakness(support_clusters, candles, is_resistance=False)
    
    # Filter: only levels below LTP
    supports = sorted(
        [s for s in support_clusters if s['price'] < ltp],
        key=lambda x: (-x['score'], -x['price'])  # Sort by score desc, then price desc
    )
    
    # ============================================================
    # 6. ADD OI-BASED LEVELS (from Option Chain)
    # ============================================================
    if oi_analysis:
        max_ce_strike = oi_analysis.get('max_ce_oi_strike', 0)
        max_pe_strike = oi_analysis.get('max_pe_oi_strike', 0)
        
        # Max CE OI = Resistance (writers don't want price above)
        if max_ce_strike and max_ce_strike > ltp:
            # Check if already exists
            exists = any(abs(r['price'] - max_ce_strike) / max_ce_strike < 0.002 for r in resistances)
            if not exists:
                resistances.append({
                    'price': max_ce_strike,
                    'volume': 0,
                    'tests': 0,
                    'score': 60,
                    'strength': 'moderate',
                    'factors': ['Max CE OI', 'OI Resistance'],
                    'is_oi_level': True
                })
        
        # Max PE OI = Support (writers don't want price below)
        if max_pe_strike and max_pe_strike < ltp:
            exists = any(abs(s['price'] - max_pe_strike) / max_pe_strike < 0.002 for s in supports)
            if not exists:
                supports.append({
                    'price': max_pe_strike,
                    'volume': 0,
                    'tests': 0,
                    'score': 60,
                    'strength': 'moderate',
                    'factors': ['Max PE OI', 'OI Support'],
                    'is_oi_level': True
                })
    
    # ============================================================
    # 7. ENSURE MINIMUM LEVELS
    # ============================================================
    if len(resistances) < 2:
        last_high = max(highs[-20:]) if len(highs) >= 20 else max(highs)
        gap = abs(ltp * 0.003)
        while len(resistances) < 2:
            next_r = (resistances[-1]['price'] + gap) if resistances else (ltp + gap)
            resistances.append({'price': round(next_r, 2), 'volume': 0, 'tests': 0, 'strength': 'weak'})
            gap += abs(ltp * 0.002)
    
    if len(supports) < 2:
        gap = abs(ltp * 0.003)
        while len(supports) < 2:
            next_s = (supports[-1]['price'] - gap) if supports else (ltp - gap)
            supports.append({'price': round(next_s, 2), 'volume': 0, 'tests': 0, 'strength': 'weak'})
            gap += abs(ltp * 0.002)
    
    return supports[:num_levels], resistances[:num_levels]


def detect_smc_patterns(candles, ltp):
    """Detect Smart Money Concepts: BOS, CHOCH, Liquidity Sweeps, Order Blocks."""
    zones = []
    if not candles or len(candles) < 10:
        return zones
    
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    opens = [c[1] for c in candles]
    
    # Track swing highs and lows for BOS/CHOCH
    swing_highs = []
    swing_lows = []
    
    for i in range(2, len(candles) - 2):
        if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
            swing_highs.append({'price': highs[i], 'idx': i})
        if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
            swing_lows.append({'price': lows[i], 'idx': i})
    
    # Detect Break of Structure (BOS)
    for i in range(1, len(swing_highs)):
        if swing_highs[i]['price'] > swing_highs[i-1]['price']:
            zones.append({
                'type': 'BOS_BULLISH',
                'price': swing_highs[i]['price'],
                'label': f"BOS ↑ {swing_highs[i]['price']:.0f}",
                'color': '#00ff88'
            })
    
    for i in range(1, len(swing_lows)):
        if swing_lows[i]['price'] < swing_lows[i-1]['price']:
            zones.append({
                'type': 'BOS_BEARISH',
                'price': swing_lows[i]['price'],
                'label': f"BOS ↓ {swing_lows[i]['price']:.0f}",
                'color': '#ff4444'
            })
    
    # Detect Change of Character (CHOCH)
    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        last_sh = swing_highs[-1]
        prev_sh = swing_highs[-2]
        last_sl = swing_lows[-1]
        prev_sl = swing_lows[-2]
        
        # Bullish CHOCH: lower lows then higher high
        if prev_sl['price'] > last_sl['price'] and ltp > prev_sh['price']:
            zones.append({
                'type': 'CHOCH_BULLISH',
                'price': prev_sh['price'],
                'label': f"CHOCH ↑ {prev_sh['price']:.0f}",
                'color': '#00ffcc'
            })
        
        # Bearish CHOCH: higher highs then lower low
        if prev_sh['price'] < last_sh['price'] and ltp < prev_sl['price']:
            zones.append({
                'type': 'CHOCH_BEARISH',
                'price': prev_sl['price'],
                'label': f"CHOCH ↓ {prev_sl['price']:.0f}",
                'color': '#ff6666'
            })
    
    # Detect Order Blocks
    for i in range(1, len(candles) - 1):
        # Bullish OB: last bearish candle before strong bullish move
        if closes[i] < opens[i] and i + 1 < len(closes):
            if closes[i+1] > opens[i+1] and (closes[i+1] - opens[i+1]) > abs(closes[i] - opens[i]) * 1.5:
                zones.append({
                    'type': 'ORDER_BLOCK_BULL',
                    'price': lows[i],
                    'price_high': highs[i],
                    'label': f"OB+ {lows[i]:.0f}",
                    'color': '#0088ff44'
                })
        
        # Bearish OB: last bullish candle before strong bearish move
        if closes[i] > opens[i] and i + 1 < len(closes):
            if closes[i+1] < opens[i+1] and abs(closes[i+1] - opens[i+1]) > (closes[i] - opens[i]) * 1.5:
                zones.append({
                    'type': 'ORDER_BLOCK_BEAR',
                    'price': highs[i],
                    'price_low': lows[i],
                    'label': f"OB- {highs[i]:.0f}",
                    'color': '#ff444444'
                })
    
    # Detect Liquidity Sweeps
    if len(swing_lows) >= 2:
        recent_low = swing_lows[-1]
        prev_low = swing_lows[-2]
        if recent_low['price'] < prev_low['price'] and ltp > prev_low['price']:
            zones.append({
                'type': 'LIQUIDITY_SWEEP_BULL',
                'price': recent_low['price'],
                'label': f"Liq Sweep ↑ {recent_low['price']:.0f}",
                'color': '#ffaa00'
            })
    
    if len(swing_highs) >= 2:
        recent_high = swing_highs[-1]
        prev_high = swing_highs[-2]
        if recent_high['price'] > prev_high['price'] and ltp < prev_high['price']:
            zones.append({
                'type': 'LIQUIDITY_SWEEP_BEAR',
                'price': recent_high['price'],
                'label': f"Liq Sweep ↓ {recent_high['price']:.0f}",
                'color': '#ff8800'
            })
    
    # Keep only recent zones near current price
    relevant_zones = []
    for z in zones:
        if abs(z['price'] - ltp) / ltp < 0.03:  # Within 3% of LTP
            relevant_zones.append(z)
    
    return relevant_zones[-10:]  # Last 10 zones


def analyze_option_chain_data(oc_data, ltp, strike_gap, num_strikes=7):
    """Analyze option chain for ATM ± num_strikes."""
    if not oc_data:
        return {
            'chain': [],
            'analysis': {
                'strongest_support': 0,
                'strongest_resistance': 0,
                'pcr': 0,
                'total_ce_oi': 0,
                'total_pe_oi': 0,
                'max_ce_oi_strike': 0,
                'max_pe_oi_strike': 0,
                'signals': []
            }
        }
    
    # Find ATM strike
    atm_strike = round(ltp / strike_gap) * strike_gap
    min_strike = atm_strike - (num_strikes * strike_gap)
    max_strike = atm_strike + (num_strikes * strike_gap)
    
    filtered_chain = []
    total_ce_oi = 0
    total_pe_oi = 0
    max_ce_oi = 0
    max_pe_oi = 0
    max_ce_oi_strike = 0
    max_pe_oi_strike = 0
    ce_oi_change_total = 0
    pe_oi_change_total = 0
    
    for item in oc_data:
        strike = item.get('strike_price', 0)
        if min_strike <= strike <= max_strike:
            call_data = item.get('call_options', {})
            put_data = item.get('put_options', {})
            
            ce_market = call_data.get('market_data', {})
            pe_market = put_data.get('market_data', {})
            ce_greeks = call_data.get('option_greeks', {})
            pe_greeks = put_data.get('option_greeks', {})
            
            ce_oi = ce_market.get('oi', 0)
            pe_oi = pe_market.get('oi', 0)
            ce_prev_oi = ce_market.get('prev_oi', 0) if ce_market.get('prev_oi') else 0
            pe_prev_oi = pe_market.get('prev_oi', 0) if pe_market.get('prev_oi') else 0
            ce_oi_chg = ce_oi - ce_prev_oi
            pe_oi_chg = pe_oi - pe_prev_oi
            
            total_ce_oi += ce_oi
            total_pe_oi += pe_oi
            ce_oi_change_total += ce_oi_chg
            pe_oi_change_total += pe_oi_chg
            
            if ce_oi > max_ce_oi:
                max_ce_oi = ce_oi
                max_ce_oi_strike = strike
            if pe_oi > max_pe_oi:
                max_pe_oi = pe_oi
                max_pe_oi_strike = strike
            
            filtered_chain.append({
                'strike': strike,
                'is_atm': strike == atm_strike,
                'ce_ltp': ce_market.get('ltp', 0),
                'ce_oi': ce_oi,
                'ce_oi_chg': ce_oi_chg,
                'ce_vol': ce_market.get('volume', 0),
                'ce_iv': ce_greeks.get('iv', 0),
                'ce_delta': ce_greeks.get('delta', 0),
                'pe_ltp': pe_market.get('ltp', 0),
                'pe_oi': pe_oi,
                'pe_oi_chg': pe_oi_chg,
                'pe_vol': pe_market.get('volume', 0),
                'pe_iv': pe_greeks.get('iv', 0),
                'pe_delta': pe_greeks.get('delta', 0),
            })
    
    # Sort by strike
    filtered_chain.sort(key=lambda x: x['strike'])
    
    pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 1
    
    # Generate OI signals
    signals = []
    
    if ce_oi_change_total > 0 and pe_oi_change_total > 0:
        if pe_oi_change_total > ce_oi_change_total * 1.5:
            signals.append({'signal': 'Strong PE Writing', 'bias': 'bullish', 'icon': '🟢'})
        elif ce_oi_change_total > pe_oi_change_total * 1.5:
            signals.append({'signal': 'Strong CE Writing', 'bias': 'bearish', 'icon': '🔴'})
    
    if ce_oi_change_total < 0:
        signals.append({'signal': 'CE Unwinding', 'bias': 'bullish', 'icon': '🟡'})
    if pe_oi_change_total < 0:
        signals.append({'signal': 'PE Unwinding', 'bias': 'bearish', 'icon': '🟡'})
    
    if pcr > 1.3:
        signals.append({'signal': f'PCR Bullish ({pcr:.2f})', 'bias': 'bullish', 'icon': '🟢'})
    elif pcr < 0.7:
        signals.append({'signal': f'PCR Bearish ({pcr:.2f})', 'bias': 'bearish', 'icon': '🔴'})
    
    return {
        'chain': filtered_chain,
        'analysis': {
            'strongest_support': max_pe_oi_strike,
            'strongest_resistance': max_ce_oi_strike,
            'pcr': round(pcr, 2),
            'total_ce_oi': total_ce_oi,
            'total_pe_oi': total_pe_oi,
            'max_ce_oi_strike': max_ce_oi_strike,
            'max_pe_oi_strike': max_pe_oi_strike,
            'ce_oi_change': ce_oi_change_total,
            'pe_oi_change': pe_oi_change_total,
            'signals': signals
        }
    }


def analyze_vwap_price_action(candles, vwap, ltp):
    """Analyze VWAP relationship and price action patterns."""
    if not candles or len(candles) < 3:
        return {'status': 'neutral', 'signals': []}
    
    signals = []
    closes = [c[4] for c in candles]
    opens = [c[1] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    
    # VWAP analysis
    if ltp > vwap:
        if closes[-2] < vwap and closes[-1] > vwap:
            signals.append({'signal': 'VWAP Reclaim', 'bias': 'bullish', 'strength': 'strong'})
        else:
            signals.append({'signal': 'Above VWAP', 'bias': 'bullish', 'strength': 'moderate'})
    else:
        if closes[-2] > vwap and closes[-1] < vwap:
            signals.append({'signal': 'VWAP Rejection', 'bias': 'bearish', 'strength': 'strong'})
        else:
            signals.append({'signal': 'Below VWAP', 'bias': 'bearish', 'strength': 'moderate'})
    
    # Momentum analysis
    recent_bodies = [abs(closes[i] - opens[i]) for i in range(-3, 0)]
    avg_body = sum(recent_bodies) / len(recent_bodies) if recent_bodies else 0
    
    last_body = abs(closes[-1] - opens[-1])
    
    if last_body > avg_body * 1.5:
        if closes[-1] > opens[-1]:
            signals.append({'signal': 'Strong Bullish Candle', 'bias': 'bullish', 'strength': 'strong'})
        else:
            signals.append({'signal': 'Strong Bearish Candle', 'bias': 'bearish', 'strength': 'strong'})
    
    # Exhaustion detection
    if len(closes) >= 5:
        last_5_trend = closes[-1] - closes[-5]
        last_2_trend = closes[-1] - closes[-2]
        
        if last_5_trend > 0 and last_2_trend < 0:
            signals.append({'signal': 'Bullish Exhaustion', 'bias': 'bearish', 'strength': 'moderate'})
        elif last_5_trend < 0 and last_2_trend > 0:
            signals.append({'signal': 'Bearish Exhaustion', 'bias': 'bullish', 'strength': 'moderate'})
    
    # Continuation pattern
    if len(closes) >= 3:
        if closes[-1] > closes[-2] > closes[-3]:
            signals.append({'signal': 'Bullish Continuation', 'bias': 'bullish', 'strength': 'moderate'})
        elif closes[-1] < closes[-2] < closes[-3]:
            signals.append({'signal': 'Bearish Continuation', 'bias': 'bearish', 'strength': 'moderate'})
    
    # Determine overall status
    bullish_count = sum(1 for s in signals if s['bias'] == 'bullish')
    bearish_count = sum(1 for s in signals if s['bias'] == 'bearish')
    
    if bullish_count > bearish_count:
        status = 'bullish'
    elif bearish_count > bullish_count:
        status = 'bearish'
    else:
        status = 'neutral'
    
    return {'status': status, 'signals': signals}


def analyze_deep_momentum(candles, oi_analysis, smc_zones, vwap, supports, resistances):
    """
    Deep Momentum Analysis Engine
    Combines: Option Chain OI, SMC, Price Action, Volume, S/R for momentum signals
    Returns chart alerts with exact candle time and price for display on chart
    """
    chart_alerts = []
    
    if not candles or len(candles) < 10:
        return chart_alerts
    
    # Extract OHLCV data
    times = [c[0] for c in candles]
    opens = [c[1] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    volumes = [c[5] if len(c) > 5 else 0 for c in candles]
    
    # ============================================================
    # 1. PRICE ACTION MOMENTUM METRICS
    # ============================================================
    
    # Calculate momentum indicators
    def calc_roc(data, period=5):
        """Rate of Change"""
        if len(data) < period + 1:
            return [0] * len(data)
        roc = [0] * period
        for i in range(period, len(data)):
            if data[i - period] != 0:
                roc.append((data[i] - data[i - period]) / data[i - period] * 100)
            else:
                roc.append(0)
        return roc
    
    def calc_ema(data, period):
        """Exponential Moving Average"""
        if len(data) < period:
            return data[:]
        ema = [sum(data[:period]) / period]
        multiplier = 2 / (period + 1)
        for i in range(period, len(data)):
            ema.append((data[i] - ema[-1]) * multiplier + ema[-1])
        return [0] * (period - 1) + ema
    
    def calc_atr(highs, lows, closes, period=14):
        """Average True Range"""
        if len(closes) < 2:
            return [0]
        tr = [highs[0] - lows[0]]
        for i in range(1, len(closes)):
            tr.append(max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            ))
        return calc_ema(tr, period)
    
    # Calculate metrics
    roc_5 = calc_roc(closes, 5)
    roc_10 = calc_roc(closes, 10)
    ema_5 = calc_ema(closes, 5)
    ema_10 = calc_ema(closes, 10)
    ema_20 = calc_ema(closes, 20)
    atr = calc_atr(highs, lows, closes, 14)
    
    # Volume analysis
    avg_vol_10 = sum(volumes[-10:]) / 10 if len(volumes) >= 10 else (sum(volumes) / len(volumes) if volumes else 1)
    
    # Body analysis
    bodies = [abs(closes[i] - opens[i]) for i in range(len(closes))]
    avg_body = sum(bodies[-10:]) / 10 if len(bodies) >= 10 else 1
    
    # ============================================================
    # 2. OI SHIFTING ANALYSIS
    # ============================================================
    
    oi_bullish_score = 0
    oi_bearish_score = 0
    oi_signals = []
    
    if oi_analysis:
        pcr = oi_analysis.get('pcr', 1)
        ce_oi_chg = oi_analysis.get('ce_oi_change', 0)
        pe_oi_chg = oi_analysis.get('pe_oi_change', 0)
        max_ce_strike = oi_analysis.get('max_ce_oi_strike', 0)
        max_pe_strike = oi_analysis.get('max_pe_oi_strike', 0)
        
        # PCR analysis
        if pcr > 1.5:
            oi_bullish_score += 3
            oi_signals.append('Extreme PE buildup')
        elif pcr > 1.2:
            oi_bullish_score += 2
            oi_signals.append('Strong PE writing')
        elif pcr < 0.6:
            oi_bearish_score += 3
            oi_signals.append('Extreme CE buildup')
        elif pcr < 0.8:
            oi_bearish_score += 2
            oi_signals.append('Strong CE writing')
        
        # OI change analysis
        if pe_oi_chg > 0 and ce_oi_chg < 0:
            oi_bullish_score += 3
            oi_signals.append('PE adding + CE unwinding')
        elif ce_oi_chg > 0 and pe_oi_chg < 0:
            oi_bearish_score += 3
            oi_signals.append('CE adding + PE unwinding')
        elif pe_oi_chg > ce_oi_chg * 2:
            oi_bullish_score += 2
            oi_signals.append('Heavy PE writing')
        elif ce_oi_chg > pe_oi_chg * 2:
            oi_bearish_score += 2
            oi_signals.append('Heavy CE writing')
        
        # Short covering / Long unwinding detection
        if ce_oi_chg < 0 and closes[-1] > closes[-2]:
            oi_bullish_score += 2
            oi_signals.append('Short covering')
        if pe_oi_chg < 0 and closes[-1] < closes[-2]:
            oi_bearish_score += 2
            oi_signals.append('Long unwinding')
    
    # ============================================================
    # 3. SMC STRUCTURE ANALYSIS
    # ============================================================
    
    smc_bullish_score = 0
    smc_bearish_score = 0
    smc_signals = []
    
    if smc_zones:
        for zone in smc_zones:
            ztype = zone.get('type', '')
            zprice = zone.get('price', 0)
            
            if 'BOS_BULLISH' in ztype or 'CHOCH_BULLISH' in ztype:
                smc_bullish_score += 2
                smc_signals.append(f"Bullish structure ({ztype.split('_')[0]})")
            elif 'BOS_BEARISH' in ztype or 'CHOCH_BEARISH' in ztype:
                smc_bearish_score += 2
                smc_signals.append(f"Bearish structure ({ztype.split('_')[0]})")
            
            if 'ORDER_BLOCK_BULL' in ztype and closes[-1] >= zprice:
                smc_bullish_score += 2
                smc_signals.append('Trading from bullish OB')
            elif 'ORDER_BLOCK_BEAR' in ztype and closes[-1] <= zprice:
                smc_bearish_score += 2
                smc_signals.append('Trading from bearish OB')
            
            if 'LIQUIDITY_SWEEP_BULL' in ztype:
                smc_bullish_score += 3
                smc_signals.append('Bullish liquidity sweep')
            elif 'LIQUIDITY_SWEEP_BEAR' in ztype:
                smc_bearish_score += 3
                smc_signals.append('Bearish liquidity sweep')
    
    # ============================================================
    # 4. SUPPORT/RESISTANCE PROXIMITY ANALYSIS
    # ============================================================
    
    sr_score = 0
    sr_signals = []
    current_price = closes[-1]
    
    # Check proximity to support
    for sup in supports[:2]:
        dist_pct = (current_price - sup['price']) / current_price * 100
        if 0 < dist_pct < 0.3:  # Very close to support
            if sup['strength'] == 'strong':
                sr_score += 2
                sr_signals.append(f"At strong support {sup['price']:.0f}")
            elif sup['strength'] == 'weak':
                sr_score -= 1
                sr_signals.append(f"Weak support may break")
    
    # Check proximity to resistance  
    for res in resistances[:2]:
        dist_pct = (res['price'] - current_price) / current_price * 100
        if 0 < dist_pct < 0.3:  # Very close to resistance
            if res['strength'] == 'strong':
                sr_score -= 2
                sr_signals.append(f"At strong resistance {res['price']:.0f}")
            elif res['strength'] == 'weak':
                sr_score += 1
                sr_signals.append(f"Weak resistance may break")
    
    # ============================================================
    # 5. DEEP MOMENTUM STATE DETECTION
    # ============================================================
    
    # Analyze last 5-10 candles for momentum state
    for i in range(-min(10, len(candles)), 0):
        if abs(i) >= len(candles):
            continue
            
        idx = len(candles) + i
        if idx < 5:
            continue
        
        candle_time = times[idx]
        candle_low = lows[idx]
        candle_high = highs[idx]
        candle_close = closes[idx]
        candle_open = opens[idx]
        candle_vol = volumes[idx] if idx < len(volumes) else 0
        
        body = abs(candle_close - candle_open)
        is_bullish = candle_close > candle_open
        
        # Momentum scores
        momentum_score = 0
        direction = 'neutral'
        reasons = []
        
        # Price action momentum
        if idx >= 3:
            # 3-candle trend
            trend_3 = closes[idx] - closes[idx-3]
            trend_1 = closes[idx] - closes[idx-1]
            
            # ROC momentum
            roc_val = roc_5[idx] if idx < len(roc_5) else 0
            
            # EMA alignment
            ema5 = ema_5[idx] if idx < len(ema_5) else closes[idx]
            ema10 = ema_10[idx] if idx < len(ema_10) else closes[idx]
            ema20 = ema_20[idx] if idx < len(ema_20) else closes[idx]
            
            ema_bullish = ema5 > ema10 > ema20
            ema_bearish = ema5 < ema10 < ema20
            
            # Volume confirmation
            vol_spike = candle_vol > avg_vol_10 * 1.5
            
            # Body size confirmation
            strong_body = body > avg_body * 1.3
            
            # ============================================================
            # MOMENTUM CONTINUATION
            # ============================================================
            if trend_3 > 0 and trend_1 > 0 and is_bullish:
                momentum_score += 2
                direction = 'bullish'
                reasons.append('Bullish trend')
                
                if ema_bullish:
                    momentum_score += 2
                    reasons.append('EMA aligned ↑')
                if vol_spike and is_bullish:
                    momentum_score += 2
                    reasons.append('Volume spike')
                if strong_body:
                    momentum_score += 1
                    reasons.append('Strong body')
                if roc_val > 0.3:
                    momentum_score += 1
                    reasons.append(f'ROC +{roc_val:.1f}%')
                
                # Add OI confirmation
                momentum_score += oi_bullish_score
                reasons.extend(oi_signals[:2])
                
                # Add SMC confirmation
                momentum_score += smc_bullish_score
                reasons.extend(smc_signals[:2])
                
                if momentum_score >= 6:
                    chart_alerts.append({
                        'time': candle_time,
                        'price': candle_low,
                        'type': 'MOMENTUM_CONTINUATION',
                        'direction': 'bullish',
                        'label': '🚀 MOMENTUM CONTINUATION',
                        'short_label': '🚀 MOM↑',
                        'score': momentum_score,
                        'reasons': reasons[:5],
                        'color': '#00ff88'
                    })
            
            elif trend_3 < 0 and trend_1 < 0 and not is_bullish:
                momentum_score += 2
                direction = 'bearish'
                reasons.append('Bearish trend')
                
                if ema_bearish:
                    momentum_score += 2
                    reasons.append('EMA aligned ↓')
                if vol_spike and not is_bullish:
                    momentum_score += 2
                    reasons.append('Volume spike')
                if strong_body:
                    momentum_score += 1
                    reasons.append('Strong body')
                if roc_val < -0.3:
                    momentum_score += 1
                    reasons.append(f'ROC {roc_val:.1f}%')
                
                momentum_score += oi_bearish_score
                reasons.extend(oi_signals[:2])
                momentum_score += smc_bearish_score
                reasons.extend(smc_signals[:2])
                
                if momentum_score >= 6:
                    chart_alerts.append({
                        'time': candle_time,
                        'price': candle_low,
                        'type': 'MOMENTUM_CONTINUATION',
                        'direction': 'bearish',
                        'label': '📉 MOMENTUM CONTINUATION',
                        'short_label': '📉 MOM↓',
                        'score': momentum_score,
                        'reasons': reasons[:5],
                        'color': '#ff4444'
                    })
            
            # ============================================================
            # MOMENTUM SLOWING
            # ============================================================
            # Bullish trend but candle weakening
            if trend_3 > 0 and body < avg_body * 0.5:
                reasons = ['Shrinking candle body', 'Trend losing steam']
                
                if candle_vol < avg_vol_10 * 0.7:
                    momentum_score += 2
                    reasons.append('Volume drying up')
                
                # Check wicks for indecision
                upper_wick = candle_high - max(candle_open, candle_close)
                lower_wick = min(candle_open, candle_close) - candle_low
                if upper_wick > body and lower_wick > body:
                    momentum_score += 2
                    reasons.append('Doji/indecision')
                
                if oi_bearish_score > oi_bullish_score:
                    momentum_score += 2
                    reasons.append('OI shifting bearish')
                    reasons.extend(oi_signals[:1])
                
                if momentum_score >= 4:
                    chart_alerts.append({
                        'time': candle_time,
                        'price': candle_low,
                        'type': 'MOMENTUM_SLOWING',
                        'direction': 'bullish_slowing',
                        'label': '⚠️ MOMENTUM SLOWING',
                        'short_label': '⚠️ SLOW',
                        'score': momentum_score,
                        'reasons': reasons[:5],
                        'color': '#ffaa00'
                    })
            
            # Bearish trend but candle weakening
            elif trend_3 < 0 and body < avg_body * 0.5:
                reasons = ['Shrinking candle body', 'Selling pressure easing']
                
                if candle_vol < avg_vol_10 * 0.7:
                    momentum_score += 2
                    reasons.append('Volume drying up')
                
                upper_wick = candle_high - max(candle_open, candle_close)
                lower_wick = min(candle_open, candle_close) - candle_low
                if upper_wick > body and lower_wick > body:
                    momentum_score += 2
                    reasons.append('Doji/indecision')
                
                if oi_bullish_score > oi_bearish_score:
                    momentum_score += 2
                    reasons.append('OI shifting bullish')
                    reasons.extend(oi_signals[:1])
                
                if momentum_score >= 4:
                    chart_alerts.append({
                        'time': candle_time,
                        'price': candle_low,
                        'type': 'MOMENTUM_SLOWING',
                        'direction': 'bearish_slowing',
                        'label': '⚠️ MOMENTUM SLOWING',
                        'short_label': '⚠️ SLOW',
                        'score': momentum_score,
                        'reasons': reasons[:5],
                        'color': '#ffaa00'
                    })
            
            # ============================================================
            # MOMENTUM EXHAUSTION
            # ============================================================
            if idx >= 5:
                trend_5 = closes[idx] - closes[idx-5]
                
                # Bullish exhaustion: strong up move but reversal candle
                if trend_5 > 0 and not is_bullish and body > avg_body:
                    momentum_score = 3
                    reasons = ['Extended bullish move', 'Bearish reversal candle']
                    
                    # Long upper wick
                    upper_wick = candle_high - max(candle_open, candle_close)
                    if upper_wick > body:
                        momentum_score += 2
                        reasons.append('Long upper wick rejection')
                    
                    # Near resistance
                    if sr_score < 0:
                        momentum_score += 2
                        reasons.extend(sr_signals[:1])
                    
                    # OI suggesting reversal
                    if oi_bearish_score > oi_bullish_score:
                        momentum_score += oi_bearish_score
                        reasons.append('OI bearish shift')
                    
                    # SMC bearish structure
                    if smc_bearish_score > smc_bullish_score:
                        momentum_score += 2
                        reasons.extend(smc_signals[:1])
                    
                    if vol_spike:
                        momentum_score += 2
                        reasons.append('Climax volume')
                    
                    if momentum_score >= 6:
                        chart_alerts.append({
                            'time': candle_time,
                            'price': candle_low,
                            'type': 'MOMENTUM_EXHAUSTION',
                            'direction': 'bullish_exhaustion',
                            'label': '🔥 MOMENTUM EXHAUSTION',
                            'short_label': '🔥 EXHAUST',
                            'score': momentum_score,
                            'reasons': reasons[:5],
                            'color': '#ff6600'
                        })
                
                # Bearish exhaustion: strong down move but reversal candle
                elif trend_5 < 0 and is_bullish and body > avg_body:
                    momentum_score = 3
                    reasons = ['Extended bearish move', 'Bullish reversal candle']
                    
                    # Long lower wick
                    lower_wick = min(candle_open, candle_close) - candle_low
                    if lower_wick > body:
                        momentum_score += 2
                        reasons.append('Long lower wick rejection')
                    
                    # Near support
                    if sr_score > 0:
                        momentum_score += 2
                        reasons.extend(sr_signals[:1])
                    
                    # OI suggesting reversal
                    if oi_bullish_score > oi_bearish_score:
                        momentum_score += oi_bullish_score
                        reasons.append('OI bullish shift')
                    
                    # SMC bullish structure
                    if smc_bullish_score > smc_bearish_score:
                        momentum_score += 2
                        reasons.extend(smc_signals[:1])
                    
                    if vol_spike:
                        momentum_score += 2
                        reasons.append('Climax volume')
                    
                    if momentum_score >= 6:
                        chart_alerts.append({
                            'time': candle_time,
                            'price': candle_low,
                            'type': 'MOMENTUM_EXHAUSTION',
                            'direction': 'bearish_exhaustion',
                            'label': '🔥 MOMENTUM EXHAUSTION',
                            'short_label': '🔥 EXHAUST',
                            'score': momentum_score,
                            'reasons': reasons[:5],
                            'color': '#ff6600'
                        })
            
            # ============================================================
            # MOMENTUM REVERSAL
            # ============================================================
            if idx >= 5:
                trend_5 = closes[idx] - closes[idx-5]
                trend_2 = closes[idx] - closes[idx-2]
                
                # Bullish reversal: was bearish, now bullish with confirmation
                if trend_5 < 0 and trend_2 > 0 and is_bullish and body > avg_body * 1.2:
                    momentum_score = 3
                    reasons = ['Bearish trend reversing', 'Strong bullish candle']
                    
                    if vol_spike:
                        momentum_score += 3
                        reasons.append('Volume breakout')
                    
                    if closes[idx] > vwap:
                        momentum_score += 2
                        reasons.append('Reclaimed VWAP')
                    
                    if oi_bullish_score > oi_bearish_score + 2:
                        momentum_score += oi_bullish_score
                        reasons.extend(oi_signals[:2])
                    
                    if smc_bullish_score > 0:
                        momentum_score += smc_bullish_score
                        reasons.extend(smc_signals[:1])
                    
                    # Break above recent swing high
                    recent_high = max(highs[idx-5:idx])
                    if candle_close > recent_high:
                        momentum_score += 3
                        reasons.append('Break of structure')
                    
                    if momentum_score >= 8:
                        chart_alerts.append({
                            'time': candle_time,
                            'price': candle_low,
                            'type': 'MOMENTUM_REVERSAL',
                            'direction': 'bullish_reversal',
                            'label': '🔄 MOMENTUM REVERSAL ↑',
                            'short_label': '🔄 REV↑',
                            'score': momentum_score,
                            'reasons': reasons[:5],
                            'color': '#00ffcc'
                        })
                
                # Bearish reversal: was bullish, now bearish with confirmation
                elif trend_5 > 0 and trend_2 < 0 and not is_bullish and body > avg_body * 1.2:
                    momentum_score = 3
                    reasons = ['Bullish trend reversing', 'Strong bearish candle']
                    
                    if vol_spike:
                        momentum_score += 3
                        reasons.append('Volume breakout')
                    
                    if closes[idx] < vwap:
                        momentum_score += 2
                        reasons.append('Lost VWAP')
                    
                    if oi_bearish_score > oi_bullish_score + 2:
                        momentum_score += oi_bearish_score
                        reasons.extend(oi_signals[:2])
                    
                    if smc_bearish_score > 0:
                        momentum_score += smc_bearish_score
                        reasons.extend(smc_signals[:1])
                    
                    # Break below recent swing low
                    recent_low = min(lows[idx-5:idx])
                    if candle_close < recent_low:
                        momentum_score += 3
                        reasons.append('Break of structure')
                    
                    if momentum_score >= 8:
                        chart_alerts.append({
                            'time': candle_time,
                            'price': candle_low,
                            'type': 'MOMENTUM_REVERSAL',
                            'direction': 'bearish_reversal',
                            'label': '🔄 MOMENTUM REVERSAL ↓',
                            'short_label': '🔄 REV↓',
                            'score': momentum_score,
                            'reasons': reasons[:5],
                            'color': '#ff00aa'
                        })
    
    # Remove duplicates (keep highest score for same time)
    unique_alerts = {}
    for alert in chart_alerts:
        key = f"{alert['time']}_{alert['type']}"
        if key not in unique_alerts or alert['score'] > unique_alerts[key]['score']:
            unique_alerts[key] = alert
    
    # Sort by time and return last 10 alerts
    result = sorted(unique_alerts.values(), key=lambda x: x['time'])
    return result[-10:]


def generate_alerts(index_name, data, oi_analysis, vwap_analysis, smc_zones, supports, resistances):
    """Generate smart alerts based on analysis."""
    alerts = []
    ltp = data['ltp']
    ts = datetime.now().strftime('%H:%M:%S')
    
    # Support/Resistance proximity alerts
    for s in supports:
        dist = abs(ltp - s['price']) / ltp
        if dist < 0.001:
            if s['strength'] == 'weak':
                alerts.append({
                    'time': ts, 'type': 'warning',
                    'message': f"Support weakening at {s['price']:.0f} — downside risk increasing"
                })
            else:
                alerts.append({
                    'time': ts, 'type': 'info',
                    'message': f"Testing support {s['price']:.0f} ({s['strength']})"
                })
    
    for r in resistances:
        dist = abs(ltp - r['price']) / ltp
        if dist < 0.001:
            if r['strength'] == 'weak':
                alerts.append({
                    'time': ts, 'type': 'bullish',
                    'message': f"Resistance weakening at {r['price']:.0f} — breakout probability increasing"
                })
            else:
                alerts.append({
                    'time': ts, 'type': 'info',
                    'message': f"Testing resistance {r['price']:.0f} ({r['strength']})"
                })
    
    # OI-based alerts
    for sig in oi_analysis.get('signals', []):
        if sig['bias'] == 'bullish':
            alerts.append({'time': ts, 'type': 'bullish', 'message': sig['signal']})
        elif sig['bias'] == 'bearish':
            alerts.append({'time': ts, 'type': 'bearish', 'message': sig['signal']})
    
    # VWAP alerts
    for sig in vwap_analysis.get('signals', []):
        atype = 'bullish' if sig['bias'] == 'bullish' else ('bearish' if sig['bias'] == 'bearish' else 'info')
        alerts.append({'time': ts, 'type': atype, 'message': sig['signal']})
    
    # SMC alerts
    for zone in smc_zones:
        if 'BOS' in zone['type']:
            alerts.append({'time': ts, 'type': 'info', 'message': zone['label']})
        elif 'CHOCH' in zone['type']:
            alerts.append({'time': ts, 'type': 'warning', 'message': zone['label']})
        elif 'LIQUIDITY' in zone['type']:
            alerts.append({'time': ts, 'type': 'warning', 'message': zone['label']})
    
    return alerts[-15:]  # Keep last 15


def generate_trade_suggestion(index_name, data, oi_analysis, vwap_analysis, smc_zones, supports, resistances, vix):
    """Generate dynamic trade suggestion based on all analysis."""
    ltp = data['ltp']
    if ltp == 0:
        return {}
    
    bullish_score = 0
    bearish_score = 0
    reasons = []
    
    # 1. VWAP analysis (weight: 2)
    if vwap_analysis.get('status') == 'bullish':
        bullish_score += 2
        reasons.append('VWAP reclaim/above VWAP')
    elif vwap_analysis.get('status') == 'bearish':
        bearish_score += 2
        reasons.append('VWAP rejection/below VWAP')
    
    # 2. Option chain signals (weight: 3)
    for sig in oi_analysis.get('signals', []):
        if sig['bias'] == 'bullish':
            bullish_score += 3
            reasons.append(sig['signal'])
        elif sig['bias'] == 'bearish':
            bearish_score += 3
            reasons.append(sig['signal'])
    
    # 3. PCR analysis (weight: 2)
    pcr = oi_analysis.get('pcr', 1)
    if pcr > 1.2:
        bullish_score += 2
        reasons.append(f'PCR bullish ({pcr:.2f})')
    elif pcr < 0.8:
        bearish_score += 2
        reasons.append(f'PCR bearish ({pcr:.2f})')
    
    # 4. SMC zones (weight: 2)
    for zone in smc_zones:
        if 'BULL' in zone['type']:
            bullish_score += 1
        elif 'BEAR' in zone['type']:
            bearish_score += 1
    
    # 5. Support/Resistance strength (weight: 2)
    if supports and supports[0]['strength'] == 'strong':
        bullish_score += 2
        reasons.append('Strong support nearby')
    if resistances and resistances[0]['strength'] == 'weak':
        bullish_score += 1
        reasons.append('Resistance weakening')
    if resistances and resistances[0]['strength'] == 'strong':
        bearish_score += 1
    if supports and supports[0]['strength'] == 'weak':
        bearish_score += 1
        reasons.append('Support weakening')
    
    # 6. VIX filter (weight: 1)
    if vix > 20:
        reasons.append(f'High VIX ({vix:.1f}) — volatile conditions')
    elif vix < 13:
        bullish_score += 1
        reasons.append(f'Low VIX ({vix:.1f}) — trending conditions')
    
    # 7. Price action signals
    for sig in vwap_analysis.get('signals', []):
        if sig.get('strength') == 'strong':
            if sig['bias'] == 'bullish':
                bullish_score += 2
            elif sig['bias'] == 'bearish':
                bearish_score += 2
    
    # Determine bias
    total_score = bullish_score + bearish_score
    if total_score == 0:
        total_score = 1
    
    strike_gap = INSTRUMENTS[index_name]['strike_gap']
    
    if bullish_score > bearish_score:
        confidence_pct = min(95, (bullish_score / total_score) * 100)
        confidence = 'High' if confidence_pct > 70 else ('Medium' if confidence_pct > 50 else 'Low')
        
        entry = round(ltp + ltp * 0.001)
        sl = round(ltp - ltp * 0.003)
        t1 = round(ltp + ltp * 0.003)
        t2 = round(ltp + ltp * 0.005)
        
        return {
            'action': 'BUY CE',
            'bias': 'BULLISH',
            'entry': f"Above {entry:,.0f}",
            'entry_price': entry,
            'stoploss': f"{sl:,.0f}",
            'sl_price': sl,
            'target1': f"{t1:,.0f}",
            'target2': f"{t2:,.0f}",
            'confidence': confidence,
            'confidence_pct': round(confidence_pct),
            'reasons': reasons[:5],
            'timestamp': datetime.now().strftime('%H:%M:%S')
        }
    elif bearish_score > bullish_score:
        confidence_pct = min(95, (bearish_score / total_score) * 100)
        confidence = 'High' if confidence_pct > 70 else ('Medium' if confidence_pct > 50 else 'Low')
        
        entry = round(ltp - ltp * 0.001)
        sl = round(ltp + ltp * 0.003)
        t1 = round(ltp - ltp * 0.003)
        t2 = round(ltp - ltp * 0.005)
        
        return {
            'action': 'BUY PE',
            'bias': 'BEARISH',
            'entry': f"Below {entry:,.0f}",
            'entry_price': entry,
            'stoploss': f"{sl:,.0f}",
            'sl_price': sl,
            'target1': f"{t1:,.0f}",
            'target2': f"{t2:,.0f}",
            'confidence': confidence,
            'confidence_pct': round(confidence_pct),
            'reasons': reasons[:5],
            'timestamp': datetime.now().strftime('%H:%M:%S')
        }
    else:
        return {
            'action': 'WAIT',
            'bias': 'NEUTRAL',
            'entry': 'No clear setup',
            'entry_price': 0,
            'stoploss': '-',
            'sl_price': 0,
            'target1': '-',
            'target2': '-',
            'confidence': 'Low',
            'confidence_pct': 30,
            'reasons': ['No clear directional bias', 'Wait for confirmation'],
            'timestamp': datetime.now().strftime('%H:%M:%S')
        }


def calculate_trend_strength(candles):
    """Calculate trend strength from recent candles."""
    if not candles or len(candles) < 5:
        return 50
    
    closes = [c[4] for c in candles[-20:]]
    if len(closes) < 2:
        return 50
    
    # Simple momentum score
    changes = [(closes[i] - closes[i-1]) / closes[i-1] * 100 for i in range(1, len(closes))]
    positive = sum(1 for c in changes if c > 0)
    total = len(changes)
    
    return int((positive / total) * 100) if total > 0 else 50


def generate_dashboard(index_name, data, supports, resistances, oi_analysis, vwap_analysis, trade_suggestion, vix, candles):
    """Generate dashboard summary."""
    ltp = data['ltp']
    
    # Market bias
    bullish_signals = sum(1 for s in vwap_analysis.get('signals', []) if s.get('bias') == 'bullish')
    bearish_signals = sum(1 for s in vwap_analysis.get('signals', []) if s.get('bias') == 'bearish')
    
    if bullish_signals > bearish_signals:
        market_bias = 'BULLISH'
    elif bearish_signals > bullish_signals:
        market_bias = 'BEARISH'
    else:
        market_bias = 'NEUTRAL'
    
    trend_strength = calculate_trend_strength(candles)
    
    # Breakout probability
    r1_dist = abs(resistances[0]['price'] - ltp) / ltp * 100 if resistances else 5
    breakout_prob = max(10, min(90, 100 - r1_dist * 20))
    if resistances and resistances[0]['strength'] == 'weak':
        breakout_prob = min(90, breakout_prob + 20)
    
    # Momentum strength
    if candles and len(candles) >= 3:
        recent_momentum = abs(candles[-1][4] - candles[-3][4]) / candles[-3][4] * 100
        momentum_str = 'Strong' if recent_momentum > 0.3 else ('Moderate' if recent_momentum > 0.1 else 'Weak')
    else:
        momentum_str = 'N/A'
    
    return {
        'market_bias': market_bias,
        'trend_strength': trend_strength,
        'strongest_support': supports[0]['price'] if supports else 0,
        'strongest_resistance': resistances[0]['price'] if resistances else 0,
        'breakout_probability': round(breakout_prob),
        'momentum': momentum_str,
        'best_trade': trade_suggestion.get('action', 'WAIT'),
        'confidence': trade_suggestion.get('confidence', 'Low'),
        'vix': round(vix, 2),
        'pcr': oi_analysis.get('pcr', 0),
        'vwap_status': vwap_analysis.get('status', 'neutral')
    }


# ============================================================
# Main Data Fetching Loop
# ============================================================

def data_fetch_loop():
    """Main loop that fetches data and pushes to frontend via SocketIO."""
    global THREAD_RUNNING, market_data
    
    print("[INFO] Data fetch loop started")
    
    # Track expiry dates
    expiry_cache = {}
    last_expiry_fetch = 0
    iteration = 0
    
    while THREAD_RUNNING:
        try:
            iteration += 1
            
            # Fetch expiry dates every 30 minutes
            if time.time() - last_expiry_fetch > 1800 or not expiry_cache:
                for idx_name, idx_info in INSTRUMENTS.items():
                    exp = fetch_option_expiries(idx_info['index_key'])
                    if exp:
                        expiry_cache[idx_name] = exp
                        print(f"[INFO] {idx_name} nearest expiry: {exp}")
                last_expiry_fetch = time.time()
            
            # Fetch India VIX
            vix = fetch_india_vix()
            
            for idx_name, idx_info in INSTRUMENTS.items():
                instrument_key = idx_info['index_key']
                
                # 1. Fetch market quote
                quote_data = fetch_market_quote(instrument_key)
                if quote_data:
                    for key, val in quote_data.items():
                        if val:
                            market_data[idx_name]['ltp'] = val.get('last_price', 0)
                            ohlc = val.get('ohlc', {})
                            market_data[idx_name]['open'] = ohlc.get('open', 0)
                            market_data[idx_name]['high'] = ohlc.get('high', 0)
                            market_data[idx_name]['low'] = ohlc.get('low', 0)
                            market_data[idx_name]['close'] = ohlc.get('close', 0)
                            market_data[idx_name]['prev_close'] = val.get('previous_day_close', ohlc.get('close', 0))
                            market_data[idx_name]['volume'] = val.get('volume', 0) if val.get('volume') else 0
                            
                            net_change = val.get('net_change', 0)
                            market_data[idx_name]['change'] = net_change if net_change else 0
                            pct_change = val.get('percent_change', 0) if val.get('percent_change') else 0
                            market_data[idx_name]['change_pct'] = pct_change
                            break
                
                ltp = market_data[idx_name]['ltp']
                if ltp == 0:
                    continue
                
                # 2. Fetch option chain FIRST (needed for S/R detection)
                expiry = expiry_cache.get(idx_name)
                if expiry:
                    oc_raw = fetch_option_chain(instrument_key, expiry)
                    oc_result = analyze_option_chain_data(oc_raw, ltp, idx_info['strike_gap'])
                    market_data[idx_name]['option_chain'] = oc_result['chain']
                    oi_analysis = oc_result['analysis']
                    market_data[idx_name]['oi_analysis'] = oi_analysis
                else:
                    oi_analysis = market_data[idx_name].get('oi_analysis', {})
                
                # 3. Fetch intraday candles
                minute_candles = fetch_intraday_candles(instrument_key, '1minute')
                
                if minute_candles:
                    # Sort ascending by time
                    minute_candles.sort(key=lambda x: x[0])
                    
                    # Aggregate to 3m and 5m
                    candles_3m = aggregate_candles(minute_candles, 3)
                    candles_5m = aggregate_candles(minute_candles, 5)
                    
                    market_data[idx_name]['candles_3m'] = candles_3m
                    market_data[idx_name]['candles_5m'] = candles_5m
                    
                    # Calculate VWAP
                    vwap_val, _ = calculate_vwap(minute_candles)
                    market_data[idx_name]['vwap'] = round(vwap_val, 2)
                    
                    # Get prev day high/low from historical
                    if iteration == 1 or iteration % 60 == 0:
                        hist_candles = fetch_historical_candles(instrument_key, 'day', 5)
                        if hist_candles and len(hist_candles) >= 2:
                            prev_day = hist_candles[-2] if hist_candles[-1][0][:10] == datetime.now().strftime('%Y-%m-%d') else hist_candles[-1]
                            market_data[idx_name]['prev_high'] = prev_day[2]
                            market_data[idx_name]['prev_low'] = prev_day[3]
                    
                    # Detect S/R with OI-based levels
                    all_candles = minute_candles
                    if len(all_candles) > 5:
                        supports, resistances = detect_support_resistance(all_candles, ltp, num_levels=2, oi_analysis=oi_analysis)
                        
                        # Add prev day high/low as levels
                        prev_h = market_data[idx_name].get('prev_high', 0)
                        prev_l = market_data[idx_name].get('prev_low', 0)
                        if prev_h and prev_h > ltp:
                            resistances.append({
                                'price': prev_h, 'volume': 0, 'tests': 0, 
                                'strength': 'prev_day_high', 'score': 55,
                                'factors': ['Previous Day High']
                            })
                        if prev_l and prev_l < ltp:
                            supports.append({
                                'price': prev_l, 'volume': 0, 'tests': 0, 
                                'strength': 'prev_day_low', 'score': 55,
                                'factors': ['Previous Day Low']
                            })
                        
                        market_data[idx_name]['supports'] = supports
                        market_data[idx_name]['resistances'] = resistances
                    
                    # SMC detection
                    smc_zones = detect_smc_patterns(candles_5m if candles_5m else minute_candles, ltp)
                    market_data[idx_name]['smc_zones'] = smc_zones
                    
                    # VWAP price action analysis
                    vwap_analysis = analyze_vwap_price_action(
                        candles_5m if candles_5m else minute_candles,
                        market_data[idx_name]['vwap'],
                        ltp
                    )
                else:
                    supports = market_data[idx_name]['supports']
                    resistances = market_data[idx_name]['resistances']
                    smc_zones = market_data[idx_name]['smc_zones']
                    vwap_analysis = {'status': 'neutral', 'signals': []}
                    candles_5m = market_data[idx_name]['candles_5m']
                
                # 4. Store VIX
                market_data[idx_name]['vix'] = vix
                
                # 5. Generate alerts
                alerts = generate_alerts(
                    idx_name, market_data[idx_name],
                    oi_analysis, vwap_analysis,
                    smc_zones, supports, resistances
                )
                # Only add new unique alerts
                existing = set(a['message'] for a in alert_history[idx_name])
                for alert in alerts:
                    if alert['message'] not in existing:
                        alert_history[idx_name].append(alert)
                
                market_data[idx_name]['alerts'] = list(alert_history[idx_name])
                
                # 6. Generate trade suggestion
                trade_suggestion = generate_trade_suggestion(
                    idx_name, market_data[idx_name],
                    oi_analysis, vwap_analysis,
                    smc_zones, supports, resistances, vix
                )
                market_data[idx_name]['trade_suggestion'] = trade_suggestion
                
                # 7. Generate dashboard
                dashboard = generate_dashboard(
                    idx_name, market_data[idx_name],
                    supports, resistances,
                    oi_analysis, vwap_analysis,
                    trade_suggestion, vix,
                    candles_5m
                )
                market_data[idx_name]['dashboard'] = dashboard
                
                # 8. Deep Momentum Analysis for Chart Alerts
                vwap_val = market_data[idx_name]['vwap']
                chart_alerts = analyze_deep_momentum(
                    candles_5m if candles_5m else minute_candles,
                    oi_analysis,
                    smc_zones,
                    vwap_val,
                    supports,
                    resistances
                )
                market_data[idx_name]['chart_alerts'] = chart_alerts
                
                # 9. Market breadth (approximate)
                market_data[idx_name]['market_breadth'] = {
                    'advance_decline': 'Positive' if market_data[idx_name]['change'] > 0 else 'Negative',
                    'bank_nifty_correlation': 'Aligned' if (idx_name == 'NIFTY' and market_data['NIFTY']['change'] > 0) else 'Diverging'
                }
                
                # 9. Institutional positioning (from OI data)
                market_data[idx_name]['institutional'] = {
                    'fii_bias': 'Long' if oi_analysis.get('pcr', 1) > 1.1 else ('Short' if oi_analysis.get('pcr', 1) < 0.9 else 'Neutral'),
                    'futures_buildup': 'Long buildup' if oi_analysis.get('pe_oi_change', 0) > 0 else 'Short buildup',
                    'directional_bias': 'Bullish' if oi_analysis.get('pcr', 1) > 1.0 else 'Bearish'
                }
            
            # Emit data to all connected clients
            payload = {
                'NIFTY': serialize_market_data('NIFTY'),
                'SENSEX': serialize_market_data('SENSEX'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            socketio.emit('market_update', payload)
            
            # Wait before next fetch (Upstox rate limits: ~1 req/sec per endpoint)
            time.sleep(5)
            
        except Exception as e:
            print(f"[ERROR] Data loop error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)
    
    print("[INFO] Data fetch loop stopped")


def serialize_market_data(idx_name):
    """Serialize market data for JSON transport."""
    d = market_data[idx_name]
    
    # Convert candles - limit to last 100
    def format_candles(candles):
        formatted = []
        for c in candles[-100:]:
            formatted.append({
                'time': c[0] if isinstance(c[0], str) else str(c[0]),
                'open': c[1],
                'high': c[2],
                'low': c[3],
                'close': c[4],
                'volume': c[5] if len(c) > 5 else 0
            })
        return formatted
    
    return {
        'ltp': d['ltp'],
        'open': d['open'],
        'high': d['high'],
        'low': d['low'],
        'close': d['close'],
        'prev_close': d['prev_close'],
        'prev_high': d.get('prev_high', 0),
        'prev_low': d.get('prev_low', 0),
        'volume': d['volume'],
        'change': d['change'],
        'change_pct': d['change_pct'],
        'vwap': d['vwap'],
        'vix': d['vix'],
        'candles_3m': format_candles(d['candles_3m']),
        'candles_5m': format_candles(d['candles_5m']),
        'option_chain': d['option_chain'],
        'oi_analysis': d['oi_analysis'],
        'supports': d['supports'],
        'resistances': d['resistances'],
        'smc_zones': d['smc_zones'],
        'alerts': d['alerts'],
        'trade_suggestion': d['trade_suggestion'],
        'dashboard': d['dashboard'],
        'market_breadth': d['market_breadth'],
        'institutional': d['institutional'],
        'chart_alerts': d.get('chart_alerts', [])
    }


# ============================================================
# Flask Routes
# ============================================================

@app.route('/')
def index():
    """Serve the frontend."""
    return send_from_directory('.', 'index.html')


@app.route('/api/set_token', methods=['POST'])
def set_token():
    """Set the Upstox access token."""
    global ACCESS_TOKEN, DATA_THREAD, THREAD_RUNNING
    
    data = request.json
    token = data.get('access_token', '').strip()
    
    if not token:
        return jsonify({'status': 'error', 'message': 'Access token is required'}), 400
    
    ACCESS_TOKEN = token
    
    # Validate token by making a test call
    try:
        url = 'https://api.upstox.com/v2/market-quote/ltp?instrument_key=NSE_INDEX|Nifty 50'
        resp = requests.get(url, headers=upstox_headers(), timeout=10)
        if resp.status_code != 200:
            return jsonify({'status': 'error', 'message': f'Invalid token. API returned: {resp.status_code}'}), 401
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Connection error: {str(e)}'}), 500
    
    # Start data thread if not running
    if not THREAD_RUNNING:
        THREAD_RUNNING = True
        DATA_THREAD = threading.Thread(target=data_fetch_loop, daemon=True)
        DATA_THREAD.start()
        print("[INFO] Data thread started")
    
    return jsonify({'status': 'success', 'message': 'Token set successfully. Live data starting...'})


@app.route('/api/status')
def api_status():
    """Check API connection status."""
    return jsonify({
        'connected': ACCESS_TOKEN is not None,
        'thread_running': THREAD_RUNNING,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })


# ============================================================
# SocketIO Events
# ============================================================

@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    print(f"[INFO] Client connected")
    if ACCESS_TOKEN and THREAD_RUNNING:
        # Send initial data
        payload = {
            'NIFTY': serialize_market_data('NIFTY'),
            'SENSEX': serialize_market_data('SENSEX'),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        emit('market_update', payload)


@socketio.on('disconnect')
def handle_disconnect():
    print(f"[INFO] Client disconnected")


@socketio.on('request_data')
def handle_request_data():
    """Handle manual data request from client."""
    if ACCESS_TOKEN:
        payload = {
            'NIFTY': serialize_market_data('NIFTY'),
            'SENSEX': serialize_market_data('SENSEX'),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        emit('market_update', payload)


# ============================================================
# Run the App
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("  NIFTY & SENSEX Trading Intelligence System")
    print("  Open http://localhost:5000 in your browser")
    print("  Enter your Upstox Access Token in the UI")
    print("=" * 60)
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
