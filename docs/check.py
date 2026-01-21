#!/usr/bin/env python3
"""
Check alert performance - fetches live prices for tickers
Run: python3 check.py
"""

import csv
import json
import sys
import os
import urllib.request
import urllib.error
import time
import random

# Rate limiting constants
REQUEST_DELAY = 0.2  # 200ms delay between requests
MAX_REQUESTS_PER_BATCH = 50  # After 50 requests, add extra delay
BATCH_DELAY = 0.5  # 0.5 second delay after every 50 requests
TIMEOUT = 5  # Timeout for API requests in seconds

# Load API key from .env
FINNHUB_API_KEY = None
if os.path.exists('.env'):
    with open('.env', 'r') as f:
        for line in f:
            if line.startswith('FINNHUB_API_KEY='):
                FINNHUB_API_KEY = line.split('=', 1)[1].strip()
                break

def fetch_stock_price(ticker, request_count):
    """Fetch live stock price and peak prices from Finnhub with rate limiting"""
    if not FINNHUB_API_KEY:
        return None, None, None, request_count
    
    # Rate limiting: pause after every 30 requests (silently, no log)
    if request_count > 0 and request_count % MAX_REQUESTS_PER_BATCH == 0:
        time.sleep(BATCH_DELAY)
    
    # Small delay before each request
    time.sleep(REQUEST_DELAY + random.uniform(0, 0.1))
    
    try:
        url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
            data = json.loads(response.read().decode())
            if 'c' in data and data['c']:  # c = current price, h = high, l = low
                current = float(data['c'])
                high = float(data.get('h', current))  # daily high
                low = float(data.get('l', current))   # daily low
                return current, high, low, request_count + 1
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, Exception) as e:
        # Silently skip on timeout/connection errors
        pass
    return None, None, None, request_count + 1

def main():
    try:
        with open('logs/track.csv', 'r') as f:
            rows = list(csv.DictReader(f))
    except FileNotFoundError:
        print("ERROR: No track.csv found.")
        sys.exit(1)
    
    if not rows:
        print("ERROR: track.csv is empty")
        sys.exit(1)
    
    # Get unique tickers from track.csv
    tickers = set()
    for row in rows:
        ticker = row.get('Ticker', '').strip()
        if ticker and ticker != 'Unknown':
            tickers.add(ticker)
    
    if not tickers:
        print("ERROR: No valid tickers in CSV")
        sys.exit(1)
    
    print(f"\nAlerts ({len(tickers)})")
    print("-" * 180)
    print(f"{'Ticker':<8} {'Alert':<10} {'Current':<10} {'Peak':<10} {'Change':<10} {'Inc':<12} {'Ops':<12} {'Filed':<19} {'Reason'}")
    print("-" * 180)
    
    total_move = 0
    winners = 0
    count = 0
    request_count = 0
    big_movers = []
    na_tickers = []
    
    for ticker in sorted(tickers):
        ticker_rows = [r for r in rows if r.get('Ticker') == ticker]
        if not ticker_rows:
            continue
        
        try:
            alert_price = float(ticker_rows[0].get('Price', 0))
        except ValueError:
            continue
        
        skip_reason = ticker_rows[0].get('Skip Reason', '')
        # Remove bonus filter text for cleaner display
        if '(Bonus:' in skip_reason:
            skip_reason = skip_reason.split('(Bonus:')[0].strip()
        incorporated = ticker_rows[0].get('Incorporated', 'N/A')[:10]
        located = ticker_rows[0].get('Located', 'N/A')[:10]
        filed_date = ticker_rows[0].get('Filed Date', 'N/A')
        filed_time = ticker_rows[0].get('Filed Time', 'N/A')
        if filed_date != 'N/A' and filed_time != 'N/A':
            filed_display = f"{filed_date} {filed_time[:5]}"
        else:
            filed_display = filed_date[:10] if filed_date != 'N/A' else 'N/A'
        
        # Fetch current live price with peak data
        current, high, low, request_count = fetch_stock_price(ticker, request_count)
        current_str = f"${current:.2f}" if current else "N/A"
        
        if current:
            # Calculate peak price (highest or lowest since alert, whichever is more extreme)
            peak_price = max(high, alert_price) if current > alert_price else min(low, alert_price)
            peak_str = f"${peak_price:.2f}"
            peak_move_pct = ((peak_price - alert_price) / alert_price) * 100
            
            move_pct = ((current - alert_price) / alert_price) * 100
            
            if current > alert_price:
                count += 1
                total_move += move_pct
                if move_pct > 50:
                    winners += 1
                elif move_pct > 20:
                    winners += 1
                elif move_pct > 0:
                    winners += 1
            elif current < alert_price:
                count += 1
                total_move += move_pct
            
            move_str = f"{move_pct:+.1f}%"
            
            # Track big movers (10% +/- threshold)
            if abs(move_pct) >= 10:
                big_movers.append({
                    'ticker': ticker,
                    'alert_price': alert_price,
                    'current_price': current,
                    'peak_price': peak_price,
                    'move_pct': move_pct,
                    'peak_move_pct': peak_move_pct,
                    'skip_reason': skip_reason,
                    'incorporated': incorporated,
                    'located': located,
                    'filed_display': filed_display
                })
        else:
            peak_str = "N/A"
            move_str = "N/A"
            na_tickers.append({'ticker': ticker, 'skip_reason': skip_reason})
        
        alert_str = f"${alert_price:.2f}"
        print(f"{ticker:<8} {alert_str:<10} {current_str:<10} {peak_str:<10} {move_str:<10} {incorporated:<12} {located:<12} {filed_display:<19} {skip_reason}")
    
    print("-" * 180)
    avg_move = total_move / count if count > 0 else 0
    print(f"Average: {avg_move:+.1f}%")
    print(f"Successful: {winners}/{count}")
    print(f"Rate limited after 30 requests (N/A count: {len(na_tickers)})\n")
    
    # Summary of big movers (>= 10%)
    if big_movers:
        print("\n" + "="*180)
        print(f"BIG MOVERS (>= 10% +/- threshold): {len(big_movers)} stocks")
        print("="*180)
        print(f"{'Ticker':<8} {'Alert Price':<12} {'Current':<12} {'Peak':<12} {'Move %':<10} {'Peak %':<10} {'Inc':<12} {'Ops':<12} {'Filed':<19} {'Skip Reason'}")
        print("-"*180)
        for mover in sorted(big_movers, key=lambda x: abs(x['move_pct']), reverse=True):
            inc = mover.get('incorporated', 'N/A')[:10]
            ops = mover.get('located', 'N/A')[:10]
            filed = mover.get('filed_display', 'N/A')
            print(f"{mover['ticker']:<8} ${mover['alert_price']:<11.2f} ${mover['current_price']:<11.2f} ${mover['peak_price']:<11.2f} {mover['move_pct']:+.1f}%{'':<3} {mover['peak_move_pct']:+.1f}%{'':<2} {inc:<12} {ops:<12} {filed:<19} {mover['skip_reason']}")
    
    # Summary of N/A (rate limited)
    if na_tickers:
        print("\n" + "="*180)
        print(f"RATE LIMITED / NOT AVAILABLE: {len(na_tickers)} stocks (exceeded 30 req limit)")
        print("="*180)
        print(f"{'Ticker':<8} {'Skip Reason'}")
        print("-"*180)
        for na in sorted(na_tickers, key=lambda x: x['ticker']):
            print(f"{na['ticker']:<8} {na['skip_reason']}")

if __name__ == '__main__':
    main()
