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

# Load API key from .env
FINNHUB_API_KEY = None
if os.path.exists('.env'):
    with open('.env', 'r') as f:
        for line in f:
            if line.startswith('FINNHUB_API_KEY='):
                FINNHUB_API_KEY = line.split('=', 1)[1].strip()
                break

def fetch_stock_price(ticker):
    """Fetch live stock price from Finnhub"""
    if not FINNHUB_API_KEY:
        return None
    try:
        url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            if 'c' in data:  # c = current price
                return float(data['c'])
    except:
        pass
    return None

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
    print("-" * 60)
    print(f"{'Ticker':<8} {'Alert':<10} {'Current':<10} {'Change':<10}")
    print("-" * 60)
    
    total_move = 0
    winners = 0
    count = 0
    
    for ticker in sorted(tickers):
        ticker_rows = [r for r in rows if r.get('Ticker') == ticker]
        if not ticker_rows:
            continue
        
        try:
            alert_price = float(ticker_rows[0].get('Price', 0))
        except ValueError:
            continue
        
        # Fetch current live price
        current = fetch_stock_price(ticker)
        current_str = f"${current:.2f}" if current else "N/A"
        
        if current:
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
        else:
            move_str = "N/A"
        
        alert_str = f"${alert_price:.2f}"
        print(f"{ticker:<8} {alert_str:<10} {current_str:<10} {move_str:<10}")
    
    print("-" * 60)
    avg_move = total_move / count if count > 0 else 0
    print(f"Average: {avg_move:+.1f}%\n")

if __name__ == '__main__':
    main()
