from flask import Flask, request, jsonify
from solders.pubkey import Pubkey
import requests
import time
from datetime import datetime
from collections import defaultdict
import threading

app = Flask(__name__)

# Memecoin addresses to monitor
MONITORED_COINS = [
    "FoCsCWb58iuY8id7XSakXKur4ifyzr4crZTGd6TPGiUm",
    "9PeqGoKvBidsG1foiyGMv5bkn9b3Pn9wPM67hUcrfQHY",
    "FBCdYn9nt8rpQkkKxBKS6h1n6jHeBiFBXTy2Gq2Dpump",
    "BNqAVSoath8dWaj1ptzD8U5TBW4mUFWwsXZPwcKB3voV",
    "HQdrX9BPLzKpY4zw9AS1xvX9WBVHfzX835wRiqQ83j6w",
    "4oVdRhLLZzzMuo3xL1w5X6YWB6xkXbnja9ExderA6Zsg",
    "CgnWirQJPGg1CccKTj3Y2Mv5pAd6kRiJASEo4UW9DB8y",
    "CXoQ54ecxWBwa3XPXMWGV4QZQFx8ZD3zSgCGEAV8dM2D",
    "ArLbAaZbhfPuTvH3S6xrczDgxfeZzRjgABCc95FxmCyz",
    "6BgWsBzgXi1r4vb1f7uLMKtafYQisCEV6to7rEL4YBMu",
    "75ojR8wwZoW7FnPr3AyUBA4t4SeNxVupNqfhBrcnVpDv"
]

# Stablecoin identifiers to ignore
STABLECOIN_KEYWORDS = ['usdt', 'usdc', 'dai', 'busd', 'tusd', 'usd', 'stable']

# Price and volume tracking
price_history = defaultdict(list)
volume_history = defaultdict(list)
last_alert = defaultdict(float)

# Pump detection thresholds
PRICE_PUMP_THRESHOLD = 15  # 15% price increase
VOLUME_SPIKE_THRESHOLD = 3  # 3x average volume
TIME_WINDOW = 300  # 5 minutes
ALERT_COOLDOWN = 600  # 10 minutes between alerts for same coin

class PumpDetector:
    def __init__(self):
        self.rpc_url = "https://api.mainnet-beta.solana.com"
        
    def is_stablecoin(self, token_address):
        """Check if token is a stablecoin based on metadata"""
        try:
            # Query token metadata
            response = requests.post(
                self.rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getAccountInfo",
                    "params": [token_address, {"encoding": "jsonParsed"}]
                },
                timeout=10
            )
            
            data = response.json()
            if 'result' in data and data['result']:
                # Check token name/symbol for stablecoin keywords
                account_data = str(data['result']).lower()
                return any(keyword in account_data for keyword in STABLECOIN_KEYWORDS)
        except Exception as e:
            print(f"Error checking stablecoin status: {e}")
        
        return False
    
    def get_token_data(self, token_address):
        """Fetch token price and volume from DexScreener"""
        try:
            response = requests.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{token_address}",
                timeout=15
            )
            data = response.json()
            
            if 'pairs' in data and len(data['pairs']) > 0:
                # Get highest liquidity pair
                pair = max(data['pairs'], key=lambda x: float(x.get('liquidity', {}).get('usd', 0)))
                
                price = float(pair.get('priceUsd', 0))
                volume = float(pair.get('volume', {}).get('h24', 0))
                
                return {
                    'price': price if price > 0 else None,
                    'volume': volume if volume > 0 else None,
                    'liquidity': float(pair.get('liquidity', {}).get('usd', 0)),
                    'pair_address': pair.get('pairAddress', ''),
                    'dex': pair.get('dexId', '')
                }
        except Exception as e:
            print(f"Error fetching data for {token_address}: {e}")
        
        return {'price': None, 'volume': None}
    
    def calculate_price_change(self, token_address, current_price):
        """Calculate price change percentage over time window"""
        history = price_history[token_address]
        current_time = time.time()
        
        # Clean old entries
        history[:] = [(t, p) for t, p in history if current_time - t < TIME_WINDOW]
        
        # Add current price
        history.append((current_time, current_price))
        
        if len(history) < 2:
            return 0
        
        # Calculate percentage change from earliest to latest
        earliest_price = history[0][1]
        latest_price = history[-1][1]
        
        if earliest_price > 0:
            return ((latest_price - earliest_price) / earliest_price) * 100
        
        return 0
    
    def detect_volume_spike(self, token_address, current_volume):
        """Detect abnormal volume spikes"""
        history = volume_history[token_address]
        current_time = time.time()
        
        # Keep 1 hour of volume data
        history[:] = [(t, v) for t, v in history if current_time - t < 3600]
        history.append((current_time, current_volume))
        
        if len(history) < 10:
            return False
        
        # Calculate average volume (excluding current)
        avg_volume = sum(v for _, v in history[:-1]) / (len(history) - 1)
        
        if avg_volume > 0:
            volume_multiplier = current_volume / avg_volume
            return volume_multiplier >= VOLUME_SPIKE_THRESHOLD
        
        return False
    
    def check_pump(self, token_address):
        """Main pump detection logic"""
        current_time = time.time()
        
        # Check alert cooldown
        if current_time - last_alert[token_address] < ALERT_COOLDOWN:
            return None
        
        # Skip if stablecoin
        if self.is_stablecoin(token_address):
            return {
                'status': 'ignored',
                'reason': 'Stablecoin detected',
                'token': token_address
            }
        
        # Get current data from DexScreener
        data = self.get_token_data(token_address)
        price = data.get('price')
        volume = data.get('volume')
        
        if price is None or volume is None:
            return None
        
        # Calculate metrics
        price_change = self.calculate_price_change(token_address, price)
        volume_spike = self.detect_volume_spike(token_address, volume)
        
        # Detect pump
        is_pumping = (price_change >= PRICE_PUMP_THRESHOLD) or volume_spike
        
        if is_pumping:
            last_alert[token_address] = current_time
            
            return {
                'status': 'PUMP_DETECTED',
                'token': token_address,
                'price': price,
                'price_change_pct': round(price_change, 2),
                'volume_24h': volume,
                'volume_spike': volume_spike,
                'liquidity_usd': data.get('liquidity', 0),
                'dex': data.get('dex', 'unknown'),
                'timestamp': datetime.now().isoformat(),
                'dexscreener_url': f"https://dexscreener.com/solana/{token_address}"
            }
        
        return None

detector = PumpDetector()

# Background monitoring thread
def monitor_coins():
    """Continuously monitor coins for pumps"""
    while True:
        for coin in MONITORED_COINS:
            try:
                result = detector.check_pump(coin)
                if result and result.get('status') == 'PUMP_DETECTED':
                    print(f"\n🚀 PUMP ALERT: {result}")
            except Exception as e:
                print(f"Error monitoring {coin}: {e}")
            
            time.sleep(3)  # Add 3 second delay between each token
        
        time.sleep(30)  # Still check every 30 seconds after full cycle

# Start monitoring in background
monitor_thread = threading.Thread(target=monitor_coins, daemon=True)
monitor_thread.start()

@app.route('/webhook', methods=['POST'])
def webhook():
    """Receive external webhook notifications"""
    try:
        data = request.get_json()
        token_address = data.get('token_address')
        
        if token_address not in MONITORED_COINS:
            return jsonify({'error': 'Token not in monitored list'}), 400
        
        result = detector.check_pump(token_address)
        
        if result:
            return jsonify(result), 200
        else:
            return jsonify({'status': 'no_pump', 'token': token_address}), 200
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/status', methods=['GET'])
def status():
    """Get current monitoring status"""
    status_data = {
        'monitored_coins': MONITORED_COINS,
        'active': True,
        'price_data_points': {coin: len(price_history[coin]) for coin in MONITORED_COINS},
        'volume_data_points': {coin: len(volume_history[coin]) for coin in MONITORED_COINS}
    }
    return jsonify(status_data), 200

@app.route('/check/<token_address>', methods=['GET'])
def check_token(token_address):
    """Manually check a specific token"""
    if token_address not in MONITORED_COINS:
        return jsonify({'error': 'Token not in monitored list'}), 400
    
    result = detector.check_pump(token_address)
    
    if result:
        return jsonify(result), 200
    else:
        return jsonify({'status': 'no_pump', 'token': token_address}), 200

if __name__ == '__main__':
    print("🚀 Solana Memecoin Pump Detector Started")
    print(f"Monitoring {len(MONITORED_COINS)} tokens")
    print(f"Thresholds: {PRICE_PUMP_THRESHOLD}% price, {VOLUME_SPIKE_THRESHOLD}x volume")
    print(f"Server running on http://localhost:5001")
    app.run(host='0.0.0.0', port=5001, debug=False)