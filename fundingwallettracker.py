import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import json
import os
import time
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

class BinanceWalletMonitor:
    def __init__(self):
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        
        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY not found in environment variables")
        
        print(f"Using Helius API key: {self.helius_api_key[:8]}...")
        self.helius_url = "https://api.helius.xyz/v0"
        
        # Target funding patterns based on your analysis
        self.target_amounts = [3.0, 3.5, 3.99, 4.0, 4.5, 5.0]  # SOL amounts to watch
        
    def get_transactions(self, wallet_address: str, limit: int = 50) -> List[Dict]:
        """Get recent transactions for a wallet"""
        url = f"{self.helius_url}/addresses/{wallet_address}/transactions"
        
        params = {
            'api-key': self.helius_api_key,
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            print(f"❌ Error fetching transactions: {e}")
            return []
    
    def check_recent_funding_transfers(self, binance_wallet: str, hours_back: int = 24) -> List[Dict]:
        """Check for recent funding transfers from Binance wallet"""
        print(f"🔍 Checking recent funding from {binance_wallet[:8]}... (last {hours_back}h)")
        
        transactions = self.get_transactions(binance_wallet, limit=100)
        recent_fundings = []
        
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        for tx in transactions:
            try:
                tx_time = datetime.fromtimestamp(tx['timestamp'])
                
                if tx_time < cutoff_time:
                    continue
                
                if 'nativeTransfers' in tx:
                    for transfer in tx['nativeTransfers']:
                        if transfer['fromUserAccount'] == binance_wallet:
                            amount_sol = transfer['amount'] / 1_000_000_000
                            
                            # Check if amount matches our target funding pattern
                            if any(abs(amount_sol - target) < 0.1 for target in self.target_amounts):
                                recent_fundings.append({
                                    'timestamp': tx_time,
                                    'funded_wallet': transfer['toUserAccount'],
                                    'amount_sol': amount_sol,
                                    'signature': tx['signature'],
                                    'minutes_ago': (datetime.now() - tx_time).total_seconds() / 60
                                })
                                
            except (KeyError, TypeError):
                continue
        
        return sorted(recent_fundings, key=lambda x: x['timestamp'], reverse=True)
    
    def check_for_token_creation(self, wallet_address: str, since_timestamp: datetime = None) -> List[Dict]:
        """Check if wallet created tokens recently"""
        print(f"🪙 Checking {wallet_address[:8]}... for token creation")
        
        transactions = self.get_transactions(wallet_address, limit=30)
        token_creations = []
        
        for tx in transactions:
            try:
                tx_time = datetime.fromtimestamp(tx['timestamp'])
                tx_type = tx.get('type', '')
                
                # Only check transactions after funding if timestamp provided
                if since_timestamp and tx_time < since_timestamp:
                    continue
                
                # Look for token creation indicators
                if any(keyword in tx_type.upper() for keyword in ['TOKEN', 'CREATE', 'MINT', 'DEPLOY', 'INITIALIZE']):
                    
                    # Calculate time since funding
                    time_since_funding = None
                    if since_timestamp:
                        time_since_funding = (tx_time - since_timestamp).total_seconds() / 60
                    
                    token_creations.append({
                        'timestamp': tx_time,
                        'type': tx_type,
                        'signature': tx['signature'],
                        'time_since_funding_minutes': time_since_funding
                    })
                    
            except (KeyError, TypeError):
                continue
        
        return token_creations
    
    def check_for_quick_trades(self, wallet_address: str, token_creation_time: datetime) -> Dict:
        """Check if wallet made trades after creating token"""
        print(f"📈 Checking {wallet_address[:8]}... for post-creation trading")
        
        transactions = self.get_transactions(wallet_address, limit=50)
        trades = []
        
        for tx in transactions:
            try:
                tx_time = datetime.fromtimestamp(tx['timestamp'])
                tx_type = tx.get('type', '')
                
                # Only check transactions after token creation
                if tx_time <= token_creation_time:
                    continue
                
                # Look for trading activity
                if any(keyword in tx_type.upper() for keyword in ['SWAP', 'TRADE', 'TRANSFER']):
                    minutes_after_creation = (tx_time - token_creation_time).total_seconds() / 60
                    
                    trades.append({
                        'timestamp': tx_time,
                        'type': tx_type,
                        'minutes_after_creation': minutes_after_creation,
                        'signature': tx['signature']
                    })
                    
            except (KeyError, TypeError):
                continue
        
        return {
            'trades': trades,
            'first_trade_minutes': min([t['minutes_after_creation'] for t in trades]) if trades else None,
            'total_trades': len(trades)
        }
    
    def monitor_binance_wallet(self, binance_wallet: str, hours_back: int = 24) -> Dict:
        """Main monitoring function for Binance wallet"""
        print(f"\n" + "="*70)
        print(f"🚨 BINANCE WALLET MONITOR - REAL-TIME TOKEN DEPLOYMENT DETECTION")
        print(f"="*70)
        print(f"Target: {binance_wallet}")
        print(f"🔗 GMGN: https://gmgn.ai/sol/address/{binance_wallet}")
        
        # Step 1: Get recent funding transfers
        recent_fundings = self.check_recent_funding_transfers(binance_wallet, hours_back)
        
        if not recent_fundings:
            print(f"❌ No recent funding transfers found in last {hours_back} hours")
            return {}
        
        print(f"✅ Found {len(recent_fundings)} recent funding transfers")
        
        # Step 2: Check each funded wallet for token creation
        token_deployment_alerts = []
        
        for funding in recent_fundings:
            funded_wallet = funding['funded_wallet']
            funding_time = funding['timestamp']
            
            print(f"\n🔍 Analyzing funded wallet: {funded_wallet[:8]}...")
            print(f"   💰 Funded: {funding['amount_sol']:.2f} SOL at {funding_time.strftime('%H:%M:%S')}")
            print(f"   🔗 GMGN: https://gmgn.ai/sol/address/{funded_wallet}")
            
            # Check for token creation
            token_creations = self.check_for_token_creation(funded_wallet, funding_time)
            
            if token_creations:
                print(f"   🚀 TOKEN CREATION DETECTED!")
                
                for creation in token_creations:
                    time_diff = creation['time_since_funding_minutes']
                    
                    if time_diff is not None and time_diff <= 30:  # Within 30 minutes
                        print(f"      ⚡ QUICK DEPLOY: {time_diff:.1f} minutes after funding")
                        
                        # Check for post-creation trading
                        trading_info = self.check_for_quick_trades(funded_wallet, creation['timestamp'])
                        
                        alert = {
                            'funded_wallet': funded_wallet,
                            'funding': funding,
                            'token_creation': creation,
                            'trading_info': trading_info,
                            'risk_level': 'HIGH' if time_diff <= 5 else 'MEDIUM'
                        }
                        
                        token_deployment_alerts.append(alert)
                        
                        if trading_info['first_trade_minutes']:
                            print(f"      📈 First trade: {trading_info['first_trade_minutes']:.1f} min after creation")
                    else:
                        print(f"      ⏰ Slower deploy: {time_diff:.1f} minutes after funding")
            else:
                print(f"   ⚪ No token creation detected yet")
            
            time.sleep(0.3)  # Rate limiting
        
        return {
            'binance_wallet': binance_wallet,
            'recent_fundings': recent_fundings,
            'token_alerts': token_deployment_alerts,
            'scan_time': datetime.now()
        }
    
    def print_alert_summary(self, results: Dict):
        """Print formatted alert summary"""
        if not results:
            return
        
        alerts = results.get('token_alerts', [])
        
        print(f"\n" + "🚨"*20)
        print(f"ALERT SUMMARY")
        print(f"🚨"*20)
        
        if not alerts:
            print(f"✅ No immediate token deployment alerts")
            print(f"📊 Monitored {len(results['recent_fundings'])} recent funding events")
        else:
            print(f"🔥 {len(alerts)} TOKEN DEPLOYMENT ALERTS!")
            
            for i, alert in enumerate(alerts, 1):
                print(f"\n🚨 ALERT #{i} - {alert['risk_level']} RISK")
                print(f"Wallet: {alert['funded_wallet'][:8]}...")
                print(f"🔗 GMGN: https://gmgn.ai/sol/address/{alert['funded_wallet']}")
                print(f"💰 Funded: {alert['funding']['amount_sol']:.2f} SOL")
                print(f"⚡ Deploy time: {alert['token_creation']['time_since_funding_minutes']:.1f} min after funding")
                
                if alert['trading_info']['first_trade_minutes']:
                    print(f"📈 First trade: {alert['trading_info']['first_trade_minutes']:.1f} min after deploy")
                    print(f"🔄 Total trades: {alert['trading_info']['total_trades']}")
                
                print(f"⚠️  POTENTIAL RUG - MONITOR CLOSELY!")
        
        print(f"\n💡 MONITORING TIPS:")
        print(f"• Watch for sudden volume spikes in new tokens")
        print(f"• Set alerts for wallets with <5 minute deploy times") 
        print(f"• Monitor dev buying patterns (usually 3-5 min after deploy)")
        print(f"• Exit signals: Dev starts selling large amounts")
        
    def save_alerts(self, results: Dict, filename: str = None):
        """Save alerts to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"binance_monitor_alerts_{timestamp}.json"
        
        def json_serial(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=json_serial)
        
        print(f"📄 Alerts saved: {filename}")

# Main execution
if __name__ == "__main__":
    monitor = BinanceWalletMonitor()
    
    # Binance 2 wallet from your investigation
    binance_wallet = input("Enter Binance wallet address (default: 5tZF..uAl9): ").strip()
    
    if not binance_wallet:
        binance_wallet = "5tZFuAl9sYe6m8zKjfXJNY8bL4ZEuNjRxTdGQd8bJbPP"  # Replace with actual address
    
    # Time window for monitoring
    hours_back = int(input("Hours to look back (default: 24): ") or 24)
    
    print(f"\n🚀 Starting Binance wallet monitoring...")
    print(f"Looking for 3-5 SOL funding transfers → quick token deployments")
    
    # Run monitoring
    results = monitor.monitor_binance_wallet(binance_wallet, hours_back)
    
    # Show alerts
    monitor.print_alert_summary(results)
    
    # Save results
    monitor.save_alerts(results)