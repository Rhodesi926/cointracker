import asyncio
import websockets
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Dict, List, Set
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class RealTimeKOLMonitor:
    def __init__(self, kol_wallets_file: str = "kol_wallets.json"):
        self.getblock_ws_url = os.getenv('GETBLOCK_WS_URL')
        
        if not self.getblock_ws_url:
            raise ValueError("GETBLOCK_WS_URL not found in environment variables. Please add your GetBlock WebSocket URL to .env file")
        
        print(f"Using GetBlock WebSocket: {self.getblock_ws_url[:50]}...")
        
        # GetBlock WebSocket URL
        self.websocket_url = self.getblock_ws_url
        
        # Load KOL wallets from JSON file
        self.kol_wallets = self.load_kol_wallets(kol_wallets_file)
        print(f"Loaded {len(self.kol_wallets)} KOL wallets for monitoring")
        
        # Real-time tracking variables
        self.token_activity = defaultdict(lambda: {
            'wallets': set(),
            'transactions': deque(maxlen=100),
            'first_seen': None,
            'token_symbol': 'Unknown'
        })
        
        # Time windows for consensus detection (in seconds)
        self.consensus_window = 300  # 5 minutes
        self.consensus_threshold = 2  # Alert when 2+ wallets buy same token
        
        # WebSocket connection
        self.websocket = None
        self.subscriptions = {}
        
        # Solana constants
        self.wsol_address = "So11111111111111111111111111111111111111112"

    def load_kol_wallets(self, file_path: str) -> Dict[str, Dict[str, str]]:
        """Load KOL wallets from JSON file (reusing logic from main script)"""
        try:
            with open(file_path, 'r') as f:
                content = f.read().strip()
            
            try:
                data = json.loads(content)
                
                if isinstance(data, list):
                    print(f"Found JSON list of {len(data)} addresses, auto-generating names...")
                    wallets = {}
                    for i, address in enumerate(data, 1):
                        wallets[address] = {
                            "name": f"KOL_{i}",
                            "twitter": f"@kol_{i}"
                        }
                    return wallets
                elif isinstance(data, dict) and len(data) == 0:
                    print(f"Found empty wallet file. Please add wallet addresses.")
                    return {}
                elif isinstance(data, dict):
                    first_key = next(iter(data)) if data else None
                    if first_key and isinstance(data[first_key], str):
                        # Old format conversion
                        wallets = {}
                        for i, (address, name) in enumerate(data.items(), 1):
                            wallets[address] = {
                                "name": name if name else f"KOL_{i}",
                                "twitter": f"@kol_{i}"
                            }
                        return wallets
                    else:
                        print(f"Successfully loaded {len(data)} wallets from {file_path}")
                        return data
                else:
                    raise ValueError("JSON file should contain a list of addresses or a dict of wallet info")
                    
            except json.JSONDecodeError:
                # Parse as simple text format
                lines = content.strip().split('\n')
                addresses = []
                
                for line in lines:
                    cleaned = line.strip().strip(',').strip('"').strip("'")
                    if cleaned and not cleaned.startswith('#') and not cleaned.startswith('//'):
                        addresses.append(cleaned)
                
                if addresses:
                    wallets = {}
                    for i, address in enumerate(addresses, 1):
                        wallets[address] = {
                            "name": f"KOL_{i}",
                            "twitter": f"@kol_{i}"
                        }
                    return wallets
                else:
                    return {}
                    
        except FileNotFoundError:
            print(f"Warning: {file_path} not found. Please create the file with wallet addresses.")
            return {}

    async def connect_websocket(self):
        """Establish WebSocket connection to Helius"""
        try:
            print("Connecting to Helius WebSocket...")
            self.websocket = await websockets.connect(
                self.websocket_url,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10
            )
            print("WebSocket connected successfully")
            return True
        except Exception as e:
            print(f"Failed to connect to WebSocket: {e}")
            return False

    async def subscribe_to_wallet(self, wallet_address: str, wallet_info: dict):
        """Subscribe to account changes for a specific wallet using standard Solana RPC"""
        subscription_request = {
            "jsonrpc": "2.0",
            "id": len(self.subscriptions) + 1,
            "method": "accountSubscribe",
            "params": [
                wallet_address,
                {
                    "commitment": "confirmed",
                    "encoding": "jsonParsed"
                }
            ]
        }
        
        try:
            await self.websocket.send(json.dumps(subscription_request))
            
            # Wait for subscription confirmation
            response = await self.websocket.recv()
            response_data = json.loads(response)
            
            if "result" in response_data:
                subscription_id = response_data["result"]
                self.subscriptions[subscription_id] = {
                    "wallet": wallet_address,
                    "info": wallet_info
                }
                print(f"Subscribed to {wallet_info['name']} ({wallet_address[:8]}...)")
                return subscription_id
            else:
                print(f"Failed to subscribe to {wallet_address}: {response_data}")
                return None
                
        except Exception as e:
            print(f"Error subscribing to {wallet_address}: {e}")
            return None

    async def subscribe_to_all_wallets(self):
        """Subscribe to all KOL wallets"""
        print(f"Subscribing to {len(self.kol_wallets)} KOL wallets...")
        
        for wallet_address, wallet_info in self.kol_wallets.items():
            await self.subscribe_to_wallet(wallet_address, wallet_info)
            # Small delay to avoid overwhelming the connection
            await asyncio.sleep(0.1)
        
        print(f"Subscribed to {len(self.subscriptions)} wallets successfully")

    def parse_transaction_for_tokens(self, log_data: dict) -> List[dict]:
        """Parse transaction logs to extract token purchase information"""
        transactions = []
        
        try:
            if "logs" not in log_data or "signature" not in log_data:
                return transactions
            
            logs = log_data["logs"]
            signature = log_data["signature"]
            
            # Look for token transfer patterns in logs
            # This is a simplified parser - you might need more sophisticated parsing
            for log in logs:
                if "Transfer" in log and "Token" in log:
                    # Extract token information from log
                    # This would need more sophisticated parsing based on actual log formats
                    transaction = {
                        "signature": signature,
                        "timestamp": datetime.now().isoformat(),
                        "log": log,
                        "parsed": False  # Indicates this needs more parsing
                    }
                    transactions.append(transaction)
        
        except Exception as e:
            print(f"Error parsing transaction: {e}")
        
        return transactions

    def detect_consensus_opportunities(self, token_mint: str) -> bool:
        """Check if a token meets consensus criteria (multiple KOLs buying within time window)"""
        if token_mint not in self.token_activity:
            return False
        
        activity = self.token_activity[token_mint]
        
        # Check if we have enough unique wallets
        if len(activity['wallets']) < self.consensus_threshold:
            return False
        
        # Check if purchases happened within the time window
        now = datetime.now()
        recent_transactions = []
        
        for tx in activity['transactions']:
            tx_time = datetime.fromisoformat(tx['timestamp'].replace('Z', '+00:00'))
            if (now - tx_time).total_seconds() <= self.consensus_window:
                recent_transactions.append(tx)
        
        # Check if recent transactions involve multiple wallets
        recent_wallets = set()
        for tx in recent_transactions:
            if 'wallet' in tx:
                recent_wallets.add(tx['wallet'])
        
        return len(recent_wallets) >= self.consensus_threshold

    def send_consensus_alert(self, token_mint: str):
        """Send alert for consensus opportunity"""
        activity = self.token_activity[token_mint]
        wallet_names = []
        
        for wallet_addr in activity['wallets']:
            if wallet_addr in self.kol_wallets:
                wallet_names.append(self.kol_wallets[wallet_addr]['name'])
        
        alert_message = f"""
🚨 CONSENSUS ALERT 🚨
Token: {activity['token_symbol']} ({token_mint[:8]}...)
KOLs involved: {', '.join(wallet_names)}
Number of KOLs: {len(activity['wallets'])}
Time window: {self.consensus_window/60:.1f} minutes
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        print(alert_message)
        
        # Here you could add integrations for:
        # - Discord notifications
        # - Telegram bot
        # - Email alerts
        # - Push notifications
        # - Webhook to trading bot

    async def process_transaction_notification(self, notification: dict):
        """Process incoming account change notifications from GetBlock/Solana RPC"""
        try:
            if "params" not in notification or "result" not in notification["params"]:
                return
            
            result = notification["params"]["result"]
            subscription_id = notification["params"]["subscription"]
            
            if subscription_id not in self.subscriptions:
                return
            
            wallet_info = self.subscriptions[subscription_id]
            wallet_address = wallet_info["wallet"]
            kol_name = wallet_info["info"]["name"]
            
            # GetBlock returns account changes, not transaction logs
            # You'll need to detect token changes from account data
            if "value" in result and "data" in result["value"]:
                account_data = result["value"]["data"]
                
                print(f"Account change detected for {kol_name} ({wallet_address[:8]}...)")
                
                # This is simplified - you'd need to parse the account data
                # to detect actual token purchases/transfers
                mock_token_mint = f"token_{len(self.token_activity)}"
                
                # Track token activity
                self.token_activity[mock_token_mint]['wallets'].add(wallet_address)
                self.token_activity[mock_token_mint]['transactions'].append({
                    'wallet': wallet_address,
                    'kol_name': kol_name,
                    'timestamp': datetime.now().isoformat(),
                    'account_change': True
                })
                
                if self.token_activity[mock_token_mint]['first_seen'] is None:
                    self.token_activity[mock_token_mint]['first_seen'] = datetime.now()
                
                # Check for consensus opportunities
                if self.detect_consensus_opportunities(mock_token_mint):
                    self.send_consensus_alert(mock_token_mint)
        
        except Exception as e:
            print(f"Error processing account notification: {e}")

    async def listen_for_transactions(self):
        """Main loop to listen for transaction notifications"""
        print("Starting real-time transaction monitoring...")
        print(f"Monitoring {len(self.kol_wallets)} KOL wallets")
        print(f"Consensus threshold: {self.consensus_threshold} KOLs within {self.consensus_window/60:.1f} minutes")
        print("Listening for transactions...")
        
        try:
            while True:
                message = await self.websocket.recv()
                data = json.loads(message)
                
                # Check if this is a transaction notification
                if "method" in data and data["method"] == "logsNotification":
                    await self.process_transaction_notification(data)
                
        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")
        except Exception as e:
            print(f"Error in transaction listener: {e}")

    async def start_monitoring(self):
        """Start the real-time monitoring system"""
        if len(self.kol_wallets) == 0:
            print("No wallets to monitor. Please add wallet addresses to kol_wallets.json")
            return
        
        # Connect to WebSocket
        if not await self.connect_websocket():
            return
        
        try:
            # Subscribe to all wallets
            await self.subscribe_to_all_wallets()
            
            # Start listening for transactions
            await self.listen_for_transactions()
            
        except KeyboardInterrupt:
            print("\nShutting down monitor...")
        except Exception as e:
            print(f"Error in monitoring: {e}")
        finally:
            if self.websocket:
                await self.websocket.close()
                print("WebSocket connection closed")

    def print_status(self):
        """Print current monitoring status"""
        print(f"\n=== Real-Time KOL Monitor Status ===")
        print(f"Wallets monitored: {len(self.kol_wallets)}")
        print(f"Active subscriptions: {len(self.subscriptions)}")
        print(f"Tokens tracked: {len(self.token_activity)}")
        print(f"Consensus threshold: {self.consensus_threshold} KOLs")
        print(f"Time window: {self.consensus_window/60:.1f} minutes")
        print("=====================================\n")

async def main():
    """Main function to run the real-time monitor"""
    try:
        # Initialize monitor
        monitor = RealTimeKOLMonitor("kol_wallets.json")
        monitor.print_status()
        
        # Start monitoring
        await monitor.start_monitoring()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())