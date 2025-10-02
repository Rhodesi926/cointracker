import requests
import time
import json
import os
from collections import deque
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class WalletNetworkMapper:
    def __init__(self):
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY not found in environment variables")
            
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
        
        # Rate limiting
        self.requests_per_second = 2
        self.last_request_time = 0
        
        # Exclude system addresses
        self.exclude_addresses = {
            '11111111111111111111111111111112',  # System Program
            'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',  # Token Program
            'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',  # Associated Token Program
            'ComputeBudget111111111111111111111111111111',  # Compute Budget
            'SysvarRent111111111111111111111111111111111',   # Rent Sysvar
            'SysvarC1ock11111111111111111111111111111111',   # Clock Sysvar
        }
    
    def _rate_limit(self):
        """Implement rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.requests_per_second
        
        if time_since_last < min_interval:
            time.sleep(min_interval - time_since_last)
        
        self.last_request_time = time.time()
    
    def get_wallet_balance(self, wallet_address):
        """Get current SOL balance for a wallet"""
        try:
            rpc_url = f"https://rpc.helius.xyz/?api-key={self.helius_api_key}"
            
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [wallet_address]
            }
            
            self._rate_limit()
            response = self.session.post(rpc_url, json=payload)
            response.raise_for_status()
            
            balance_data = response.json()
            
            if "error" in balance_data:
                print(f"Balance Error for {wallet_address}: {balance_data['error']}")
                return 0.0
            
            # Convert lamports to SOL
            balance_lamports = balance_data.get("result", {}).get("value", 0)
            balance_sol = balance_lamports / 1e9
            
            return balance_sol
            
        except Exception as e:
            print(f"Error getting balance for {wallet_address}: {e}")
            return 0.0

    def get_wallet_transactions(self, wallet_address, limit=200):
        """Get all transactions for a wallet using Solana RPC"""
        try:
            rpc_url = f"https://rpc.helius.xyz/?api-key={self.helius_api_key}"
            
            # Get signatures for address
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [
                    wallet_address,
                    {"limit": limit}
                ]
            }
            
            self._rate_limit()
            response = self.session.post(rpc_url, json=payload)
            response.raise_for_status()
            
            signatures_data = response.json()
            
            if "error" in signatures_data:
                print(f"RPC Error for {wallet_address}: {signatures_data['error']}")
                return []
            
            signatures = signatures_data.get("result", [])
            print(f"Found {len(signatures)} signatures for {wallet_address}")
            
            if not signatures:
                return []
            
            # Get transaction details for each signature
            all_transactions = []
            
            # Process signatures in batches
            batch_size = 10
            for i in range(0, min(len(signatures), 50), batch_size):  # Limit to first 50 for speed
                batch = signatures[i:i + batch_size]
                
                for sig_info in batch:
                    signature = sig_info["signature"]
                    
                    # Get transaction details
                    tx_payload = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getTransaction",
                        "params": [
                            signature,
                            {
                                "encoding": "json",
                                "commitment": "confirmed",
                                "maxSupportedTransactionVersion": 0
                            }
                        ]
                    }
                    
                    self._rate_limit()
                    tx_response = self.session.post(rpc_url, json=tx_payload)
                    
                    if tx_response.status_code == 200:
                        tx_data = tx_response.json()
                        if "result" in tx_data and tx_data["result"]:
                            all_transactions.append(tx_data["result"])
                
                # Small delay between batches
                time.sleep(0.1)
            
            print(f"Successfully retrieved {len(all_transactions)} transaction details for {wallet_address}")
            return all_transactions
            
        except Exception as e:
            print(f"Error getting transactions for {wallet_address}: {e}")
            return []
    
    def find_largest_outbound(self, wallet_address, transactions):
        """Find the single largest SOL transfer OUT of a wallet"""
        largest_transfer = None
        max_amount = 0
        
        print(f"Analyzing {len(transactions)} transactions for outbound transfers...")
        
        for tx in transactions:
            try:
                # Get account keys and balance changes
                transaction_data = tx.get('transaction', {})
                message = transaction_data.get('message', {})
                account_keys = message.get('accountKeys', [])
                
                # Find the index of our wallet
                wallet_index = None
                for i, account in enumerate(account_keys):
                    if account == wallet_address:
                        wallet_index = i
                        break
                
                if wallet_index is None:
                    continue
                
                # Check balance changes
                meta = tx.get('meta', {})
                if 'preBalances' in meta and 'postBalances' in meta:
                    pre_balances = meta['preBalances']
                    post_balances = meta['postBalances']
                    
                    if wallet_index < len(pre_balances) and wallet_index < len(post_balances):
                        # Calculate change for our wallet (negative = outbound)
                        pre_balance = pre_balances[wallet_index]
                        post_balance = post_balances[wallet_index]
                        change = (post_balance - pre_balance) / 1e9  # Convert to SOL
                        
                        # Only interested in outbound transfers (negative change)
                        if change < -0.01:  # At least 0.01 SOL
                            outbound_amount = abs(change)
                            
                            # Find who received the funds (look for biggest positive change)
                            best_recipient = None
                            best_recipient_amount = 0
                            
                            for i, (pre, post) in enumerate(zip(pre_balances, post_balances)):
                                if i != wallet_index and i < len(account_keys):
                                    recipient_change = (post - pre) / 1e9
                                    
                                    # Look for positive changes (received funds)
                                    if recipient_change > 0.01 and recipient_change > best_recipient_amount:
                                        recipient = account_keys[i]
                                        if recipient not in self.exclude_addresses:
                                            best_recipient = recipient
                                            best_recipient_amount = recipient_change
                            
                            # If this is the largest outbound transfer we've seen
                            if outbound_amount > max_amount and best_recipient:
                                max_amount = outbound_amount
                                largest_transfer = {
                                    'amount': outbound_amount,
                                    'recipient': best_recipient,
                                    'timestamp': tx.get('blockTime', 0),
                                    'signature': tx.get('transaction', {}).get('signatures', [''])[0] if tx.get('transaction', {}).get('signatures') else ''
                                }
                                print(f"Found larger transfer: {outbound_amount:.4f} SOL → {best_recipient[:8]}...")
                
            except Exception as e:
                print(f"Error analyzing transfer: {e}")
                continue
        
        return largest_transfer

    def follow_money_trail(self, starting_wallet, max_depth=10):
        """Follow the money trail by tracking the single largest outbound transfer"""
        print(f"🔍 Starting money trail from: {starting_wallet}")
        print(f"📏 Max depth: {max_depth}")
        
        money_trail = []
        current_wallet = starting_wallet
        
        for depth in range(max_depth):
            print(f"\n=== STEP {depth + 1} ===")
            print(f"📋 Analyzing: {current_wallet}")
            
            # Get current balance
            balance = self.get_wallet_balance(current_wallet)
            print(f"💰 Balance: {balance:.4f} SOL")
            
            # Get transactions
            print("🔄 Fetching transactions...")
            transactions = self.get_wallet_transactions(current_wallet, limit=200)
            if not transactions:
                print(f"❌ No transactions found for {current_wallet}")
                break
            
            # Find the largest outbound transfer
            print("🔎 Looking for largest outbound transfer...")
            largest_transfer = self.find_largest_outbound(current_wallet, transactions)
            
            if not largest_transfer:
                print(f"❌ No outbound transfers found for {current_wallet}")
                # Add this wallet to trail even if it's the end
                wallet_info = {
                    'wallet': current_wallet,
                    'balance': balance,
                    'depth': depth,
                    'largest_outbound': 0,
                    'next_wallet': None,
                    'transfer_signature': None,
                    'transfer_timestamp': None
                }
                money_trail.append(wallet_info)
                break
            
            # Store wallet info
            wallet_info = {
                'wallet': current_wallet,
                'balance': balance,
                'depth': depth,
                'largest_outbound': largest_transfer['amount'],
                'next_wallet': largest_transfer['recipient'],
                'transfer_signature': largest_transfer['signature'],
                'transfer_timestamp': largest_transfer['timestamp']
            }
            
            money_trail.append(wallet_info)
            
            print(f"✅ Found largest transfer: {largest_transfer['amount']:.4f} SOL")
            print(f"🎯 Recipient: {largest_transfer['recipient']}")
            
            # Move to next wallet
            next_wallet = largest_transfer['recipient']
            
            # Check for loops
            if next_wallet in [w['wallet'] for w in money_trail]:
                print(f"🔄 Loop detected! {next_wallet} already analyzed.")
                break
            
            # Continue with next wallet
            current_wallet = next_wallet
        
        print(f"\n🏁 MONEY TRAIL COMPLETE")
        print(f"📊 Traced through {len(money_trail)} wallets")
        
        return money_trail
    
    def save_results(self, money_trail, filename_prefix="money_trail"):
        """Save money trail results to JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save money trail
        trail_file = f"{filename_prefix}_{timestamp}.json"
        with open(trail_file, 'w') as f:
            json.dump(money_trail, f, indent=2)
        
        print(f"Money trail saved to {trail_file}")
        return trail_file


def main():
    """Follow the money trail"""
    # Initialize the mapper
    mapper = WalletNetworkMapper()
    
    # Replace with your starting wallet address
    starting_wallet = "BDFMkurHjWM8HCibLRncxaSF8eQ3oJJztPSr4jUEo3nZ"
    
    # Follow the money trail
    money_trail = mapper.follow_money_trail(
        starting_wallet=starting_wallet,
        max_depth=8
    )
    
    # Save results
    mapper.save_results(money_trail)
    
    # Print detailed summary
    print(f"\n🎯 DETAILED MONEY TRAIL")
    for i, wallet_info in enumerate(money_trail):
        print(f"\nStep {i+1}:")
        print(f"  Wallet: {wallet_info['wallet']}")
        print(f"  Balance: {wallet_info['balance']:.4f} SOL")
        if wallet_info['largest_outbound'] > 0:
            print(f"  Sent: {wallet_info['largest_outbound']:.4f} SOL")
            print(f"  → To: {wallet_info['next_wallet']}")
    
    # Find the final wallet
    if money_trail:
        final_wallet = money_trail[-1]
        print(f"\n🎯 FINAL WALLET: {final_wallet['wallet']}")
        print(f"💰 Final Balance: {final_wallet['balance']:.4f} SOL")


if __name__ == "__main__":
    main()