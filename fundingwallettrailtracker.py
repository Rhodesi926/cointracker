import requests
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class WalletScanner:
    def __init__(self):
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        
        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY not found in environment variables")
        
        print(f"Connected to Helius API: {self.helius_api_key[:8]}...")
        self.helius_url = "https://api.helius.xyz/v0"
        
        # SOL has 9 decimals, so 1 SOL = 1,000,000,000 lamports
        self.lamports_per_sol = 1_000_000_000
        
    def validate_wallet_address(self, address: str) -> bool:
        """Check if wallet address is valid (should be 32-44 characters for Solana)"""
        if len(address) < 32 or len(address) > 44:
            return False
        # Basic check - should only contain base58 characters
        valid_chars = set('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz')
        return all(c in valid_chars for c in address)

    def get_wallet_transactions(self, wallet: str, days_back: int = 400) -> List[Dict]:
        """Get transactions for a specific wallet"""
        all_transactions = []
        before = None
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_back)
        cutoff_timestamp = cutoff_date.timestamp()
        
        print(f"Scanning transactions from {cutoff_date.strftime('%Y-%m-%d')} to now...")
        
        page = 1
        while True:
            print(f"Fetching page {page}...")
            
            url = f"{self.helius_url}/addresses/{wallet}/transactions"
            params = {
                'api-key': self.helius_api_key,
                'limit': 100,  # Maximum per request
            }
            
            if before:
                params['before'] = before
                
            try:
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    print("Rate limited, waiting 2 seconds...")
                    time.sleep(2)
                    continue
                    
                response.raise_for_status()
                transactions = response.json()
                
                if not transactions:
                    print("No more transactions found")
                    break
                
                # Filter transactions by date
                recent_txs = []
                oldest_tx_time = None
                
                for tx in transactions:
                    tx_time = tx['timestamp']
                    oldest_tx_time = tx_time  # Keep track of oldest in this batch
                    
                    if tx_time >= cutoff_timestamp:
                        recent_txs.append(tx)
                    else:
                        print(f"Reached cutoff date at transaction from {datetime.fromtimestamp(tx_time).strftime('%Y-%m-%d')}")
                        return all_transactions + recent_txs
                
                all_transactions.extend(recent_txs)
                print(f"Found {len(recent_txs)} transactions in this batch (total: {len(all_transactions)})")
                
                # Set up for next page
                if len(transactions) == 100:  # Full page, likely more data
                    before = transactions[-1]['signature']
                    page += 1
                    time.sleep(0.1)  # Small delay to be nice to API
                else:
                    print("Reached end of available transactions")
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"Error fetching transactions: {e}")
                time.sleep(2)
                continue
                
        return all_transactions

    def find_sol_transfers(self, transactions: List[Dict], source_wallet: str) -> List[Dict]:
        """Find SOL transfers between 3-5 SOL from the source wallet"""
        sol_transfers = []
        
        print(f"Analyzing {len(transactions)} transactions for SOL transfers...")
        
        for tx in transactions:
            try:
                # Look for native SOL transfers
                if 'nativeTransfers' in tx and tx['nativeTransfers']:
                    for transfer in tx['nativeTransfers']:
                        if (transfer.get('fromUserAccount') == source_wallet and 
                            transfer.get('toUserAccount') and 
                            transfer.get('toUserAccount') != source_wallet):
                            
                            amount_lamports = transfer.get('amount', 0)
                            amount_sol = amount_lamports / self.lamports_per_sol
                            
                            # Check if amount is between 3-5 SOL
                            if 3.0 <= amount_sol <= 5.0:
                                transfer_data = {
                                    'timestamp': datetime.fromtimestamp(tx['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                                    'to_wallet': transfer['toUserAccount'],
                                    'amount_sol': round(amount_sol, 4),
                                    'signature': tx['signature']
                                }
                                sol_transfers.append(transfer_data)
                                
            except Exception as e:
                print(f"Error parsing transaction {tx.get('signature', 'unknown')}: {e}")
                continue
                
        return sol_transfers

    def scan_wallet(self, wallet_address: str, days_back: int = 400) -> List[Dict]:
        """Main function to scan a wallet for SOL transfers"""
        print(f"\nScanning wallet: {wallet_address}")
        print(f"Looking for SOL transfers between 3-5 SOL over the last {days_back} days")
        print("-" * 60)
        
        # Get all transactions
        transactions = self.get_wallet_transactions(wallet_address, days_back)
        
        if not transactions:
            print("No transactions found")
            return []
            
        # Find SOL transfers in the 3-5 SOL range
        sol_transfers = self.find_sol_transfers(transactions, wallet_address)
        
        return sol_transfers

def main():
    try:
        scanner = WalletScanner()
        
        while True:
            wallet_input = input("\nEnter wallet address (or 'quit' to exit): ").strip()
            
            if wallet_input.lower() in ['quit', 'exit', 'q']:
                print("Goodbye!")
                break
                
            if not scanner.validate_wallet_address(wallet_input):
                print("❌ Invalid wallet address. Please enter a valid Solana wallet address.")
                continue
                
            # Scan the wallet
            transfers = scanner.scan_wallet(wallet_input)
            
            if transfers:
                print(f"\n🎯 Found {len(transfers)} SOL transfers between 3-5 SOL:")
                print("-" * 80)
                
                for i, transfer in enumerate(transfers, 1):
                    print(f"{i:2d}. {transfer['timestamp']} | {transfer['amount_sol']} SOL → {transfer['to_wallet']}")
                
                # Save to file
                filename = f"sol_transfers_{wallet_input[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                with open(filename, 'w') as f:
                    f.write(f"SOL Transfers (3-5 SOL) from wallet: {wallet_input}\n")
                    f.write(f"Scan date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("=" * 80 + "\n\n")
                    
                    for transfer in transfers:
                        f.write(f"{transfer['timestamp']} | {transfer['amount_sol']} SOL → {transfer['to_wallet']}\n")
                        f.write(f"Transaction: {transfer['signature']}\n\n")
                
                print(f"\n💾 Results saved to: {filename}")
                
            else:
                print("\n❌ No SOL transfers found between 3-5 SOL in the specified time period.")
                
    except KeyboardInterrupt:
        print("\n\nScan interrupted by user.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()