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

class KOLTracker:
    def __init__(self, kol_wallets_file: str = "kol_wallets.json"):
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        
        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY not found in environment variables")
        
        print(f"Using Helius API key: {self.helius_api_key[:8]}...")
        self.helius_url = "https://api.helius.xyz/v0"
        
        # Load KOL wallets from JSON file
        self.kol_wallets = self.load_kol_wallets(kol_wallets_file)
        print(f"Loaded {len(self.kol_wallets)} KOL wallets from {kol_wallets_file}")
        
        self.wsol_address = "So11111111111111111111111111111111111111112"
        
        # Rate limiting variables
        self.last_request_time = 0
        self.min_request_interval = 0.15  # 150ms between requests (6.7 req/sec, under 10 req/sec limit)

    def rate_limited_request(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    def load_kol_wallets(self, file_path: str) -> Dict[str, Dict[str, str]]:
        """Load KOL wallets from JSON file with fallback options"""
        try:
            with open(file_path, 'r') as f:
                content = f.read().strip()
            
            # Try to parse as JSON first
            try:
                data = json.loads(content)
                
                # Check if it's just a list of addresses or empty dict
                if isinstance(data, list):
                    print(f"Found JSON list of {len(data)} addresses, auto-generating names...")
                    wallets = self.generate_wallet_info_from_list(data)
                    # Save the enhanced version back to file
                    self.save_wallets_to_file(wallets, file_path)
                    return wallets
                elif isinstance(data, dict) and len(data) == 0:
                    print(f"Found empty wallet file. Please add wallet addresses.")
                    return {}
                elif isinstance(data, dict):
                    # Check if values are just strings (addresses only) or proper dict format
                    first_key = next(iter(data)) if data else None
                    if first_key and isinstance(data[first_key], str):
                        # Old format: {"address": "name"} - convert to new format
                        print("Converting old format to new format...")
                        wallets = {}
                        for i, (address, name) in enumerate(data.items(), 1):
                            wallets[address] = {
                                "name": name if name else f"KOL_{i}",
                                "twitter": f"@kol_{i}"
                            }
                        self.save_wallets_to_file(wallets, file_path)
                        return wallets
                    else:
                        print(f"Successfully loaded {len(data)} wallets from {file_path}")
                        return data
                else:
                    raise ValueError("JSON file should contain a list of addresses or a dict of wallet info")
                    
            except json.JSONDecodeError:
                # If JSON parsing fails, try to parse as simple text (one address per line)
                print("JSON parsing failed, trying to parse as simple text format...")
                return self.parse_text_addresses(content, file_path)
                
        except FileNotFoundError:
            print(f"Warning: {file_path} not found. Creating empty template...")
            self.create_empty_template_file(file_path)
            return {}

    def parse_text_addresses(self, content: str, file_path: str) -> Dict[str, Dict[str, str]]:
        """Parse wallet addresses from simple text format (one per line)"""
        lines = content.strip().split('\n')
        addresses = []
        
        for line in lines:
            # Clean up each line - remove whitespace, commas, quotes
            cleaned = line.strip().strip(',').strip('"').strip("'")
            # Skip empty lines and comments
            if cleaned and not cleaned.startswith('#') and not cleaned.startswith('//'):
                addresses.append(cleaned)
        
        if addresses:
            print(f"Found {len(addresses)} addresses in text format, converting to JSON...")
            wallets = self.generate_wallet_info_from_list(addresses)
            # Save as proper JSON format
            json_file_path = file_path.replace('.txt', '.json')
            self.save_wallets_to_file(wallets, json_file_path)
            print(f"Converted to JSON format and saved as {json_file_path}")
            return wallets
        else:
            print("No valid addresses found in text format")
            return {}

    def generate_wallet_info_from_list(self, addresses: List[str]) -> Dict[str, Dict[str, str]]:
        """Generate wallet info from a simple list of addresses"""
        wallets = {}
        for i, address in enumerate(addresses, 1):
            wallets[address] = {
                "name": f"KOL_{i}",
                "twitter": f"@kol_{i}"
            }
        return wallets

    def save_wallets_to_file(self, wallets: Dict[str, Dict[str, str]], file_path: str):
        """Save wallets dictionary to JSON file"""
        try:
            with open(file_path, 'w') as f:
                json.dump(wallets, f, indent=2)
            print(f"Saved {len(wallets)} wallets to {file_path}")
        except Exception as e:
            print(f"Error saving wallets to file: {e}")

    def create_empty_template_file(self, file_path: str):
        """Create an empty template file with instructions"""
        instructions = """# KOL Wallet Addresses
# SUPER EASY: Just paste wallet addresses one per line (no commas needed!)
# Example:
# BGA6DoNa6aJzRWWt1CGoF2GLtNdf2sN5s9eHi1Tgcjas
# 5xK1SVsWJ89Y1yNSPKRckkB5xzisRFoCFUFsaKsYAqm
# 4KPW4Gx9jcBcBpEXwAbx7WwLc4R6KQzTbTeWWFrk2HBr

# Delete these comments and add your addresses below:

"""
        
        try:
            with open(file_path, 'w') as f:
                f.write(instructions)
            print(f"Created template file at {file_path}")
            print("Just paste wallet addresses one per line - no commas needed!")
            print("Then run the script again")
        except Exception as e:
            print(f"Error creating template file: {e}")

    def add_wallet(self, address: str, name: str, twitter: str, file_path: str = "kol_wallets.json"):
        """Add a new wallet to the JSON file"""
        try:
            # Load existing wallets
            with open(file_path, 'r') as f:
                wallets = json.load(f)
            
            # Add new wallet
            wallets[address] = {"name": name, "twitter": twitter}
            
            # Save back to file
            with open(file_path, 'w') as f:
                json.dump(wallets, f, indent=2)
            
            # Update internal dictionary
            self.kol_wallets = wallets
            print(f"Added wallet {name} ({address}) to {file_path}")
            
        except Exception as e:
            print(f"Error adding wallet: {e}")

    def remove_wallet(self, address: str, file_path: str = "kol_wallets.json"):
        """Remove a wallet from the JSON file"""
        try:
            # Load existing wallets
            with open(file_path, 'r') as f:
                wallets = json.load(f)
            
            if address in wallets:
                removed_wallet = wallets.pop(address)
                
                # Save back to file
                with open(file_path, 'w') as f:
                    json.dump(wallets, f, indent=2)
                
                # Update internal dictionary
                self.kol_wallets = wallets
                print(f"Removed wallet {removed_wallet['name']} ({address}) from {file_path}")
            else:
                print(f"Wallet {address} not found in {file_path}")
                
        except Exception as e:
            print(f"Error removing wallet: {e}")

    def load_progress(self) -> tuple:
        """Load previous progress if it exists"""
        try:
            with open('kol_progress.json', 'r') as f:
                progress_data = json.load(f)
            
            print(f"Found previous progress from {progress_data['timestamp']}")
            print(f"Completed {progress_data['wallet_index']} wallets")
            
            return progress_data['trades'], progress_data['wallet_index']
        except FileNotFoundError:
            return [], 0

    def save_progress(self, trades: List[Dict], wallet_index: int):
        """Save current progress to a file"""
        progress_data = {
            'wallet_index': wallet_index,
            'trades': trades,
            'timestamp': datetime.now().isoformat()
        }
        
        with open('kol_progress.json', 'w') as f:
            json.dump(progress_data, f, indent=2)

    def get_wallet_transactions(self, wallet: str, hours_back: int = 24) -> List[Dict]:
        """Get transactions for a specific wallet using Helius API"""
        url = f"{self.helius_url}/addresses/{wallet}/transactions"
        
        params = {
            'api-key': self.helius_api_key,
            'limit': 50,
            'parsed': 'true'
        }
        
        max_retries = 5  # Increased retry count
        base_wait_time = 1
        
        for attempt in range(max_retries):
            # Apply rate limiting before each request
            self.rate_limited_request()
            
            try:
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    wait_time = base_wait_time * (2 ** attempt)  # Exponential backoff
                    print(f"    Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                if response.status_code == 404:
                    print(f"    Wallet not found: {wallet}")
                    return []
                
                if response.status_code != 200:
                    print(f"    HTTP {response.status_code}: {response.text[:100]}")
                    if attempt == max_retries - 1:
                        return []
                    time.sleep(base_wait_time * (attempt + 1))
                    continue
                    
                response.raise_for_status()
                transactions = response.json()
                
                # Handle None response
                if transactions is None:
                    print(f"    API returned None for wallet: {wallet}")
                    return []
                
                # Handle empty list
                if not isinstance(transactions, list):
                    print(f"    Unexpected response format: {type(transactions)}")
                    return []
                    
                cutoff_time = datetime.now() - timedelta(hours=hours_back)
                recent_txs = []
                
                for tx in transactions:
                    if tx and 'timestamp' in tx:
                        tx_time = datetime.fromtimestamp(tx['timestamp'])
                        if tx_time >= cutoff_time:
                            recent_txs.append(tx)
                        
                return recent_txs
                
            except requests.exceptions.Timeout:
                print(f"    Timeout on attempt {attempt + 1}")
                if attempt == max_retries - 1:
                    print(f"    Failed after {max_retries} timeout attempts")
                    return []
                time.sleep(base_wait_time * (attempt + 1))
                continue
                
            except requests.exceptions.RequestException as e:
                print(f"    Request error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    print(f"    Failed after {max_retries} attempts")
                    return []
                time.sleep(base_wait_time * (attempt + 1))
                continue
                
            except Exception as e:
                print(f"    Unexpected error: {e}")
                if attempt == max_retries - 1:
                    return []
                time.sleep(base_wait_time * (attempt + 1))
                continue
        
        return []

    def parse_swap_transaction(self, tx: Dict, wallet: str) -> List[Dict]:
        """Parse swap transaction to extract token purchases"""
        trades = []
        
        try:
            if not tx or 'tokenTransfers' not in tx:
                return trades
                
            token_transfers = tx.get('tokenTransfers', [])
            if not token_transfers:
                return trades
                
            for transfer in token_transfers:
                if (transfer.get('toUserAccount') == wallet and 
                    transfer.get('mint') != self.wsol_address and
                    transfer.get('mint')):
                    
                    token_amount = transfer.get('tokenAmount', 0)
                    usd_amount = token_amount * 0.01 if token_amount else 100
                    
                    if usd_amount > 100:
                        trade = {
                            'wallet': wallet,
                            'token_mint': transfer['mint'],
                            'token_symbol': transfer.get('tokenSymbol', 'Unknown'),
                            'amount_usd': usd_amount,
                            'timestamp': tx.get('timestamp', 0),
                            'signature': tx.get('signature', '')
                        }
                        trades.append(trade)
                        
        except Exception as e:
            print(f"Error parsing transaction: {e}")
            
        return trades

    def get_all_kol_trades(self, hours_back: int = 12, start_from: int = 0) -> List[Dict]:
        """Get all trades from KOL wallets with resume capability"""
        all_trades = []
        
        total_wallets = len(self.kol_wallets)
        estimated_time = total_wallets * self.min_request_interval / 60  # minutes
        
        print(f"Fetching trades from {total_wallets} KOL wallets...")
        print(f"Estimated time: {estimated_time:.1f} minutes at {1/self.min_request_interval:.1f} req/sec")
        
        if start_from > 0:
            print(f"Resuming from wallet {start_from + 1}...")
        print("Running with rate limiting and enhanced retry logic...")
        
        wallets_list = list(self.kol_wallets.items())
        start_time = time.time()
        
        for i in range(start_from, len(wallets_list)):
            wallet_address, kol_info = wallets_list[i]
            
            # Progress reporting
            elapsed = time.time() - start_time
            if i > start_from:
                avg_time_per_wallet = elapsed / (i - start_from)
                remaining_wallets = len(wallets_list) - i - 1
                eta_seconds = remaining_wallets * avg_time_per_wallet
                eta_minutes = eta_seconds / 60
                print(f"Processing {kol_info['name']} [{i+1}/{total_wallets}] - ETA: {eta_minutes:.1f}min")
            else:
                print(f"Processing {kol_info['name']} [{i+1}/{total_wallets}]...")
            
            try:
                transactions = self.get_wallet_transactions(wallet_address, hours_back)
                
                trade_count = 0
                if transactions:  # Check if transactions is not empty
                    for tx in transactions:
                        if tx:  # Check if tx is not None
                            trades = self.parse_swap_transaction(tx, wallet_address)
                            for trade in trades:
                                trade['kol_name'] = kol_info['name']
                                trade['kol_twitter'] = kol_info['twitter']
                                all_trades.append(trade)
                            trade_count += len(trades)
                
                if trade_count > 0:
                    print(f"  Found {trade_count} trades")
                
                # Save progress every 50 wallets
                if (i + 1) % 50 == 0:
                    self.save_progress(all_trades, i + 1)
                    print(f"  Progress saved at wallet {i + 1}")
                    
            except KeyboardInterrupt:
                print(f"\nInterrupted! Saving progress at wallet {i + 1}...")
                self.save_progress(all_trades, i + 1)
                print(f"Progress saved. Resume by running the script again.")
                raise
            except Exception as e:
                print(f"  Error processing {kol_info['name']}: {e}")
                continue
                    
        return all_trades

    def aggregate_trades(self, trades: List[Dict]) -> pd.DataFrame:
        """Aggregate trades by token"""
        token_stats = defaultdict(lambda: {
            'token_symbol': '',
            'unique_kols': set(),
            'total_buys': 0,
            'total_volume': 0,
            'kol_names': set()
        })
    
        for trade in trades:
            token_mint = trade['token_mint']
            token_stats[token_mint]['token_symbol'] = trade['token_symbol']
            token_stats[token_mint]['unique_kols'].add(trade['kol_name'])
            token_stats[token_mint]['kol_names'].add(trade['kol_name'])
            token_stats[token_mint]['total_buys'] += 1
            token_stats[token_mint]['total_volume'] += trade['amount_usd']
    
        results = []
        for token_mint, stats in token_stats.items():
            results.append({
                'token_symbol': stats['token_symbol'],
                'contract_address': token_mint,
                'unique_kols': len(stats['unique_kols']),
                'total_buys': stats['total_buys'],
                'total_volume': round(stats['total_volume'], 2),
                'kol_names': ', '.join(sorted(stats['kol_names'])),
            })
    
        df = pd.DataFrame(results)
        return df.sort_values(['unique_kols', 'total_volume'], ascending=[False, False])

    def run_analysis(self, hours_back: int = 12, resume: bool = True) -> pd.DataFrame:
        """Run the complete analysis with resume capability"""
        if len(self.kol_wallets) == 0:
            print("No wallets loaded! Please add wallet addresses to kol_wallets.json")
            return pd.DataFrame()
        
        if resume:
            existing_trades, start_from = self.load_progress()
            if existing_trades:
                user_input = input(f"Resume from wallet {start_from + 1}? (y/n): ").lower()
                if user_input == 'y':
                    new_trades = self.get_all_kol_trades(hours_back, start_from)
                    all_trades = existing_trades + new_trades
                else:
                    all_trades = self.get_all_kol_trades(hours_back, 0)
            else:
                all_trades = self.get_all_kol_trades(hours_back, 0)
        else:
            all_trades = self.get_all_kol_trades(hours_back, 0)
        
        print(f"Found {len(all_trades)} trades from KOLs")
        
        if not all_trades:
            return pd.DataFrame()
        
        results_df = self.aggregate_trades(all_trades)
        
        try:
            os.remove('kol_progress.json')
            print("Progress file cleaned up")
        except FileNotFoundError:
            pass
            
        return results_df

def main():
    try:
        # You can specify a different JSON file path here if needed
        tracker = KOLTracker("kol_wallets.json")
        results = tracker.run_analysis(hours_back=4)
        
        if not results.empty:
            print("\nTOP KOL PICKS (Last 72 hours):")
            print(results.to_string(index=False))
            
            results.to_csv('kol_tracking_results.csv', index=False)
            print(f"\nResults saved to kol_tracking_results.csv")
            
            consensus_picks = results[results['unique_kols'] >= 2]
            if not consensus_picks.empty:
                print(f"\nCONSENSUS PICKS ({len(consensus_picks)} tokens with 2+ KOLs):")
                print(consensus_picks[['token_symbol', 'unique_kols', 'total_volume', 'kol_names']].to_string(index=False))
        else:
            print("No trading activity found in the specified time period.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()