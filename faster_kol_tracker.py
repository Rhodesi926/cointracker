import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import json
import os
import time
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
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
            print(f"✅ Created template file at {file_path}")
            print("📝 Just paste wallet addresses one per line - no commas needed!")
            print("🔄 Then run the script again")
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
            'limit': 100,  # Increased from 50 to reduce API calls
            'parsed': 'true'
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    print(f"    Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                    
                response.raise_for_status()
                transactions = response.json()
                
                cutoff_time = datetime.now() - timedelta(hours=hours_back)
                recent_txs = []
                
                for tx in transactions:
                    tx_time = datetime.fromtimestamp(tx['timestamp'])
                    if tx_time >= cutoff_time:
                        recent_txs.append(tx)
                        
                return recent_txs
                
            except requests.exceptions.Timeout:
                print(f"    Timeout on attempt {attempt + 1}")
                if attempt == max_retries - 1:
                    print(f"    Failed after {max_retries} timeout attempts")
                    return []
                time.sleep(2)
                continue
            except requests.exceptions.RequestException as e:
                print(f"    Request error on attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    print(f"    Failed after {max_retries} attempts")
                    return []
                time.sleep(2)
                continue

    def parse_swap_transaction(self, tx: Dict, wallet: str) -> List[Dict]:
        """Parse swap transaction to extract token purchases"""
        trades = []
        
        try:
            if 'tokenTransfers' in tx:
                for transfer in tx['tokenTransfers']:
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
                                'timestamp': tx['timestamp'],
                                'signature': tx['signature']
                            }
                            trades.append(trade)
                            
        except Exception as e:
            print(f"Error parsing transaction: {e}")
            
        return trades

    def process_wallet_batch(self, wallet_batch: List[tuple], hours_back: int) -> List[Dict]:
        """Process a batch of wallets with rate limiting"""
        batch_trades = []
        
        for wallet_address, kol_info in wallet_batch:
            try:
                transactions = self.get_wallet_transactions(wallet_address, hours_back)
                
                trade_count = 0
                for tx in transactions:
                    trades = self.parse_swap_transaction(tx, wallet_address)
                    for trade in trades:
                        trade['kol_name'] = kol_info['name']
                        trade['kol_twitter'] = kol_info['twitter']
                        batch_trades.append(trade)
                        trade_count += len(trades)
                
                if trade_count > 0:
                    print(f"  {kol_info['name']}: Found {trade_count} trades")
                
                # Rate limiting: 50 RPS / 5 workers = 10 RPS per worker
                time.sleep(0.1)
                
            except Exception as e:
                print(f"  Error processing {kol_info['name']}: {e}")
                continue
        
        return batch_trades

    def get_all_kol_trades_concurrent(self, hours_back: int = 12, start_from: int = 0, max_workers: int = 5) -> List[Dict]:
        """Process wallets concurrently while respecting rate limits"""
        all_trades = []
        wallets_list = list(self.kol_wallets.items())[start_from:]
        
        print(f"🚀 Processing {len(wallets_list)} wallets with {max_workers} concurrent workers...")
        print("⚡ Expected completion time: ~3-4 minutes for 1000 wallets")
        
        # Process in batches to respect rate limits
        batch_size = max_workers
        batches = []
        
        for i in range(0, len(wallets_list), batch_size):
            batch = wallets_list[i:i+batch_size]
            batches.append(batch)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            for i, batch in enumerate(batches):
                future = executor.submit(self.process_wallet_batch, batch, hours_back)
                futures.append(future)
            
            for i, future in enumerate(futures):
                try:
                    batch_trades = future.result()
                    all_trades.extend(batch_trades)
                    progress = ((i + 1) / len(futures)) * 100
                    print(f"📊 Progress: {progress:.1f}% - Batch {i+1}/{len(futures)} complete")
                except Exception as e:
                    print(f"❌ Error in batch {i+1}: {e}")
        
        return all_trades

    def get_all_kol_trades(self, hours_back: int = 12, start_from: int = 0) -> List[Dict]:
        """Get all trades from KOL wallets with resume capability (Sequential version)"""
        all_trades = []
        
        print(f"Fetching trades from {len(self.kol_wallets)} KOL wallets...")
        if start_from > 0:
            print(f"Resuming from wallet {start_from + 1}...")
        print("Running with timeouts and retry logic...")
        
        wallets_list = list(self.kol_wallets.items())
        
        for i in range(start_from, len(wallets_list)):
            wallet_address, kol_info = wallets_list[i]
            print(f"Processing {kol_info['name']} [{i+1}/{len(self.kol_wallets)}]...")
            
            try:
                transactions = self.get_wallet_transactions(wallet_address, hours_back)
                
                trade_count = 0
                for tx in transactions:
                    trades = self.parse_swap_transaction(tx, wallet_address)
                    for trade in trades:
                        trade['kol_name'] = kol_info['name']
                        trade['kol_twitter'] = kol_info['twitter']
                        all_trades.append(trade)
                        trade_count += len(trades)
                
                if trade_count > 0:
                    print(f"  Found {trade_count} trades")
                
                if (i + 1) % 10 == 0:
                    self.save_progress(all_trades, i + 1)
                    print(f"  Progress saved at wallet {i + 1}")
                
                if i < len(wallets_list) - 1:
                    print(f"  Waiting... ({i+1}/{len(self.kol_wallets)} complete)")
                    time.sleep(0.02)  # 50 RPS = 20ms between requests
                    
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

    def run_analysis(self, hours_back: int = 12, resume: bool = True, use_concurrent: bool = True, max_workers: int = 5) -> pd.DataFrame:
        """Run the complete analysis with optional concurrent processing"""
        if len(self.kol_wallets) == 0:
            print("❌ No wallets loaded! Please add wallet addresses to kol_wallets.json")
            return pd.DataFrame()
        
        if use_concurrent:
            print("🚀 Using concurrent processing for maximum speed...")
            all_trades = self.get_all_kol_trades_concurrent(hours_back, 0, max_workers)
        else:
            print("🐌 Using sequential processing...")
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
        
        print(f"🎯 Found {len(all_trades)} trades from KOLs")
        
        if not all_trades:
            return pd.DataFrame()
        
        results_df = self.aggregate_trades(all_trades)
        
        # Clean up progress file if using concurrent processing
        if use_concurrent:
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
        
        # Use concurrent processing by default for speed
        # Set use_concurrent=False if you want the old sequential method
        results = tracker.run_analysis(
            hours_back=72, 
            use_concurrent=True,  # 🚀 SPEED BOOST: Set to False for sequential processing
            max_workers=5         # Adjust based on your rate limits (5 works well for 50 RPS)
        )
        
        if not results.empty:
            print("\n🏆 TOP KOL PICKS (Last 72 hours):")
            print(results.to_string(index=False))
            
            results.to_csv('kol_tracking_results.csv', index=False)
            print(f"\n💾 Results saved to kol_tracking_results.csv")
            
            consensus_picks = results[results['unique_kols'] >= 2]
            if not consensus_picks.empty:
                print(f"\n🤝 CONSENSUS PICKS ({len(consensus_picks)} tokens with 2+ KOLs):")
                print(consensus_picks[['token_symbol', 'unique_kols', 'total_volume', 'kol_names']].to_string(index=False))
        else:
            print("No trading activity found in the specified time period.")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()