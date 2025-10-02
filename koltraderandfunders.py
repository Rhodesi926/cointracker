import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import json
import os
import time
from dotenv import load_dotenv
from typing import List, Dict, Any, Set, Tuple

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
        
        # Cache for funding wallet relationships to avoid repeated API calls
        self.funding_cache = {}
        
        # Create reverse lookup for wallet addresses to KOL info
        self.address_to_kol = {}
        for address, info in self.kol_wallets.items():
            self.address_to_kol[address] = info

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

    def find_funding_wallet(self, wallet_address: str, days_back: int = 30) -> Tuple[str, str]:
        """Find the primary funding wallet for a given wallet address"""
        if wallet_address in self.funding_cache:
            return self.funding_cache[wallet_address]
        
        print(f"    🔍 Analyzing funding sources for {wallet_address[:8]}...")
        
        try:
            # Get transactions for the wallet to find funding sources
            url = f"{self.helius_url}/addresses/{wallet_address}/transactions"
            params = {
                'api-key': self.helius_api_key,
                'limit': 100,  # Get more transactions to find funding patterns
                'parsed': 'true'
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            transactions = response.json()
            
            # Look for SOL transfers TO this wallet (funding events)
            funding_sources = defaultdict(lambda: {'total_sol': 0, 'count': 0, 'last_funding': 0})
            cutoff_time = datetime.now() - timedelta(days=days_back)
            
            for tx in transactions:
                tx_time = datetime.fromtimestamp(tx['timestamp'])
                if tx_time < cutoff_time:
                    continue
                
                # Debug: print transaction structure for first few transactions
                if len(funding_sources) == 0:
                    print(f"    🔍 Debug - Transaction structure:")
                    print(f"    Keys: {list(tx.keys())}")
                    if 'nativeTransfers' in tx:
                        print(f"    Native transfers found: {len(tx['nativeTransfers'])}")
                        if tx['nativeTransfers']:
                            print(f"    Sample transfer: {tx['nativeTransfers'][0]}")
                
                # Look for native SOL transfers TO our target wallet
                native_transfers = tx.get('nativeTransfers', [])
                for transfer in native_transfers:
                    # Validate that we have proper wallet addresses
                    to_account = transfer.get('toUserAccount')
                    from_account = transfer.get('fromUserAccount')
                    
                    # Debug the transfer structure
                    print(f"    🔍 Transfer: {from_account[:8] if from_account else 'None'}... → {to_account[:8] if to_account else 'None'}...")
                    
                    # Validate addresses are proper Solana addresses (44 characters, base58)
                    if (to_account == wallet_address and 
                        from_account and 
                        from_account != wallet_address and
                        len(from_account) == 44 and  # Solana addresses are 44 chars
                        transfer.get('amount', 0) > 0.1 * 10**9):  # More than 0.1 SOL
                        
                        # Additional validation - check if it looks like a valid Solana address
                        try:
                            # Basic check - should be base58 and right length
                            import base58
                            base58.b58decode(from_account)
                            
                            sol_amount = transfer.get('amount', 0) / 10**9  # Convert lamports to SOL
                            funding_sources[from_account]['total_sol'] += sol_amount
                            funding_sources[from_account]['count'] += 1
                            funding_sources[from_account]['last_funding'] = max(
                                funding_sources[from_account]['last_funding'], 
                                tx['timestamp']
                            )
                            print(f"    ✅ Valid funding: {sol_amount:.2f} SOL from {from_account[:8]}...")
                            
                        except Exception:
                            print(f"    ❌ Invalid address format: {from_account}")
                            continue
            
            # Find the primary funding wallet (highest total SOL sent)
            if funding_sources:
                print(f"    📊 Found {len(funding_sources)} funding sources:")
                for addr, data in funding_sources.items():
                    print(f"      {addr[:8]}...: {data['total_sol']:.2f} SOL ({data['count']} transfers)")
                
                primary_funder = max(funding_sources.items(), 
                                   key=lambda x: (x[1]['total_sol'], x[1]['count']))
                primary_address = primary_funder[0]
                
                # Double-check the address is valid
                if len(primary_address) != 44:
                    print(f"    ❌ Invalid address length: {len(primary_address)} chars")
                    self.funding_cache[wallet_address] = (None, None)
                    return (None, None)
                
                # Check if this funding wallet is in our KOL list
                if primary_address in self.address_to_kol:
                    funder_info = self.address_to_kol[primary_address]
                    result = (primary_address, funder_info['name'])
                    print(f"    💰 Found known KOL funder: {funder_info['name']} ({primary_address})")
                else:
                    # Create a name for unknown funding wallet
                    funder_name = f"Funder_{primary_address[:8]}"
                    result = (primary_address, funder_name)
                    print(f"    💰 Found funding wallet: {funder_name} ({primary_address})")
                
                # Cache the result
                self.funding_cache[wallet_address] = result
                return result
            else:
                print(f"    ❌ No significant funding sources found")
                self.funding_cache[wallet_address] = (None, None)
                return (None, None)
                
        except Exception as e:
            print(f"    ❌ Error finding funding wallet: {e}")
            self.funding_cache[wallet_address] = (None, None)
            return (None, None)

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
            self.address_to_kol[address] = {"name": name, "twitter": twitter}
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
                
                # Update internal dictionaries
                self.kol_wallets = wallets
                if address in self.address_to_kol:
                    del self.address_to_kol[address]
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
            
            # Load funding cache if it exists
            if 'funding_cache' in progress_data:
                self.funding_cache = progress_data['funding_cache']
                print(f"Loaded {len(self.funding_cache)} funding relationships from cache")
            
            return progress_data['trades'], progress_data['wallet_index']
        except FileNotFoundError:
            return [], 0

    def save_progress(self, trades: List[Dict], wallet_index: int):
        """Save current progress to a file"""
        progress_data = {
            'wallet_index': wallet_index,
            'trades': trades,
            'funding_cache': self.funding_cache,  # Save funding cache too
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

    def get_all_kol_trades(self, hours_back: int = 12, start_from: int = 0, 
                          include_funding: bool = True) -> List[Dict]:
        """Get all trades from KOL wallets with resume capability and funding analysis"""
        all_trades = []
        
        print(f"Fetching trades from {len(self.kol_wallets)} KOL wallets...")
        if include_funding:
            print("🔍 Including funding wallet analysis...")
        if start_from > 0:
            print(f"Resuming from wallet {start_from + 1}...")
        print("Running with timeouts and retry logic...")
        
        wallets_list = list(self.kol_wallets.items())
        
        for i in range(start_from, len(wallets_list)):
            wallet_address, kol_info = wallets_list[i]
            print(f"Processing {kol_info['name']} [{i+1}/{len(self.kol_wallets)}]...")
            
            try:
                # Get funding wallet info if requested
                funding_address, funding_name = (None, None)
                if include_funding:
                    funding_address, funding_name = self.find_funding_wallet(wallet_address)
                
                transactions = self.get_wallet_transactions(wallet_address, hours_back)
                
                trade_count = 0
                for tx in transactions:
                    trades = self.parse_swap_transaction(tx, wallet_address)
                    for trade in trades:
                        trade['kol_name'] = kol_info['name']
                        trade['kol_twitter'] = kol_info['twitter']
                        
                        # Add funding wallet info
                        if funding_address and funding_name:
                            trade['funding_wallet'] = funding_address
                            trade['funding_name'] = funding_name
                        else:
                            trade['funding_wallet'] = None
                            trade['funding_name'] = None
                            
                        all_trades.append(trade)
                        trade_count += len(trades)
                
                if trade_count > 0:
                    print(f"  Found {trade_count} trades")
                    if funding_name:
                        print(f"  Funded by: {funding_name}")
                
                if (i + 1) % 10 == 0:
                    self.save_progress(all_trades, i + 1)
                    print(f"  Progress saved at wallet {i + 1}")
                
                if i < len(wallets_list) - 1:
                    print(f"  Waiting .005s... ({i+1}/{len(self.kol_wallets)} complete)")
                    time.sleep(.005)
                    
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
        """Aggregate trades by token, including funding wallet information"""
        token_stats = defaultdict(lambda: {
            'token_symbol': '',
            'unique_kols': set(),
            'unique_traders': set(),  # Track both traders and funders
            'total_buys': 0,
            'total_volume': 0,
            'kol_names': set(),
            'funding_info': set()
        })
    
        for trade in trades:
            token_mint = trade['token_mint']
            stats = token_stats[token_mint]
            
            # Basic trade info
            stats['token_symbol'] = trade['token_symbol']
            stats['unique_kols'].add(trade['kol_name'])
            stats['kol_names'].add(trade['kol_name'])
            stats['total_buys'] += 1
            stats['total_volume'] += trade['amount_usd']
            
            # Track unique traders (combination of trader + funder)
            trader_id = f"{trade['kol_name']} (trader)"
            stats['unique_traders'].add(trader_id)
            
            # Add funding wallet info if available
            if trade.get('funding_name'):
                funder_id = f"{trade['funding_name']} (funder)"
                stats['unique_traders'].add(funder_id)
                stats['funding_info'].add(f"{trade['kol_name']} ← {trade['funding_name']}")
            else:
                stats['funding_info'].add(f"{trade['kol_name']} (no funding data)")
    
        results = []
        for token_mint, stats in token_stats.items():
            # Create detailed KOL info string
            kol_details = []
            for info in sorted(stats['funding_info']):
                kol_details.append(info)
            
            results.append({
                'token_symbol': stats['token_symbol'],
                'contract_address': token_mint,
                'unique_kols': len(stats['unique_kols']),
                'unique_entities': len(stats['unique_traders']),  # Total unique traders + funders
                'total_buys': stats['total_buys'],
                'total_volume': round(stats['total_volume'], 2),
                'kol_details': ' | '.join(kol_details),
            })
    
        df = pd.DataFrame(results)
        return df.sort_values(['unique_entities', 'unique_kols', 'total_volume'], 
                             ascending=[False, False, False])

    def run_analysis(self, hours_back: int = 12, resume: bool = True, 
                    include_funding: bool = True) -> pd.DataFrame:
        """Run the complete analysis with resume capability and funding analysis"""
        if len(self.kol_wallets) == 0:
            print("❌ No wallets loaded! Please add wallet addresses to kol_wallets.json")
            return pd.DataFrame()
        
        if resume:
            existing_trades, start_from = self.load_progress()
            if existing_trades:
                user_input = input(f"Resume from wallet {start_from + 1}? (y/n): ").lower()
                if user_input == 'y':
                    new_trades = self.get_all_kol_trades(hours_back, start_from, include_funding)
                    all_trades = existing_trades + new_trades
                else:
                    all_trades = self.get_all_kol_trades(hours_back, 0, include_funding)
            else:
                all_trades = self.get_all_kol_trades(hours_back, 0, include_funding)
        else:
            all_trades = self.get_all_kol_trades(hours_back, 0, include_funding)
        
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
        
        # Run analysis with funding wallet detection enabled
        results = tracker.run_analysis(hours_back=12, include_funding=True)
        
        if not results.empty:
            print("\nTOP KOL PICKS WITH FUNDING ANALYSIS (Last 12 hours):")
            print(results.to_string(index=False))
            
            results.to_csv('kol_tracking_results_with_funding.csv', index=False)
            print(f"\nResults saved to kol_tracking_results_with_funding.csv")
            
            consensus_picks = results[results['unique_entities'] >= 3]  # 3+ entities (traders + funders)
            if not consensus_picks.empty:
                print(f"\nHIGH CONSENSUS PICKS ({len(consensus_picks)} tokens with 3+ entities):")
                print(consensus_picks[['token_symbol', 'unique_kols', 'unique_entities', 'total_volume', 'kol_details']].to_string(index=False))
        else:
            print("No trading activity found in the specified time period.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()