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
        self.birdeye_api_key = os.getenv('BIRDEYE_API_KEY')
        
        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY not found in environment variables")
        
        if not self.birdeye_api_key:
            raise ValueError("BIRDEYE_API_KEY not found in environment variables. Add it to your .env file")
        
        print(f"Using Helius API key: {self.helius_api_key[:8]}...")
        print(f"Using Birdeye API key: {self.birdeye_api_key[:8]}...")
        
        self.helius_url = "https://api.helius.xyz/v0"
        self.birdeye_url = "https://public-api.birdeye.so"
        self.dexscreener_url = "https://api.dexscreener.com/latest"
        
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
            'limit': 100,
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

    def get_token_metrics_birdeye(self, token_address: str) -> Dict[str, Any]:
        """Get token liquidity from Birdeye API with retry logic for rate limits"""
        url = f"{self.birdeye_url}/defi/price"
        
        params = {
            'address': token_address,
            'include_liquidity': 'true'
        }
        
        headers = {
            'accept': 'application/json',
            'x-chain': 'solana',
            'X-API-KEY': self.birdeye_api_key
        }
        
        max_retries = 3
        base_wait_time = 5  # Start with 5 seconds
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=10)
                
                # Handle rate limiting with exponential backoff
                if response.status_code == 429:
                    wait_time = base_wait_time * (2 ** attempt)  # 5s, 10s, 20s
                    print(f"    Rate limited! Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                if data.get('success') and data.get('data'):
                    return {
                        'liquidity': data['data'].get('liquidity', 0),
                        'price': data['data'].get('value', 0)
                    }
                return {'liquidity': 0, 'price': 0}
                
            except requests.exceptions.HTTPError as e:
                if attempt == max_retries - 1:
                    print(f"    Birdeye failed after {max_retries} attempts for {token_address[:8]}")
                    return {'liquidity': 0, 'price': 0}
                continue
                
            except Exception as e:
                print(f"    Birdeye API error for {token_address[:8]}: {e}")
                return {'liquidity': 0, 'price': 0}
        
        return {'liquidity': 0, 'price': 0}

    def get_token_metrics_dexscreener(self, token_address: str) -> Dict[str, Any]:
        """Get token metrics from DexScreener API"""
        url = f"{self.dexscreener_url}/dex/tokens/{token_address}"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('pairs') and len(data['pairs']) > 0:
                # Get the most liquid pair
                pair = max(data['pairs'], key=lambda x: float(x.get('liquidity', {}).get('usd', 0)))
                
                return {
                    'market_cap': float(pair.get('fdv', 0) or pair.get('marketCap', 0) or 0),
                    'volume_h1': float(pair.get('volume', {}).get('h1', 0) or 0),
                    'volume_h24': float(pair.get('volume', {}).get('h24', 0) or 0),
                    'liquidity_dex': float(pair.get('liquidity', {}).get('usd', 0) or 0)
                }
            return {'market_cap': 0, 'volume_h1': 0, 'volume_h24': 0, 'liquidity_dex': 0}
            
        except Exception as e:
            print(f"    DexScreener API error for {token_address[:8]}: {e}")
            return {'market_cap': 0, 'volume_h1': 0, 'volume_h24': 0, 'liquidity_dex': 0}

    def get_token_holders(self, token_address: str) -> int:
        """Get token holder count - Currently returns 0 (N/A)
        
        Note: Helius doesn't provide holder count directly.
        Future options: Solscan API or on-chain calculation
        """
        return 0  # Return 0 to display as N/A

    def calculate_token_score(self, metrics: Dict[str, Any]) -> tuple[float, Dict[str, float]]:
        """
        Calculate token score based on metrics (0-100+ scale)
        
        INVERTED MARKET CAP STRATEGY:
        - Lower market cap = Higher score (early opportunity)
        - Ideal range: $10K-$50K (maximum points)
        - Paired with high volume/sec = momentum before the pump
        
        Targets:
        - Market Cap: $10K-$50K ideal range (50 pts max, inverse scoring)
        - Volume/sec: ≥$320/sec (25 pts)
        - Liquidity: ≥$50K (25 pts)
        
        Bonus for exceptional metrics (exponential):
        - 2x target = +25% bonus
        - 5x target = +50% bonus
        - 10x target = +100% bonus
        - 100x target = +200% bonus
        """
        
        # Targets
        TARGET_VOLUME_SEC = 320
        TARGET_LIQUIDITY = 50_000
        
        # Extract metrics
        market_cap = metrics.get('market_cap', 0)
        volume_h1 = metrics.get('volume_h1', 0)
        liquidity = max(metrics.get('liquidity', 0), metrics.get('liquidity_dex', 0))
        
        # Calculate volume per second
        volume_per_sec = volume_h1 / 3600 if volume_h1 > 0 else 0
        
        # Base scoring (50 pts for market cap, 25 pts each for volume & liquidity)
        scores = {
            'market_cap': 0,
            'volume_sec': 0,
            'liquidity': 0
        }
        
        # INVERTED MARKET CAP SCORING (Lower = Better)
        if market_cap == 0:
            scores['market_cap'] = 0  # Unknown/invalid
        elif market_cap < 10_000:
            # Too small/risky - partial credit
            scores['market_cap'] = 25 * (market_cap / 10_000)
        elif 10_000 <= market_cap <= 50_000:
            # IDEAL RANGE - Maximum points (50)
            scores['market_cap'] = 50
        elif 50_000 < market_cap <= 200_000:
            # Still good - decreasing score
            scores['market_cap'] = 35
        elif 200_000 < market_cap <= 500_000:
            # Okay - lower score
            scores['market_cap'] = 20
        elif 500_000 < market_cap <= 1_000_000:
            # Getting high - minimal score
            scores['market_cap'] = 10
        else:
            # > $1M - already pumped, very low score
            scores['market_cap'] = 5
        
        # Volume/sec scoring (HIGHER = BETTER, with exponential bonuses)
        if volume_per_sec >= TARGET_VOLUME_SEC:
            scores['volume_sec'] = 25
            multiplier = volume_per_sec / TARGET_VOLUME_SEC
            if multiplier >= 100:
                scores['volume_sec'] += 50  # 200% bonus
            elif multiplier >= 10:
                scores['volume_sec'] += 25  # 100% bonus
            elif multiplier >= 5:
                scores['volume_sec'] += 12.5  # 50% bonus
            elif multiplier >= 2:
                scores['volume_sec'] += 6.25  # 25% bonus
        else:
            scores['volume_sec'] = 25 * (volume_per_sec / TARGET_VOLUME_SEC)
        
        # Liquidity scoring (HIGHER = BETTER, with exponential bonuses)
        if liquidity >= TARGET_LIQUIDITY:
            scores['liquidity'] = 25
            multiplier = liquidity / TARGET_LIQUIDITY
            if multiplier >= 100:
                scores['liquidity'] += 50  # 200% bonus
            elif multiplier >= 10:
                scores['liquidity'] += 25  # 100% bonus
            elif multiplier >= 5:
                scores['liquidity'] += 12.5  # 50% bonus
            elif multiplier >= 2:
                scores['liquidity'] += 6.25  # 25% bonus
        else:
            scores['liquidity'] = 25 * (liquidity / TARGET_LIQUIDITY)
        
        # Total score
        total_score = sum(scores.values())
        
        return total_score, scores

    def enrich_tokens_with_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich aggregated tokens with metrics and scores"""
        print(f"\n📊 Fetching metrics for {len(df)} tokens...")
        
        enriched_data = []
        
        for idx, row in df.iterrows():
            token_address = row['contract_address']
            print(f"  [{idx+1}/{len(df)}] Analyzing {row['token_symbol']}...")
            
            # Get metrics from multiple sources
            birdeye_data = self.get_token_metrics_birdeye(token_address)
            dex_data = self.get_token_metrics_dexscreener(token_address)
            
            # Combine metrics (holders set to 0/N/A)
            metrics = {
                'market_cap': dex_data['market_cap'],
                'volume_h1': dex_data['volume_h1'],
                'liquidity': birdeye_data['liquidity'],
                'liquidity_dex': dex_data['liquidity_dex'],
                'price': birdeye_data['price']
            }
            
            # Calculate score
            score, score_breakdown = self.calculate_token_score(metrics)
            
            # Calculate volume per second
            volume_per_sec = dex_data['volume_h1'] / 3600 if dex_data['volume_h1'] > 0 else 0
            
            enriched_data.append({
                'score': f"{score:.0f}%",
                'score_value': score,  # For sorting
                'unique_kols': row['unique_kols'],
                'total_buys': row['total_buys'],
                'total_volume': row['total_volume'],
                'contract_address': row['contract_address'],
                'kol_names': row['kol_names'],
                'market_cap': f"${metrics['market_cap']:,.0f}" if metrics['market_cap'] > 0 else "N/A",
                'liquidity': f"${max(metrics['liquidity'], metrics['liquidity_dex']):,.0f}" if max(metrics['liquidity'], metrics['liquidity_dex']) > 0 else "N/A",
                'volume_per_sec': f"${volume_per_sec:.2f}/s" if volume_per_sec > 0 else "N/A"
            })
            
            # Rate limiting - be more aggressive to avoid hitting limits
            time.sleep(4)  # 4 seconds between each token analysis
        
        enriched_df = pd.DataFrame(enriched_data)
        
        # Sort by unique_kols (overlaps) first, then by score
        enriched_df = enriched_df.sort_values(['unique_kols', 'score_value'], ascending=[False, False])
        
        # Drop the score_value column (used only for sorting)
        enriched_df = enriched_df.drop('score_value', axis=1)
        
        return enriched_df

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
                    time.sleep(0.02)
                    
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
        
        # Aggregate trades
        results_df = self.aggregate_trades(all_trades)
        
        # Enrich with metrics and scores
        enriched_df = self.enrich_tokens_with_metrics(results_df)
        
        # Clean up progress file if using concurrent processing
        if use_concurrent:
            try:
                os.remove('kol_progress.json')
                print("Progress file cleaned up")
            except FileNotFoundError:
                pass
            
        return enriched_df

def main():
    try:
        tracker = KOLTracker("kol_wallets.json")
        
        results = tracker.run_analysis(
            hours_back=72, 
            use_concurrent=True,
            max_workers=5
        )
        
        if not results.empty:
            print("\n🏆 TOP KOL PICKS WITH RANKINGS (Last 72 hours):")
            print(results.to_string(index=False))
            
            results.to_csv('kol_tracking_results.csv', index=False)
            print(f"\n💾 Results saved to kol_tracking_results.csv")
            
            # Show high scoring tokens
            high_scores = results[results['score'].str.rstrip('%').astype(float) >= 100]
            if not high_scores.empty:
                print(f"\n⭐ EXCELLENT TOKENS ({len(high_scores)} tokens scoring 100%+):")
                print(high_scores[['score', 'unique_kols', 'market_cap', 'liquidity', 'volume_per_sec']].to_string(index=False))
            
            consensus_picks = results[results['unique_kols'] >= 2]
            if not consensus_picks.empty:
                print(f"\n🤝 CONSENSUS PICKS ({len(consensus_picks)} tokens with 2+ KOLs):")
                print(consensus_picks[['score', 'unique_kols', 'total_volume', 'kol_names']].to_string(index=False))
        else:
            print("No trading activity found in the specified time period.")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()