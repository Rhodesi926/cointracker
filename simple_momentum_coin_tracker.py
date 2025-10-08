import requests
import pandas as pd
from datetime import datetime
import json
import os
import time
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

class NewGemsScanner:
    def __init__(self):
        self.birdeye_api_key = os.getenv('BIRDEYE_API_KEY')
        
        if not self.birdeye_api_key:
            raise ValueError("BIRDEYE_API_KEY not found in environment variables. Add it to your .env file")
        
        print(f"Using Birdeye API key: {self.birdeye_api_key[:8]}...")
        
        self.birdeye_url = "https://public-api.birdeye.so"
        self.dexscreener_url = "https://api.dexscreener.com/latest"
        self.solscan_url = "https://public-api.solscan.io"  # Using public API (no key needed)

    def get_latest_tokens(self, limit: int = 200) -> List[Dict]:
        """Get latest tokens from DexScreener - searches boosted tokens as proxy for new/trending"""
        # DexScreener doesn't have a direct "latest" endpoint
        # We'll use the search endpoint to find recently created tokens
        # Alternative: Use token profiles endpoint or search for new pairs
        
        url = "https://api.dexscreener.com/token-profiles/latest/v1"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            tokens = []
            seen_addresses = set()
            
            # Token profiles returns recently promoted tokens
            if isinstance(data, list):
                for item in data:
                    token_address = item.get('tokenAddress')
                    chain_id = item.get('chainId')
                    
                    # Only Solana tokens
                    if chain_id == 'solana' and token_address and token_address not in seen_addresses:
                        seen_addresses.add(token_address)
                        tokens.append({
                            'address': token_address,
                            'symbol': item.get('symbol', 'Unknown'),
                            'pair_created': 0
                        })
                        
                        if len(tokens) >= limit:
                            break
            
            if tokens:
                print(f"Found {len(tokens)} tokens from DexScreener profiles")
                return tokens
            
            # Fallback: If no tokens from profiles, we'll need to use a different approach
            # Try searching for pump.fun tokens (common new token platform)
            print("Trying fallback: searching for new tokens...")
            return self.get_tokens_fallback(limit)
            
        except Exception as e:
            print(f"Error fetching from DexScreener profiles: {e}")
            print("Trying fallback method...")
            return self.get_tokens_fallback(limit)
    
    def get_tokens_fallback(self, limit: int = 200) -> List[Dict]:
        """Fallback: Get tokens from Birdeye's new listings or trending tokens"""
        url = "https://public-api.birdeye.so/defi/v3/token/trending"
        
        headers = {
            'accept': 'application/json',
            'x-chain': 'solana',
            'X-API-KEY': self.birdeye_api_key
        }
        
        params = {
            'sort_by': 'rank',
            'sort_type': 'asc',
            'offset': 0,
            'limit': min(limit, 50)  # Birdeye may have limits
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            tokens = []
            
            if data.get('success') and data.get('data', {}).get('tokens'):
                for token in data['data']['tokens']:
                    tokens.append({
                        'address': token.get('address'),
                        'symbol': token.get('symbol', 'Unknown'),
                        'pair_created': 0
                    })
                
                print(f"Found {len(tokens)} trending tokens from Birdeye")
                return tokens
            
            return []
            
        except Exception as e:
            print(f"Fallback also failed: {e}")
            print("Unable to fetch token list. Please check API endpoints.")
            return []

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
        base_wait_time = 5
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=10)
                
                if response.status_code == 429:
                    wait_time = base_wait_time * (2 ** attempt)
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
                return {'liquidity': 0, 'price': 0}
        
        return {'liquidity': 0, 'price': 0}

    def get_token_holders(self, token_address: str) -> int:
        """Holder count disabled - requires paid Solscan Pro API"""
        return 0

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
                    'liquidity_dex': float(pair.get('liquidity', {}).get('usd', 0) or 0),
                    'price_change_5m': float(pair.get('priceChange', {}).get('m5', 0) or 0),
                    'price_change_1h': float(pair.get('priceChange', {}).get('h1', 0) or 0)
                }
            return {
                'market_cap': 0, 
                'volume_h1': 0, 
                'volume_h24': 0, 
                'liquidity_dex': 0,
                'price_change_5m': 0,
                'price_change_1h': 0
            }
            
        except Exception as e:
            return {
                'market_cap': 0, 
                'volume_h1': 0, 
                'volume_h24': 0, 
                'liquidity_dex': 0,
                'price_change_5m': 0,
                'price_change_1h': 0
            }
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
            return {'market_cap': 0, 'volume_h1': 0, 'volume_h24': 0, 'liquidity_dex': 0}

    def calculate_token_score(self, metrics: Dict[str, Any]) -> float:
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
        
        # Base scoring
        scores = {
            'market_cap': 0,
            'volume_sec': 0,
            'liquidity': 0
        }
        
        # INVERTED MARKET CAP SCORING (Lower = Better)
        if market_cap == 0:
            scores['market_cap'] = 0
        elif market_cap < 10_000:
            scores['market_cap'] = 25 * (market_cap / 10_000)
        elif 10_000 <= market_cap <= 50_000:
            scores['market_cap'] = 50
        elif 50_000 < market_cap <= 200_000:
            scores['market_cap'] = 35
        elif 200_000 < market_cap <= 500_000:
            scores['market_cap'] = 20
        elif 500_000 < market_cap <= 1_000_000:
            scores['market_cap'] = 10
        else:
            scores['market_cap'] = 5
        
        # Volume/sec scoring with exponential bonuses
        if volume_per_sec >= TARGET_VOLUME_SEC:
            scores['volume_sec'] = 25
            multiplier = volume_per_sec / TARGET_VOLUME_SEC
            if multiplier >= 100:
                scores['volume_sec'] += 50
            elif multiplier >= 10:
                scores['volume_sec'] += 25
            elif multiplier >= 5:
                scores['volume_sec'] += 12.5
            elif multiplier >= 2:
                scores['volume_sec'] += 6.25
        else:
            scores['volume_sec'] = 25 * (volume_per_sec / TARGET_VOLUME_SEC)
        
        # Liquidity scoring with exponential bonuses
        if liquidity >= TARGET_LIQUIDITY:
            scores['liquidity'] = 25
            multiplier = liquidity / TARGET_LIQUIDITY
            if multiplier >= 100:
                scores['liquidity'] += 50
            elif multiplier >= 10:
                scores['liquidity'] += 25
            elif multiplier >= 5:
                scores['liquidity'] += 12.5
            elif multiplier >= 2:
                scores['liquidity'] += 6.25
        else:
            scores['liquidity'] = 25 * (liquidity / TARGET_LIQUIDITY)
        
        return sum(scores.values())

    def scan_and_score_tokens(self, max_scan: int = 200, min_score: int = 80, top_n: int = 25) -> pd.DataFrame:
        """Scan tokens and return top scorers"""
        print(f"\nFetching latest {max_scan} tokens...")
        
        tokens = self.get_latest_tokens(max_scan)
        
        if not tokens:
            print("No tokens found!")
            return pd.DataFrame()
        
        print(f"\nScanning and scoring {len(tokens)} tokens...")
        print(f"Looking for tokens with {min_score}%+ score...")
        
        results = []
        
        for idx, token in enumerate(tokens):
            token_address = token['address']
            print(f"  [{idx+1}/{len(tokens)}] Analyzing {token['symbol']}...")
            
            # Get metrics
            birdeye_data = self.get_token_metrics_birdeye(token_address)
            dex_data = self.get_token_metrics_dexscreener(token_address)
            holders = self.get_token_holders(token_address)
            
            metrics = {
                'market_cap': dex_data['market_cap'],
                'volume_h1': dex_data['volume_h1'],
                'liquidity': birdeye_data['liquidity'],
                'liquidity_dex': dex_data['liquidity_dex'],
                'price': birdeye_data['price'],
                'holders': holders
            }
            
            # Calculate score
            score = self.calculate_token_score(metrics)
            
            volume_per_sec = dex_data['volume_h1'] / 3600 if dex_data['volume_h1'] > 0 else 0
            
            # Add all tokens to see what we're getting
            results.append({
                'score': f"{score:.0f}%",
                'score_value': score,
                'symbol': token['symbol'],
                'contract_address': token_address,
                'market_cap': f"${metrics['market_cap']:,.0f}" if metrics['market_cap'] > 0 else "N/A",
                'liquidity': f"${max(metrics['liquidity'], metrics['liquidity_dex']):,.0f}" if max(metrics['liquidity'], metrics['liquidity_dex']) > 0 else "N/A",
                'volume_per_sec': f"${volume_per_sec:.2f}/s" if volume_per_sec > 0 else "N/A",
                'price': f"${metrics['price']:.8f}" if metrics['price'] > 0 else "N/A"
            })
            
            if score >= min_score:
                print(f"    ✅ FOUND GEM: {score:.0f}% score!")
            else:
                print(f"    Score: {score:.0f}%")
            
            # Rate limiting
            time.sleep(4)
        
        if not results:
            print(f"\nNo tokens found.")
            return pd.DataFrame()
        
        # Create DataFrame and sort by score
        df = pd.DataFrame(results)
        df = df.sort_values('score_value', ascending=False)
        
        # Show all results first, then filter for top scorers
        print(f"\n📊 ALL SCANNED TOKENS (showing scores):")
        print(df[['score', 'symbol', 'market_cap', 'volume_per_sec']].to_string(index=False))
        
        # Filter for gems above threshold
        gems_df = df[df['score_value'] >= min_score].copy()
        
        if gems_df.empty:
            print(f"\n❌ No tokens scored {min_score}%+")
            print(f"💡 Highest score found: {df['score'].iloc[0]}")
            # Return top N anyway
            return df.head(top_n).drop('score_value', axis=1)
        
        gems_df = gems_df.drop('score_value', axis=1)
        return gems_df.head(top_n)

def main():
    try:
        scanner = NewGemsScanner()
        
        # Scan up to 200 tokens, find top 25 with 80%+ score
        results = scanner.scan_and_score_tokens(
            max_scan=200,
            min_score=80,
            top_n=25
        )
        
        if not results.empty:
            print(f"\n🏆 TOP TOKENS FOUND:")
            print(results.to_string(index=False))
            
            results.to_csv('new_gems_scanner.csv', index=False)
            print(f"\n💾 Results saved to new_gems_scanner.csv")
            
            # Show summary
            gems_count = len(results[results['score'].str.rstrip('%').astype(float) >= 80])
            print(f"\n📊 SUMMARY:")
            print(f"Total tokens scanned: {len(results)}")
            print(f"Tokens with 80%+ score: {gems_count}")
            if len(results) > 0:
                print(f"Highest score: {results['score'].iloc[0]}")
        else:
            print("\n❌ No tokens found. API endpoints may have changed or rate limits hit.")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()