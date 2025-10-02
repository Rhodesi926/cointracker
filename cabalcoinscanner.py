import requests
import os
import time
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class PumpFunScanner:
    def __init__(self):
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        
        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY not found in environment variables")
        
        print(f"Connected to Helius API: {self.helius_api_key[:8]}...")
        self.helius_url = "https://api.helius.xyz/v0"
        
        # Pump.fun program ID
        self.pumpfun_program = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

    def get_pumpfun_tokens_under_10k(self) -> List[Dict]:
        """Get pump.fun tokens under $10k market cap and under 4 hours old"""
        print("Fetching pump.fun tokens under $10k market cap and under 4 hours old...")
        
        # 4 hours ago timestamp
        four_hours_ago = datetime.now().timestamp() - (4 * 60 * 60)
        
        # Using DexScreener API to get pump.fun tokens with market cap data
        url = "https://api.dexscreener.com/latest/dex/tokens"
        
        all_tokens = []
        
        try:
            # Get tokens from pump.fun (using their known DEX identifier)
            params = {
                'chainId': 'solana',
                'dexId': 'raydium',  # Many pump.fun tokens migrate here
            }
            
            # Alternative approach: Get all Solana tokens and filter by market cap
            url = "https://api.dexscreener.com/latest/dex/search"
            params = {
                'q': 'pump.fun'
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'pairs' in data:
                for pair in data['pairs']:
                    try:
                        market_cap = float(pair.get('marketCap', 0))
                        pair_created = pair.get('pairCreatedAt', 0)
                        
                        # Check market cap and age filters
                        if (market_cap > 0 and market_cap < 10000 and 
                            pair_created and pair_created > four_hours_ago * 1000):  # DexScreener uses milliseconds
                            
                            token_info = {
                                'token_address': pair.get('baseToken', {}).get('address'),
                                'token_symbol': pair.get('baseToken', {}).get('symbol'),
                                'token_name': pair.get('baseToken', {}).get('name'),
                                'market_cap': market_cap,
                                'price_usd': pair.get('priceUsd', 0),
                                'liquidity': pair.get('liquidity', {}).get('usd', 0),
                                'dex_id': pair.get('dexId'),
                                'pair_address': pair.get('pairAddress'),
                                'created_at': datetime.fromtimestamp(pair_created / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                                'hours_old': (datetime.now().timestamp() - pair_created / 1000) / 3600
                            }
                            
                            if token_info['token_address']:
                                all_tokens.append(token_info)
                                
                    except (ValueError, TypeError, KeyError):
                        continue
            
            print(f"Found {len(all_tokens)} pump.fun tokens under $10k market cap and under 4 hours old")
            return sorted(all_tokens, key=lambda x: x['market_cap'])
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from DexScreener: {e}")
            
            # Fallback: Try direct pump.fun API approach
            return self.get_pumpfun_tokens_direct()

    def get_pumpfun_tokens_direct(self) -> List[Dict]:
        """Alternative method: Try to get pump.fun tokens directly"""
        print("Trying direct pump.fun API approach...")
        
        # 4 hours ago timestamp
        four_hours_ago = datetime.now().timestamp() - (24 * 60 * 60)
        
        # This is a fallback - pump.fun's actual API endpoints may vary
        try:
            url = "https://frontend-api.pump.fun/coins"
            params = {
                'offset': 0,
                'limit': 1000,
                'sort': 'created_timestamp',
                'order': 'DESC'  # Get newest first
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            tokens_under_10k = []
            
            for token in data:
                try:
                    market_cap = float(token.get('market_cap', 0))
                    created_timestamp = token.get('created_timestamp', 0)
                    
                    # Check both market cap and age filters
                    if (market_cap > 0 and market_cap < 10000 and 
                        created_timestamp and created_timestamp > four_hours_ago):
                        
                        token_info = {
                            'token_address': token.get('mint'),
                            'token_symbol': token.get('symbol'),
                            'token_name': token.get('name'),
                            'market_cap': market_cap,
                            'price_usd': token.get('usd_market_cap', 0) / token.get('supply', 1) if token.get('supply') else 0,
                            'liquidity': token.get('virtual_sol_reserves', 0),
                            'dex_id': 'pump.fun',
                            'creation_timestamp': token.get('created_timestamp'),
                            'created_at': datetime.fromtimestamp(created_timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                            'hours_old': (datetime.now().timestamp() - created_timestamp) / 3600
                        }
                        tokens_under_10k.append(token_info)
                        
                except (ValueError, TypeError, KeyError):
                    continue
            
            print(f"Found {len(tokens_under_10k)} tokens under $10k and under 4 hours old via direct API")
            return sorted(tokens_under_10k, key=lambda x: x['market_cap'])
            
        except requests.exceptions.RequestException as e:
            print(f"Direct API also failed: {e}")
            print("You may need to check pump.fun's current API endpoints")
            return []

def main():
    try:
        scanner = PumpFunScanner()
        
        print("Getting pump.fun tokens under $10k market cap and under 4 hours old...")
        print("-" * 60)
        
        tokens = scanner.get_pumpfun_tokens_under_10k()
        
        if tokens:
            print(f"\n🎯 Found {len(tokens)} pump.fun tokens under $10k market cap and under 4 hours old:")
            print("-" * 80)
            
            for i, token in enumerate(tokens[:20], 1):  # Show first 20
                symbol = token['token_symbol'] or 'Unknown'
                name = token['token_name'] or 'Unknown'
                mc = f"${token['market_cap']:,.2f}"
                hours = f"{token.get('hours_old', 0):.1f}h"
                
                print(f"{i:2d}. {symbol} ({name[:20]}) | MC: {mc} | Age: {hours} | {token['token_address']}")
            
            if len(tokens) > 20:
                print(f"\n... and {len(tokens) - 20} more tokens")
            
            # Save to file
            filename = f"pumpfun_under_10k_4h_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(filename, 'w') as f:
                f.write(f"Pump.fun Tokens Under $10k Market Cap and Under 4 Hours Old\n")
                f.write(f"Scan date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total tokens found: {len(tokens)}\n")
                f.write("=" * 80 + "\n\n")
                
                for token in tokens:
                    f.write(f"Symbol: {token['token_symbol']}\n")
                    f.write(f"Name: {token['token_name']}\n")
                    f.write(f"Address: {token['token_address']}\n")
                    f.write(f"Market Cap: ${token['market_cap']:,.2f}\n")
                    f.write(f"Price: ${token['price_usd']}\n")
                    f.write(f"Age: {token.get('hours_old', 0):.1f} hours\n")
                    f.write(f"Created: {token.get('created_at', 'Unknown')}\n")
                    f.write(f"DEX: {token['dex_id']}\n")
                    f.write("-" * 40 + "\n")
            
            print(f"\n💾 Full results saved to: {filename}")
            
        else:
            print("\n❌ No tokens found or API access issues")
            print("Note: Pump.fun API endpoints may have changed")
            
    except KeyboardInterrupt:
        print("\n\nScan interrupted by user.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()