import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

class CommonTokensFinder:
    def __init__(self):
        self.api_key = os.getenv('HELIUS_API_KEY')
        if not self.api_key:
            raise ValueError("Please set HELIUS_API_KEY in .env file")

        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={self.api_key}"
        
        # Add wallet addresses here (comma separated)
        self.wallet_addresses = [
            "DNfuF1L62WWyW3pNakVkyGGFzVVhj4Yr52jSmdTyeBHm",
            "suqh5sHtr8HyJ7q8scBimULPkPpA557prMG47xCHQfK",
            "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o", 
            "BieeZkdnBAgNYknzo3RH2vku7FcPkFZMZmRJANh2TpW", 
            "CqbZDrJ2nMC8jdUdcevsBMb3GK1eiNZTPuixPMTd1ftr", 
            "6FCjkdFwpufcTuTaV9iPG94wnEvv6BvPe3ppDH853rGU",
            "3Q9PsNqp71xFJtUbvpWVNsxh1P1G77tGngNhWKxZ2hbX",
            "5FmAcKjZjSMTqzghfhwVbnHeuRaxifA7XHmRCv1U4afP",
            "8ECJ4iWesHKodBE65MeoLCsrftvfWEqbPXCjyG95qNUe",
            "2thk1oFT7LrNhK2mVHCJmdQV2zSLDFLnNfqMs45Zx3C1",
            "EJ3UpzC8k9ziuUjyEM9mGM2TKThRa1QcVrJBvbNy3myt",
            "2fmuvK1RJw3D7iWuF9CqCxKKjFyyCVWGdh9aL55UaEL8",
            "5zfhFP91Trn3siLYDMyd5dW4LS2bwuqWWq7Jb5gH3QNk",
            "HmfNkXQevLbqM52h9XV3GGnTRPWxxKR4mvJtxJxni68Z",
            "7jqvX5h9h1kpvfZVC4KwPmGZXwwLdJyNmNDLv3Pn5PKZ",
            "EfiwkmZzpaCVQNUzETzYHRsYMAnueWvXmqT3ryWTtQQP",
            "Fh29YWFsrxtdq9uUwqNqquecu9h6Sg6cCTcaG7szv9RJ",
            "FhJx2UCHRWAVwjHfRCpiyxgQmeRL2U27dTopwxwSMBq5",
            "B5Hq8B8avSoV22RhNCT4zHmf3FNdgEzoBkPWWHCbC9Vw"

        ]
        
        # Large stable coins to ignore
        self.excluded_tokens = {
            "So11111111111111111111111111111111111111112",  # Wrapped SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
            "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",  # ETH
            "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",  # BTC
            "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",   # mSOL
            "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",  # stSOL
            "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",   # bSOL
        }
        
        # Setup robust retries and session
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount('https://', adapter)
        self.timeout = 30

    def get_token_holder_count(self, mint):
        """Get the number of holders for a token"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccounts",
            "params": {
                "mint": mint
            }
        }
        
        try:
            response = self.session.post(self.rpc_url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            if 'result' in data and 'token_accounts' in data['result']:
                # Count unique holders with balance > 0
                holders = 0
                for account in data['result']['token_accounts']:
                    if float(account.get('amount', 0)) > 0:
                        holders += 1
                return holders
                
        except requests.exceptions.RequestException:
            pass
        
        return 0

    def get_wallet_tokens(self, wallet_address):
        """Get 200 most recently acquired tokens by a wallet"""
        print(f"    Getting recent transactions...")
        
        # Get recent transactions
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                wallet_address,
                {"limit": 1000}
            ]
        }
        
        try:
            response = self.session.post(self.rpc_url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            if 'result' not in data:
                return set()
            
            # Get all unique tokens from recent transactions (ordered newest to oldest)
            recent_tokens = []
            seen_tokens = set()
            
            for sig_info in data['result']:
                tokens_in_tx = self.get_tokens_from_transaction(sig_info['signature'], wallet_address)
                
                for token in tokens_in_tx:
                    if token not in seen_tokens and token not in self.excluded_tokens:
                        recent_tokens.append(token)
                        seen_tokens.add(token)
                        
                        # Stop when we have 200 unique tokens
                        if len(recent_tokens) >= 200:
                            break
                
                if len(recent_tokens) >= 200:
                    break
                
                # Rate limiting
                if len(recent_tokens) % 50 == 0:
                    import time
                    time.sleep(0.1)
            
            print(f"    Found {len(recent_tokens)} recent unique tokens")
            
            # Apply holder count filter
            filtered_tokens = set()
            low_holders_count = 0
            high_holders_count = 0
            
            for token in recent_tokens:
                holder_count = self.get_token_holder_count(token)
                
                # Filter by holder count: 50-5000 holders
                if holder_count < 50:
                    low_holders_count += 1
                    continue
                
                if holder_count > 5000:
                    high_holders_count += 1
                    continue
                
                filtered_tokens.add(token)
            
            print(f"    Filtered: {low_holders_count} tokens with <50 holders, {high_holders_count} tokens with >5000 holders")
            print(f"    Final count: {len(filtered_tokens)} qualifying tokens")
            
            return filtered_tokens
            
        except requests.exceptions.RequestException as e:
            print(f"Error getting recent transactions for {wallet_address}: {e}")
            return set()

    def get_tokens_from_transaction(self, signature, wallet_address):
        """Get all tokens involved in a transaction for this wallet"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getParsedTransaction",
            "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
        }
        
        try:
            response = self.session.post(self.rpc_url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            if 'result' not in data or not data['result']:
                return set()
            
            tx = data['result']
            meta = tx['meta']
            
            # Get all token balances for this wallet
            tokens_in_tx = set()
            
            # Check pre and post token balances
            for balance_list in [meta.get('preTokenBalances', []), meta.get('postTokenBalances', [])]:
                for balance in balance_list:
                    if balance.get('owner') == wallet_address:
                        tokens_in_tx.add(balance['mint'])
            
            return tokens_in_tx
            
        except requests.exceptions.RequestException:
            return set()

    def get_token_metadata(self, mint_address):
        """Get token metadata (name, symbol) for a mint address"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAsset",
            "params": {
                "id": mint_address
            }
        }
        
        try:
            response = self.session.post(self.rpc_url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            if 'result' in data and data['result']:
                content = data['result'].get('content', {})
                metadata = content.get('metadata', {})
                return {
                    'name': metadata.get('name', 'Unknown'),
                    'symbol': metadata.get('symbol', 'Unknown'),
                    'mint': mint_address
                }
                
        except requests.exceptions.RequestException:
            pass
        
        # Fallback: return mint address if metadata not found
        return {
            'name': f"Token {mint_address[:8]}...",
            'symbol': f"{mint_address[:6]}",
            'mint': mint_address
        }

    def find_common_tokens(self):
        """Find tokens that are shared by at least two wallets"""
        if not self.wallet_addresses:
            print("No wallet addresses specified! Please add wallet addresses to the wallet_addresses list.")
            return []
        
        print(f"Analyzing {len(self.wallet_addresses)} wallets for tokens shared by at least two wallets...")
        print("Excluded tokens: SOL, USDC, USDT, ETH, BTC, mSOL, stSOL, bSOL\n")
        
        # Get tokens for each wallet
        wallet_tokens = {}
        for i, wallet in enumerate(self.wallet_addresses, 1):
            print(f"Getting tokens for wallet {i}: {wallet}")
            tokens = self.get_wallet_tokens(wallet)
            wallet_tokens[wallet] = tokens
            print(f"  Found {len(tokens)} tokens")
        
        if not wallet_tokens:
            print("No tokens found in any wallet!")
            return []
        
        print(f"\nDetailed token counts per wallet:")
        for wallet, tokens in wallet_tokens.items():
            print(f"  {wallet[:8]}... has {len(tokens)} tokens")
        
        # Count token occurrences across all wallets
        token_counts = {}
        for wallet, tokens in wallet_tokens.items():
            for token in tokens:
                if token not in token_counts:
                    token_counts[token] = {'count': 0, 'wallets': []}
                token_counts[token]['count'] += 1
                token_counts[token]['wallets'].append(wallet)
        
        # Filter tokens that appear in at least two wallets
        shared_tokens = {token: info for token, info in token_counts.items() if info['count'] >= 2}
        
        print(f"\nFinal result: {len(shared_tokens)} tokens shared by at least two wallets")
        
        if not shared_tokens:
            print("No tokens found shared by at least two wallets!")
            
            # Show some debugging info
            print("\nDebugging info - tokens in each wallet:")
            for wallet, tokens in wallet_tokens.items():
                sample_tokens = list(tokens)[:5]  # Show first 5 tokens
                print(f"  {wallet[:8]}...: {sample_tokens}")
            
            return []
        
        # Get metadata for shared tokens
        shared_tokens_info = []
        for mint, info in shared_tokens.items():
            metadata = self.get_token_metadata(mint)
            metadata['wallets'] = info['wallets']  # Add the list of wallets holding this token
            shared_tokens_info.append(metadata)
            print(f"  {metadata['symbol']} ({metadata['name']}) - {mint} (Held by {len(info['wallets'])} wallets: {', '.join([w[:8] + '...' for w in info['wallets']])})")
        
        # Sort tokens by the number of wallets holding them (descending)
        shared_tokens_info.sort(key=lambda x: len(x['wallets']), reverse=True)
        
        # Save to file
        with open('common_tokens.txt', 'w') as f:
            f.write(f"Tokens Shared by At Least Two of {len(self.wallet_addresses)} Wallets:\n")
            f.write("=" * 50 + "\n\n")
            for token in shared_tokens_info:
                f.write(f"{token['symbol']} - {token['name']}\n")
                f.write(f"Mint: {token['mint']}\n")
                f.write(f"Held by {len(token['wallets'])} wallets: {', '.join([w[:8] + '...' for w in token['wallets']])}\n\n")
        
        print(f"\nResults saved to common_tokens.txt")
        return shared_tokens_info

if __name__ == "__main__":
    finder = CommonTokensFinder()
    common_tokens = finder.find_common_tokens()