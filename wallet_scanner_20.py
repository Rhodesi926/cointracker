import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class WalletScanner:
    def __init__(self):
        self.api_key = os.getenv('HELIUS_API_KEY')
        if not self.api_key:
            raise ValueError("Please set HELIUS_API_KEY in .env file")

        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={self.api_key}"
        self.found_wallets = set()
        self.target_count = 20
        
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
        self.timeout = 10

    def get_recent_signatures(self, offset=0):
        """Get recent signatures from Jupiter DEX"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
                {"limit": 100}
            ]
        }
        try:
            response = self.session.post(self.rpc_url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if 'result' in data:
                return [sig['signature'] for sig in data['result']]
        except requests.exceptions.Timeout:
            print("⏱️ Timeout occurred while getting recent signatures.")
        except requests.exceptions.ConnectionError:
            print("🔌 Connection error while getting recent signatures.")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error in get_recent_signatures: {e}")
        return []

    def get_wallet_from_transaction(self, signature):
        """Extract the main wallet (first signer) from transaction"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [signature, {"encoding": "json"}]
        }
        try:
            response = self.session.post(self.rpc_url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if 'result' in data and data['result']:
                try:
                    accounts = data['result']['transaction']['message']['accountKeys']
                    if accounts and len(accounts) > 0:
                        wallet = accounts[0]
                        # Basic validation
                        if len(wallet) == 44 and wallet not in [
                            "11111111111111111111111111111111",
                            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
                        ]:
                            return wallet
                except Exception as e:
                    print(f"⚠️ Inner exception parsing transaction: {e}")
        except requests.exceptions.Timeout:
            print(f"⏱️ Timeout occurred on get_wallet_from_transaction, sig={signature}")
        except requests.exceptions.ConnectionError:
            print(f"🔌 Connection error on get_wallet_from_transaction, sig={signature}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error in get_wallet_from_transaction: {e}")
        return None

    def scan_until_target(self):
        """Keep scanning until we have 20 unique wallets"""
        print(f"🎯 Target: Find {self.target_count} unique wallets\n")
        
        signatures_checked = 0

        while len(self.found_wallets) < self.target_count:
            print(f"📊 Progress: {len(self.found_wallets)}/{self.target_count} wallets found")
            
            # Get batch of signatures
            signatures = self.get_recent_signatures()
            if not signatures:
                print("⚠️  No signatures found, retrying...")
                time.sleep(2)
                continue
            
            # Check each signature
            for sig in signatures:
                if len(self.found_wallets) >= self.target_count:
                    break
                signatures_checked += 1
                wallet = self.get_wallet_from_transaction(sig)
                
                if wallet and wallet not in self.found_wallets:
                    self.found_wallets.add(wallet)
                    print(f"✅ [{len(self.found_wallets)}/{self.target_count}] Found: {wallet}")
                
                # Rate limiting
                time.sleep(0.05)
            
            if len(self.found_wallets) < self.target_count:
                print(f"⏳ Checked {signatures_checked} transactions so far...")
                time.sleep(1)

        print(f"\n🎉 Success! Found {self.target_count} unique wallets")
        
        # Save to file
        with open('wallets_found.txt', 'w') as f:
            for i, wallet in enumerate(self.found_wallets, 1):
                f.write(f"{wallet}\n")
        
        print("💾 Saved to wallets_found.txt")
        
        # Display summary
        print("\n📋 Wallet List:")
        for i, wallet in enumerate(list(self.found_wallets)[:20], 1):
            print(f"{i:2}. {wallet}")

        return list(self.found_wallets)

if __name__ == "__main__":
    scanner = WalletScanner()
    wallets = scanner.scan_until_target()
    print(f"\n✨ Done! Collected exactly {len(wallets)} wallets")
