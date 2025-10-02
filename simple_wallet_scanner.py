import os
import json
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Set
from dotenv import load_dotenv

load_dotenv()

class SimpleWalletScanner:
    def __init__(self):
        self.api_key = os.getenv('HELIUS_API_KEY')
        if not self.api_key:
            raise ValueError("Please set HELIUS_API_KEY in .env file")
        
        self.base_url = "https://api.helius.xyz/v0"
        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={self.api_key}"
        self.discovered_wallets = set()
    
    def get_recent_signatures(self, program_address: str, limit: int = 100) -> List[str]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                program_address,
                {"limit": limit, "commitment": "finalized"}
            ]
        }
        
        response = requests.post(self.rpc_url, json=payload)
        if response.status_code == 200:
            data = response.json()
            if 'result' in data:
                return [sig['signature'] for sig in data['result']]
        return []
    
    def get_transaction_wallets(self, signature: str) -> Set[str]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "finalized",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        }
        
        wallets = set()
        response = requests.post(self.rpc_url, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            if 'result' in data and data['result']:
                try:
                    accounts = data['result']['transaction']['message']['accountKeys']
                    for account in accounts:
                        pubkey = account.get('pubkey')
                        if pubkey and self.is_wallet(pubkey):
                            wallets.add(pubkey)
                except:
                    pass
        
        return wallets
    
    def is_wallet(self, address: str) -> bool:
        programs = [
            "11111111111111111111111111111111",
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        ]
        
        return address not in programs and len(address) == 44
    
    def scan_for_new_wallets(self, minutes_back: int = 5) -> List[str]:
        print(f"\n🔍 Scanning for wallets active in last {minutes_back} minutes...")
        
        dex_programs = [
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        ]
        
        new_wallets = []
        
        for program in dex_programs:
            print(f"  Checking {program[:8]}...")
            
            signatures = self.get_recent_signatures(program, limit=50)
            
            for sig in signatures[:20]:
                wallets = self.get_transaction_wallets(sig)
                
                for wallet in wallets:
                    if wallet not in self.discovered_wallets:
                        self.discovered_wallets.add(wallet)
                        new_wallets.append(wallet)
                        print(f"    ✅ New wallet: {wallet}")
                
                time.sleep(0.1)
        
        return new_wallets
    
    def get_wallet_info(self, wallet: str) -> Dict:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [wallet]
        }
        
        response = requests.post(self.rpc_url, json=payload)
        sol_balance = 0
        
        if response.status_code == 200:
            data = response.json()
            if 'result' in data:
                sol_balance = data['result']['value'] / 1e9
        
        return {
            "address": wallet,
            "sol_balance": sol_balance,
            "discovered_at": datetime.now().isoformat()
        }
    
    def run_continuous_scan(self, interval_seconds: int = 60):
        print("🚀 Starting continuous wallet scanner...")
        print(f"   Scanning every {interval_seconds} seconds")
        print("   Press Ctrl+C to stop\n")
        
        while True:
            try:
                new_wallets = self.scan_for_new_wallets()
                
                if new_wallets:
                    print(f"\n📊 Found {len(new_wallets)} new wallets!")
                    
                    for wallet in new_wallets[:5]:
                        info = self.get_wallet_info(wallet)
                        print(f"  • {wallet[:8]}... | {info['sol_balance']:.4f} SOL")
                    
                    with open('discovered_wallets.txt', 'a') as f:
                        for wallet in new_wallets:
                            f.write(f"{wallet},{datetime.now().isoformat()}\n")
                    
                    print(f"  💾 Saved to discovered_wallets.txt")
                else:
                    print("  No new wallets found")
                
                print(f"\n⏰ Next scan in {interval_seconds} seconds...")
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                print("\n👋 Stopping scanner...")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                time.sleep(interval_seconds)

def main():
    if not os.path.exists('.env'):
        print("Creating .env file...")
        with open('.env', 'w') as f:
            f.write("HELIUS_API_KEY=your_api_key_here\n")
        print("Please add your Helius API key to .env file")
        return
    
    scanner = SimpleWalletScanner()
    
    print("Running single scan...")
    new_wallets = scanner.scan_for_new_wallets()
    print(f"Found {len(new_wallets)} wallets")
    
    response = input("\nRun continuous scanning? (y/n): ")
    if response.lower() == 'y':
        scanner.run_continuous_scan(interval_seconds=30)

if __name__ == "__main__":
    main()
