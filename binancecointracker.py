import os
import time
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("HELIUS_API_KEY")  # Set this in your .env file
BASE_URL = "https://api.helius.xyz/v0/addresses/{address}/transactions/?api-key={api_key}"

# Calculate date window
today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
start_date = today - timedelta(days=260)
end_date = today - timedelta(days=200)

def fetch_transactions(address, before=None):
    url = BASE_URL.format(address=address, api_key=API_KEY)
    params = {
        "limit": 100
    }
    if before:
        params["before"] = before
    
    for attempt in range(5):
        try:
            response = requests.get(url, params=params)
            if response.status_code == 429:
                print("⏳ Rate limited, waiting...")
                time.sleep(2)
                continue
            response.raise_for_status()
            time.sleep(0.5)  # Stay under rate limits
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
            time.sleep(1)
    
    raise Exception("Too many retries")

def scan_wallet(address):
    print(f"🔍 Scanning wallet: {address}")
    found_addresses = set()
    checked = 0
    in_date_range = 0
    before = None
    
    while True:
        txs = fetch_transactions(address, before)
        if not txs:
            break
            
        print(f"📊 Fetched batch of {len(txs)} transactions")
        
        for tx in txs:
            checked += 1
            block_time = tx.get("timestamp")
            if not block_time:
                continue
                
            tx_time = datetime.fromtimestamp(block_time, timezone.utc)
            if not (start_date <= tx_time <= end_date):
                continue
                
            in_date_range += 1
            
            # Only look for transfer events
            if tx.get("type") != "TRANSFER":
                continue
                
            for event in tx.get("events", []):
                if event.get("type") == "TRANSFER":
                    sender = event.get("source")
                    recipient = event.get("destination")
                    amount = event.get("amount", 0) / 1e9
                    
                    # Outbound: source == our address, sent between 3.8 and 4.2 SOL
                    if sender == address and 3.8 < amount < 4.2:
                        found_addresses.add(recipient)
                        print(f"✅ Found recipient: {recipient} ({amount:.3f} SOL)")
            
            before = tx.get("signature")
            
        if len(txs) < 100:
            break
    
    print(f"\n🎉 Scan complete! Checked {checked} transactions")
    print(f"📅 Transactions in target date range: {in_date_range}")
    print(f"💰 Found {len(found_addresses)} matching SOL transfers")
    return list(found_addresses)

if __name__ == "__main__":
    while True:
        address = input("\nEnter wallet address (or 'quit' to exit): ").strip()
        if address.lower() == 'quit':
            break
            
        if len(address) != 44:
            print("❌ Invalid wallet address length")
            continue
            
        results = scan_wallet(address)
        
        if results:
            print("\n📋 Found funded addresses:")
            for i, addr in enumerate(results, 1):
                print(f"{i:2}. {addr}")
        else:
            print("🔍 No matching transactions found")