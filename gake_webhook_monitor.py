from flask import Flask, request, jsonify
from datetime import datetime
import json

app = Flask(__name__)

WHALE_WALLET = "DNfuF1L62WWyW3pNfjhrJXsros5DXWaJyXMCtoChe7vq1nYwLExUgzx9VJAazMecYvfwHNxehtMY49JGJ7LXaZo91yJ7nhFkhP"

COORDINATOR_WALLETS = [
    "EHmNFgxSABW8Z8aXWV7LZM2GZzY4Y6K",
    "3JAqUyZX8fxnnE34VGde",
    "5yq84fkYLYtrwCSahYEe",
    "3chq5EzC6EK65HocprwB",
    "8bntBT3ELkhAumgMez69",
    "AG1vcvY2g5FsHecsZbCc",
    "2jjpiuDcLkd2cvxH676Z",
]

def log_alert(alert_data):
    with open("whale_alerts.jsonl", 'a') as f:
        f.write(json.dumps(alert_data) + '\n')
    print("\n" + "="*80)
    print(f"🚨 ALERT: {alert_data['type']}")
    print(f"⏰ {alert_data['timestamp']}")
    print(f"📝 {alert_data['message']}")
    print("="*80)

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    try:
        data = request.json
        for tx in data:
            timestamp = datetime.now().isoformat()
            
            token_transfers = tx.get('tokenTransfers', [])
            for transfer in token_transfers:
                to_addr = transfer.get('toUserAccount', '')
                from_addr = transfer.get('fromUserAccount', '')
                amount = transfer.get('tokenAmount', 0)
                mint = transfer.get('mint', '')
                
                if to_addr == WHALE_WALLET and from_addr in COORDINATOR_WALLETS:
                    log_alert({
                        'type': 'TOKEN_TRANSFER_TO_WHALE',
                        'timestamp': timestamp,
                        'token_mint': mint,
                        'amount': amount,
                        'message': f'🔥 BUY SIGNAL: Whale received {amount} tokens of {mint[:8]}...'
                    })
        
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'status': 'error'}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    print("🐋 Whale Tracker Started")
    print(f"Monitoring: {WHALE_WALLET[:20]}...")
    app.run(host='0.0.0.0', port=5000)