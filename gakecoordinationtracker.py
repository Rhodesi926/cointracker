import requests
import json
import os
import time
import signal
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from dotenv import load_dotenv
from typing import List, Dict, Any
import csv
import logging

load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('coordination_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PatternCoordinationAnalyzer:
    def __init__(self):
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY not found in environment variables")
        
        self.target_wallet = "DNfuF1L62WWyW3pNakVkyGGFzVVhj4Yr52jSmdTyeBHm"  # GAKE wallet
        self.wsol_address = "So11111111111111111111111111111111111111112"
        
        # Extended analysis parameters
        self.discovered_coordinated_wallets = set()  # Track all discovered wallets
        self.wallet_relationships = {}  # Track relationships between wallets
        
        # Prevent sleep/suspension
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)
        
        logger.info("PatternCoordinationAnalyzer initialized")

    def handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, saving progress and continuing...")
        # Continue execution instead of exiting

    def get_wallet_transactions(self, wallet: str, limit: int = 100) -> List[Dict]:
        """Get transactions for a specific wallet using Helius RPC"""
        url = f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [wallet, {"limit": min(limit, 1000), "commitment": "confirmed"}]
        }
        
        try:
            response = requests.post(url, json=payload, timeout=30)
            if response.status_code != 200:
                return []
            
            data = response.json()
            if 'result' not in data:
                return []
            
            signatures = data['result']
            transactions = []
            
            # Get detailed transaction data (limit to avoid excessive API calls)
            for sig_info in signatures[:limit]:
                tx_payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        sig_info['signature'],
                        {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}
                    ]
                }
                
                tx_response = requests.post(url, json=tx_payload, timeout=10)
                if tx_response.status_code == 200:
                    tx_data = tx_response.json()
                    if 'result' in tx_data and tx_data['result']:
                        tx_detail = tx_data['result']
                        tx_detail['timestamp'] = sig_info.get('blockTime', 0)
                        tx_detail['signature'] = sig_info['signature']
                        transactions.append(tx_detail)
                
                time.sleep(0.05)
            
            return transactions
            
        except Exception as e:
            print(f"Error fetching transactions for {wallet[:8]}...: {e}")
            return []

    def get_token_purchases(self, wallet: str, max_tokens: int = 50) -> List[Dict]:
        """Extract token purchases from wallet transactions"""
        transactions = self.get_wallet_transactions(wallet, max_tokens * 2)
        purchases = []
        
        for tx in transactions:
            if len(purchases) >= max_tokens:
                break
                
            try:
                if tx.get('meta', {}).get('err'):
                    continue
                
                meta = tx.get('meta', {})
                pre_balances = meta.get('preTokenBalances', [])
                post_balances = meta.get('postTokenBalances', [])
                
                for post_balance in post_balances:
                    if post_balance.get('owner') == wallet:
                        mint = post_balance.get('mint')
                        if mint and mint != self.wsol_address:
                            post_ui_amount = post_balance.get('uiTokenAmount', {}).get('uiAmount')
                            if post_ui_amount is None:
                                continue
                            post_amount = float(post_ui_amount)
                            
                            pre_amount = 0
                            for pre_balance in pre_balances:
                                if (pre_balance.get('owner') == wallet and 
                                    pre_balance.get('mint') == mint):
                                    pre_ui_amount = pre_balance.get('uiTokenAmount', {}).get('uiAmount')
                                    if pre_ui_amount is not None:
                                        pre_amount = float(pre_ui_amount)
                                    break
                            
                            if post_amount > pre_amount:
                                token_increase = post_amount - pre_amount
                                estimated_usd = token_increase * 0.001 if token_increase > 1000 else token_increase * 10
                                
                                purchases.append({
                                    'token_mint': mint,
                                    'token_symbol': f"Token_{mint[:6]}",
                                    'amount_tokens': token_increase,
                                    'amount_usd': estimated_usd,
                                    'timestamp': tx.get('timestamp', 0),
                                    'signature': tx.get('signature', '')
                                })
                                break
                            
            except Exception:
                continue
                
        return purchases[:max_tokens]

    def find_systematic_front_runners(self, gake_purchases: List[Dict], min_pattern_count: int = 2) -> Dict:
        """Find wallets that systematically front-run GAKE across multiple tokens - EXTENDED ANALYSIS"""
        print(f"\n=== DEEP SYSTEMATIC FRONT-RUNNER ANALYSIS ===")
        print(f"Analyzing coordination patterns across ALL {len(gake_purchases)} GAKE purchases...")
        print(f"Looking for wallets with {min_pattern_count}+ coordinated trades...")
        
        # Get top holders for ALL tokens GAKE bought (not just 20)
        potential_coordinated_wallets = set()
        
        for i, purchase in enumerate(gake_purchases):  # Analyze ALL GAKE purchases
            logger.info(f"Scanning holders for token {i+1}/{len(gake_purchases)}: {purchase['token_symbol']}")
            
            holders = self.get_top_token_holders(purchase['token_mint'], 25)  # Increased from 15 to 25
            potential_coordinated_wallets.update(holders)
            time.sleep(0.2)
        
        print(f"Found {len(potential_coordinated_wallets)} unique wallets to analyze")
        
        # Analyze EVERY potential wallet (not just top 50)
        wallet_patterns = {}
        
        for i, wallet in enumerate(list(potential_coordinated_wallets)):
            if wallet == self.target_wallet:
                continue
                
            print(f"Deep analysis for wallet {i+1}/{len(potential_coordinated_wallets)}: {wallet[:8]}...")
            
            wallet_purchases = self.get_token_purchases(wallet, 100)  # Increased from 30 to 100
            coordination_matches = []
            
            # Check coordination with ALL GAKE purchases (not subset)
            for gake_purchase in gake_purchases:
                for wallet_purchase in wallet_purchases:
                    if (wallet_purchase['token_mint'] == gake_purchase['token_mint'] and
                        wallet_purchase['timestamp'] < gake_purchase['timestamp']):
                        
                        time_diff_hours = (gake_purchase['timestamp'] - wallet_purchase['timestamp']) / 3600
                        
                        # Extended time window for coordination detection (1-168 hours = 1 week)
                        if 1 <= time_diff_hours <= 168:
                            coordination_matches.append({
                                'token': gake_purchase['token_symbol'],
                                'token_mint': gake_purchase['token_mint'],
                                'hours_before_gake': time_diff_hours,
                                'wallet_amount': wallet_purchase['amount_usd'],
                                'gake_amount': gake_purchase['amount_usd'],
                                'wallet_timestamp': wallet_purchase['timestamp'],
                                'gake_timestamp': gake_purchase['timestamp']
                            })
            
            # Lower threshold to catch more patterns
            if len(coordination_matches) >= min_pattern_count:
                wallet_patterns[wallet] = {
                    'coordination_count': len(coordination_matches),
                    'coordinated_tokens': coordination_matches,
                    'avg_lead_time_hours': sum(m['hours_before_gake'] for m in coordination_matches) / len(coordination_matches),
                    'total_coordinated_volume': sum(m['wallet_amount'] for m in coordination_matches),
                    'coordination_rate': len(coordination_matches) / len(gake_purchases) * 100
                }
                
                # Add to discovered wallets for network analysis
                self.discovered_coordinated_wallets.add(wallet)
                
                print(f"  ✓ COORDINATION PATTERN: {len(coordination_matches)} trades ({len(coordination_matches)/len(gake_purchases)*100:.1f}% rate)")
            
            time.sleep(0.05)  # Reduced delay for faster processing
        
        return wallet_patterns

    def get_top_token_holders(self, token_mint: str, limit: int = 15) -> List[str]:
        """Get top holders of a specific token"""
        url = f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenLargestAccounts",
            "params": [token_mint, {"commitment": "confirmed"}]
        }
        
        try:
            response = requests.post(url, json=payload, timeout=15)
            if response.status_code != 200:
                return []
                
            data = response.json()
            
            if 'result' in data and 'value' in data['result']:
                holders = []
                for account in data['result']['value'][:limit]:
                    if float(account.get('uiAmount', 0)) > 100:  # Only significant holders
                        owner_payload = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "getAccountInfo",
                            "params": [account['address'], {"encoding": "jsonParsed", "commitment": "confirmed"}]
                        }
                        
                        owner_response = requests.post(url, json=owner_payload, timeout=5)
                        if owner_response.status_code == 200:
                            owner_data = owner_response.json()
                            try:
                                owner = owner_data['result']['value']['data']['parsed']['info']['owner']
                                holders.append(owner)
                            except (KeyError, TypeError):
                                continue
                        
                        time.sleep(0.05)
                        
                        if len(holders) >= 10:
                            break
                
                return holders
            
            return []
        except Exception:
            return []

    def find_timing_anomalies(self, gake_purchases: List[Dict]) -> Dict:
        """Find unusual timing clusters that suggest coordination"""
        print(f"\n=== ANALYZING TIMING ANOMALIES ===")
        
        timing_clusters = {}
        
        for purchase in gake_purchases[:10]:  # Analyze top 10 for timing anomalies
            print(f"Checking timing cluster for: {purchase['token_symbol']}")
            
            # Get all transactions for this token in a 48-hour window around GAKE's purchase
            target_time = purchase['timestamp']
            window_start = target_time - (48 * 3600)  # 48 hours before
            window_end = target_time + (6 * 3600)     # 6 hours after
            
            holders = self.get_top_token_holders(purchase['token_mint'], 20)
            
            # Check when each holder bought this token
            buy_times = []
            
            for holder in holders:
                if holder == self.target_wallet:
                    continue
                    
                holder_purchases = self.get_token_purchases(holder, 20)
                
                for holder_purchase in holder_purchases:
                    if (holder_purchase['token_mint'] == purchase['token_mint'] and
                        window_start <= holder_purchase['timestamp'] <= window_end):
                        
                        hours_from_gake = (holder_purchase['timestamp'] - target_time) / 3600
                        buy_times.append({
                            'wallet': holder,
                            'hours_from_gake': hours_from_gake,
                            'amount': holder_purchase['amount_usd']
                        })
                        break
                
                time.sleep(0.1)
            
            # Analyze for unusual clustering
            if len(buy_times) >= 3:  # Need at least 3 wallets for pattern
                # Count buys in different time windows
                before_24h = len([t for t in buy_times if -24 <= t['hours_from_gake'] < 0])
                before_6h = len([t for t in buy_times if -6 <= t['hours_from_gake'] < 0])
                after_gake = len([t for t in buy_times if 0 < t['hours_from_gake'] <= 6])
                
                # Flag as anomalous if too many wallets bought in narrow windows
                if before_6h >= 3 or before_24h >= 5:
                    timing_clusters[purchase['token_mint']] = {
                        'token_symbol': purchase['token_symbol'],
                        'total_wallets_in_window': len(buy_times),
                        'buys_24h_before': before_24h,
                        'buys_6h_before': before_6h,
                        'buys_after_gake': after_gake,
                        'buy_details': buy_times,
                        'anomaly_score': before_6h * 3 + before_24h  # Higher score = more suspicious
                    }
                    
                    print(f"  ⚠️  TIMING ANOMALY: {before_6h} wallets bought within 6h before GAKE")
        
        return timing_clusters

    def analyze_network_effects(self, systematic_patterns: Dict) -> Dict:
        """Analyze relationships between coordinated wallets - NETWORK MAPPING"""
        print(f"\n=== NETWORK EFFECT ANALYSIS ===")
        print(f"Mapping relationships between {len(systematic_patterns)} coordinated wallets...")
        
        network_relationships = {}
        coordinated_wallets = list(systematic_patterns.keys())
        
        # Get trading data for all coordinated wallets
        wallet_trading_data = {}
        for wallet in coordinated_wallets:
            print(f"Getting extended trading data for: {wallet[:8]}...")
            wallet_trading_data[wallet] = self.get_token_purchases(wallet, 200)  # Get more trading history
            time.sleep(0.1)
        
        # Analyze cross-wallet relationships
        for i, wallet1 in enumerate(coordinated_wallets):
            network_relationships[wallet1] = {
                'shared_tokens_with_other_coordinated_wallets': [],
                'timing_correlations': {},
                'similar_trading_patterns': 0
            }
            
            for j, wallet2 in enumerate(coordinated_wallets):
                if i >= j:  # Skip self and avoid duplicates
                    continue
                
                # Find shared tokens between coordinated wallets
                wallet1_tokens = {trade['token_mint']: trade for trade in wallet_trading_data[wallet1]}
                wallet2_tokens = {trade['token_mint']: trade for trade in wallet_trading_data[wallet2]}
                
                shared_tokens = set(wallet1_tokens.keys()) & set(wallet2_tokens.keys())
                
                if len(shared_tokens) >= 3:  # Significant overlap
                    timing_correlations = []
                    
                    for token_mint in shared_tokens:
                        wallet1_time = wallet1_tokens[token_mint]['timestamp']
                        wallet2_time = wallet2_tokens[token_mint]['timestamp']
                        time_diff_hours = abs(wallet1_time - wallet2_time) / 3600
                        
                        timing_correlations.append({
                            'token': wallet1_tokens[token_mint]['token_symbol'],
                            'time_diff_hours': time_diff_hours,
                            'coordinated': time_diff_hours <= 24  # Within 24 hours = coordinated
                        })
                    
                    coordination_rate = sum(1 for tc in timing_correlations if tc['coordinated']) / len(timing_correlations)
                    
                    network_relationships[wallet1]['shared_tokens_with_other_coordinated_wallets'].append({
                        'partner_wallet': wallet2,
                        'shared_token_count': len(shared_tokens),
                        'timing_correlations': timing_correlations,
                        'coordination_rate': coordination_rate
                    })
        
        return network_relationships

    def find_second_degree_coordination(self, systematic_patterns: Dict) -> Dict:
        """Find wallets that coordinate with the coordinated wallets - 2ND DEGREE ANALYSIS"""
        print(f"\n=== SECOND-DEGREE COORDINATION ANALYSIS ===")
        print(f"Looking for wallets that coordinate with the coordinated wallets...")
        
        second_degree_patterns = {}
        primary_coordinated_wallets = list(systematic_patterns.keys())
        
        # For each primary coordinated wallet, find who coordinates with THEM
        for primary_wallet in primary_coordinated_wallets[:5]:  # Analyze top 5 to avoid excessive API calls
            print(f"Finding coordination with: {primary_wallet[:8]}...")
            
            primary_purchases = self.get_token_purchases(primary_wallet, 50)
            
            # For each token the primary wallet bought, see who else bought it around the same time
            for purchase in primary_purchases[:20]:  # Analyze their top 20 purchases
                holders = self.get_top_token_holders(purchase['token_mint'], 30)
                
                for holder in holders:
                    if holder in [self.target_wallet] + primary_coordinated_wallets:
                        continue  # Skip already known wallets
                    
                    holder_purchases = self.get_token_purchases(holder, 30)
                    
                    # Check if this holder bought similar tokens at similar times
                    for holder_purchase in holder_purchases:
                        if (holder_purchase['token_mint'] == purchase['token_mint'] and
                            abs(holder_purchase['timestamp'] - purchase['timestamp']) <= 24 * 3600):  # Within 24 hours
                            
                            if holder not in second_degree_patterns:
                                second_degree_patterns[holder] = {
                                    'coordinates_with': [],
                                    'total_second_degree_matches': 0
                                }
                            
                            second_degree_patterns[holder]['coordinates_with'].append({
                                'primary_wallet': primary_wallet,
                                'token': purchase['token_symbol'],
                                'time_diff_hours': abs(holder_purchase['timestamp'] - purchase['timestamp']) / 3600
                            })
                            second_degree_patterns[holder]['total_second_degree_matches'] += 1
                            break
                
                time.sleep(0.05)
        
        # Filter for significant second-degree coordination
        filtered_second_degree = {
            wallet: data for wallet, data in second_degree_patterns.items()
            if data['total_second_degree_matches'] >= 3
        }
        
        return filtered_second_degree

    def analyze_liquidity_coordination(self, systematic_patterns: Dict) -> Dict:
        """Analyze if coordinated wallets also provide liquidity together - LP ANALYSIS"""
        print(f"\n=== LIQUIDITY PROVISION COORDINATION ANALYSIS ===")
        print(f"Checking if coordinated wallets also provide liquidity together...")
        
        liquidity_analysis = {}
        
        for wallet in list(systematic_patterns.keys())[:10]:  # Analyze top 10
            print(f"Checking LP activity for: {wallet[:8]}...")
            
            # Get recent transactions to look for LP activity
            transactions = self.get_wallet_transactions(wallet, 100)
            lp_activity = []
            
            for tx in transactions:
                try:
                    # Look for liquidity-related instructions
                    if 'transaction' in tx and 'message' in tx['transaction']:
                        instructions = tx['transaction']['message'].get('instructions', [])
                        for instruction in instructions:
                            if 'parsed' in instruction:
                                parsed = instruction['parsed']
                                if 'type' in parsed:
                                    # Check for common LP instruction types
                                    if any(lp_keyword in parsed['type'].lower() for lp_keyword in 
                                          ['liquidity', 'pool', 'swap', 'addliquidity', 'removeliquidity']):
                                        lp_activity.append({
                                            'timestamp': tx.get('timestamp', 0),
                                            'type': parsed['type'],
                                            'signature': tx.get('signature', '')
                                        })
                                        break
                except Exception:
                    continue
            
            if lp_activity:
                liquidity_analysis[wallet] = {
                    'lp_transaction_count': len(lp_activity),
                    'recent_lp_activity': lp_activity[:5],  # Store recent 5
                    'liquidity_provider': True
                }
        
        return liquidity_analysis

    def analyze_volume_patterns(self, systematic_patterns: Dict) -> Dict:
        """Analyze volume patterns for coordinated wallets"""
        print(f"\n=== VOLUME PATTERN ANALYSIS ===")
        
        volume_patterns = {}
        
        for wallet, data in systematic_patterns.items():
            wallet_volumes = [match['wallet_amount'] for match in data['coordinated_tokens']]
            gake_volumes = [match['gake_amount'] for match in data['coordinated_tokens']]
            
            if wallet_volumes and gake_volumes:
                avg_wallet_volume = sum(wallet_volumes) / len(wallet_volumes)
                avg_gake_volume = sum(gake_volumes) / len(gake_volumes)
                
                # Calculate similarity in sizing
                volume_ratio = avg_wallet_volume / avg_gake_volume if avg_gake_volume > 0 else 0
                
                # Check for consistent sizing patterns
                volume_consistency = len(set(int(v/1000) for v in wallet_volumes)) <= 3  # Similar order of magnitude
                
                volume_patterns[wallet] = {
                    'avg_volume': avg_wallet_volume,
                    'volume_ratio_to_gake': volume_ratio,
                    'size_similarity_to_gake': min(volume_ratio, 1/volume_ratio) if volume_ratio > 0 else 0,
                    'consistent_sizing': volume_consistency,
                    'volume_range': max(wallet_volumes) - min(wallet_volumes) if len(wallet_volumes) > 1 else 0
                }
        
        return volume_patterns

    def generate_comprehensive_report(self):
        """Generate EXTENDED systematic coordination analysis report"""
        print("=== EXTENDED COORDINATION NETWORK ANALYSIS ===")
        print(f"Target Wallet (GAKE): {self.target_wallet}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("DEEP SCANNING - This may take 10-20 minutes...")
        
        # Step 1: Get GAKE's purchases (EXTENDED)
        print(f"\nFetching EXTENDED GAKE trading history...")
        gake_purchases = self.get_token_purchases(self.target_wallet, 200)  # Increased from 50 to 200
        
        if not gake_purchases:
            print("❌ No GAKE purchases found")
            return
            
        print(f"Found {len(gake_purchases)} GAKE token purchases for analysis")
        
        # Step 2: Find systematic front-runners (EXTENDED)
        systematic_patterns = self.find_systematic_front_runners(gake_purchases, 2)  # Lowered threshold
        
        # Step 3: Find timing anomalies (MORE TOKENS)
        timing_anomalies = self.find_timing_anomalies(gake_purchases[:30])  # Increased from 10 to 30
        
        # Step 4: NEW - Network effect analysis
        network_effects = self.analyze_network_effects(systematic_patterns) if systematic_patterns else {}
        
        # Step 5: NEW - Second-degree coordination
        second_degree = self.find_second_degree_coordination(systematic_patterns) if systematic_patterns else {}
        
        # Step 6: NEW - Liquidity coordination analysis  
        liquidity_coordination = self.analyze_liquidity_coordination(systematic_patterns) if systematic_patterns else {}
        
        # Step 7: Enhanced volume patterns
        volume_patterns = self.analyze_volume_patterns(systematic_patterns) if systematic_patterns else {}
        
        # Generate EXTENDED Report
        print(f"\n" + "="*80)
        print(f"EXTENDED COORDINATION NETWORK ANALYSIS RESULTS")
        print(f"="*80)
        
        if systematic_patterns:
            print(f"\n🎯 PRIMARY COORDINATION NETWORK:")
            print(f"Discovered {len(systematic_patterns)} wallets with systematic coordination patterns")
            
            ranked_patterns = sorted(systematic_patterns.items(), 
                                   key=lambda x: x[1]['coordination_count'], reverse=True)
            
            for i, (wallet, data) in enumerate(ranked_patterns, 1):
                print(f"\n{i}. PRIMARY WALLET: {wallet}")
                print(f"   Coordination Count: {data['coordination_count']}/{len(gake_purchases)} tokens ({data['coordination_rate']:.1f}%)")
                print(f"   Average Lead Time: {data['avg_lead_time_hours']:.1f} hours")
                print(f"   Total Volume: ${data['total_coordinated_volume']:.0f}")
                
                # Network relationships
                if wallet in network_effects and network_effects[wallet]['shared_tokens_with_other_coordinated_wallets']:
                    print(f"   🔗 Network Connections:")
                    for connection in network_effects[wallet]['shared_tokens_with_other_coordinated_wallets']:
                        print(f"     • Connected to {connection['partner_wallet'][:8]}... ({connection['shared_token_count']} shared tokens, {connection['coordination_rate']*100:.0f}% synchronized)")
                
                # Liquidity analysis
                if wallet in liquidity_coordination:
                    print(f"   💧 Liquidity Provider: YES ({liquidity_coordination[wallet]['lp_transaction_count']} LP transactions)")
                
                print(f"   Recent Coordinated Tokens:")
                for token in data['coordinated_tokens'][:3]:
                    print(f"     • {token['token']}: {token['hours_before_gake']:.1f}h before GAKE (${token['wallet_amount']:.0f})")
        
        if second_degree:
            print(f"\n🔄 SECOND-DEGREE COORDINATION NETWORK:")
            print(f"Found {len(second_degree)} additional wallets coordinating with the primary network")
            
            sorted_second_degree = sorted(second_degree.items(), 
                                        key=lambda x: x[1]['total_second_degree_matches'], reverse=True)
            
            for wallet, data in sorted_second_degree[:5]:  # Show top 5
                print(f"\n   SECONDARY WALLET: {wallet}")
                print(f"   Coordinates with {len(data['coordinates_with'])} primary wallets")
                print(f"   Total coordination instances: {data['total_second_degree_matches']}")
                for connection in data['coordinates_with'][:2]:
                    print(f"     • Syncs with {connection['primary_wallet'][:8]}... on {connection['token']}")
        
        if timing_anomalies:
            print(f"\n⚠️  TIMING ANOMALIES (EXTENDED ANALYSIS):")
            print(f"Suspicious clustering detected on {len(timing_anomalies)} tokens")
            
            sorted_anomalies = sorted(timing_anomalies.items(), 
                                    key=lambda x: x[1]['anomaly_score'], reverse=True)
            
            for token_mint, anomaly in sorted_anomalies[:5]:
                print(f"\n   TOKEN: {anomaly['token_symbol']}")
                print(f"   Anomaly Score: {anomaly['anomaly_score']}/10")
                print(f"   Coordinated Wallets (6h window): {anomaly['buys_6h_before']}")
                print(f"   Coordinated Wallets (24h window): {anomaly['buys_24h_before']}")
        
        # Network Summary
        total_network_size = len(systematic_patterns) + len(second_degree)
        print(f"\n📊 NETWORK SUMMARY:")
        print(f"   Total Network Size: {total_network_size} wallets")
        print(f"   Primary Coordinators: {len(systematic_patterns)} wallets")
        print(f"   Secondary Coordinators: {len(second_degree)} wallets") 
        print(f"   Liquidity Providers: {sum(1 for w in liquidity_coordination.values() if w.get('liquidity_provider'))} wallets")
        print(f"   Network Density: {'HIGH' if total_network_size > 20 else 'MODERATE' if total_network_size > 10 else 'LOW'}")
        
        # Save EXTENDED results
        self.save_extended_results(systematic_patterns, timing_anomalies, network_effects, 
                                 second_degree, liquidity_coordination, volume_patterns)
        
        print(f"\n=== EXTENDED FILES GENERATED ===")
        print("- primary_coordination.csv (Main coordinated wallets)")
        print("- secondary_coordination.csv (2nd degree wallets)")  
        print("- network_relationships.csv (Wallet connections)")
        print("- liquidity_analysis.csv (LP coordination)")
        print("- timing_anomalies_extended.csv (All suspicious timing)")
        print("- complete_network_analysis.json (Full data)")

    def save_extended_results(self, systematic_patterns, timing_anomalies, network_effects, 
                            second_degree, liquidity_coordination, volume_patterns):
        """Save EXTENDED analysis results to CSV files without pandas"""
        
        # Primary coordination patterns
        if systematic_patterns:
            with open('primary_coordination.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['wallet_address', 'coordination_count', 'coordination_rate_percent', 
                               'avg_lead_time_hours', 'total_volume', 'is_liquidity_provider', 'network_connections'])
                
                for wallet, data in systematic_patterns.items():
                    writer.writerow([
                        wallet,
                        data['coordination_count'],
                        data['coordination_rate'],
                        data['avg_lead_time_hours'],
                        data['total_coordinated_volume'],
                        wallet in liquidity_coordination,
                        len(network_effects.get(wallet, {}).get('shared_tokens_with_other_coordinated_wallets', []))
                    ])
        
        # Secondary coordination  
        if second_degree:
            with open('secondary_coordination.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['wallet_address', 'coordination_matches', 'primary_wallet_connections'])
                
                for wallet, data in second_degree.items():
                    writer.writerow([
                        wallet,
                        data['total_second_degree_matches'],
                        len(data['coordinates_with'])
                    ])
        
        # Network relationships
        if network_effects:
            with open('network_relationships.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['wallet_1', 'wallet_2', 'shared_tokens', 'coordination_rate'])
                
                for wallet1, connections in network_effects.items():
                    for connection in connections['shared_tokens_with_other_coordinated_wallets']:
                        writer.writerow([
                            wallet1,
                            connection['partner_wallet'],
                            connection['shared_token_count'],
                            connection['coordination_rate'] * 100
                        ])
        
        # Timing anomalies
        if timing_anomalies:
            with open('timing_anomalies_extended.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['token_symbol', 'token_mint', 'anomaly_score', 'wallets_6h_before', 'wallets_24h_before', 'total_wallets'])
                
                for token_mint, data in timing_anomalies.items():
                    writer.writerow([
                        data['token_symbol'],
                        token_mint,
                        data['anomaly_score'],
                        data['buys_6h_before'],
                        data['buys_24h_before'],
                        data['total_wallets_in_window']
                    ])
        
        # Liquidity analysis
        if liquidity_coordination:
            with open('liquidity_analysis.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['wallet_address', 'lp_transaction_count', 'is_liquidity_provider'])
                
                for wallet, data in liquidity_coordination.items():
                    writer.writerow([
                        wallet,
                        data['lp_transaction_count'],
                        data['liquidity_provider']
                    ])
        
        # Complete data dump
        complete_data = {
            'analysis_timestamp': datetime.now().isoformat(),
            'target_wallet': self.target_wallet,
            'network_size': len(systematic_patterns) + len(second_degree),
            'primary_coordination': systematic_patterns,
            'secondary_coordination': second_degree,
            'network_effects': network_effects,
            'timing_anomalies': timing_anomalies,
            'liquidity_coordination': liquidity_coordination
        }
        
        with open('complete_network_analysis.json', 'w') as f:
            json.dump(complete_data, f, indent=2, default=str)

    def generate_simple_report(self):
        """Generate basic coordination analysis report"""
        print("=== COORDINATION ANALYSIS ===")
        print(f"Target Wallet (GAKE): {self.target_wallet}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Get GAKE's purchases
        print(f"\nFetching GAKE trading history...")
        gake_purchases = self.get_token_purchases(self.target_wallet, 50)
        
        if not gake_purchases:
            print("❌ No GAKE purchases found")
            return
            
        print(f"Found {len(gake_purchases)} GAKE token purchases")
        
        # Find systematic patterns  
        systematic_patterns = self.find_systematic_front_runners(gake_purchases, 3)
        
        # Find timing anomalies
        timing_anomalies = self.find_timing_anomalies(gake_purchases[:10])
        
        # Analyze volume patterns
        volume_patterns = self.analyze_volume_patterns(systematic_patterns) if systematic_patterns else {}
        
        # Generate Report
        print(f"\n" + "="*60)
        print(f"COORDINATION ANALYSIS RESULTS")
        print(f"="*60)
        
        if systematic_patterns:
            print(f"\n🎯 SYSTEMATIC COORDINATION PATTERNS:")
            print(f"Found {len(systematic_patterns)} wallets with consistent front-running")
            
            ranked_patterns = sorted(systematic_patterns.items(), 
                                   key=lambda x: x[1]['coordination_count'], reverse=True)
            
            for i, (wallet, data) in enumerate(ranked_patterns[:10], 1):  # Show top 10
                print(f"\n{i}. WALLET: {wallet}")
                print(f"   Coordinated on {data['coordination_count']} tokens ({data['coordination_rate']:.1f}% of GAKE trades)")
                print(f"   Average lead time: {data['avg_lead_time_hours']:.1f} hours")
                print(f"   Total coordinated volume: ${data['total_coordinated_volume']:.0f}")
                
                print(f"   Recent coordinated tokens:")
                for token in data['coordinated_tokens'][:3]:
                    print(f"     • {token['token']}: {token['hours_before_gake']:.1f}h before GAKE")
        else:
            print(f"\n✅ No systematic coordination patterns detected")
            print("wallets showed consistent front-running across 3+ tokens")
        
        if timing_anomalies:
            print(f"\n⚠️  TIMING ANOMALIES DETECTED:")
            print(f"Found unusual clustering patterns on {len(timing_anomalies)} tokens\n")
            
            sorted_anomalies = sorted(timing_anomalies.items(), 
                                    key=lambda x: x[1]['anomaly_score'], reverse=True)
            
            for token_mint, anomaly in sorted_anomalies:
                print(f"TOKEN: {anomaly['token_symbol']}")
                print(f"   Anomaly Score: {anomaly['anomaly_score']}/10")
                print(f"   Wallets bought 6h before GAKE: {anomaly['buys_6h_before']}")
                print(f"   Wallets bought 24h before GAKE: {anomaly['buys_24h_before']}")
                print(f"   Statistical likelihood: {'VERY SUSPICIOUS' if anomaly['anomaly_score'] > 8 else 'SUSPICIOUS' if anomaly['anomaly_score'] > 5 else 'NOTABLE'}")
                print()
        
        # Save detailed results
        self.save_results(systematic_patterns, timing_anomalies, volume_patterns)
        
        print(f"\n=== FILES GENERATED ===")
        print("- systematic_coordination.csv")
        print("- timing_anomalies.csv")
        print("- detailed_patterns.json")

    def save_results(self, systematic_patterns, timing_anomalies, volume_patterns):
        """Save analysis results to files"""
        
        # Systematic patterns
        if systematic_patterns:
            with open('systematic_coordination.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['wallet_address', 'coordination_count', 'coordination_rate_percent', 
                               'avg_lead_time_hours', 'total_volume', 'size_similarity', 'consistent_sizing'])
                
                for wallet, data in systematic_patterns.items():
                    writer.writerow([
                        wallet,
                        data['coordination_count'],
                        data['coordination_rate'],
                        data['avg_lead_time_hours'],
                        data['total_coordinated_volume'],
                        volume_patterns.get(wallet, {}).get('size_similarity_to_gake', 0) * 100,
                        volume_patterns.get(wallet, {}).get('consistent_sizing', False)
                    ])
        
        # Timing anomalies
        if timing_anomalies:
            with open('timing_anomalies.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['token_symbol', 'token_mint', 'anomaly_score', 'wallets_6h_before', 'wallets_24h_before', 'total_wallets'])
                
                for token_mint, data in timing_anomalies.items():
                    writer.writerow([
                        data['token_symbol'],
                        token_mint,
                        data['anomaly_score'],
                        data['buys_6h_before'],
                        data['buys_24h_before'],
                        data['total_wallets_in_window']
                    ])
        
        # Detailed data
        detailed_data = {
            'analysis_timestamp': datetime.now().isoformat(),
            'target_wallet': self.target_wallet,
            'systematic_patterns': systematic_patterns,
            'timing_anomalies': timing_anomalies,
            'volume_patterns': volume_patterns
        }
        
        with open('detailed_patterns.json', 'w') as f:
            json.dump(detailed_data, f, indent=2, default=str)

def main():
    try:
        analyzer = PatternCoordinationAnalyzer()
        
        # Choose which report to generate
        print("Select analysis type:")
        print("1. Basic analysis (faster, 3+ coordination threshold)")
        print("2. Extended analysis (comprehensive, 2+ coordination threshold)")
        
        choice = input("Enter choice (1 or 2): ").strip()
        
        if choice == "2":
            analyzer.generate_comprehensive_report()
        else:
            analyzer.generate_simple_report()
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()