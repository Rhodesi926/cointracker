import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import json
import os
import time
from dotenv import load_dotenv
from typing import List, Dict, Any
import statistics

load_dotenv()

class StatisticalCoordinationAnalyzer:
    def __init__(self):
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        if not self.helius_api_key:
            raise ValueError("HELIUS_API_KEY not found in environment variables")
        
        self.target_wallet = "DNfuF1L62WWyW3pNakVkyGGFzVVhj4Yr52jSmdTyeBHm"  # GAKE wallet
        self.wsol_address = "So11111111111111111111111111111111111111112"
        
        # Analysis parameters
        self.error_log = []
        self.retry_max = 3
        self.retry_delay_base = 2
        
        # Phase 1: Baseline data
        self.baseline_data = {
            'token_timing_distributions': {},  # When did people buy each token?
            'random_wallet_overlaps': [],       # What % overlap is normal?
            'all_token_buyers': {}              # All buyers for each token
        }
        
        # Results storage
        self.coordination_scores = {}
        self.detailed_findings = []

    def get_wallet_transactions(self, wallet: str, limit: int = 100) -> List[Dict]:
        """Get transactions for a specific wallet with retry logic"""
        url = f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [wallet, {"limit": min(limit, 1000), "commitment": "confirmed"}]
        }
        
        for attempt in range(self.retry_max):
            try:
                response = requests.post(url, json=payload, timeout=30)
                if response.status_code != 200:
                    if attempt < self.retry_max - 1:
                        time.sleep(self.retry_delay_base ** attempt)
                        continue
                    return []
                
                data = response.json()
                if 'result' not in data:
                    return []
                
                signatures = data['result']
                transactions = []
                
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
                    
                    for tx_attempt in range(self.retry_max):
                        try:
                            tx_response = requests.post(url, json=tx_payload, timeout=15)
                            if tx_response.status_code == 200:
                                tx_data = tx_response.json()
                                if 'result' in tx_data and tx_data['result']:
                                    tx_detail = tx_data['result']
                                    tx_detail['timestamp'] = sig_info.get('blockTime', 0)
                                    tx_detail['signature'] = sig_info['signature']
                                    transactions.append(tx_detail)
                                    break
                        except:
                            if tx_attempt < self.retry_max - 1:
                                time.sleep(1)
                                continue
                    
                    time.sleep(0.05)
                
                return transactions
                
            except Exception as e:
                if attempt < self.retry_max - 1:
                    time.sleep(self.retry_delay_base ** attempt)
                    continue
                else:
                    self.error_log.append({
                        'wallet': wallet,
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    })
                    return []
        
        return []

    def get_token_purchases(self, wallet: str, max_tokens: int = 100) -> List[Dict]:
        """Extract token purchases from wallet transactions"""
        transactions = self.get_wallet_transactions(wallet, max_tokens * 2)
        purchases = []
        seen_tokens = set()  # Track tokens we've already recorded
        
        for tx in transactions:
            if len(purchases) >= max_tokens:
                break
                
            try:
                if tx.get('meta', {}).get('err'):
                    continue
                
                meta = tx.get('meta', {})
                pre_balances = meta.get('preTokenBalances', [])
                post_balances = meta.get('postTokenBalances', [])
                
                # Create lookup dict for pre-balances by account index
                pre_balance_map = {pb.get('accountIndex'): pb for pb in pre_balances}
                
                for post_balance in post_balances:
                    mint = post_balance.get('mint')
                    account_index = post_balance.get('accountIndex')
                    
                    # Skip if not a valid token or already recorded this token
                    if not mint or mint == self.wsol_address or mint in seen_tokens:
                        continue
                    
                    # Get amounts
                    post_ui_amount = post_balance.get('uiTokenAmount', {}).get('uiAmount')
                    if post_ui_amount is None:
                        continue
                    post_amount = float(post_ui_amount)
                    
                    # Get pre-amount from matching account index
                    pre_amount = 0
                    if account_index in pre_balance_map:
                        pre_balance = pre_balance_map[account_index]
                        if pre_balance.get('mint') == mint:
                            pre_ui_amount = pre_balance.get('uiTokenAmount', {}).get('uiAmount')
                            if pre_ui_amount is not None:
                                pre_amount = float(pre_ui_amount)
                    
                    # Check if this is a purchase (balance increased)
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
                        seen_tokens.add(mint)
                        break  # Move to next transaction
                            
            except Exception as e:
                continue
                
        return purchases[:max_tokens]

    def get_top_token_holders(self, token_mint: str, limit: int = 20) -> List[str]:
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
                    if float(account.get('uiAmount', 0)) > 100:
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
                        
                        if len(holders) >= 15:
                            break
                
                return holders
            
            return []
        except Exception:
            return []

    # ==================== PHASE 1: BASELINE ANALYSIS ====================
    
    def build_baseline_data(self, gake_purchases: List[Dict]):
        """Phase 1: Establish statistical baselines"""
        print("\n" + "="*80)
        print("PHASE 1: BUILDING STATISTICAL BASELINES")
        print("="*80)
        
        # 1. Get all buyers for each token GAKE bought
        print("\n[1/3] Collecting all buyers for GAKE's tokens...")
        for i, purchase in enumerate(gake_purchases[:20]):  # Analyze top 20 tokens
            print(f"  Token {i+1}/20: {purchase['token_symbol']}")
            
            holders = self.get_top_token_holders(purchase['token_mint'], 30)
            self.baseline_data['all_token_buyers'][purchase['token_mint']] = holders
            
            # Get purchase timestamps for these holders
            token_purchase_times = []
            for holder in holders[:20]:  # Sample 20 holders per token
                holder_purchases = self.get_token_purchases(holder, 50)
                for hp in holder_purchases:
                    if hp['token_mint'] == purchase['token_mint']:
                        token_purchase_times.append(hp['timestamp'])
                        break
                time.sleep(0.1)
            
            self.baseline_data['token_timing_distributions'][purchase['token_mint']] = token_purchase_times
            time.sleep(0.2)
        
        # 2. Calculate normal overlap percentages
        print("\n[2/3] Calculating normal wallet overlap rates...")
        gake_token_set = set([p['token_mint'] for p in gake_purchases])
        
        # Sample 50 random wallets that bought at least one of GAKE's tokens
        sampled_wallets = set()
        for token_mint, holders in self.baseline_data['all_token_buyers'].items():
            sampled_wallets.update(holders[:10])
            if len(sampled_wallets) >= 50:
                break
        
        for wallet in list(sampled_wallets)[:50]:
            if wallet == self.target_wallet:
                continue
            
            wallet_purchases = self.get_token_purchases(wallet, 100)
            wallet_tokens = set([p['token_mint'] for p in wallet_purchases])
            
            overlap = len(wallet_tokens & gake_token_set)
            overlap_pct = (overlap / len(gake_token_set)) * 100 if gake_token_set else 0
            
            self.baseline_data['random_wallet_overlaps'].append(overlap_pct)
            time.sleep(0.1)
        
        # 3. Calculate baseline statistics
        print("\n[3/3] Computing baseline statistics...")
        if self.baseline_data['random_wallet_overlaps']:
            mean_overlap = statistics.mean(self.baseline_data['random_wallet_overlaps'])
            median_overlap = statistics.median(self.baseline_data['random_wallet_overlaps'])
            percentile_95 = np.percentile(self.baseline_data['random_wallet_overlaps'], 95)
            
            print(f"\n  Random Wallet Overlap Statistics:")
            print(f"    Mean: {mean_overlap:.2f}%")
            print(f"    Median: {median_overlap:.2f}%")
            print(f"    95th Percentile: {percentile_95:.2f}%")
            print(f"    Threshold for anomaly: >{percentile_95:.2f}%")
        
        print(f"\n  Baseline data collection complete!")
        print(f"    Analyzed {len(self.baseline_data['all_token_buyers'])} tokens")
        print(f"    Sampled {len(self.baseline_data['random_wallet_overlaps'])} random wallets")

    # ==================== PHASE 2: MULTI-SIGNAL SCORING ====================
    
    def calculate_overlap_score(self, wallet_tokens: set, gake_tokens: set) -> float:
        """A. Calculate overlap score (0-100)"""
        if not gake_tokens:
            return 0
        
        overlap = len(wallet_tokens & gake_tokens)
        overlap_pct = (overlap / len(gake_tokens)) * 100
        
        # Score based on overlap percentage
        if overlap_pct >= 15:
            return min(100, overlap_pct * 5)  # Scale up for visualization
        return overlap_pct * 2

    def calculate_lead_time_consistency(self, shared_token_timings: List[float]) -> float:
        """B. Calculate lead time consistency score (0-100)"""
        if len(shared_token_timings) < 2:
            return 0
        
        # Calculate standard deviation of lead times
        std_dev = statistics.stdev(shared_token_timings)
        
        # Low std dev = high score (consistent timing = suspicious)
        # High std dev = low score (random timing = not suspicious)
        if std_dev < 2:  # Within 2 hours variance
            return 100
        elif std_dev < 12:  # Within 12 hours variance
            return max(0, 100 - (std_dev * 7))
        else:
            return 0

    def calculate_statistical_unlikelihood(self, wallet: str, shared_purchases: List[Dict], 
                                          gake_purchases: List[Dict]) -> float:
        """C. Calculate how statistically unlikely this timing is (0-100)"""
        if not shared_purchases:
            return 0
        
        unlikelihood_scores = []
        
        for shared in shared_purchases:
            token_mint = shared['token_mint']
            wallet_time = shared['wallet_timestamp']
            gake_time = shared['gake_timestamp']
            time_diff = abs(wallet_time - gake_time) / 3600  # hours
            
            # Check against baseline: how many other buyers had similar timing?
            if token_mint in self.baseline_data['token_timing_distributions']:
                all_times = self.baseline_data['token_timing_distributions'][token_mint]
                
                # Count how many buyers were within same time window relative to GAKE
                similar_timing_count = 0
                for buyer_time in all_times:
                    buyer_diff = abs(buyer_time - gake_time) / 3600
                    if abs(buyer_diff - time_diff) < 2:  # Within 2 hours of same pattern
                        similar_timing_count += 1
                
                # If <10% of buyers had similar timing, it's suspicious
                if all_times:
                    similar_pct = (similar_timing_count / len(all_times)) * 100
                    if similar_pct < 10:
                        unlikelihood_scores.append(100 - similar_pct * 5)
                    else:
                        unlikelihood_scores.append(0)
        
        return statistics.mean(unlikelihood_scores) if unlikelihood_scores else 0

    def calculate_cross_pattern_correlation(self, wallet: str, all_flagged_wallets: Dict) -> float:
        """D. Check if multiple wallets show same pattern (0-100)"""
        # This will be calculated after identifying initial candidates
        return 0  # Placeholder for now

    def calculate_coordination_score(self, wallet: str, wallet_purchases: List[Dict], 
                                     gake_purchases: List[Dict]) -> Dict:
        """Phase 2: Calculate multi-signal coordination score"""
        
        wallet_tokens = set([p['token_mint'] for p in wallet_purchases])
        gake_tokens = set([p['token_mint'] for p in gake_purchases])
        
        # Find shared tokens where wallet bought BEFORE GAKE
        shared_purchases = []
        lead_times = []
        
        for gake_p in gake_purchases:
            for wallet_p in wallet_purchases:
                if (wallet_p['token_mint'] == gake_p['token_mint'] and 
                    wallet_p['timestamp'] < gake_p['timestamp']):
                    
                    time_diff_hours = (gake_p['timestamp'] - wallet_p['timestamp']) / 3600
                    
                    if 1 <= time_diff_hours <= 168:  # 1 hour to 1 week
                        shared_purchases.append({
                            'token_mint': gake_p['token_mint'],
                            'token_symbol': gake_p['token_symbol'],
                            'wallet_timestamp': wallet_p['timestamp'],
                            'gake_timestamp': gake_p['timestamp'],
                            'lead_time_hours': time_diff_hours,
                            'wallet_signature': wallet_p['signature'],
                            'gake_signature': gake_p['signature']
                        })
                        lead_times.append(time_diff_hours)
                        break
        
        # Must have at least 2 shared tokens bought before GAKE
        if len(shared_purchases) < 2:
            return None
        
        # Calculate scores
        overlap_score = self.calculate_overlap_score(wallet_tokens, gake_tokens)
        lead_consistency_score = self.calculate_lead_time_consistency(lead_times)
        unlikelihood_score = self.calculate_statistical_unlikelihood(wallet, shared_purchases, gake_purchases)
        
        # Overall coordination score (weighted average)
        final_score = (
            overlap_score * 0.3 +
            lead_consistency_score * 0.4 +
            unlikelihood_score * 0.3
        )
        
        # Check against baseline threshold
        percentile_95 = np.percentile(self.baseline_data['random_wallet_overlaps'], 95) if self.baseline_data['random_wallet_overlaps'] else 10
        overlap_pct = (len(shared_purchases) / len(gake_tokens)) * 100
        
        return {
            'wallet': wallet,
            'final_score': final_score,
            'overlap_score': overlap_score,
            'lead_consistency_score': lead_consistency_score,
            'unlikelihood_score': unlikelihood_score,
            'shared_token_count': len(shared_purchases),
            'overlap_percentage': overlap_pct,
            'exceeds_baseline': overlap_pct > percentile_95,
            'lead_time_std_dev': statistics.stdev(lead_times) if len(lead_times) > 1 else 0,
            'avg_lead_time_hours': statistics.mean(lead_times),
            'shared_purchases': shared_purchases
        }

    # ==================== PHASE 3: NETWORK ANALYSIS ====================
    
    def analyze_network_patterns(self, high_score_wallets: Dict):
        """Phase 3: Analyze relationships between high-scoring wallets"""
        print("\n" + "="*80)
        print("PHASE 3: NETWORK PATTERN ANALYSIS")
        print("="*80)
        
        if len(high_score_wallets) < 2:
            print("\n  Not enough high-scoring wallets for network analysis")
            return {}
        
        network_findings = {}
        wallet_list = list(high_score_wallets.keys())
        
        print(f"\n  Analyzing relationships between {len(wallet_list)} wallets...")
        
        for i, wallet1 in enumerate(wallet_list):
            network_findings[wallet1] = {
                'correlated_wallets': [],
                'shared_timing_patterns': 0,
                'funding_analysis': 'Not implemented',  # Would require additional API calls
            }
            
            for j, wallet2 in enumerate(wallet_list):
                if i >= j:
                    continue
                
                # Find shared tokens between these two wallets
                wallet1_purchases = high_score_wallets[wallet1]['shared_purchases']
                wallet2_purchases = high_score_wallets[wallet2]['shared_purchases']
                
                wallet1_tokens = {p['token_mint']: p for p in wallet1_purchases}
                wallet2_tokens = {p['token_mint']: p for p in wallet2_purchases}
                
                shared_tokens = set(wallet1_tokens.keys()) & set(wallet2_tokens.keys())
                
                if len(shared_tokens) >= 2:  # They both bought same tokens before GAKE
                    # Check if their timing is also correlated
                    timing_correlations = []
                    for token_mint in shared_tokens:
                        time_diff = abs(wallet1_tokens[token_mint]['wallet_timestamp'] - 
                                      wallet2_tokens[token_mint]['wallet_timestamp']) / 3600
                        timing_correlations.append(time_diff)
                    
                    avg_time_diff = statistics.mean(timing_correlations)
                    
                    # If they bought within 6 hours of each other on average
                    if avg_time_diff < 6:
                        network_findings[wallet1]['correlated_wallets'].append({
                            'wallet': wallet2,
                            'shared_tokens': len(shared_tokens),
                            'avg_time_difference_hours': avg_time_diff
                        })
                        network_findings[wallet1]['shared_timing_patterns'] += 1
                        
                        # Log detailed finding
                        for token_mint in shared_tokens:
                            self.detailed_findings.append({
                                'finding_type': 'NETWORK_CORRELATION',
                                'wallet_1': wallet1,
                                'wallet_2': wallet2,
                                'token_mint': token_mint,
                                'token_symbol': wallet1_tokens[token_mint]['token_symbol'],
                                'wallet_1_timestamp': wallet1_tokens[token_mint]['wallet_timestamp'],
                                'wallet_1_datetime': datetime.fromtimestamp(wallet1_tokens[token_mint]['wallet_timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                                'wallet_2_timestamp': wallet2_tokens[token_mint]['wallet_timestamp'],
                                'wallet_2_datetime': datetime.fromtimestamp(wallet2_tokens[token_mint]['wallet_timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                                'time_difference_hours': abs(wallet1_tokens[token_mint]['wallet_timestamp'] - wallet2_tokens[token_mint]['wallet_timestamp']) / 3600,
                                'wallet_1_signature': wallet1_tokens[token_mint]['wallet_signature'],
                                'wallet_2_signature': wallet2_tokens[token_mint]['wallet_signature']
                            })
        
        return network_findings

    def run_complete_analysis(self):
        """Run all three phases of analysis"""
        print("\n" + "="*80)
        print("STATISTICAL COORDINATION ANALYSIS")
        print("="*80)
        print(f"Target Wallet: {self.target_wallet}")
        print(f"Analysis Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Get GAKE's purchases
        print(f"\nFetching target wallet trading history...")
        gake_purchases = self.get_token_purchases(self.target_wallet, 100)
        
        if not gake_purchases:
            print("❌ No purchases found for target wallet")
            print("\nDEBUG: Checking raw transaction data...")
            
            # Debug: Get raw transactions
            raw_txs = self.get_wallet_transactions(self.target_wallet, 10)
            print(f"  Found {len(raw_txs)} raw transactions")
            
            if raw_txs:
                print(f"  Sample transaction structure:")
                sample_tx = raw_txs[0]
                print(f"    Has 'meta': {'meta' in sample_tx}")
                if 'meta' in sample_tx:
                    print(f"    Has 'preTokenBalances': {'preTokenBalances' in sample_tx['meta']}")
                    print(f"    Has 'postTokenBalances': {'postTokenBalances' in sample_tx['meta']}")
                    if 'postTokenBalances' in sample_tx['meta']:
                        print(f"    postTokenBalances count: {len(sample_tx['meta']['postTokenBalances'])}")
                print(f"\n  This suggests the wallet may not have token purchase transactions in the recent history.")
                print(f"  The wallet may only have SOL transfers or other non-token transactions.")
            else:
                print(f"  Could not retrieve any transactions. Check API connectivity.")
            
            return
        
        print(f"✓ Found {len(gake_purchases)} token purchases")
        
        # PHASE 1: Build baselines
        self.build_baseline_data(gake_purchases)
        
        # PHASE 2: Score potential coordinated wallets
        print("\n" + "="*80)
        print("PHASE 2: CALCULATING COORDINATION SCORES")
        print("="*80)
        
        # Get all potential wallets to analyze
        potential_wallets = set()
        for purchase in gake_purchases[:20]:
            holders = self.get_top_token_holders(purchase['token_mint'], 20)
            potential_wallets.update(holders)
        
        print(f"\n  Analyzing {len(potential_wallets)} potential coordinated wallets...")
        
        scored_wallets = {}
        for i, wallet in enumerate(potential_wallets):
            if wallet == self.target_wallet:
                continue
            
            if (i + 1) % 10 == 0:
                print(f"  Progress: {i+1}/{len(potential_wallets)}")
            
            wallet_purchases = self.get_token_purchases(wallet, 100)
            score_data = self.calculate_coordination_score(wallet, wallet_purchases, gake_purchases)
            
            if score_data and score_data['final_score'] >= 40:  # Threshold for flagging
                scored_wallets[wallet] = score_data
                
                # Log primary coordination findings
                for shared in score_data['shared_purchases']:
                    self.detailed_findings.append({
                        'finding_type': 'PRIMARY_COORDINATION',
                        'coordinated_wallet': wallet,
                        'target_wallet': self.target_wallet,
                        'coordination_score': score_data['final_score'],
                        'token_mint': shared['token_mint'],
                        'token_symbol': shared['token_symbol'],
                        'coordinated_wallet_timestamp': shared['wallet_timestamp'],
                        'coordinated_wallet_datetime': datetime.fromtimestamp(shared['wallet_timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                        'target_wallet_timestamp': shared['gake_timestamp'],
                        'target_wallet_datetime': datetime.fromtimestamp(shared['gake_timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                        'lead_time_hours': shared['lead_time_hours'],
                        'coordinated_wallet_signature': shared['wallet_signature'],
                        'target_wallet_signature': shared['gake_signature']
                    })
            
            time.sleep(0.1)
        
        print(f"\n  ✓ Found {len(scored_wallets)} wallets with coordination scores ≥40")
        
        # PHASE 3: Network analysis
        high_score_wallets = {k: v for k, v in scored_wallets.items() if v['final_score'] >= 70}
        network_analysis = {}
        
        if high_score_wallets:
            network_analysis = self.analyze_network_patterns(high_score_wallets)
        
        # Generate final report
        self.generate_report(scored_wallets, network_analysis, gake_purchases)

    def generate_report(self, scored_wallets: Dict, network_analysis: Dict, gake_purchases: List[Dict]):
        """Generate comprehensive analysis report"""
        print("\n" + "="*80)
        print("ANALYSIS RESULTS")
        print("="*80)
        
        if not scored_wallets:
            print("\n✓ No statistically significant coordination detected")
            print("  All wallets fall within normal baseline parameters")
            return
        
        # Sort by score
        sorted_wallets = sorted(scored_wallets.items(), key=lambda x: x[1]['final_score'], reverse=True)
        
        # Categorize by score
        strong_coordination = [(w, d) for w, d in sorted_wallets if d['final_score'] >= 70]
        possible_coordination = [(w, d) for w, d in sorted_wallets if 40 <= d['final_score'] < 70]
        
        print(f"\n📊 SUMMARY:")
        print(f"  Strong Coordination (score ≥70): {len(strong_coordination)} wallets")
        print(f"  Possible Coordination (score 40-69): {len(possible_coordination)} wallets")
        print(f"  Total Flagged: {len(scored_wallets)} wallets")
        
        # Detailed results for strong coordination
        if strong_coordination:
            print(f"\n🚨 STRONG COORDINATION DETECTED ({len(strong_coordination)} wallets):")
            
            for wallet, data in strong_coordination[:10]:  # Show top 10
                print(f"\n  Wallet: {wallet}")
                print(f"    Coordination Score: {data['final_score']:.1f}/100")
                print(f"    ├─ Overlap Score: {data['overlap_score']:.1f}/100")
                print(f"    ├─ Timing Consistency: {data['lead_consistency_score']:.1f}/100")
                print(f"    └─ Statistical Unlikelihood: {data['unlikelihood_score']:.1f}/100")
                print(f"    ")
                print(f"    Shared Tokens: {data['shared_token_count']} ({data['overlap_percentage']:.1f}%)")
                print(f"    Avg Lead Time: {data['avg_lead_time_hours']:.1f} hours")
                print(f"    Lead Time Consistency: ±{data['lead_time_std_dev']:.1f} hours")
                print(f"    Exceeds Baseline: {'YES' if data['exceeds_baseline'] else 'NO'}")
                
                # Network connections
                if wallet in network_analysis and network_analysis[wallet]['correlated_wallets']:
                    print(f"    Network Correlations: {len(network_analysis[wallet]['correlated_wallets'])} other wallets")
                    for corr in network_analysis[wallet]['correlated_wallets'][:3]:
                        print(f"      • {corr['wallet']} ({corr['shared_tokens']} shared tokens)")
                
                # Show sample coordinated purchases
                print(f"    Sample Coordinated Purchases:")
                for purchase in data['shared_purchases'][:3]:
                    print(f"      • {purchase['token_symbol']}: {purchase['lead_time_hours']:.1f}h before target")
        
        # Possible coordination
        if possible_coordination:
            print(f"\n⚠️  POSSIBLE COORDINATION ({len(possible_coordination)} wallets):")
            for wallet, data in possible_coordination[:5]:
                print(f"\n  Wallet: {wallet}")
                print(f"    Score: {data['final_score']:.1f}/100 | Shared: {data['shared_token_count']} tokens | Lead: {data['avg_lead_time_hours']:.1f}h")
        
        # Save results
        self.save_results(scored_wallets, network_analysis)
        
        print(f"\n" + "="*80)
        print("FILES GENERATED:")
        print("="*80)
        print("  • coordination_scores.csv - All scored wallets")
        print("  • high_confidence_coordination.csv - Score ≥70 wallets")
        print("  • network_analysis.csv - Wallet-to-wallet correlations")
        print("  • detailed_findings.csv - All coordination events with full addresses")
        print("  • analysis_summary.json - Complete analysis data")
        if self.error_log:
            print("  • error_log.csv - API errors encountered")

    def save_results(self, scored_wallets: Dict, network_analysis: Dict):
        """Save all analysis results"""
        
        # 1. Coordination scores
        if scored_wallets:
            scores_df = pd.DataFrame([
                {
                    'wallet_address': wallet,
                    'coordination_score': data['final_score'],
                    'overlap_score': data['overlap_score'],
                    'timing_consistency_score': data['lead_consistency_score'],
                    'statistical_unlikelihood_score': data['unlikelihood_score'],
                    'shared_tokens': data['shared_token_count'],
                    'overlap_percentage': data['overlap_percentage'],
                    'exceeds_baseline': data['exceeds_baseline'],
                    'avg_lead_time_hours': data['avg_lead_time_hours'],
                    'lead_time_std_dev': data['lead_time_std_dev'],
                    'classification': 'STRONG' if data['final_score'] >= 70 else 'POSSIBLE'
                }
                for wallet, data in scored_wallets.items()
            ])
            scores_df = scores_df.sort_values('coordination_score', ascending=False)
            scores_df.to_csv('coordination_scores.csv', index=False)
        
        # 2. High confidence coordination only
        high_confidence = {w: d for w, d in scored_wallets.items() if d['final_score'] >= 70}
        if high_confidence:
            hc_df = pd.DataFrame([
                {
                    'wallet_address': wallet,
                    'coordination_score': data['final_score'],
                    'shared_tokens': data['shared_token_count'],
                    'overlap_percentage': data['overlap_percentage'],
                    'avg_lead_time_hours': data['avg_lead_time_hours'],
                    'timing_consistency_std_dev': data['lead_time_std_dev']
                }
                for wallet, data in high_confidence.items()
            ])
            hc_df = hc_df.sort_values('coordination_score', ascending=False)
            hc_df.to_csv('high_confidence_coordination.csv', index=False)
        
        # 3. Network analysis
        if network_analysis:
            network_rows = []
            for wallet, analysis in network_analysis.items():
                for corr in analysis['correlated_wallets']:
                    network_rows.append({
                        'wallet_1': wallet,
                        'wallet_2': corr['wallet'],
                        'shared_tokens': corr['shared_tokens'],
                        'avg_time_difference_hours': corr['avg_time_difference_hours']
                    })
            
            if network_rows:
                network_df = pd.DataFrame(network_rows)
                network_df.to_csv('network_analysis.csv', index=False)
        
        # 4. Detailed findings
        if self.detailed_findings:
            findings_df = pd.DataFrame(self.detailed_findings)
            findings_df = findings_df.sort_values(['finding_type', 'coordination_score'], ascending=[True, False])
            findings_df.to_csv('detailed_findings.csv', index=False)
        
        # 5. Complete summary JSON
        summary = {
            'analysis_timestamp': datetime.now().isoformat(),
            'target_wallet': self.target_wallet,
            'baseline_statistics': {
                'mean_overlap': statistics.mean(self.baseline_data['random_wallet_overlaps']) if self.baseline_data['random_wallet_overlaps'] else 0,
                'median_overlap': statistics.median(self.baseline_data['random_wallet_overlaps']) if self.baseline_data['random_wallet_overlaps'] else 0,
                'percentile_95': float(np.percentile(self.baseline_data['random_wallet_overlaps'], 95)) if self.baseline_data['random_wallet_overlaps'] else 0,
                'sampled_wallets': len(self.baseline_data['random_wallet_overlaps']),
                'analyzed_tokens': len(self.baseline_data['all_token_buyers'])
            },
            'results': {
                'total_flagged_wallets': len(scored_wallets),
                'strong_coordination': len([w for w, d in scored_wallets.items() if d['final_score'] >= 70]),
                'possible_coordination': len([w for w, d in scored_wallets.items() if 40 <= d['final_score'] < 70]),
                'network_correlations': sum(len(a['correlated_wallets']) for a in network_analysis.values()) if network_analysis else 0
            },
            'coordination_scores': {wallet: {k: v for k, v in data.items() if k != 'shared_purchases'} 
                                   for wallet, data in scored_wallets.items()},
            'network_analysis': network_analysis,
            'errors': len(self.error_log)
        }
        
        with open('analysis_summary.json', 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        # 6. Error log
        if self.error_log:
            error_df = pd.DataFrame(self.error_log)
            error_df.to_csv('error_log.csv', index=False)

def main():
    try:
        analyzer = StatisticalCoordinationAnalyzer()
        analyzer.run_complete_analysis()
    except Exception as e:
        print(f"Fatal Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()