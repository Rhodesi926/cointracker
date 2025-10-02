import requests
import time
import os
from collections import defaultdict
from typing import List, Dict, Tuple
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

class SolanaWalletAnalyzer:
    """
    Analyzes top profitable trader wallets for a specified Solana token and traces funding sources.
    Uses Helius API for Solana blockchain data.
    """
    
    def __init__(self, helius_api_key: str):
        """
        Initialize the analyzer with Helius API.
        
        Args:
            helius_api_key: Your Helius API key
        """
        self.api_key = helius_api_key
        self.base_url = f"https://api.helius.xyz/v0"
        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
    
    def get_token_holders(self, token_mint: str, limit: int = 100) -> List[Dict]:
        """
        Get token holders for a specified token mint address.
        
        Args:
            token_mint: Token mint address
            limit: Number of holders to retrieve
            
        Returns:
            List of holder information
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenLargestAccounts",
            "params": [token_mint]
        }
        
        try:
            response = requests.post(self.rpc_url, json=payload, timeout=10)
            data = response.json()
            
            if "result" in data and "value" in data["result"]:
                accounts = data["result"]["value"]
                return accounts[:limit]
            else:
                print(f"Error getting token holders: {data}")
                return []
        except Exception as e:
            print(f"Error fetching token holders: {e}")
            return []
    
    def get_parsed_transactions(self, address: str, limit: int = 100) -> List[Dict]:
        """
        Get parsed transaction history for a wallet using standard RPC method.
        
        Args:
            address: Wallet address to analyze
            limit: Number of transactions to retrieve
            
        Returns:
            List of parsed transactions
        """
        # First, get signatures
        signatures = self.get_signature_for_address(address, limit=limit)
        
        if not signatures:
            return []
        
        # Then get transaction details for each signature
        transactions = []
        for sig in signatures[:limit]:  # Limit to avoid too many requests
            tx_details = self.get_transaction_details(sig)
            if tx_details:
                transactions.append(tx_details)
            time.sleep(0.05)  # Small delay to avoid rate limiting
        
        return transactions
    
    def get_token_accounts_by_owner(self, owner_address: str, token_mint: str) -> List[Dict]:
        """
        Get token accounts owned by a specific address for a specific mint.
        
        Args:
            owner_address: Owner wallet address
            token_mint: Token mint address
            
        Returns:
            List of token accounts
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                owner_address,
                {"mint": token_mint},
                {"encoding": "jsonParsed"}
            ]
        }
        
        try:
            response = requests.post(self.rpc_url, json=payload, timeout=10)
            data = response.json()
            
            if "result" in data and "value" in data["result"]:
                return data["result"]["value"]
            return []
        except Exception as e:
            print(f"Error fetching token accounts: {e}")
            return []
    
    def calculate_trader_pnl(self, wallet_address: str, token_mint: str, debug: bool = False) -> Dict:
        """
        Calculate comprehensive trading metrics including PnL, ROI, and win rate.
        
        Args:
            wallet_address: Wallet to analyze
            token_mint: Token mint address
            debug: Print debug information
            
        Returns:
            Dictionary with detailed trading metrics
        """
        transactions = self.get_parsed_transactions(wallet_address, limit=100)
        
        if debug:
            print(f"\n    DEBUG: Found {len(transactions)} transactions for {wallet_address[:10]}...")
        
        total_bought = 0  # Total tokens bought
        total_sold = 0    # Total tokens sold
        total_spent_sol = 0  # Total SOL spent buying
        total_received_sol = 0  # Total SOL received from selling
        
        buy_trades = []
        sell_trades = []
        all_trades = []
        
        current_holdings = 0
        
        for tx in transactions:
            if not tx or "meta" not in tx or "transaction" not in tx:
                continue
            
            meta = tx.get("meta", {})
            transaction = tx.get("transaction", {})
            message = transaction.get("message", {})
            block_time = tx.get("blockTime", 0)
            
            # Get account keys
            account_keys = message.get("accountKeys", [])
            pre_token_balances = meta.get("preTokenBalances", [])
            post_token_balances = meta.get("postTokenBalances", [])
            pre_balances = meta.get("preBalances", [])
            post_balances = meta.get("postBalances", [])
            
            # Track token changes for this specific mint
            token_change = 0
            sol_change = 0
            
            # Find wallet's SOL balance change
            wallet_index = None
            for i, account in enumerate(account_keys):
                account_pubkey = account if isinstance(account, str) else account.get("pubkey", "")
                if account_pubkey == wallet_address:
                    wallet_index = i
                    break
            
            if wallet_index is not None and wallet_index < len(pre_balances):
                sol_change = (post_balances[wallet_index] - pre_balances[wallet_index]) / 1e9
            
            # Find token balance changes for our token
            for pre_bal in pre_token_balances:
                if pre_bal.get("mint") == token_mint and pre_bal.get("owner") == wallet_address:
                    pre_amount = float(pre_bal.get("uiTokenAmount", {}).get("uiAmount", 0))
                    account_index = pre_bal.get("accountIndex")
                    
                    # Find corresponding post balance
                    for post_bal in post_token_balances:
                        if post_bal.get("accountIndex") == account_index:
                            post_amount = float(post_bal.get("uiTokenAmount", {}).get("uiAmount", 0))
                            token_change = post_amount - pre_amount
                            break
            
            if debug and (token_change != 0 or abs(sol_change) > 0.001):
                print(f"    DEBUG: TX - Token change: {token_change:.2f}, SOL change: {sol_change:.4f}")
            
            # Determine if this was a buy or sell
            if token_change > 0 and sol_change < -0.001:
                # Bought tokens with SOL
                total_bought += token_change
                total_spent_sol += abs(sol_change)
                current_holdings += token_change
                
                buy_price = abs(sol_change) / token_change if token_change > 0 else 0
                buy_trades.append({
                    "timestamp": block_time,
                    "amount": token_change,
                    "price_per_token": buy_price,
                    "total_cost": abs(sol_change)
                })
                all_trades.append(("BUY", block_time, token_change, buy_price))
                
                if debug:
                    print(f"    DEBUG: BUY detected - {token_change:.2f} tokens for {abs(sol_change):.4f} SOL")
            
            elif token_change < 0 and sol_change > 0.001:
                # Sold tokens for SOL
                total_sold += abs(token_change)
                total_received_sol += sol_change
                current_holdings -= abs(token_change)
                
                sell_price = sol_change / abs(token_change) if token_change != 0 else 0
                sell_trades.append({
                    "timestamp": block_time,
                    "amount": abs(token_change),
                    "price_per_token": sell_price,
                    "total_received": sol_change
                })
                all_trades.append(("SELL", block_time, abs(token_change), sell_price))
                
                if debug:
                    print(f"    DEBUG: SELL detected - {abs(token_change):.2f} tokens for {sol_change:.4f} SOL")
        
        if debug:
            print(f"    DEBUG: Total trades: {len(buy_trades)} buys, {len(sell_trades)} sells")
        
        # Calculate current token holdings value
        current_price = 0
        if all_trades:
            recent_trades = sorted(all_trades, key=lambda x: x[1], reverse=True)[:5]
            if recent_trades:
                current_price = sum(t[3] for t in recent_trades) / len(recent_trades)
        
        current_holdings = max(0, current_holdings)
        current_holdings_value_sol = current_holdings * current_price
        
        # Calculate PnL
        realized_pnl = total_received_sol - total_spent_sol
        unrealized_pnl = current_holdings_value_sol - (total_spent_sol - total_received_sol) if current_holdings > 0 else 0
        total_pnl = realized_pnl + unrealized_pnl
        
        # Calculate ROI
        roi = (total_pnl / total_spent_sol * 100) if total_spent_sol > 0 else 0
        
        # Calculate win rate
        winning_trades = 0
        total_closed_trades = 0
        
        if buy_trades and sell_trades:
            avg_buy_price = sum(t["price_per_token"] for t in buy_trades) / len(buy_trades)
            for sell in sell_trades:
                total_closed_trades += 1
                if sell["price_per_token"] > avg_buy_price:
                    winning_trades += 1
        
        win_rate = (winning_trades / total_closed_trades * 100) if total_closed_trades > 0 else 0
        
        # Calculate early entry score
        early_entry_score = 0
        if buy_trades:
            earliest_buy = min(buy_trades, key=lambda x: x["timestamp"])
            avg_buy_time = sum(t["timestamp"] for t in buy_trades) / len(buy_trades)
            time_diff = avg_buy_time - earliest_buy["timestamp"]
            early_entry_score = 100 - min(100, time_diff / 86400)
        
        return {
            "address": wallet_address,
            "total_pnl_sol": total_pnl,
            "realized_pnl_sol": realized_pnl,
            "unrealized_pnl_sol": unrealized_pnl,
            "roi_percent": roi,
            "win_rate_percent": win_rate,
            "total_bought": total_bought,
            "total_sold": total_sold,
            "current_holdings": current_holdings,
            "total_spent_sol": total_spent_sol,
            "total_received_sol": total_received_sol,
            "num_buy_trades": len(buy_trades),
            "num_sell_trades": len(sell_trades),
            "total_trades": len(buy_trades) + len(sell_trades),
            "early_entry_score": early_entry_score,
            "first_buy_timestamp": min(buy_trades, key=lambda x: x["timestamp"])["timestamp"] if buy_trades else 0,
            "avg_buy_price": sum(t["price_per_token"] for t in buy_trades) / len(buy_trades) if buy_trades else 0,
            "avg_sell_price": sum(t["price_per_token"] for t in sell_trades) / len(sell_trades) if sell_trades else 0
        }
    
    def get_signature_for_address(self, address: str, limit: int = 1000) -> List[str]:
        """
        Get transaction signatures for an address.
        
        Args:
            address: Wallet address
            limit: Number of signatures to retrieve
            
        Returns:
            List of transaction signatures
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                address,
                {"limit": limit}
            ]
        }
        
        try:
            response = requests.post(self.rpc_url, json=payload, timeout=15)
            data = response.json()
            
            if "result" in data:
                return [sig["signature"] for sig in data["result"]]
            return []
        except Exception as e:
            print(f"Error getting signatures: {e}")
            return []
    
    def get_transaction_details(self, signature: str) -> Dict:
        """
        Get detailed transaction information.
        
        Args:
            signature: Transaction signature
            
        Returns:
            Transaction details
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        }
        
        try:
            response = requests.post(self.rpc_url, json=payload, timeout=10)
            data = response.json()
            return data.get("result", {})
        except Exception as e:
            return {}
    
    def find_funding_source(self, wallet_address: str, min_amount: float = 0.01) -> Tuple[str, float]:
        """
        Find the primary funding source for a wallet.
        Looks for the first significant incoming SOL transaction.
        
        Args:
            wallet_address: Wallet to trace
            min_amount: Minimum SOL amount to consider as funding (default: 0.01)
            
        Returns:
            Tuple of (funding_wallet_address, amount_in_sol)
        """
        # Get all transaction signatures
        signatures = self.get_signature_for_address(wallet_address, limit=1000)
        
        if not signatures:
            return "Unknown", 0.0
        
        # Check the oldest transactions (reverse order)
        signatures_to_check = signatures[-50:]  # Check last 50 (oldest) transactions
        
        for sig in reversed(signatures_to_check):  # Start from the very first transaction
            tx_details = self.get_transaction_details(sig)
            
            if not tx_details or "meta" not in tx_details:
                continue
            
            meta = tx_details.get("meta", {})
            transaction = tx_details.get("transaction", {})
            message = transaction.get("message", {})
            
            # Get account keys
            account_keys = message.get("accountKeys", [])
            if not account_keys:
                continue
            
            # Get pre and post balances to find SOL transfers
            pre_balances = meta.get("preBalances", [])
            post_balances = meta.get("postBalances", [])
            
            # Find the wallet's index
            wallet_index = None
            for i, account in enumerate(account_keys):
                account_pubkey = account if isinstance(account, str) else account.get("pubkey", "")
                if account_pubkey == wallet_address:
                    wallet_index = i
                    break
            
            if wallet_index is None or wallet_index >= len(pre_balances):
                continue
            
            # Check if wallet received SOL
            balance_change = (post_balances[wallet_index] - pre_balances[wallet_index]) / 1e9
            
            if balance_change >= min_amount:
                # Find who sent it - look for account with negative balance change
                for i, account in enumerate(account_keys):
                    if i < len(pre_balances) and i < len(post_balances):
                        sender_change = (post_balances[i] - pre_balances[i]) / 1e9
                        account_pubkey = account if isinstance(account, str) else account.get("pubkey", "")
                        
                        if sender_change < 0 and account_pubkey != wallet_address:
                            return account_pubkey, balance_change
            
            time.sleep(0.1)  # Rate limiting
        
        return "Unknown", 0.0
    
    def get_wallet_owner_from_token_account(self, token_account: str) -> str:
        """
        Get the owner of a token account.
        
        Args:
            token_account: Token account address
            
        Returns:
            Owner wallet address
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                token_account,
                {"encoding": "jsonParsed"}
            ]
        }
        
        try:
            response = requests.post(self.rpc_url, json=payload, timeout=10)
            data = response.json()
            
            if "result" in data and data["result"] and "value" in data["result"]:
                parsed = data["result"]["value"].get("data", {}).get("parsed", {})
                info = parsed.get("info", {})
                return info.get("owner", "Unknown")
            return "Unknown"
        except Exception as e:
            print(f"Error getting account owner: {e}")
            return "Unknown"
    
    def find_top_traders_and_funders(self, token_mint: str, top_n: int = 10, 
                                     min_trades: int = 3, rank_by: str = "pnl", debug: bool = False) -> Dict:
        """
        Main function to find top profitable traders and their funding sources.
        
        Args:
            token_mint: Token mint address
            top_n: Number of top traders to analyze
            min_trades: Minimum number of trades required
            rank_by: Ranking criteria - "pnl", "roi", "win_rate", or "volume"
            
        Returns:
            Dictionary with analysis results
        """
        print(f"Analyzing Solana token: {token_mint}")
        print(f"Finding top {top_n} most profitable traders (ranked by {rank_by.upper()})...\n")
        
        # Get largest token holders
        token_accounts = self.get_token_holders(token_mint, limit=100)
        
        if not token_accounts:
            print("Could not retrieve token holders. Check API key and token mint.")
            return {}
        
        # Get owner addresses for token accounts
        print("Resolving token account owners...")
        trader_addresses = []
        
        for account in token_accounts[:top_n * 5]:  # Get extra to ensure we have enough active traders
            account_address = account.get("address", "")
            if account_address:
                owner = self.get_wallet_owner_from_token_account(account_address)
                if owner != "Unknown":
                    trader_addresses.append(owner)
                time.sleep(0.2)  # Rate limiting
        
        # Remove duplicates
        trader_addresses = list(set(trader_addresses))
        
        # Analyze trading profitability for each holder
        print(f"\nAnalyzing profitability for {len(trader_addresses)} wallets...")
        trader_metrics = []
        
        for i, wallet in enumerate(trader_addresses, 1):
            print(f"Analyzing wallet {i}/{len(trader_addresses)}: {wallet[:10]}...")
            
            # Enable debug for first 2 wallets to see what's happening
            show_debug = debug or (i <= 2)
            metrics = self.calculate_trader_pnl(wallet, token_mint, debug=show_debug)
            
            # Only include traders with minimum number of trades
            if metrics["total_trades"] >= min_trades:
                trader_metrics.append(metrics)
            elif show_debug:
                print(f"    SKIPPED: Only {metrics['total_trades']} trades (minimum: {min_trades})")
            
            time.sleep(0.5)  # Rate limiting
        
        # Sort by chosen metric
        if rank_by == "pnl":
            top_traders = sorted(trader_metrics, key=lambda x: x["total_pnl_sol"], reverse=True)[:top_n]
        elif rank_by == "roi":
            top_traders = sorted(trader_metrics, key=lambda x: x["roi_percent"], reverse=True)[:top_n]
        elif rank_by == "win_rate":
            top_traders = sorted(trader_metrics, key=lambda x: x["win_rate_percent"], reverse=True)[:top_n]
        elif rank_by == "volume":
            top_traders = sorted(trader_metrics, key=lambda x: x["total_spent_sol"], reverse=True)[:top_n]
        else:
            top_traders = sorted(trader_metrics, key=lambda x: x["total_pnl_sol"], reverse=True)[:top_n]
        
        print(f"\n{'='*80}")
        print(f"TOP {top_n} PROFITABLE TRADERS (Ranked by {rank_by.upper()})")
        print(f"{'='*80}\n")
        
        # Find funding sources
        funding_sources = defaultdict(list)
        results = {
            "token_mint": token_mint,
            "ranking_criteria": rank_by,
            "top_traders": [],
            "funding_sources": {}
        }
        
        for rank, trader in enumerate(top_traders, 1):
            wallet = trader["address"]
            
            print(f"{rank}. Wallet: {wallet}")
            print(f"   💰 Total PnL: {trader['total_pnl_sol']:.4f} SOL (Realized: {trader['realized_pnl_sol']:.4f}, Unrealized: {trader['unrealized_pnl_sol']:.4f})")
            print(f"   📈 ROI: {trader['roi_percent']:.2f}%")
            print(f"   🎯 Win Rate: {trader['win_rate_percent']:.2f}%")
            print(f"   📊 Trades: {trader['total_trades']} ({trader['num_buy_trades']} buys, {trader['num_sell_trades']} sells)")
            print(f"   💵 Volume: Spent {trader['total_spent_sol']:.4f} SOL, Received {trader['total_received_sol']:.4f} SOL")
            print(f"   🪙 Holdings: {trader['current_holdings']:.2f} tokens")
            
            if trader['first_buy_timestamp'] > 0:
                buy_date = datetime.fromtimestamp(trader['first_buy_timestamp']).strftime('%Y-%m-%d')
                print(f"   📅 First Buy: {buy_date}")
            
            # Find funding source
            print(f"   🔍 Tracing funding source...")
            funder, amount = self.find_funding_source(wallet, min_amount=0.01)
            
            if funder == "Unknown":
                print(f"   💸 Funded by: Could not trace (may be an old wallet or funded via tokens)")
                print(f"   Initial funding: N/A\n")
            else:
                print(f"   💸 Funded by: {funder}")
                print(f"   Initial funding: {amount:.4f} SOL\n")
            
            # Track funding sources
            funding_sources[funder].append({
                "trader": wallet,
                "amount": amount,
                "pnl": trader['total_pnl_sol']
            })
            
            results["top_traders"].append({
                **trader,
                "funder": funder,
                "funding_amount": amount
            })
            
            time.sleep(0.5)  # Rate limiting
        
        # Summarize funding sources
        print(f"\n{'='*80}")
        print(f"FUNDING SOURCES SUMMARY")
        print(f"{'='*80}\n")
        
        for funder, wallets in sorted(funding_sources.items(), 
                                      key=lambda x: len(x[1]), 
                                      reverse=True):
            print(f"Funder: {funder}")
            print(f"  📊 Funded {len(wallets)} wallet(s) in top {top_n}")
            total_funded = sum(w["amount"] for w in wallets)
            total_pnl = sum(w["pnl"] for w in wallets)
            print(f"  💵 Total funded: {total_funded:.4f} SOL")
            print(f"  💰 Combined PnL of funded wallets: {total_pnl:.4f} SOL")
            print()
            
            results["funding_sources"][funder] = {
                "wallets_funded": len(wallets),
                "total_amount": total_funded,
                "combined_pnl": total_pnl,
                "funded_traders": [w["trader"] for w in wallets]
            }
        
        return results
    
    def export_wallet_lists(self, results: Dict, output_file: str = "wallet_lists.txt"):
        """
        Export trading wallets and funding wallets to separate lists.
        
        Args:
            results: Results dictionary from find_top_traders_and_funders
            output_file: Output filename
        """
        if not results or "top_traders" not in results:
            print("No results to export")
            return
        
        # Extract trading wallets
        trading_wallets = [trader["address"] for trader in results.get("top_traders", [])]
        
        # Extract funding wallets (exclude "Unknown")
        funding_wallets = []
        for trader in results.get("top_traders", []):
            funder = trader.get("funder", "Unknown")
            if funder != "Unknown" and funder not in funding_wallets:
                funding_wallets.append(funder)
        
        # Print to console
        print("\n" + "="*80)
        print("WALLET LISTS")
        print("="*80 + "\n")
        
        print(f"TRADING WALLETS ({len(trading_wallets)} total):")
        print("-" * 80)
        for i, wallet in enumerate(trading_wallets, 1):
            print(f"{i}. {wallet}")
        
        print(f"\n\nFUNDING WALLETS ({len(funding_wallets)} total):")
        print("-" * 80)
        for i, wallet in enumerate(funding_wallets, 1):
            print(f"{i}. {wallet}")
        
        # Save to file
        with open(output_file, 'w') as f:
            f.write("TRADING WALLETS\n")
            f.write("=" * 80 + "\n")
            for wallet in trading_wallets:
                f.write(f"{wallet}\n")
            
            f.write("\n\nFUNDING WALLETS\n")
            f.write("=" * 80 + "\n")
            for wallet in funding_wallets:
                f.write(f"{wallet}\n")
        
        print(f"\n\nWallet lists saved to: {output_file}")
        
        return {
            "trading_wallets": trading_wallets,
            "funding_wallets": funding_wallets
        }


# Example usage
if __name__ == "__main__":
    # Configuration - Load from environment variables
    HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
    
    if not HELIUS_API_KEY:
        raise ValueError("HELIUS_API_KEY not found in .env file")
    
    TOKEN_MINT = "4E5gVEbkZTwGBbipC1AEqiXkFZSarckthAi5EbPepump"  # Your token
    TOP_N = 10
    MIN_TRADES = 3  # Minimum trades required to be considered
    RANK_BY = "pnl"  # Options: "pnl", "roi", "win_rate", "volume"
    DEBUG = True  # Enable debug output for first 2 wallets
    
    # Initialize analyzer
    analyzer = SolanaWalletAnalyzer(helius_api_key=HELIUS_API_KEY)
    
    # Run analysis
    results = analyzer.find_top_traders_and_funders(
        token_mint=TOKEN_MINT,
        top_n=TOP_N,
        min_trades=MIN_TRADES,
        rank_by=RANK_BY,
        debug=DEBUG
    )
    
    # Export wallet lists
    wallet_lists = analyzer.export_wallet_lists(results, output_file="wallet_lists.txt")
    
    # Results are stored in the 'results' dictionary
    print("\nAnalysis complete!")
    print(f"Found {len(results.get('top_traders', []))} top traders")
    print(f"Identified {len(results.get('funding_sources', {}))} unique funding sources")
    
    # You can also access the lists programmatically
    if wallet_lists:
        print(f"\nTrading wallets list: {len(wallet_lists['trading_wallets'])} addresses")
        print(f"Funding wallets list: {len(wallet_lists['funding_wallets'])} addresses")