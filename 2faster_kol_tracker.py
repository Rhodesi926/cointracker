import asyncio
import aiohttp
from datetime import datetime, timedelta
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Set, Dict, List, Optional, Any
import json
import time
import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

@dataclass
class WalletNode:
    address: str
    first_seen: datetime = field(default_factory=datetime.now)
    last_active: Optional[datetime] = None
    transaction_count: int = 0
    connected_wallets: Set[str] = field(default_factory=set)
    funding_sources: Set[str] = field(default_factory=set)
    funded_wallets: Set[str] = field(default_factory=set)
    token_interactions: Dict[str, int] = field(default_factory=dict)
    suspicion_score: float = 0.0
    depth_discovered: int = 0
    transaction_signatures: List[str] = field(default_factory=list)
    
    def to_dict(self):
        return {
            'address': self.address,
            'first_seen': self.first_seen.isoformat(),
            'last_active': self.last_active.isoformat() if self.last_active else None,
            'transaction_count': self.transaction_count,
            'connected_wallets': list(self.connected_wallets),
            'funding_sources': list(self.funding_sources),
            'funded_wallets': list(self.funded_wallets),
            'token_interactions': self.token_interactions,
            'suspicion_score': self.suspicion_score,
            'depth_discovered': self.depth_discovered
        }


class SolanaBotnetCrawler:
    def __init__(self, rpc_endpoints: List[str], rate_limit_per_second: int = 50):
        self.rpc_endpoints = rpc_endpoints
        self.current_endpoint_idx = 0
        self.rate_limit = rate_limit_per_second  # Keep for compatibility
        self.last_request_time = 0
        
        # Storage
        self.discovered_wallets: Dict[str, WalletNode] = {}
        self.transaction_cache: Dict[str, dict] = {}
        
        # Tracking
        self.total_requests = 0
        self.failed_requests = 0
        self.start_time = datetime.now()
        
        # Configuration
        self.max_depth = 5
        self.min_suspicion_threshold = 0.3
        self.max_transactions_per_wallet = 1000
        
    def get_rpc_endpoint(self):
        """Rotate through RPC endpoints for load balancing"""
        endpoint = self.rpc_endpoints[self.current_endpoint_idx]
        self.current_endpoint_idx = (self.current_endpoint_idx + 1) % len(self.rpc_endpoints)
        return endpoint
    
    async def rate_limited_request(self):
        """Simple rate limiting - removed, now handled in rpc_call"""
        pass
    
    async def rpc_call(self, session: aiohttp.ClientSession, method: str, params: list, retry_count: int = 3):
        """Make RPC call with retry logic - adapted from KOL tracker"""
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }
        
        for attempt in range(retry_count):
            try:
                endpoint = self.get_rpc_endpoint()
                self.total_requests += 1
                
                async with session.post(endpoint, json=payload, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        if 'error' in result:
                            print(f"RPC Error: {result['error']}")
                            self.failed_requests += 1
                            return None
                        # Add small delay after successful request
                        await asyncio.sleep(0.02)  # 20ms = 50 RPS max
                        return result.get('result')
                    elif response.status == 429:
                        # Rate limit hit - exponential backoff
                        wait_time = 2 ** attempt  # 1s, 2s, 4s
                        print(f"    Rate limited, waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        print(f"HTTP {response.status} on attempt {attempt + 1}")
                        
            except asyncio.TimeoutError:
                print(f"    Timeout on attempt {attempt + 1}")
                if attempt == retry_count - 1:
                    print(f"    Failed after {retry_count} timeout attempts")
                    self.failed_requests += 1
                    return None
                await asyncio.sleep(2)
                continue
            except Exception as e:
                print(f"    Request error on attempt {attempt + 1}: {e}")
                if attempt == retry_count - 1:
                    print(f"    Failed after {retry_count} attempts")
                    self.failed_requests += 1
                    return None
                await asyncio.sleep(2)
                continue
        
        self.failed_requests += 1
        return None
    
    async def get_signatures(self, session: aiohttp.ClientSession, address: str, limit: int = 1000):
        """Get transaction signatures for an address"""
        all_signatures = []
        before = None
        max_attempts = 3  # Limit pagination attempts
        attempt = 0
        
        while len(all_signatures) < limit and attempt < max_attempts:
            attempt += 1
            params = [address, {"limit": min(100, limit - len(all_signatures))}]  # Smaller batches
            if before:
                params[1]["before"] = before
            
            result = await self.rpc_call(session, "getSignaturesForAddress", params)
            
            if not result or len(result) == 0:
                break
            
            all_signatures.extend(result)
            before = result[-1]["signature"]
            
            if len(result) < 100:
                break
        
        return all_signatures[:limit]
    
    async def get_transaction_details(self, session: aiohttp.ClientSession, signature: str):
        """Get detailed transaction information"""
        if signature in self.transaction_cache:
            return self.transaction_cache[signature]
        
        params = [
            signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0
            }
        ]
        
        result = await self.rpc_call(session, "getTransaction", params)
        
        if result:
            self.transaction_cache[signature] = result
        
        return result
    
    def extract_wallet_relationships(self, transaction_data: dict, source_wallet: str) -> Dict[str, Set[str]]:
        """Extract wallet relationships from transaction data"""
        relationships = {
            'senders': set(),
            'receivers': set(),
            'token_accounts': set(),
            'program_interactions': set()
        }
        
        if not transaction_data or 'meta' not in transaction_data:
            return relationships
        
        try:
            # Get account keys
            tx = transaction_data.get('transaction', {})
            message = tx.get('message', {})
            account_keys = message.get('accountKeys', [])
            
            # Extract pre and post balances to identify transfers
            meta = transaction_data['meta']
            pre_balances = meta.get('preBalances', [])
            post_balances = meta.get('postBalances', [])
            
            # Find accounts with balance changes
            for idx, (pre, post) in enumerate(zip(pre_balances, post_balances)):
                if idx < len(account_keys):
                    account = account_keys[idx]
                    if isinstance(account, dict):
                        account_pubkey = account.get('pubkey', '')
                    else:
                        account_pubkey = account
                    
                    if account_pubkey and account_pubkey != source_wallet:
                        if post > pre:  # Received funds
                            relationships['receivers'].add(account_pubkey)
                        elif pre > post:  # Sent funds
                            relationships['senders'].add(account_pubkey)
            
            # Extract token transfers
            if 'innerInstructions' in meta:
                for inner in meta['innerInstructions']:
                    for instruction in inner.get('instructions', []):
                        parsed = instruction.get('parsed', {})
                        if parsed.get('type') in ['transfer', 'transferChecked']:
                            info = parsed.get('info', {})
                            source = info.get('source', '')
                            destination = info.get('destination', '')
                            
                            if source:
                                relationships['token_accounts'].add(source)
                            if destination:
                                relationships['token_accounts'].add(destination)
            
            # Extract from top-level instructions
            instructions = message.get('instructions', [])
            for instruction in instructions:
                parsed = instruction.get('parsed', {})
                if parsed.get('type') in ['transfer', 'transferChecked']:
                    info = parsed.get('info', {})
                    source = info.get('source', '')
                    destination = info.get('destination', '')
                    
                    if source and source != source_wallet:
                        relationships['senders'].add(source)
                    if destination and destination != source_wallet:
                        relationships['receivers'].add(destination)
                
                # Track program interactions
                program_id = instruction.get('programId', '')
                if program_id:
                    relationships['program_interactions'].add(program_id)
        
        except Exception as e:
            print(f"Error extracting relationships: {str(e)}")
        
        return relationships
    
    def calculate_suspicion_score(self, wallet_node: WalletNode, network_stats: dict) -> float:
        """Calculate suspicion score based on behavioral patterns"""
        score = 0.0
        
        # High transaction count in short time
        if wallet_node.transaction_count > 100:
            score += 0.2
        
        # Multiple funding sources (trying to obfuscate)
        if len(wallet_node.funding_sources) > 3:
            score += 0.15
        
        # Funds many wallets (possible distributor)
        if len(wallet_node.funded_wallets) > 5:
            score += 0.25
        
        # Interacts with many tokens (pump and dump pattern)
        if len(wallet_node.token_interactions) > 10:
            score += 0.2
        
        # Connected to many other wallets
        if len(wallet_node.connected_wallets) > 20:
            score += 0.2
        
        return min(score, 1.0)
    
    async def crawl_wallet(self, session: aiohttp.ClientSession, address: str, depth: int) -> WalletNode:
        """Crawl a single wallet and extract all relationships"""
        print(f"[Depth {depth}] Crawling wallet: {address[:8]}...")
        
        # Check if already processed
        if address in self.discovered_wallets:
            return self.discovered_wallets[address]
        
        # Create wallet node
        wallet_node = WalletNode(
            address=address,
            depth_discovered=depth
        )
        
        try:
            # Get transaction signatures
            signatures = await self.get_signatures(session, address, self.max_transactions_per_wallet)
            wallet_node.transaction_count = len(signatures)
            wallet_node.transaction_signatures = [sig['signature'] for sig in signatures]
            
            if signatures:
                # Get first and last transaction times
                wallet_node.first_seen = datetime.fromtimestamp(signatures[-1]['blockTime'])
                wallet_node.last_active = datetime.fromtimestamp(signatures[0]['blockTime'])
            
            # Sample transactions for analysis (analyze subset to save time)
            sample_size = min(20, len(signatures))  # Reduced sample size
            sample_indices = [i * len(signatures) // sample_size for i in range(sample_size)] if sample_size > 0 else []
            
            for idx in sample_indices:
                sig = signatures[idx]['signature']
                tx_data = await self.get_transaction_details(session, sig)
                
                if tx_data:
                    relationships = self.extract_wallet_relationships(tx_data, address)
                    
                    # Update connections
                    wallet_node.connected_wallets.update(relationships['senders'])
                    wallet_node.connected_wallets.update(relationships['receivers'])
                    wallet_node.funding_sources.update(relationships['senders'])
                    wallet_node.funded_wallets.update(relationships['receivers'])
                    
                    # Track token interactions
                    for token_account in relationships['token_accounts']:
                        wallet_node.token_interactions[token_account] = \
                            wallet_node.token_interactions.get(token_account, 0) + 1
            
            # Calculate suspicion score
            network_stats = {'total_wallets': len(self.discovered_wallets)}
            wallet_node.suspicion_score = self.calculate_suspicion_score(wallet_node, network_stats)
            
            # Store in discovered wallets
            self.discovered_wallets[address] = wallet_node
            
            print(f"  ✓ Found {len(wallet_node.connected_wallets)} connections, "
                  f"suspicion: {wallet_node.suspicion_score:.2f}")
            
        except Exception as e:
            print(f"  ✗ Error crawling {address[:8]}: {str(e)}")
        
        return wallet_node
    
    async def crawl_network(self, seed_wallets: List[str]):
        """Main BFS crawler"""
        queue = deque([(addr, 0) for addr in seed_wallets])
        visited = set()
        
        async with aiohttp.ClientSession() as session:
            while queue:
                address, depth = queue.popleft()
                
                # Skip if already visited or max depth reached
                if address in visited or depth > self.max_depth:
                    continue
                
                visited.add(address)
                
                # Crawl the wallet
                wallet_node = await self.crawl_wallet(session, address, depth)
                
                # Add connected wallets to queue if they meet criteria
                if wallet_node.suspicion_score >= self.min_suspicion_threshold or depth < 2:
                    for connected_wallet in wallet_node.connected_wallets:
                        if connected_wallet not in visited:
                            queue.append((connected_wallet, depth + 1))
                
                # Progress update
                if len(self.discovered_wallets) % 10 == 0:
                    self.print_progress()
            
        self.print_final_stats()
    
    def print_progress(self):
        """Print crawling progress"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.total_requests / elapsed if elapsed > 0 else 0
        
        print(f"\n--- Progress Update ---")
        print(f"Wallets discovered: {len(self.discovered_wallets)}")
        print(f"Total requests: {self.total_requests}")
        print(f"Failed requests: {self.failed_requests}")
        print(f"Request rate: {rate:.1f} req/s")
        print(f"Elapsed time: {elapsed:.0f}s")
        print(f"---\n")
    
    def print_final_stats(self):
        """Print final statistics"""
        print("\n" + "="*60)
        print("CRAWL COMPLETE")
        print("="*60)
        
        total_connections = sum(len(w.connected_wallets) for w in self.discovered_wallets.values())
        high_suspicion = sum(1 for w in self.discovered_wallets.values() if w.suspicion_score > 0.6)
        
        print(f"\nTotal wallets discovered: {len(self.discovered_wallets)}")
        print(f"Total connections mapped: {total_connections}")
        print(f"High suspicion wallets (>0.6): {high_suspicion}")
        print(f"Total API requests: {self.total_requests}")
        print(f"Failed requests: {self.failed_requests}")
        print(f"Total time: {(datetime.now() - self.start_time).total_seconds():.0f}s")
        
        # Top suspicious wallets
        print("\n--- Top 10 Most Suspicious Wallets ---")
        sorted_wallets = sorted(
            self.discovered_wallets.values(),
            key=lambda w: w.suspicion_score,
            reverse=True
        )[:10]
        
        for i, wallet in enumerate(sorted_wallets, 1):
            print(f"{i}. {wallet.address[:12]}... "
                  f"(score: {wallet.suspicion_score:.2f}, "
                  f"connections: {len(wallet.connected_wallets)}, "
                  f"txns: {wallet.transaction_count})")
    
    def export_results(self, filename: str = "botnet_analysis.json"):
        """Export results to JSON"""
        data = {
            'metadata': {
                'crawl_date': datetime.now().isoformat(),
                'total_wallets': len(self.discovered_wallets),
                'total_requests': self.total_requests,
                'failed_requests': self.failed_requests
            },
            'wallets': {addr: node.to_dict() for addr, node in self.discovered_wallets.items()}
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\n✓ Results exported to {filename}")
    
    def export_graph_format(self, filename: str = "botnet_graph.json"):
        """Export in graph format for visualization tools"""
        nodes = []
        edges = []
        
        for addr, wallet in self.discovered_wallets.items():
            nodes.append({
                'id': addr,
                'label': addr[:8],
                'suspicion_score': wallet.suspicion_score,
                'transaction_count': wallet.transaction_count,
                'depth': wallet.depth_discovered
            })
            
            for connected in wallet.funded_wallets:
                if connected in self.discovered_wallets:
                    edges.append({
                        'source': addr,
                        'target': connected,
                        'type': 'funded'
                    })
        
        graph_data = {
            'nodes': nodes,
            'edges': edges
        }
        
        with open(filename, 'w') as f:
            json.dump(graph_data, f, indent=2)
        
        print(f"✓ Graph data exported to {filename}")


# Main execution
async def main():
    # Seed wallets from your screenshot - COMPLETE ADDRESSES
    seed_wallets = [
        "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9",
        "oH8tg1q1xaFVPRUsuAsRYvK6YxNhjqkyX5vjao8TZBn",
        "H2XHvRQyEAVwVjpixoRX72BZH1uYKYX8f6tdEHGniVHN",
        "DQhm7Dz5dkHDCeNLgLrhyVuPsFKEnWX8wMTbNnVhqi8M",
        "zDwdiwJecy8TQ2KtqPUNRdgRnDdRtZG4CgNPwTHEx16",
        "DNfuF1L62WWyW3pNakVkyGGFzVVhj4Yr52jSmdTyeBHm",
        "8rhi1Pqm25D5ooZZT2eyau9CtVDPJzwYsaFcbSZkgojQ",
        "4CSwxraRKviuVSbwMjVFHwD51hsJn1dokEE99WRdJG8R",
        "GWYgwaKEkvB3BE86xquX4q3ypaJiQtdWaBnoWtW5CkxG",
        "66hibHhPi27RkMDiwvK2KY1o7cDPDuN8N3M3GuhXFWuq",
        "EokUVZ8ixAvMcLgQ7wQxTJxd4JC5WfQWeFabfZhVrVFY",
        "2LxzSVmKerk3K4BCB4ztgUkmRSN5RrhqmSEnE4vGTYxY",
        "DuXZqhVwmzBAXxbkUEW9cAApCCDEEqk7ENLYQHuJcr7q",
        "F1VYzwbBbMSeD5NjR2TErcfHuRBtbJehUZSUavvSWFY",
        "7xxMv17QdandiZsDQN1DyAWDyTG3kQTCa1PRCcJ1EnUm",
        "67BeSYGbx3aSkS9tAvPMUyWstHkrCnm5xKkPW15CUJ5y",
        "EdTYqYwayQfSY1TgmhCUMgcEUJjJXNj2toEdpNBRem1H",
        "BTV91nopjoq8vfkadCmePhnXtdZkNC4moNa1MCBvhYg5",
        "8NJWSKuXbsY3p9ArSazWo8xVfW9M96Bg3E24WWTYqZwQ",
        "EwttU26A9pNAR1xE3QKgonZ9fkfpMS4jAMoaCt4UH6U6",
        "HWdpTNFEoKdM1RJPk8Tu2dZUVXKDUz6FoZkxXWVgqg8g",
        "6K63TXGcgjTzBN87WvjStrY9Wngeo36mH6boUTic8GMH",
        "488FiH8f8WxqgfNTaEMgzs14hZy3cso4Dyo3oo5kMDwJ",
        "3dUFE4CWnQUdLc2M9saTzDqXuGntBZyX4aREusM68Mgt",
        "GZ9PJZ8fg5XTpnTj3bEAZRKRmDG1oEV1vwtCCqxQSy4W",
        "YWwtkDoizqmHLDGTqPq2Sm61TGUGSEpAS7FXVVQVm7n",
        "HnxKKffU93zyw7gBJAEb3duTYq159QRdXBynoUyKKsFW",
        "Gw5NmgmiBSNHhETz1v89u1aif3SGJ6m2cMVrJdcYqqpu",
        "7vHRYPTCDahmz6Mq6XmUn5gc5xVPH7X8V9XnKXUc7hyT",
        "DNfuF1L62WWyW3pNakVkyGGFzVVhj4Yr52jSmdTyeBHm"
    ]
    
    # Get Helius API key from environment
    helius_api_key = os.getenv('HELIUS_API_KEY')
    
    if not helius_api_key:
        print("ERROR: HELIUS_API_KEY not found in environment variables!")
        print("Please set your API key in a .env file or environment variable")
        return
    
    # RPC endpoints with Helius
    rpc_endpoints = [
        f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}",
        # Helius allows multiple requests, so we can add the same endpoint multiple times
        # or add fallback to public RPC if needed
        "https://api.mainnet-beta.solana.com",
    ]
    
    # Initialize crawler
    crawler = SolanaBotnetCrawler(
        rpc_endpoints=rpc_endpoints,
        rate_limit_per_second=50  # Matches KOL tracker settings
    )
    
    # Configure crawl parameters
    crawler.max_depth = 3  # Reduced depth to minimize API calls
    crawler.min_suspicion_threshold = 0.4  # Higher threshold to be more selective
    crawler.max_transactions_per_wallet = 100  # Smaller to reduce API load
    
    print("Starting Solana Botnet Crawler...")
    print(f"Using Helius RPC: {helius_api_key[:8]}...")
    print(f"Seed wallets: {len(seed_wallets)}")
    print(f"Max depth: {crawler.max_depth}")
    print(f"Suspicion threshold: {crawler.min_suspicion_threshold}")
    print("\n" + "="*60 + "\n")
    
    # Run the crawler
    await crawler.crawl_network(seed_wallets)
    
    # Export results
    crawler.export_results("botnet_analysis.json")
    crawler.export_graph_format("botnet_graph.json")
    
    print("\n✓ Crawl complete! Check the exported JSON files for results.")

if __name__ == "__main__":
    asyncio.run(main())