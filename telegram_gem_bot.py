import requests
import pandas as pd
from datetime import datetime
import json
import os
import time
import asyncio
import logging
from dotenv import load_dotenv
from typing import List, Dict, Any
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from collections import defaultdict

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class TelegramGemBot:
    def __init__(self, score_threshold=60):
        self.birdeye_api_key = os.getenv('BIRDEYE_API_KEY')
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.score_threshold = score_threshold
        
        if not self.birdeye_api_key:
            raise ValueError("BIRDEYE_API_KEY not found in .env")
        if not self.telegram_bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN not found in .env")
        
        self.birdeye_url = "https://public-api.birdeye.so"
        self.dexscreener_url = "https://api.dexscreener.com/latest"
        
        # Track monitoring state per user
        self.monitoring_active = {}
        self.alerted_tokens = set()
        self.load_alerted_tokens()
    
    def load_alerted_tokens(self):
        try:
            with open('alerted_tokens.json', 'r') as f:
                data = json.load(f)
                self.alerted_tokens = set(data.get('tokens', []))
        except FileNotFoundError:
            self.alerted_tokens = set()
    
    def save_alerted_token(self, token_address: str):
        self.alerted_tokens.add(token_address)
        try:
            with open('alerted_tokens.json', 'w') as f:
                json.dump({'tokens': list(self.alerted_tokens)}, f, indent=2)
        except Exception as e:
            print(f"Error saving alerted token: {e}")
    
    def get_latest_tokens(self, limit: int = 50) -> List[Dict]:
        url = "https://api.dexscreener.com/token-profiles/latest/v1"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            tokens = []
            seen_addresses = set()
            
            if isinstance(data, list):
                for item in data:
                    token_address = item.get('tokenAddress')
                    chain_id = item.get('chainId')
                    
                    if chain_id == 'solana' and token_address and token_address not in seen_addresses:
                        seen_addresses.add(token_address)
                        tokens.append({
                            'address': token_address,
                            'symbol': item.get('symbol', 'Unknown'),
                            'pair_created': 0
                        })
                        
                        if len(tokens) >= limit:
                            break
            
            return tokens
        except Exception as e:
            print(f"Error fetching tokens: {e}")
            return []
    
    def get_token_metrics_birdeye(self, token_address: str) -> Dict[str, Any]:
        url = f"{self.birdeye_url}/defi/price"
        params = {'address': token_address, 'include_liquidity': 'true'}
        headers = {'accept': 'application/json', 'x-chain': 'solana', 'X-API-KEY': self.birdeye_api_key}
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=10)
                if response.status_code == 429:
                    time.sleep(5 * (2 ** attempt))
                    continue
                response.raise_for_status()
                data = response.json()
                if data.get('success') and data.get('data'):
                    return {'liquidity': data['data'].get('liquidity', 0), 'price': data['data'].get('value', 0)}
                return {'liquidity': 0, 'price': 0}
            except:
                if attempt == max_retries - 1:
                    return {'liquidity': 0, 'price': 0}
        return {'liquidity': 0, 'price': 0}
    
    def get_token_metrics_dexscreener(self, token_address: str) -> Dict[str, Any]:
        url = f"{self.dexscreener_url}/dex/tokens/{token_address}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('pairs') and len(data['pairs']) > 0:
                pair = max(data['pairs'], key=lambda x: float(x.get('liquidity', {}).get('usd', 0)))
                return {
                    'market_cap': float(pair.get('fdv', 0) or pair.get('marketCap', 0) or 0),
                    'volume_h1': float(pair.get('volume', {}).get('h1', 0) or 0),
                    'liquidity_dex': float(pair.get('liquidity', {}).get('usd', 0) or 0),
                    'price_change_5m': float(pair.get('priceChange', {}).get('m5', 0) or 0),
                }
            return {'market_cap': 0, 'volume_h1': 0, 'liquidity_dex': 0, 'price_change_5m': 0}
        except:
            return {'market_cap': 0, 'volume_h1': 0, 'liquidity_dex': 0, 'price_change_5m': 0}
    
    def calculate_token_score(self, metrics: Dict[str, Any]) -> float:
        TARGET_VOLUME_SEC = 320
        TARGET_LIQUIDITY = 50_000
        
        market_cap = metrics.get('market_cap', 0)
        volume_h1 = metrics.get('volume_h1', 0)
        liquidity = max(metrics.get('liquidity', 0), metrics.get('liquidity_dex', 0))
        price_change_5m = metrics.get('price_change_5m', 0)
        volume_per_sec = volume_h1 / 3600 if volume_h1 > 0 else 0
        
        scores = {'market_cap': 0, 'volume_sec': 0, 'liquidity': 0, 'momentum_bonus': 0}
        
        # Market cap scoring (inverted)
        if market_cap == 0:
            scores['market_cap'] = 0
        elif market_cap < 10_000:
            scores['market_cap'] = 25 * (market_cap / 10_000)
        elif 10_000 <= market_cap <= 50_000:
            scores['market_cap'] = 50
        elif 50_000 < market_cap <= 200_000:
            scores['market_cap'] = 35
        elif 200_000 < market_cap <= 500_000:
            scores['market_cap'] = 20
        elif 500_000 < market_cap <= 1_000_000:
            scores['market_cap'] = 10
        else:
            scores['market_cap'] = 5
        
        # Volume/sec scoring
        if volume_per_sec >= TARGET_VOLUME_SEC:
            scores['volume_sec'] = 25
            multiplier = volume_per_sec / TARGET_VOLUME_SEC
            if multiplier >= 100:
                scores['volume_sec'] += 50
            elif multiplier >= 10:
                scores['volume_sec'] += 25
            elif multiplier >= 5:
                scores['volume_sec'] += 12.5
            elif multiplier >= 2:
                scores['volume_sec'] += 6.25
        else:
            scores['volume_sec'] = 25 * (volume_per_sec / TARGET_VOLUME_SEC)
        
        # Liquidity scoring
        if liquidity >= TARGET_LIQUIDITY:
            scores['liquidity'] = 25
            multiplier = liquidity / TARGET_LIQUIDITY
            if multiplier >= 100:
                scores['liquidity'] += 50
            elif multiplier >= 10:
                scores['liquidity'] += 25
            elif multiplier >= 5:
                scores['liquidity'] += 12.5
            elif multiplier >= 2:
                scores['liquidity'] += 6.25
        else:
            scores['liquidity'] = 25 * (liquidity / TARGET_LIQUIDITY)
        
        # Momentum bonus
        if price_change_5m > 0:
            if price_change_5m >= 200:
                scores['momentum_bonus'] = 30
            elif price_change_5m >= 100:
                scores['momentum_bonus'] = 20
            elif price_change_5m >= 50:
                scores['momentum_bonus'] = 10
            elif price_change_5m >= 25:
                scores['momentum_bonus'] = 5
            else:
                scores['momentum_bonus'] = price_change_5m * 0.2
        
        return sum(scores.values())
    
    async def scan_tokens(self, update: Update, quick: bool = False) -> List[Dict]:
        chat_id = update.effective_chat.id
        limit = 20 if quick else 50
        
        logger.info(f"Starting scan for chat {chat_id}, limit={limit}, threshold={self.score_threshold}%")
        
        tokens = self.get_latest_tokens(limit)
        if not tokens:
            logger.warning("No tokens found from DexScreener")
            return []
        
        logger.info(f"Retrieved {len(tokens)} tokens to analyze")
        
        gems = []
        for idx, token in enumerate(tokens):
            logger.info(f"[{idx+1}/{len(tokens)}] Analyzing {token['symbol']} ({token['address'][:8]}...)")
            
            birdeye_data = self.get_token_metrics_birdeye(token['address'])
            dex_data = self.get_token_metrics_dexscreener(token['address'])
            
            metrics = {
                'market_cap': dex_data['market_cap'],
                'volume_h1': dex_data['volume_h1'],
                'liquidity': birdeye_data['liquidity'],
                'liquidity_dex': dex_data['liquidity_dex'],
                'price': birdeye_data['price'],
                'price_change_5m': dex_data['price_change_5m']
            }
            
            score = self.calculate_token_score(metrics)
            volume_per_sec = dex_data['volume_h1'] / 3600 if dex_data['volume_h1'] > 0 else 0
            
            # Log detailed metrics
            logger.info(f"  Metrics - MC: ${metrics['market_cap']:,.0f}, Liq: ${max(metrics['liquidity'], metrics['liquidity_dex']):,.0f}, Vol/s: ${volume_per_sec:.2f}/s, 5m: {metrics['price_change_5m']:.1f}%")
            logger.info(f"  Score: {score:.0f}% {'✅ GEM!' if score >= self.score_threshold else '❌ Rejected'}")
            
            if score >= self.score_threshold:
                gems.append({
                    'score': score,
                    'symbol': token['symbol'],
                    'address': token['address'],
                    'market_cap': metrics['market_cap'],
                    'liquidity': max(metrics['liquidity'], metrics['liquidity_dex']),
                    'volume_per_sec': volume_per_sec,
                    'price_change_5m': metrics['price_change_5m'],
                    'price': metrics['price']
                })
                logger.info(f"  🎯 Added to gems list!")
            
            time.sleep(1)
        
        logger.info(f"Scan complete. Found {len(gems)} gems out of {len(tokens)} tokens")
        return gems
    
    async def send_gem_alert(self, update: Update, gem: Dict):
        message = f"""
🚨 *GEM ALERT!* 🚨

*Score:* {gem['score']:.0f}%
*Symbol:* {gem['symbol']}
*Contract:* `{gem['address']}`

📊 *Metrics:*
• Market Cap: ${gem['market_cap']:,.0f}
• Liquidity: ${gem['liquidity']:,.0f}
• Volume/sec: ${gem['volume_per_sec']:.2f}/s
• 5m Change: {gem['price_change_5m']:.1f}%

💰 *Price:* ${gem['price']:.8f}

🔗 [View on DexScreener](https://dexscreener.com/solana/{gem['address']})
"""
        await update.effective_chat.send_message(text=message, parse_mode='Markdown')

# Bot command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot_data['scanner']
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    logger.info(f"START command from user_id={user_id}, chat_id={chat_id}, username={username}")
    
    await update.message.reply_text(
        "💎 *Gem Scanner Bot* 💎\n\n"
        f"✅ Connected! Your chat_id is: `{chat_id}`\n"
        f"🎯 Current threshold: *{bot.score_threshold}%*\n\n"
        "Commands:\n"
        "/scan - Quick scan (20 tokens)\n"
        "/deepscan - Deep scan (50 tokens)\n"
        "/monitor - Start continuous monitoring\n"
        "/stop - Stop monitoring\n"
        "/status - Check monitoring status\n"
        "/setthreshold <number> - Set score threshold\n"
        "/clear - Clear alerted tokens list",
        parse_mode='Markdown'
    )

async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Scan command received from user {update.effective_user.id}")
    bot = context.bot_data['scanner']
    await update.message.reply_text(f"🔍 Starting quick scan of 20 tokens (threshold: {bot.score_threshold}%)...")
    gems = await bot.scan_tokens(update, quick=True)
    
    if gems:
        logger.info(f"Sending {len(gems)} gems to user")
        await update.message.reply_text(f"✅ Found {len(gems)} gems!")
        for gem in gems:
            if gem['address'] not in bot.alerted_tokens:
                await bot.send_gem_alert(update, gem)
                bot.save_alerted_token(gem['address'])
    else:
        logger.info("No gems found in quick scan")
        await update.message.reply_text(f"😴 No {bot.score_threshold}%+ gems found in this scan.")

async def deepscan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot_data['scanner']
    await update.message.reply_text(f"🔍 Starting deep scan of 50 tokens (threshold: {bot.score_threshold}%)... (this may take 1-2 minutes)")
    gems = await bot.scan_tokens(update, quick=False)
    
    if gems:
        await update.message.reply_text(f"✅ Found {len(gems)} gems!")
        for gem in gems:
            if gem['address'] not in bot.alerted_tokens:
                await bot.send_gem_alert(update, gem)
                bot.save_alerted_token(gem['address'])
    else:
        await update.message.reply_text(f"😴 No {bot.score_threshold}%+ gems found in this scan.")

async def monitor_callback(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    bot = context.bot_data['scanner']
    chat_id = job.chat_id
    
    try:
        # Create a mock update object for scanning
        class MockUpdate:
            class MockChat:
                def __init__(self, chat_id):
                    self.id = chat_id
                async def send_message(self, text, parse_mode=None):
                    await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
            
            def __init__(self, chat_id):
                self.effective_chat = self.MockChat(chat_id)
        
        update = MockUpdate(chat_id)
        gems = await bot.scan_tokens(update, quick=False)
        
        if gems:
            for gem in gems:
                if gem['address'] not in bot.alerted_tokens:
                    await bot.send_gem_alert(update, gem)
                    bot.save_alerted_token(gem['address'])
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Error during monitoring scan: {e}")

async def monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    bot = context.bot_data['scanner']
    
    if chat_id in bot.monitoring_active and bot.monitoring_active[chat_id]:
        await update.message.reply_text("⚠️ Monitoring already active!")
        return
    
    # Default 15 minute interval
    interval = 15
    if context.args and len(context.args) > 0:
        try:
            interval = int(context.args[0])
            if interval < 5:
                await update.message.reply_text("⚠️ Minimum interval is 5 minutes")
                return
        except:
            pass
    
    context.job_queue.run_repeating(
        monitor_callback,
        interval=interval * 60,
        first=10,
        chat_id=chat_id,
        name=str(chat_id)
    )
    
    bot.monitoring_active[chat_id] = True
    await update.message.reply_text(
        f"✅ Monitoring started!\n"
        f"📊 Scanning every {interval} minutes\n"
        f"🎯 Will alert on {bot.score_threshold}%+ gems\n\n"
        f"Use /stop to stop monitoring"
    )

async def stop_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    bot = context.bot_data['scanner']
    
    current_jobs = context.job_queue.get_jobs_by_name(str(chat_id))
    if not current_jobs:
        await update.message.reply_text("⚠️ No active monitoring to stop")
        return
    
    for job in current_jobs:
        job.schedule_removal()
    
    bot.monitoring_active[chat_id] = False
    await update.message.reply_text("🛑 Monitoring stopped")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    bot = context.bot_data['scanner']
    
    is_active = bot.monitoring_active.get(chat_id, False)
    alerted_count = len(bot.alerted_tokens)
    
    status_text = (
        f"📊 *Bot Status*\n\n"
        f"Monitoring: {'✅ Active' if is_active else '❌ Inactive'}\n"
        f"Score Threshold: {bot.score_threshold}%\n"
        f"Alerted tokens: {alerted_count}\n"
    )
    await update.message.reply_text(status_text, parse_mode='Markdown')

async def setthreshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot_data['scanner']
    
    if not context.args or len(context.args) == 0:
        await update.message.reply_text(
            f"📊 *Current threshold:* {bot.score_threshold}%\n\n"
            f"*Usage:* /setthreshold <percentage>\n"
            f"*Example:* /setthreshold 60\n\n"
            f"*Recommendations:*\n"
            f"• 80-100% - Very selective (original setting)\n"
            f"• 60-79% - Balanced\n"
            f"• 40-59% - More results\n"
            f"• Below 40% - High volume",
            parse_mode='Markdown'
        )
        return
    
    try:
        new_threshold = float(context.args[0])
        if new_threshold < 0 or new_threshold > 100:
            await update.message.reply_text("⚠️ Threshold must be between 0 and 100")
            return
        
        old_threshold = bot.score_threshold
        bot.score_threshold = new_threshold
        logger.info(f"Threshold changed from {old_threshold}% to {new_threshold}% by user {update.effective_user.id}")
        await update.message.reply_text(f"✅ Threshold updated: {old_threshold}% → *{new_threshold}%*", parse_mode='Markdown')
    except ValueError:
        await update.message.reply_text("⚠️ Invalid number. Please provide a valid percentage.")

async def clear_alerted(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot_data['scanner']
    count = len(bot.alerted_tokens)
    bot.alerted_tokens.clear()
    bot.save_alerted_token('')  # Save empty set
    await update.message.reply_text(f"🗑️ Cleared {count} alerted tokens")

def main():
    scanner = TelegramGemBot(score_threshold=60)  # Default threshold set to 60%
    
    application = Application.builder().token(scanner.telegram_bot_token).build()
    application.bot_data['scanner'] = scanner
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("scan", scan))
    application.add_handler(CommandHandler("deepscan", deepscan))
    application.add_handler(CommandHandler("monitor", monitor))
    application.add_handler(CommandHandler("stop", stop_monitor))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("setthreshold", setthreshold))
    application.add_handler(CommandHandler("clear", clear_alerted))
    
    logger.info("🤖 Bot started! Send /start in Telegram to begin")
    logger.info(f"📊 Default score threshold: {scanner.score_threshold}%")
    logger.info("Bot is now polling for updates...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()