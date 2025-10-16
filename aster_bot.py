#!/usr/bin/env python3

import asyncio
import json
import logging
import os
import signal
import sys
import websockets
from datetime import datetime
from telegram import Bot
from telegram.error import TelegramError
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AsterLiquidationBot:
    def __init__(self):
        load_dotenv()
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.min_size = float(os.getenv('MIN_LIQUIDATION_SIZE', '1000'))
        
        if not self.bot_token or not self.chat_id:
            logger.error("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID environment variables")
            sys.exit(1)
        self.telegram_bot = Bot(token=self.bot_token)
        self.ws = None
        self.running = False
        self.keepalive_task = None
        
        self.aster_ws_url = os.getenv(
            "ASTER_WS_URL",
            "wss://fstream.asterdex.com/stream?streams=!forceOrder@arr"
        )
    
    async def send_telegram_message(self, message):
        try:
            await self.telegram_bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='HTML',
                disable_web_page_preview=True,
            )
            logger.info("Message sent to Telegram")
        except TelegramError as e:
            logger.error(f"Failed to send Telegram message: {e}")
    
    async def send_startup_message(self):
        message = f"""
🤖 <b>Aster Liquidation Bot Started</b>

✅ Monitoring Aster all-market liquidation stream
📊 Minimum size: ${self.min_size:,.0f}
⚡ Realtime monitoring

<a href="https://github.com/MaxTraderDev">More bots here -></a>

Bot is now running...
        """.strip()
        await self.send_telegram_message(message)
    
    async def format_liquidation_alert(self, data):
        symbol = data.get('symbol', 'Unknown')
        side = data.get('side', 'Unknown')
        size = data.get('size', '0')
        price = data.get('price', '0')
        amount = data.get('amount', '0')
        time = data.get('time', 'Unknown')
        
        side_emoji = "🔴" if side == "Sell" else "🟢"
        
        message = f"""
🚨 <b>LIQUIDATION ALERT</b> 🚨

{side_emoji} <b>{symbol}</b> {side} Liquidation
💰 Size: {size}
💸 Amount: ${amount}
💵 Price: ${price}
⏰ Time: {time}

#Liquidation #{symbol.replace('USDT', '')}
        """.strip()
        
        return message
    
    
    async def connect_to_aster(self):
        try:
            self.ws = await websockets.connect(self.aster_ws_url, ping_interval=None)
            logger.info("Connected to Aster liquidation stream")
            if self.keepalive_task and not self.keepalive_task.done():
                self.keepalive_task.cancel()
            self.keepalive_task = asyncio.create_task(self._send_unsolicited_pongs())
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            await self.send_telegram_message(f"❌ Connection failed: {str(e)}")
            return False
    
    async def listen_for_liquidations(self):
        if not self.ws:
            return
        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)
                    payload = data.get('data', data)
                    if payload.get('e') == 'forceOrder' and payload.get('o'):
                        await self.process_liquidation(payload)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            await self.send_telegram_message("⚠️ WebSocket connection lost")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            await self.send_telegram_message(f"❌ WebSocket error: {str(e)}")
    
    async def process_liquidation(self, payload):
        if not payload:
            return
        try:
            order = payload.get('o', {})
            symbol = order.get('s', '')
            side = order.get('S', '')
            price_str = order.get('ap') or order.get('p') or '0'
            qty_str = order.get('q') or order.get('l') or order.get('z') or '0'
            price = float(price_str)
            qty = float(qty_str)
            liquidation_value = qty * price
            if liquidation_value >= self.min_size:
                timestamp = order.get('T') or payload.get('E') or ''
                if timestamp:
                    try:
                        dt = datetime.fromtimestamp(int(timestamp) / 1000)
                        formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        formatted_time = str(timestamp)
                else:
                    formatted_time = "Unknown"
                notification_data = {
                    'symbol': symbol,
                    'side': side.title() if side else 'Unknown',
                    'size': f"{qty:,.4f}",
                    'price': f"{price:,.2f}",
                    'amount': f"{liquidation_value:,.2f}",
                    'time': formatted_time
                }
                message = await self.format_liquidation_alert(notification_data)
                await self.send_telegram_message(message)
                logger.info(f"Alert sent: {symbol} {side} ${liquidation_value:,.2f}")
        except Exception as e:
            logger.error(f"Error processing liquidation: {e}")
    
    async def start_monitoring(self):
        self.running = True
        logger.info("Starting Aster Liquidation Bot...")
        await self.send_startup_message()
        while self.running:
            try:
                if await self.connect_to_aster():
                    await self.listen_for_liquidations()
                if self.running:
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                if self.running:
                    await asyncio.sleep(5)
    
    async def stop(self):
        self.running = False
        if self.keepalive_task and not self.keepalive_task.done():
            self.keepalive_task.cancel()
        if self.ws:
            await self.ws.close()
        logger.info("Bot stopped")

    async def _send_unsolicited_pongs(self):
        try:
            while self.running and self.ws:
                await asyncio.sleep(4 * 60)
                try:
                    await self.ws.pong()
                    logger.debug("Sent unsolicited pong")
                except Exception as e:
                    logger.warning(f"Failed to send unsolicited pong: {e}")
                    return
        except asyncio.CancelledError:
            return

async def main():
    bot = AsterLiquidationBot()
    try:
        await bot.start_monitoring()
    except KeyboardInterrupt:
        logger.info("Bot interrupted by user")
    finally:
        await bot.stop()

def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
