# Aster Liquidation Bot

Aster liquidation bot - **Telegram bot** that monitors **Aster futures liquidations** in real time and sends instant, formatted notifications.  
Ideal for traders, who like to trade based on Aster liquidaitons.

## Features

- 🔴 Real-time monitoring of all Aster futures liquidations
- 📱 Instant **Telegram alerts** with detailed trade data
- 🔄 Auto-reconnect on connection loss
- 📦 Single file - easy to understand and deploy
- 📈 Covers all Linear futures contracts

---

## Quick Start

### Option 1: Docker (Recommended)


```bash
# 1. Clone the repository
git clone https://github.com/MaxTraderDev/aster-liquidations-bot
cd aster-liquidations-bot 

# 2. Setup environment
nano .env  # edit credentials
# Example
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=yout_chat_id
MIN_LIQUIDATION_SIZE=10000

# 3. Start container
docker-compose up -d

# 4. View logs
docker-compose logs -f
```

### Option 2: Python

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set environment variables
export TELEGRAM_BOT_TOKEN="your_token"
export TELEGRAM_CHAT_ID="your_chat_id"

# 3. Run the bot
python aster_bot.py
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `TELEGRAM_BOT_TOKEN` | Your Telegram bot token (required) | - |
| `TELEGRAM_CHAT_ID` | Your Telegram chat ID (required) | - |
| `MIN_LIQUIDATION_SIZE` | Minimum liquidation size in USD | 1000 |

## Get Telegram Credentials

### 1. Create Bot
1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Send `/newbot` and follow instructions
3. Copy the bot token

### 2. Get Chat ID
1. Start a chat with your bot
2. Send any message to your bot
3. Visit: `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`
4. Find your chat ID in the response

## Example Message

```
🚨 LIQUIDATION ALERT 🚨

🔴 BTCUSDT Sell Liquidation
💰 Size: 1,234.56
💸 Amount: $56,789.12
💵 Price: $45,678.90
⏰ Time: 2024-01-15 14:30:25

#Liquidation #BTC
```

## Docker Commands

```bash
# Start
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down

# Recreate
docker-compose up -d --build --force-recreate

# View logs
docker-compose logs -f
```

## 💼 Work With Me

Need a **custom trading bot**, exchange integration, or strategy automation?  

📩 **Telegram:** [@maxtraderdev](https://t.me/maxtraderdev)  
🌐 **Website:** [maxtraderdev.github.io](https://maxtraderdev.github.io)
