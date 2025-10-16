import os

def create_env_file():
    """Create .env file"""
    env_content = """# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here

# Bot Settings
MIN_LIQUIDATION_SIZE=1000
"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(env_content)
        print("✅ Created .env file")
        return True
    else:
        print("ℹ️  .env file already exists")
        return False

def main():
    print("🚀 Setting up Simple Bybit Liquidation Bot...")
    
    create_env_file()
    
    print("\n📋 Next steps:")
    print("1. Get Telegram Bot Token:")
    print("   - Message @BotFather on Telegram")
    print("   - Send /newbot and follow instructions")
    
    print("\n2. Get your Chat ID:")
    print("   - Start a chat with your bot")
    print("   - Send any message to your bot")
    print("   - Visit: https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates")
    
    print("\n3. Edit .env file with your tokens")
    
    print("\n4. Run the bot:")
    print("   # With Docker:")
    print("   docker-compose -f docker-compose.simple.yml up -d")
    print("")
    print("   # Or with Python:")
    print("   pip install -r requirements-simple.txt")
    print("   python bybit_bot.py")
    
    print("\n✅ Setup complete!")

if __name__ == "__main__":
    main()
