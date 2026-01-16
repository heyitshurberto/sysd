#!/bin/bash

# SEC Filing Monitor - Raspberry Pi Zero 2 W Setup
# Just flash the SSD, plug in Pi, run: bash setup.sh
# That's it - app starts automatically!

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "🚀 SEC Filing Monitor - Raspberry Pi Setup"
echo "=========================================="
echo ""

# Check if running as pi user or root
if [ "$USER" != "pi" ] && [ "$EUID" != 0 ]; then
  echo "❌ Error: Please run as pi user or with sudo"
  exit 1
fi

# Update system packages
echo "📦 Updating system packages..."
sudo apt-get update -qq
sudo apt-get upgrade -qq

# Install Node.js 18+ if needed
if ! command -v node &> /dev/null; then
  echo "📥 Installing Node.js 18+..."
  curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash - > /dev/null 2>&1
  sudo apt-get install -y nodejs > /dev/null 2>&1
fi

NODE_VERSION=$(node -v)
echo "✅ Node.js installed: $NODE_VERSION"

# Install npm packages
echo "📚 Installing npm dependencies..."
npm install --silent 2>/dev/null || npm install

# Create logs directory and files
echo "📁 Creating logs directory..."
mkdir -p logs
touch logs/alert.json logs/stocks.json logs/quote.json

# Initialize JSON files with proper format
echo "⚙️  Initializing log files..."
[ ! -s logs/alert.json ] && echo "[]" > logs/alert.json
[ ! -s logs/stocks.json ] && echo "[]" > logs/stocks.json
[ ! -s logs/quote.json ] && echo "{}" > logs/quote.json

# Load environment variables from .env
if [ -f .env ]; then
  set -a
  source .env
  set +a
  echo "✅ Environment variables loaded from .env"
else
  echo "⚠️  Warning: .env file not found"
fi

# Setup git if not already done
if [ ! -d .git ]; then
  echo "🔧 Initializing git repository..."
  git init > /dev/null 2>&1
  git config user.email "${GIT_EMAIL:-pi@domain.com}"
  git config user.name "${GIT_NAME:-SEC Monitor Pi}"
  git add .
  git commit -m "Initial commit from Pi setup" > /dev/null 2>&1 || true
fi

# Setup systemd service
echo "🛠️  Setting up systemd service..."
sudo cp sysd.service /etc/systemd/system/ 2>/dev/null || echo "⚠️  Warning: Could not copy service file"
sudo systemctl daemon-reload 2>/dev/null || true
sudo systemctl enable sysd 2>/dev/null || true

echo ""
echo "✨ Setup Complete!"
echo ""
echo "🚀 Starting the service..."
sudo systemctl start sysd
sleep 2

# Check if service started successfully
if sudo systemctl is-active --quiet sysd; then
  echo "✅ Service is running!"
  echo ""
  echo "📋 View live logs:"
  echo "   sudo journalctl -u sysd -f"
  echo ""
  echo "📊 Check service status:"
  echo "   sudo systemctl status sysd"
  echo ""
  echo "🔌 Restart service:"
  echo "   sudo systemctl restart sysd"
  echo ""
  echo "⏹️  Stop service:"
  echo "   sudo systemctl stop sysd"
  echo ""
else
  echo "❌ Service failed to start. Checking logs..."
  sudo journalctl -u sysd -n 20
fi
