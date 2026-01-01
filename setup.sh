#!/bin/bash

# SEC Filing Monitor - Raspberry Pi Zero 2 W Setup
# Run on Pi with: bash setup.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "SEC Filing Monitor - Pi Setup"
echo ""

# Check if running as pi user or root
if [ "$USER" != "pi" ] && [ "$EUID" != 0 ]; then
  echo "Error: Please run as pi user or with sudo"
  exit 1
fi

# Update system packages
echo "Updating system packages..."
sudo apt-get update -qq

# Install Node.js 18+ if needed
if ! command -v node &> /dev/null; then
  echo "Installing Node.js 18+..."
  curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash - > /dev/null 2>&1
  sudo apt-get install -y nodejs > /dev/null 2>&1
fi

NODE_VERSION=$(node -v)
echo "Node.js installed: $NODE_VERSION"

# Install npm packages
echo "Installing npm dependencies..."
npm install --silent 2>/dev/null || npm install

# Create logs directory and files
echo "Creating logs directory..."
mkdir -p logs
touch logs/alert.json logs/stocks.json logs/quote.json

# Initialize JSON files with empty arrays if empty
echo "Initializing log files..."
[ ! -s logs/alert.json ] && echo "[]" > logs/alert.json
[ ! -s logs/stocks.json ] && echo "[]" > logs/stocks.json
[ ! -s logs/quote.json ] && echo "{}" > logs/quote.json

# Load environment variables from .env
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# Setup git if not already done
if [ ! -d .git ]; then
  echo "Initializing git repository..."
  git init > /dev/null 2>&1
  git config user.email "${GIT_EMAIL:-pi@domain.com}"
  git config user.name "${GIT_NAME:-SEC Monitor Pi}"
  git add .
  git commit -m "Initial commit" > /dev/null 2>&1 || true
fi

# Add remote if not already configured
if ! git remote get-url origin > /dev/null 2>&1; then
  echo ""
  echo "GitHub remote not configured. You need:"
  echo "1. Create repo on GitHub (https://github.com/new)"
  echo "2. Run: git remote add origin git@github.com:YOUR_USERNAME/YOUR_REPO.git"
  echo "3. Run: git push -u origin main"
else
  echo "Git remote already configured: $(git remote get-url origin)"
fi

# Setup systemd service
echo "Setting up systemd service..."
sudo cp sysd.service /etc/systemd/system/ 2>/dev/null || echo "Warning: Could not copy service file"
sudo systemctl daemon-reload 2>/dev/null || true
sudo systemctl enable sysd 2>/dev/null || true

echo ""
echo "Setup Complete!"
echo ""
echo "Next steps:"
echo ""
echo "1. Configure GitHub SSH (if not already done):"
echo "   ssh-keygen -t rsa -b 4096 -f ~/.ssh/github_rsa -N ''"
echo "   cat ~/.ssh/github_rsa.pub"
echo "   (Add key to https://github.com/settings/keys)"
echo ""
echo "2. Configure your git remote:"
echo "   git remote add origin git@github.com:YOUR_USERNAME/YOUR_REPO.git"
echo "   git branch -M main"
echo ""
echo "3. Start the service:"
echo "   sudo systemctl start sysd"
echo ""
echo "4. View live logs:"
echo "   sudo journalctl -u sysd -f"
echo ""
echo "5. Check service status:"
echo "   sudo systemctl status sysd"
echo ""
