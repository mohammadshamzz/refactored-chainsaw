#!/bin/bash

# This script removes redundant and obsolete Python files from the project.
# It assumes you are running it from the 'fast_api_trading_platform' root directory.

# Exit immediately if a command exits with a non-zero status.
set -e
# Print each command to the terminal before executing it for clarity.
set -x

echo "Removing 17 redundant and obsolete Python files..."

rm \
    app/services/advanced_charting.py \
    app/services/agent_manager.py \
    app/services/alpaca_broker.py \
    app/services/analysis.py \
    app/services/analytics.py \
    app/services/broker_factory.py \
    app/services/broker_yfinance.py \
    app/services/config_validator.py \
    app/services/database.py \
    app/services/enhanced_ict.py \
    app/services/external_notifications.py \
    app/services/llm_market_analysis.py \
    app/services/notifications.py \
    app/services/risk_manager.py \
    app/services/trading_journal.py \
    app/services/trailing_stops.py \
    app/agent/trailing_stops.py

echo "Cleanup complete."
