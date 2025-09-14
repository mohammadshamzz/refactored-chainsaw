import logging
from .database import execute_db_query

async def setup_database_tables(db_pool):
    """
    Ensures all necessary tables and indexes are created in the database.
    This function is idempotent and safe to run on every startup.
    """
    logging.info("Setting up database schema.")

    queries = [
        """
        -- New table for caching historical market data (OHLCV)
        CREATE TABLE IF NOT EXISTS market_data (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume BIGINT NOT NULL,
            PRIMARY KEY (symbol, timeframe, timestamp)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_market_data_symbol_timeframe ON market_data(symbol, timeframe);",
        
        # --- Existing tables from your previous definition ---
        
        """-- Main trades table to log historical trades
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            trade_id TEXT UNIQUE NOT NULL,
            instrument TEXT NOT NULL,
            status TEXT NOT NULL, -- 'Open', 'Closed'
            side TEXT NOT NULL, -- 'buy' or 'sell'
            qty REAL NOT NULL,
            entry_price REAL,
            exit_price REAL,
            stop_loss REAL,
            take_profit REAL,
            entry_time TIMESTAMP,
            exit_time TIMESTAMP,
            pnl REAL,
            exit_reason TEXT
        );""",
        """-- Live order tracking for the order manager
        CREATE TABLE IF NOT EXISTS live_orders (
            id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            quantity REAL NOT NULL,
            price REAL,
            stop_price REAL,
            status TEXT NOT NULL,
            filled_qty REAL DEFAULT 0,
            avg_fill_price REAL,
            submitted_at TIMESTAMP,
            filled_at TIMESTAMP,
            cancelled_at TIMESTAMP,
            broker_order_id TEXT,
            parent_trade_id TEXT,
            notes TEXT,
            tp_levels JSONB, -- JSON storage
            stop_loss_order_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        """-- Table for the trailing stop manager
        CREATE TABLE IF NOT EXISTS trailing_stops (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            order_id TEXT NOT NULL,
            original_stop REAL NOT NULL,
            current_stop REAL NOT NULL,
            trailing_amount REAL NOT NULL,
            highest_price REAL,
            lowest_price REAL,
            side TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        """-- Table for the detailed trading journal
        CREATE TABLE IF NOT EXISTS trading_journal (
            id SERIAL PRIMARY KEY,
            trade_id INTEGER REFERENCES trades (id),
            entry_date DATE,
            symbol TEXT NOT NULL,
            setup_type TEXT,
            entry_reason TEXT,
            exit_reason TEXT,
            lessons_learned TEXT,
            market_conditions TEXT,
            confluence_score REAL,
            emotional_state TEXT,
            pre_trade_plan TEXT,
            post_trade_review TEXT,
            rating INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        """-- Table for daily performance tracking
        CREATE TABLE IF NOT EXISTS daily_performance (
            id SERIAL PRIMARY KEY,
            trade_date DATE UNIQUE NOT NULL,
            net_pnl REAL NOT NULL,
            total_trades INTEGER NOT NULL,
            winning_trades INTEGER,
            losing_trades INTEGER,
            gross_profit REAL,
            gross_loss REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        """
        -- Agent state persistence (now only for key-value pairs)
        CREATE TABLE IF NOT EXISTS agent_variables (
            key VARCHAR(50) PRIMARY KEY,
            value_json JSONB NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        """
        -- Watchlist table for setups
        CREATE TABLE IF NOT EXISTS watchlist (
            symbol TEXT PRIMARY KEY,
            direction TEXT NOT NULL,
            poi_bottom REAL NOT NULL,
            poi_top REAL NOT NULL,
            htf_stop_loss REAL NOT NULL,
            entry_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            max_duration_seconds REAL NOT NULL,
            state TEXT NOT NULL,
            score REAL NOT NULL,
            poi_type TEXT NOT NULL
        );""",
        """-- Daily equity tracking for loss limits
        CREATE TABLE IF NOT EXISTS daily_equity_tracking (
            id SERIAL PRIMARY KEY,
            date_tracked DATE NOT NULL,
            equity_start REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        "CREATE INDEX IF NOT EXISTS idx_daily_equity_date ON daily_equity_tracking(date_tracked);",
        """
        -- Master list of all tradable US equity symbols from Alpaca
        CREATE TABLE IF NOT EXISTS tradable_assets (
            symbol TEXT PRIMARY KEY,
            last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        -- The LLM's daily list of symbols to focus on
        CREATE TABLE IF NOT EXISTS daily_focus_list (
            id SERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol TEXT NOT NULL,
            source TEXT DEFAULT 'llm_scanner',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_date, symbol)
        );
        """
    ]

    try:
        for query in queries:
            await execute_db_query(db_pool, query)
        logging.info("Database schema setup complete.")
    except Exception as e:
        logging.critical(f"FATAL: Could not set up database schema: {e}", exc_info=True)
        raise

