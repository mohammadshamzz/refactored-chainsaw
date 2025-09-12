import os
import logging
from psycopg2 import pool
from psycopg2.extras import DictCursor
from tenacity import retry, stop_after_attempt, wait_fixed
from dotenv import load_dotenv

load_dotenv()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_db_connection_pool(config=None):
    """Create a PostgreSQL connection pool."""
    logging.info(
        f"Connecting to PostgreSQL database... "
        f"{ { 'dbname': os.getenv('DB_NAME'), 'user': os.getenv('DB_USER'), 'host': os.getenv('DB_HOST'), 'port': os.getenv('DB_PORT') } }"
    )
    return pool.SimpleConnectionPool(
        1,
        10,
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        cursor_factory=DictCursor,
    )


def get_valid_connection(db_pool):
    """Ensure the connection is valid before returning it."""
    conn = db_pool.getconn()
    if conn.closed:
        logging.warning("Connection was closed unexpectedly. Reconnecting...")
        conn = db_pool.getconn()  # Re-acquire a valid connection
    return conn


async def execute_db_query(db_pool, query, params=None, fetch=None, many=False):
    conn = None
    try:
        conn = get_valid_connection(db_pool)
        cur = conn.cursor()

        if many:
            cur.executemany(query, params or [])
        else:
            cur.execute(query, params or ())

        result = None
        if fetch == "one":
            result = cur.fetchone()
        elif fetch == "all":
            result = cur.fetchall()

        conn.commit()
        return result
    except Exception as e:
        logging.error(f"Database query failed: {e}")
        if conn and not conn.closed:
            conn.rollback()
        raise
    finally:
        if conn:
            db_pool.putconn(conn)


async def setup_database(db_pool):
    schema_file = "schemas.sql"
    if not os.path.exists(schema_file):
        logging.warning(f"Schema file {schema_file} not found. Skipping DB setup.")
        return

    with open(schema_file, "r") as f:
        schema_queries = f.read()

    conn = get_valid_connection(db_pool)
    try:
        with conn.cursor() as cur:
            cur.execute(schema_queries)
        conn.commit()
        logging.info("Database setup complete.")
    finally:
        db_pool.putconn(conn)


async def get_trade_history(db_pool):
    return await execute_db_query(
        db_pool,
        "SELECT * FROM trades ORDER BY entry_time DESC LIMIT 100;",
        fetch="all",
    )


async def update_daily_performance(db_pool, trade_date, pnl):
    query = """
    INSERT INTO daily_performance (trade_date, net_pnl, total_trades)
    VALUES (%s, %s, 1)
    ON CONFLICT (trade_date) DO UPDATE SET
        net_pnl = daily_performance.net_pnl + EXCLUDED.net_pnl,
        total_trades = daily_performance.total_trades + 1;
    """
    await execute_db_query(db_pool, query, (trade_date, pnl))
