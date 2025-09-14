import os
import asyncio
import logging
import json
from typing import List, Optional
from datetime import date

try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logging.warning(
        "google-genai SDK not available. Install with: pip install google-genai"
    )

from .database import execute_db_query
from .alpaca_broker import AlpacaBroker


class LLMScanner:
    """
    Runs once-a-day market intelligence tasks via Gemini-2.5-Flash,
    with Google Search grounding to fetch live market context.
    """

    def __init__(self, db_pool, broker: AlpacaBroker):
        self.db_pool = db_pool
        self.broker = broker
        self.client: Optional[genai.Client] = None
        self.model_name: Optional[str] = None

        if GEMINI_AVAILABLE and (api_key := os.getenv("GEMINI_API_KEY")):
            try:
                self.client = genai.Client(api_key=api_key)
                self.model_name = "gemini-2.5-flash"

                logging.info("LLMScanner ready. Model: %s", self.model_name)

            except Exception as e:
                logging.error("Failed to initialize Gemini client", exc_info=True)

    def _create_scanner_prompt(self) -> str:
        return """
        Act as a professional stock market analyst preparing for the upcoming trading day.
        Your task is to identify 100 US stock symbols that are most likely to be "in play"
        and experience significant volatility or directional movement today.

        Use your live web search capabilities to analyze:
        1. **Pre-market movers:** Significant pre-market volume and price changes right now.
        2. **Recent News:** Earnings, mergers, FDA approvals, product announcements in the last 24 hours.
        3. **Sector Trends:** Any unusual strength or weakness in specific sectors.
        4. **Analyst Ratings:** Notable upgrades/downgrades reported recently.

        Based on your findings, return a JSON array of the most promising stock symbols.
        **Important:** Respond ONLY with a valid JSON array of strings.
        Example: ["AAPL", "TSLA", "GOOG"]
        """

    async def _get_llm_focus_list(self) -> List[str]:
        if not self.client or not self.model_name:
            return []

        prompt = self._create_scanner_prompt()

        try:
            # Define the grounding tool
            grounding_tool = types.Tool(
                google_search=types.GoogleSearch()
            )

            # Configure generation settings
            config = types.GenerateContentConfig(
                tools=[grounding_tool]
            )

            # Run sync Gemini call in thread
            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=config,
            )

            response_text = response.text.strip()

            try:
                symbols = json.loads(response_text)
            except json.JSONDecodeError:
                start = response_text.find("[")
                end = response_text.rfind("]") + 1
                symbols = json.loads(response_text[start:end])

            if isinstance(symbols, list) and all(isinstance(s, str) for s in symbols):
                return [s.upper().strip() for s in symbols]

            logging.warning("LLM response was not a valid list of stock symbols")
        except Exception:
            logging.error("Error generating/parsing LLM focus list", exc_info=True)

        return []



    async def _sync_and_validate_assets(self, symbols_from_llm: List[str]) -> List[str]:
        """Syncs Alpaca's master asset list with our DB and validates the LLM's list."""
        logging.info("Syncing local tradable assets list with Alpaca...")

        try:
            assets = await asyncio.to_thread(self.broker.get_tradable_assets)
            alpaca_symbols = {asset.symbol for asset in assets}

            db_symbols_rows = await execute_db_query(
                self.db_pool, "SELECT symbol FROM tradable_assets;", fetch="all"
            )
            db_symbols = {row["symbol"] for row in db_symbols_rows}

            newly_listed = alpaca_symbols - db_symbols
            delisted = db_symbols - alpaca_symbols

            # Convert to list of 1-tuples for executemany
            if newly_listed:
                records_to_insert = [(s,) for s in newly_listed]
                await execute_db_query(
                    self.db_pool,
                    "INSERT INTO tradable_assets (symbol) VALUES (%s) ON CONFLICT (symbol) DO NOTHING;",
                    records_to_insert,
                    many=True,
                )

            if delisted:
                records_to_delete = [(s,) for s in delisted]
                await execute_db_query(
                    self.db_pool,
                    "DELETE FROM tradable_assets WHERE symbol = %s;",
                    records_to_delete,
                    many=True,
                )
            
            validated = list(set(symbols_from_llm).intersection(alpaca_symbols))
            logging.info(f"LLM list validated. {len(validated)} of {len(symbols_from_llm)} symbols are tradable.")
            return validated

        except Exception as e:
            logging.error(f"FATAL: Failed to sync and validate assets: {e}", exc_info=True)
            return []


    async def _update_daily_focus_list_db(self, symbols: List[str]):
        """Clears today's old list and inserts the new one."""
        today = date.today()
        # Delete old records for today
        await execute_db_query(
            self.db_pool,
            "DELETE FROM daily_focus_list WHERE trade_date = %s;",
            (today,)
        )

        if symbols:
            # Convert symbols to list of tuples for executemany
            records = [(today, symbol) for symbol in symbols]
            await execute_db_query(
                self.db_pool,
                "INSERT INTO daily_focus_list (trade_date, symbol) VALUES (%s, %s);",
                records,
                many=True
            )

        logging.info(f"Updated daily focus list in database with {len(symbols)} symbols.")


    async def run_daily_scan(self):
        """Main orchestration of daily scan."""
        logging.info("--- Starting Daily LLM Market Scan ---")
        llm_symbols = await self._get_llm_focus_list()
        if llm_symbols:
            validated_symbols = await self._sync_and_validate_assets(llm_symbols)
            await self._update_daily_focus_list_db(validated_symbols)
        logging.info("--- Daily LLM Market Scan Finished ---")