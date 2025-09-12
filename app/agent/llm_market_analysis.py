import logging
import os
import asyncio
import json
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logging.warning("Google Generative AI not available. Install with: pip install google-generativeai")
from .analytics import get_symbol_performance

class LLMMarketAnalyzer:
    def __init__(self, db_pool, config):
        self.db_pool = db_pool
        self.config = config
        self.enabled = False
        if GEMINI_AVAILABLE and (api_key := os.getenv('GEMINI_API_KEY')):
            try:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel('gemini-pro')
                self.enabled = True
                logging.info("LLM Market Analyzer initialized with Gemini.")
            except Exception as e:
                logging.error(f"Failed to initialize Gemini: {e}")
    
    async def analyze_portfolio_sentiment(self, symbols: list):
        if not self.enabled: return None
        performance_data = [perf for s in symbols[:5] if (perf := await get_symbol_performance(self.db_pool, s)) and perf.get('total_trades', 0) > 0]
        if not performance_data: return {'summary': 'No recent trading data for analysis.'}
        
        prompt = "Analyze the following portfolio performance and provide a JSON response with keys: 'overall_sentiment' (-1 to 1), 'sentiment_label' ('Bullish'/'Bearish'/'Neutral'), 'confidence' (0 to 1), and 'summary'. Data:\n" + json.dumps(performance_data)
        return await self._get_json_response(prompt)

    async def get_trade_signals(self, symbols: list):
        if not self.enabled: return []
        tasks = [self._analyze_symbol_for_signal(s) for s in symbols[:3]]
        signals = [s for s in await asyncio.gather(*tasks) if s]
        return sorted(signals, key=lambda x: x.get('confidence', 0), reverse=True)

    async def _analyze_symbol_for_signal(self, symbol: str):
        perf = await get_symbol_performance(self.db_pool, symbol)
        if not perf or perf.get('total_trades', 0) < 1: return None
        prompt = f"Analyze the performance for {symbol} and provide a trade signal JSON with keys: 'signal_type' ('BUY'/'SELL'/'HOLD'), 'strength' ('Strong'/'Medium'/'Weak'), 'confidence' (0 to 1), and 'recommendation'. Data:\n" + json.dumps(perf)
        signal = await self._get_json_response(prompt)
        if signal: signal['symbol'] = symbol
        return signal

    async def _get_json_response(self, prompt: str):
        try:
            response_text = await asyncio.to_thread(self.model.generate_content(prompt).text)
            json_str = response_text[response_text.find('{'):response_text.rfind('}')+1]
            return json.loads(json_str)
        except Exception as e:
            logging.error(f"Error getting or parsing LLM JSON response: {e}")
            return None
