import plotly.graph_objects as go
import plotly.subplots as sp
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import yfinance as yf

import logging
from .database import execute_db_query
from .analysis import find_order_blocks, find_fair_value_gaps, find_swing_points
from .enhanced_ict import find_liquidity_sweeps, find_premium_discount_zones

class AdvancedChartingEngine:
    def __init__(self, db_pool):
        self.db_pool = db_pool
    
    async def create_ict_chart(self, symbol: str, timeframe: str = '1d', days: int = 30) -> go.Figure:
        """Create comprehensive ICT analysis chart with automatic data fetching"""
        
        # Try to get data from database first
        df = await self._get_market_data_from_db(symbol, timeframe, days)
        
        # If no data in database, fetch from Yahoo Finance
        if df is None or df.empty:
            df = self._fetch_live_data(symbol, days, timeframe)
        
        if df is None or df.empty:
            return self._create_empty_chart(f"No data available for {symbol}")
        
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Ensure we have the required columns
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df.columns:
                if col == 'volume':
                    df[col] = 0  # Default volume if not available
                else:
                    return self._create_empty_chart(f"Missing required data for {symbol}")
        
        # Create subplot figure
        fig = sp.make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=(f'{symbol} Price Action with ICT Analysis', 'Volume', 'Technical Indicators'),
            row_heights=[0.6, 0.2, 0.2]
        )
        
        # Main candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=df.index,
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='Price',
                increasing_line_color='green',
                decreasing_line_color='red'
            ),
            row=1, col=1
        )
        
        # Add ICT analysis
        self._add_ict_levels(fig, df, symbol)
        
        # Volume chart
        colors = ['green' if close >= open else 'red' 
                 for close, open in zip(df['close'], df['open'])]
        
        fig.add_trace(
            go.Bar(
                x=df.index,
                y=df['volume'],
                name='Volume',
                marker_color=colors,
                opacity=0.7
            ),
            row=2, col=1
        )
        
        # Technical indicators
        self._add_technical_indicators(fig, df)
        
        # Update layout
        fig.update_layout(
            title=f'{symbol} - ICT Analysis Chart ({timeframe})',
            xaxis_title='Time',
            yaxis_title='Price',
            template='plotly_dark',
            height=800,
            showlegend=True,
            xaxis_rangeslider_visible=False
        )
        
        return fig
    
    async def _get_market_data_from_db(self, symbol: str, timeframe: str, days: int) -> Optional[pd.DataFrame]:
        """Get market data from database"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            query = """
            SELECT timestamp, open, high, low, close, volume 
            FROM market_data 
            WHERE instrument = %s AND timeframe = %s AND timestamp >= %s
            ORDER BY timestamp ASC;
            """
            
            data = await execute_db_query(self.db_pool, query, (symbol, timeframe, start_date), fetch='all')
            
            if data:
                df = pd.DataFrame([dict(row) for row in data])
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.set_index('timestamp')
                return df
            
            return None
            
        except Exception as e:
            logging.error(f"Error getting data from database: {e}")
            return None
    
    def _fetch_live_data(self, symbol: str, days: int, timeframe: str = '1d') -> Optional[pd.DataFrame]:
        """Fetch and resample live market data from Yahoo Finance."""
        try:
            # Define the fetch interval, defaulting to '1h' for '4h' timeframe
            fetch_timeframe = '1h' if timeframe == '4h' else timeframe
            
            interval_map = {
                '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
                '1h': '1h', '1d': '1d', '1w': '1wk', '1M': '1mo'
            }
            yf_interval = interval_map.get(fetch_timeframe)

            if not yf_interval:
                logging.error(f"Unsupported timeframe for yfinance: {timeframe}")
                return None

            # Calculate start and end dates respecting yfinance limits
            end_date = datetime.now()
            if yf_interval in ['1m']:
                start_date = end_date - timedelta(days=min(days, 7))
            elif yf_interval in ['5m', '15m', '30m', '1h']:
                start_date = end_date - timedelta(days=min(days, 59))
            else:
                start_date = end_date - timedelta(days=days)

            # Fetch data
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date, interval=yf_interval)
            
            if df.empty:
                logging.warning(f"No data returned from Yahoo Finance for {symbol}")
                return None

            # --- NEW: Resample data if a custom timeframe like '4h' was requested ---
            if timeframe == '4h':
                logging.info(f"Resampling 1h data to 4h for {symbol}...")
                # Define how to aggregate the columns
                agg_dict = {
                    'Open': 'first',
                    'High': 'max',
                    'Low': 'min',
                    'Close': 'last',
                    'Volume': 'sum'
                }
                df = df.resample('4h').agg(agg_dict)
                df.dropna(inplace=True) # Drop intervals with no data

            # Standardize column names
            df.columns = [col.lower() for col in df.columns]
            if 'adj close' in df.columns:
                df = df.rename(columns={'adj close': 'adj_close'})
            if 'volume' not in df.columns:
                df['volume'] = 0
                
            logging.info(f"Fetched {len(df)} data points for {symbol} at {timeframe} timeframe")
            return df
            
        except Exception as e:
            logging.error(f"Error fetching live data for {symbol} with timeframe {timeframe}: {e}")
            return None
    
    def create_live_dashboard_chart(self, symbol: str) -> go.Figure:
        """Create a live dashboard chart with real-time data"""
        try:
            # Get recent data (last 50 periods)
            df = self._fetch_live_data(symbol, days=50, timeframe='1h')
            
            if df is None or df.empty:
                return self._create_empty_chart(f"No live data available for {symbol}")
            
            # Create figure with subplots
            fig = sp.make_subplots(
                rows=4, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                subplot_titles=(
                    f'{symbol} - Live Price Action',
                    'Volume Profile',
                    'Technical Indicators',
                    'Order Flow'
                ),
                row_heights=[0.5, 0.2, 0.2, 0.1]
            )
            
            # Main price chart with enhanced features
            fig.add_trace(
                go.Candlestick(
                    x=df.index,
                    open=df['open'],
                    high=df['high'],
                    low=df['low'],
                    close=df['close'],
                    name='Price',
                    increasing_line_color='#00ff88',
                    decreasing_line_color='#ff4444',
                    increasing_fillcolor='rgba(0, 255, 136, 0.3)',
                    decreasing_fillcolor='rgba(255, 68, 68, 0.3)'
                ),
                row=1, col=1
            )
            
            # Add moving averages
            df['sma_20'] = df['close'].rolling(20).mean()
            df['sma_50'] = df['close'].rolling(50).mean()
            df['ema_12'] = df['close'].ewm(span=12).mean()
            df['ema_26'] = df['close'].ewm(span=26).mean()
            
            fig.add_trace(
                go.Scatter(x=df.index, y=df['sma_20'], name='SMA 20', 
                          line=dict(color='yellow', width=1)),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(x=df.index, y=df['sma_50'], name='SMA 50', 
                          line=dict(color='orange', width=1)),
                row=1, col=1
            )
            
            # Bollinger Bands
            bb_period = 20
            bb_std = 2
            df['bb_middle'] = df['close'].rolling(bb_period).mean()
            df['bb_std'] = df['close'].rolling(bb_period).std()
            df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * bb_std)
            df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * bb_std)
            
            fig.add_trace(
                go.Scatter(x=df.index, y=df['bb_upper'], name='BB Upper',
                          line=dict(color='purple', width=1, dash='dash')),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(x=df.index, y=df['bb_lower'], name='BB Lower',
                          line=dict(color='purple', width=1, dash='dash'),
                          fill='tonexty', fillcolor='rgba(128, 0, 128, 0.1)'),
                row=1, col=1
            )
            
            # Volume with color coding
            colors = ['#00ff88' if close >= open else '#ff4444' 
                     for close, open in zip(df['close'], df['open'])]
            
            fig.add_trace(
                go.Bar(x=df.index, y=df['volume'], name='Volume',
                      marker_color=colors, opacity=0.7),
                row=2, col=1
            )
            
            # Volume moving average
            df['vol_sma'] = df['volume'].rolling(20).mean()
            fig.add_trace(
                go.Scatter(x=df.index, y=df['vol_sma'], name='Vol SMA',
                          line=dict(color='white', width=1)),
                row=2, col=1
            )
            
            # Technical indicators
            self._add_advanced_indicators(fig, df)
            
            # Order flow simulation (mock data for demonstration)
            self._add_order_flow_indicator(fig, df)
            
            # Add current price line
            current_price = df['close'].iloc[-1]
            fig.add_hline(y=current_price, line_dash="solid", line_color="cyan",
                         annotation_text=f"Current: ${current_price:.2f}",
                         row=1, col=1)
            
            # Update layout
            fig.update_layout(
                title=f'{symbol} - Live Trading Dashboard',
                template='plotly_dark',
                height=900,
                showlegend=True,
                xaxis_rangeslider_visible=False,
                font=dict(size=10)
            )
            
            # Update axes
            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
            fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
            
            return fig
            
        except Exception as e:
            logging.error(f"Error creating live dashboard chart: {e}")
            return self._create_empty_chart(f"Error creating chart for {symbol}")
    
    def _add_advanced_indicators(self, fig: go.Figure, df: pd.DataFrame):
        """Add advanced technical indicators"""
        try:
            # RSI Calculation
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            fig.add_trace(
                go.Scatter(x=df.index, y=df['rsi'], name='RSI',
                          line=dict(color='#ff6b6b', width=2)),
                row=3, col=1
            )
            
            # RSI levels
            fig.add_hline(y=70, line_dash="dash", line_color="red", 
                         annotation_text="Overbought", row=3, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green",
                         annotation_text="Oversold", row=3, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color="gray", row=3, col=1)
            
            # MACD Calculation
            exp1 = df['close'].ewm(span=12, adjust=False).mean()
            exp2 = df['close'].ewm(span=26, adjust=False).mean()
            df['macd'] = exp1 - exp2
            df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
            df['macd_histogram'] = df['macd'] - df['macd_signal']
            
            # Add MACD as secondary y-axis on the same subplot
            fig.add_trace(
                go.Scatter(x=df.index, y=df['macd'], name='MACD',
                          line=dict(color='blue', width=1), yaxis='y2'),
                row=3, col=1
            )
            
            fig.add_trace(
                go.Scatter(x=df.index, y=df['macd_signal'], name='MACD Signal',
                          line=dict(color='red', width=1), yaxis='y2'),
                row=3, col=1
            )
            
        except Exception as e:
            logging.error(f"Error adding advanced indicators: {e}")
    
    def _add_order_flow_indicator(self, fig: go.Figure, df: pd.DataFrame):
        """Add order flow indicator (simulated)"""
        try:
            # Simulate order flow based on volume and price action
            df['buy_volume'] = df['volume'] * (df['close'] > df['open']).astype(int)
            df['sell_volume'] = df['volume'] * (df['close'] <= df['open']).astype(int)
            
            # Calculate cumulative delta
            df['delta'] = df['buy_volume'] - df['sell_volume']
            df['cumulative_delta'] = df['delta'].cumsum()
            
            fig.add_trace(
                go.Scatter(x=df.index, y=df['cumulative_delta'], 
                          name='Cumulative Delta',
                          line=dict(color='cyan', width=2),
                          fill='tozeroy', fillcolor='rgba(0, 255, 255, 0.1)'),
                row=4, col=1
            )
            
            # Add zero line
            fig.add_hline(y=0, line_dash="solid", line_color="white", row=4, col=1)
            
        except Exception as e:
            logging.error(f"Error adding order flow indicator: {e}")
    
    def create_multi_timeframe_chart(self, symbol: str) -> go.Figure:
        """Create multi-timeframe analysis chart"""
        try:
            # Get data for different timeframes
            timeframes = ['1d', '4h', '1h']
            data = {}
            
            for tf in timeframes:
                if tf == '1d':
                    df = self._fetch_live_data(symbol, days=100, timeframe=tf)
                elif tf == '4h':
                    df = self._fetch_live_data(symbol, days=30, timeframe=tf)
                else:  # 1h
                    df = self._fetch_live_data(symbol, days=7, timeframe=tf)
                
                if df is not None and not df.empty:
                    data[tf] = df
            
            if not data:
                return self._create_empty_chart(f"No multi-timeframe data for {symbol}")
            
            # Create subplots for each timeframe
            fig = sp.make_subplots(
                rows=len(timeframes), cols=1,
                shared_xaxes=False,
                vertical_spacing=0.05,
                subplot_titles=[f'{symbol} - {tf.upper()} Timeframe' for tf in timeframes]
            )
            
            for i, (tf, df) in enumerate(data.items(), 1):
                # Add candlestick chart
                fig.add_trace(
                    go.Candlestick(
                        x=df.index,
                        open=df['open'],
                        high=df['high'],
                        low=df['low'],
                        close=df['close'],
                        name=f'{tf} Price',
                        increasing_line_color='green',
                        decreasing_line_color='red'
                    ),
                    row=i, col=1
                )
                
                # Add key moving averages
                df['sma_20'] = df['close'].rolling(20).mean()
                df['sma_50'] = df['close'].rolling(50).mean()
                
                fig.add_trace(
                    go.Scatter(x=df.index, y=df['sma_20'], 
                              name=f'{tf} SMA20',
                              line=dict(color='yellow', width=1)),
                    row=i, col=1
                )
                
                fig.add_trace(
                    go.Scatter(x=df.index, y=df['sma_50'], 
                              name=f'{tf} SMA50',
                              line=dict(color='orange', width=1)),
                    row=i, col=1
                )
                
                # Add ICT levels for each timeframe
                self._add_ict_levels_for_row(fig, df, symbol, row=i)
            
            fig.update_layout(
                title=f'{symbol} - Multi-Timeframe ICT Analysis',
                template='plotly_dark',
                height=1200,
                showlegend=True,
                xaxis_rangeslider_visible=False
            )
            
            return fig
            
        except Exception as e:
            logging.error(f"Error creating multi-timeframe chart: {e}")
            return self._create_empty_chart(f"Error creating multi-timeframe chart for {symbol}")
    
    def _add_ict_levels_for_row(self, fig: go.Figure, df: pd.DataFrame, symbol: str, row: int):
        """Add ICT levels for specific subplot row"""
        try:
            # Simplified ICT levels for multi-timeframe view
            # Order Blocks
            order_blocks = find_order_blocks(df)
            for i, ob in enumerate(order_blocks[-2:]):  # Show last 2 order blocks
                color = 'rgba(0, 255, 0, 0.2)' if ob['type'] == 'Bullish' else 'rgba(255, 0, 0, 0.2)'
                
                fig.add_shape(
                    type="rect",
                    x0=df.index[0], x1=df.index[-1],
                    y0=ob['bottom'], y1=ob['top'],
                    fillcolor=color,
                    line=dict(color=color.replace('0.2', '0.6'), width=1),
                    row=row, col=1
                )
            
            # Key support/resistance levels
            recent_high = df['high'].tail(20).max()
            recent_low = df['low'].tail(20).min()
            
            fig.add_hline(y=recent_high, line_dash="dash", line_color="red",
                         annotation_text="Resistance", row=row, col=1)
            fig.add_hline(y=recent_low, line_dash="dash", line_color="green",
                         annotation_text="Support", row=row, col=1)
            
        except Exception as e:
            logging.error(f"Error adding ICT levels for row {row}: {e}")
    
    def create_portfolio_correlation_matrix(self, symbols: List[str]) -> go.Figure:
        """Create portfolio correlation matrix"""
        try:
            # Fetch data for all symbols
            price_data = {}
            
            for symbol in symbols:
                df = self._fetch_live_data(symbol, days=60, timeframe='1d')
                if df is not None and not df.empty:
                    price_data[symbol] = df['close']
            
            if len(price_data) < 2:
                return self._create_empty_chart("Need at least 2 symbols for correlation")
            
            # Create correlation matrix
            correlation_df = pd.DataFrame(price_data).corr()
            
            # Create heatmap
            fig = go.Figure(data=go.Heatmap(
                z=correlation_df.values,
                x=correlation_df.columns,
                y=correlation_df.columns,
                colorscale='RdBu',
                zmid=0,
                text=correlation_df.round(3).values,
                texttemplate="%{text}",
                textfont={"size": 12},
                hoverongaps=False
            ))
            
            fig.update_layout(
                title='Portfolio Correlation Matrix',
                template='plotly_dark',
                height=500,
                width=500
            )
            
            return fig
            
        except Exception as e:
            logging.error(f"Error creating correlation matrix: {e}")
            return self._create_empty_chart("Error creating correlation matrix")
    
    def _add_ict_levels(self, fig: go.Figure, df: pd.DataFrame, symbol: str):
        """Add ICT analysis levels to chart"""
        
        # Order Blocks
        order_blocks = find_order_blocks(df)
        for i, ob in enumerate(order_blocks[-5:]):  # Show last 5 order blocks
            color = 'rgba(0, 255, 0, 0.3)' if ob['type'] == 'Bullish' else 'rgba(255, 0, 0, 0.3)'
            
            fig.add_shape(
                type="rect",
                x0=df.index[0], x1=df.index[-1],
                y0=ob['bottom'], y1=ob['top'],
                fillcolor=color,
                line=dict(color=color.replace('0.3', '0.8'), width=1),
                name=f"OB {ob['type']}"
            )
            
            # Add annotation
            fig.add_annotation(
                x=df.index[-1],
                y=(ob['top'] + ob['bottom']) / 2,
                text=f"OB-{ob['type'][0]}",
                showarrow=False,
                font=dict(size=10, color='white'),
                bgcolor=color.replace('0.3', '0.8')
            )
        
        # Fair Value Gaps
        fvgs = find_fair_value_gaps(df)
        for i, fvg in enumerate(fvgs[-3:]):  # Show last 3 FVGs
            color = 'rgba(0, 0, 255, 0.2)' if fvg['type'] == 'Bullish' else 'rgba(255, 165, 0, 0.2)'
            
            fig.add_shape(
                type="rect",
                x0=df.index[0], x1=df.index[-1],
                y0=fvg['bottom'], y1=fvg['top'],
                fillcolor=color,
                line=dict(color=color.replace('0.2', '0.8'), width=1, dash='dash'),
                name=f"FVG {fvg['type']}"
            )
        
        # Swing Points
        swing_highs, swing_lows = find_swing_points(df)
        
        if not swing_highs.empty:
            fig.add_trace(
                go.Scatter(
                    x=swing_highs.index,
                    y=swing_highs.values,
                    mode='markers',
                    marker=dict(symbol='triangle-down', size=10, color='red'),
                    name='Swing Highs'
                ),
                row=1, col=1
            )
        
        if not swing_lows.empty:
            fig.add_trace(
                go.Scatter(
                    x=swing_lows.index,
                    y=swing_lows.values,
                    mode='markers',
                    marker=dict(symbol='triangle-up', size=10, color='green'),
                    name='Swing Lows'
                ),
                row=1, col=1
            )
        
        # Premium/Discount Zones
        zones = find_premium_discount_zones(df)
        if zones:
            # Premium zone
            fig.add_shape(
                type="rect",
                x0=df.index[0], x1=df.index[-1],
                y0=zones['premium_zone']['start'], y1=zones['premium_zone']['end'],
                fillcolor='rgba(255, 0, 0, 0.1)',
                line=dict(color='red', width=1, dash='dot'),
                name="Premium Zone"
            )
            
            # Discount zone
            fig.add_shape(
                type="rect",
                x0=df.index[0], x1=df.index[-1],
                y0=zones['discount_zone']['start'], y1=zones['discount_zone']['end'],
                fillcolor='rgba(0, 255, 0, 0.1)',
                line=dict(color='green', width=1, dash='dot'),
                name="Discount Zone"
            )
            
            # Add zone labels
            fig.add_annotation(
                x=df.index[-10],
                y=zones['premium_zone']['end'],
                text="Premium Zone",
                showarrow=False,
                font=dict(size=12, color='red')
            )
            
            fig.add_annotation(
                x=df.index[-10],
                y=zones['discount_zone']['start'],
                text="Discount Zone",
                showarrow=False,
                font=dict(size=12, color='green')
            )
        
        # Liquidity Sweeps
        try:
            # Get both bullish and bearish sweeps
            bullish_sweeps = find_liquidity_sweeps(df, 'buy')
            bearish_sweeps = find_liquidity_sweeps(df, 'sell')
            all_sweeps = bullish_sweeps + bearish_sweeps
            
            for sweep in all_sweeps[-3:]:  # Show last 3 sweeps
                color = 'yellow' if 'bullish' in sweep['type'] else 'orange'
                sweep_level = sweep.get('level', sweep.get('sweep_level', 0))
                
                fig.add_annotation(
                    x=sweep['timestamp'],
                    y=sweep_level,
                    text="SWEEP",
                    showarrow=True,
                    arrowhead=2,
                    arrowcolor=color,
                    font=dict(size=16, color=color)
                )
        except Exception as e:
            logging.error(f"Error adding liquidity sweeps to chart: {e}")
    
    def _add_technical_indicators(self, fig: go.Figure, df: pd.DataFrame):
        """Add technical indicators to chart"""
        
        # Calculate RSI
        def calculate_rsi(data, window=14):
            diff = data.diff(1)
            gain = diff.where(diff > 0, 0)
            loss = -diff.where(diff < 0, 0)
            avg_gain = gain.rolling(window=window, min_periods=1).mean()
            avg_loss = loss.rolling(window=window, min_periods=1).mean()
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            return rsi

        df['rsi'] = calculate_rsi(df['close'])
        
        # Add RSI
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df['rsi'],
                name='RSI',
                line=dict(color='purple', width=2)
            ),
            row=3, col=1
        )
        
        # RSI levels
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=3, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=3, col=1)
        fig.add_hline(y=50, line_dash="dot", line_color="gray", row=3, col=1)

        # MACD Calculation
        def calculate_macd(data, span_fast=12, span_slow=26, span_signal=9):
            ema_fast = data.ewm(span=span_fast, adjust=False).mean()
            ema_slow = data.ewm(span=span_slow, adjust=False).mean()
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=span_signal, adjust=False).mean()
            histogram = macd_line - signal_line
            return macd_line, signal_line, histogram

        df['macd'], df['macd_signal'], df['macd_histogram'] = calculate_macd(df['close'])

        # Add MACD as secondary y-axis on the same subplot
        fig.add_trace(
            go.Scatter(x=df.index, y=df['macd'], name='MACD',
                      line=dict(color='blue', width=1), yaxis='y2'),
            row=3, col=1
        )
        
        fig.add_trace(
            go.Scatter(x=df.index, y=df['macd_signal'], name='MACD Signal',
                      line=dict(color='red', width=1), yaxis='y2'),
            row=3, col=1
        )
    
    def _create_empty_chart(self, message: str) -> go.Figure:
        """Create empty chart with message"""
        fig = go.Figure()
        fig.add_annotation(
            x=0.5, y=0.5,
            text=message,
            showarrow=False,
            font=dict(size=20, color='gray'),
            xref="paper", yref="paper"
        )
        fig.update_layout(
            template='plotly_dark',
            height=400,
            showlegend=False
        )
        return fig
    
    async def create_portfolio_heatmap(self, symbols: List[str]) -> go.Figure:
        """Create portfolio performance heatmap"""
        
        # Get recent performance data for each symbol
        performance_data = []
        
        for symbol in symbols:
            query = """
            SELECT 
                DATE(entry_time) as trade_date,
                SUM(COALESCE(pnl, 0)) as daily_pnl
            FROM trades 
            WHERE instrument = %s AND entry_time >= %s
            GROUP BY DATE(entry_time)
            ORDER BY trade_date DESC
            LIMIT 30;
            """
            
            data = await execute_db_query(
                self.db_pool, 
                query, 
                (symbol, datetime.now() - timedelta(days=30)), 
                fetch='all'
            )
            
            if data:
                symbol_data = {row['trade_date']: float(row['daily_pnl']) for row in data}
                performance_data.append((symbol, symbol_data))
        
        if not performance_data:
            return self._create_empty_chart("No performance data available")
        
        # Create heatmap data
        all_dates = set()
        for _, symbol_data in performance_data:
            all_dates.update(symbol_data.keys())
        
        all_dates = sorted(list(all_dates))
        
        z_data = []
        y_labels = []
        
        for symbol, symbol_data in performance_data:
            row_data = [symbol_data.get(date, 0) for date in all_dates]
            z_data.append(row_data)
            y_labels.append(symbol)
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=z_data,
            x=[date.strftime('%m-%d') for date in all_dates],
            y=y_labels,
            colorscale='RdYlGn',
            zmid=0,
            text=[[f'${val:.2f}' for val in row] for row in z_data],
            texttemplate="%{text}",
            textfont={"size": 10},
            hoverongaps=False
        ))
        
        fig.update_layout(
            title='Portfolio Daily P&L Heatmap',
            xaxis_title='Date',
            yaxis_title='Symbol',
            template='plotly_dark',
            height=400
        )
        
        return fig
    
    def create_confluence_radar_chart(self, symbol: str, analysis_data: Dict) -> go.Figure:
        """Create radar chart showing confluence factors"""
        try:
            categories = [
                'Order Block Score',
                'FVG Score', 
                'Zone Score',
                'Sweep Score',
                'Volume Score',
                'Structure Score'
            ]
            
            # Extract scores from analysis data
            values = [
                analysis_data.get('order_block_score', 0),
                analysis_data.get('fvg_score', 0),
                analysis_data.get('zone_score', 0),
                analysis_data.get('sweep_score', 0),
                analysis_data.get('volume_score', 0),
                analysis_data.get('structure_score', 0)
            ]
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatterpolar(
                r=values,
                theta=categories,
                fill='toself',
                name=f'{symbol} Confluence',
                line_color='cyan',
                fillcolor='rgba(0, 255, 255, 0.2)'
            ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 100]
                    )),
                showlegend=True,
                title=f'{symbol} - ICT Confluence Analysis',
                template='plotly_dark'
            )
            
            return fig
            
        except Exception as e:
            logging.error(f"Error creating confluence radar chart: {e}")
            return self._create_empty_chart(f"Error creating confluence chart for {symbol}")
        
    def create_equity_curve(self) -> go.Figure:
        """Create equity curve chart from trade history."""
        try:
            # Mock data for now - would come from actual trade history
            dates = pd.date_range(start='2024-01-01', end=datetime.now(), freq='D')
            equity = [100000]  # Starting equity
            
            # Simulate equity curve with some random walk
            np.random.seed(42)
            for i in range(1, len(dates)):
                daily_return = np.random.normal(0.001, 0.02)  # 0.1% daily return, 2% volatility
                equity.append(equity[-1] * (1 + daily_return))
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=dates,
                y=equity,
                mode='lines',
                name='Account Equity',
                line=dict(color='#1f77b4', width=2)
            ))
            
            fig.update_layout(
                title='Portfolio Equity Curve',
                xaxis_title='Date',
                yaxis_title='Account Value ($)',
                height=400,
                showlegend=False
            )
            
            return fig
            
        except Exception as e:
            logging.error(f"Error creating equity curve: {e}")
            return self._create_empty_chart("Error creating equity curve")
    
    def create_timeframe_chart(self, symbol: str, timeframe: str) -> go.Figure:
        """Create a simple chart for a specific timeframe."""
        try:
            # Fetch data for the specific timeframe
            if timeframe == "1d":
                days = 60
            elif timeframe == "4h":
                days = 30
            else:  # 15m
                days = 7
            
            df = self._fetch_live_data(symbol, days, timeframe)
            
            if df is None or df.empty:
                return self._create_empty_chart(f"No data for {symbol} {timeframe}")
            
            # Create candlestick chart
            fig = go.Figure(data=go.Candlestick(
                x=df.index,
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name=f'{symbol} {timeframe}'
            ))
            
            fig.update_layout(
                title=f'{symbol} - {timeframe}',
                xaxis_title='Time',
                yaxis_title='Price',
                height=300,
                showlegend=False,
                xaxis_rangeslider_visible=False
            )
            
            return fig
            
        except Exception as e:
            logging.error(f"Error creating timeframe chart: {e}")
            return self._create_empty_chart(f"Error loading {symbol} {timeframe}")