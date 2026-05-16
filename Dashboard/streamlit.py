import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import clickhouse_connect
from datetime import datetime, timedelta
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Crypto Trading Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 10px;
    }
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 1rem;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize ClickHouse connection
@st.cache_resource
def init_clickhouse():
    """Initialize ClickHouse connection"""
    try:
        # Update with your ClickHouse connection details
        client = clickhouse_connect.get_client(
            host='localhost',  # or your ClickHouse host
            port=8123,  # default HTTP port
            username='default',
            password='',  # add your password if needed
            database='crypto'
        )
        return client
    except Exception as e:
        st.error(f"Failed to connect to ClickHouse: {e}")
        return None

# Cache data loading functions
@st.cache_data(ttl=30)  # Cache for 30 seconds for real-time updates
def load_latest_ticker(client):
    """Load latest ticker data for all symbols"""
    query = """
    SELECT 
        symbol,
        price,
        change_percent,
        volume,
        event_time
    FROM crypto.ticker
    WHERE (symbol, event_time) IN (
        SELECT symbol, max(event_time)
        FROM crypto.ticker
        GROUP BY symbol
    )
    ORDER BY volume DESC
    """
    return client.query_df(query)

@st.cache_data(ttl=60)
def load_klines(client, symbol, interval, days=7):
    """Load kline data for specific symbol and interval"""
    query = f"""
    SELECT 
        start_time,
        open,
        high,
        low,
        close,
        volume
    FROM crypto.klines
    WHERE symbol = '{symbol}'
        AND interval = '{interval}'
        AND start_time >= now() - INTERVAL {days} DAY
    ORDER BY start_time
    """
    df = client.query_df(query)
    if not df.empty:
        df['start_time'] = pd.to_datetime(df['start_time'])
    return df

@st.cache_data(ttl=30)
def load_recent_trades(client, symbol, limit=100):
    """Load recent trades for specific symbol"""
    query = f"""
    SELECT 
        trade_time,
        price,
        qty
    FROM crypto.trades
    WHERE symbol = '{symbol}'
    ORDER BY trade_time DESC
    LIMIT {limit}
    """
    df = client.query_df(query)
    if not df.empty:
        df['trade_time'] = pd.to_datetime(df['trade_time'])
    return df

@st.cache_data(ttl=300)
def load_available_symbols(client):
    """Load all available symbols from ticker table"""
    query = """
    SELECT DISTINCT symbol 
    FROM crypto.ticker 
    ORDER BY symbol
    """
    df = client.query_df(query)
    return df['symbol'].tolist()

def create_candlestick_chart(df, symbol, interval):
    """Create interactive candlestick chart"""
    fig = go.Figure(data=[
        go.Candlestick(
            x=df['start_time'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='Price'
        )
    ])
    
    # Add volume bar chart
    fig.add_trace(go.Bar(
        x=df['start_time'],
        y=df['volume'],
        name='Volume',
        yaxis='y2',
        marker_color='rgba(0,100,200,0.3)'
    ))
    
    fig.update_layout(
        title=f'{symbol} - {interval} Candlestick Chart',
        yaxis_title='Price (USD)',
        yaxis2=dict(
            title='Volume',
            overlaying='y',
            side='right',
            showgrid=False
        ),
        xaxis_title='Date',
        template='plotly_dark',
        height=600,
        hovermode='x unified'
    )
    
    return fig

def create_price_scatter(df, trades_df):
    """Create price scatter plot with trade data"""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add price line from klines
    fig.add_trace(
        go.Scatter(
            x=df['start_time'],
            y=df['close'],
            mode='lines',
            name='Close Price',
            line=dict(color='#00ff00', width=2)
        ),
        secondary_y=False
    )
    
    # Add trades as scatter points
    if not trades_df.empty:
        fig.add_trace(
            go.Scatter(
                x=trades_df['trade_time'],
                y=trades_df['price'],
                mode='markers',
                name='Individual Trades',
                marker=dict(
                    size=trades_df['qty'] / trades_df['qty'].max() * 20,
                    color='rgba(255, 255, 0, 0.5)',
                    symbol='circle'
                ),
                text=[f"Qty: {qty:.4f}" for qty in trades_df['qty']],
                hoverinfo='text+x+y'
            ),
            secondary_y=False
        )
    
    fig.update_layout(
        title='Price Movement with Individual Trades',
        xaxis_title='Date',
        template='plotly_dark',
        height=500
    )
    fig.update_yaxes(title_text="Price (USD)", secondary_y=False)
    
    return fig

def main():
    st.markdown('<p class="main-header">📊 Cryptocurrency Trading Dashboard</p>', unsafe_allow_html=True)
    
    # Initialize ClickHouse connection
    client = init_clickhouse()
    if client is None:
        st.stop()
    
    # Sidebar
    st.sidebar.title("🔧 Controls")
    
    # Load available symbols
    symbols = load_available_symbols(client)
    if not symbols:
        st.warning("No data found in database. Please ensure ticker data exists.")
        st.stop()
    
    # Symbol selection
    selected_symbol = st.sidebar.selectbox("Select Cryptocurrency", symbols)
    
    # Time interval selection
    interval_options = ['1m', '5m', '15m', '1h', '4h', '1d']
    selected_interval = st.sidebar.selectbox("Select Time Interval", interval_options, index=2)
    
    # Days to display
    days = st.sidebar.slider("Days to Display", 1, 30, 7)
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30 seconds)", value=False)
    
    if auto_refresh:
        st.sidebar.info("Auto-refresh is enabled. Data updates every 30 seconds.")
    
    # Main content area
    col1, col2, col3, col4 = st.columns(4)
    
    # Load latest ticker data
    ticker_df = load_latest_ticker(client)
    
    if not ticker_df.empty:
        # Get current symbol data
        current_data = ticker_df[ticker_df['symbol'] == selected_symbol]
        if not current_data.empty:
            current = current_data.iloc[0]
            
            with col1:
                st.metric(
                    label="💰 Current Price",
                    value=f"${current['price']:,.2f}",
                    delta=f"{current['change_percent']:.2f}%"
                )
            
            with col2:
                st.metric(
                    label="📊 24h Volume",
                    value=f"${current['volume']:,.0f}"
                )
            
            with col3:
                high_low = load_klines(client, selected_symbol, selected_interval, 1)
                if not high_low.empty:
                    high = high_low['high'].max()
                    low = high_low['low'].min()
                    st.metric(
                        label="📈 24h High/Low",
                        value=f"${high:,.2f}",
                        delta=f"Low: ${low:,.2f}"
                    )
            
            with col4:
                # Calculate price change from open
                if not high_low.empty:
                    open_price = high_low.iloc[-1]['open']
                    change = ((current['price'] - open_price) / open_price) * 100
                    st.metric(
                        label="🔄 Change from Open",
                        value=f"${current['price'] - open_price:,.2f}",
                        delta=f"{change:.2f}%"
                    )
    
    # Candlestick Chart
    st.subheader("📈 Price Chart")
    kline_df = load_klines(client, selected_symbol, selected_interval, days)
    
    if not kline_df.empty:
        fig1 = create_candlestick_chart(kline_df, selected_symbol, selected_interval)
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info(f"No kline data available for {selected_symbol} with {selected_interval} interval")
    
    # Recent Trades and Price Scatter
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🔄 Recent Trades with Price Action")
        trades_df = load_recent_trades(client, selected_symbol, 200)
        if not trades_df.empty and not kline_df.empty:
            fig2 = create_price_scatter(kline_df, trades_df)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No trade data available")
    
    with col2:
        st.subheader("📋 Recent Trades")
        if not trades_df.empty:
            st.dataframe(
                trades_df.head(20).style.format({
                    'price': '${:,.2f}',
                    'qty': '{:.4f}'
                }),
                use_container_width=True,
                height=500
            )
        else:
            st.info("No recent trades")
    
    # Market Overview
    st.subheader("🏦 Market Overview")
    
    if not ticker_df.empty:
        # Prepare market data
        market_df = ticker_df.copy()
        market_df['price'] = market_df['price'].apply(lambda x: f"${x:,.2f}")
        market_df['change_percent'] = market_df['change_percent'].apply(lambda x: f"{x:.2f}%")
        market_df['volume'] = market_df['volume'].apply(lambda x: f"${x:,.0f}")
        market_df['event_time'] = pd.to_datetime(market_df['event_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        st.dataframe(
            market_df[['symbol', 'price', 'change_percent', 'volume', 'event_time']],
            use_container_width=True,
            hide_index=True
        )
    
    # Statistics Section
    st.subheader("📊 Statistics")
    
    if not kline_df.empty and not trades_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_price = kline_df['close'].mean()
            volatility = kline_df['close'].pct_change().std() * np.sqrt(252) * 100
            st.metric("Average Price", f"${avg_price:,.2f}")
            st.metric("Annualized Volatility", f"{volatility:.2f}%")
        
        with col2:
            total_volume = trades_df['qty'].sum()
            avg_trade_size = trades_df['qty'].mean()
            st.metric("Total Trading Volume", f"{total_volume:,.2f}")
            st.metric("Average Trade Size", f"{avg_trade_size:.4f}")
        
        with col3:
            max_price = kline_df['high'].max()
            min_price = kline_df['low'].min()
            price_range = max_price - min_price
            st.metric("Price Range", f"${min_price:,.2f} - ${max_price:,.2f}")
            st.metric("Range Width", f"${price_range:,.2f}")
    
    # Auto-refresh logic
    if auto_refresh:
        import time
        time.sleep(30)
        st.rerun()

if __name__ == "__main__":
    main()