import pandas as pd
import yfinance as yf
import streamlit as st
import duckdb
import time

# ------------------------------
# Load tickers
tickers_df = pd.read_csv("tickers.csv")
tickers = tickers_df['ticker'].tolist()

# ------------------------------
# Streamlit layout
st.set_page_config(page_title="Real-Time Stock Dashboard", layout="wide")
st.title("📈 Real-Time Stock Market Dashboard (NSE + US)")

# ------------------------------
# Connect DuckDB (local lightweight DB)
conn = duckdb.connect(database='stocks.duckdb')

# ------------------------------
# Function to fetch latest price data
def fetch_prices(tickers):
    data = yf.download(tickers=tickers, period="1d", interval="1m", progress=False)
    
    # MultiIndex for multiple tickers
    if isinstance(data.columns, pd.MultiIndex):
        data = data.stack(level=1).reset_index()
        data.rename(columns={'level_1': 'Ticker', 'Datetime': 'Datetime'}, inplace=True)
    else:
        data = data.reset_index()
        data['Ticker'] = tickers[0]
        data.rename(columns={'Date': 'Datetime'}, inplace=True)
    
    # Ensure Datetime exists
    if 'Datetime' not in data.columns:
        data['Datetime'] = pd.to_datetime(data.index)
    
    return data

# ------------------------------
# Function to calculate moving averages
def add_moving_averages(df):
    df = df.sort_values(["Ticker", "Datetime"])
    df['MA20'] = df.groupby("Ticker")['Close'].transform(lambda x: x.rolling(20, min_periods=1).mean())
    df['MA50'] = df.groupby("Ticker")['Close'].transform(lambda x: x.rolling(50, min_periods=1).mean())
    return df

# ------------------------------
# Streamlit placeholder
placeholder = st.empty()
update_interval = 30  # seconds

# ------------------------------
# Main loop
while True:
    try:
        # Fetch latest prices
        df = fetch_prices(tickers)
        df = add_moving_averages(df)
        
        # Save to DuckDB
        conn.execute("CREATE TABLE IF NOT EXISTS stock_prices AS SELECT * FROM df")
        conn.execute("INSERT INTO stock_prices SELECT * FROM df")
        
        # Display top 20 stocks by latest Close price
        top_stocks = df.groupby("Ticker").tail(1).sort_values("Close", ascending=False).head(20)
        top_stocks_display = top_stocks[['Ticker','Close','MA20','MA50']]
        
        with placeholder.container():
            st.subheader("Top 20 Stocks by Latest Price")
            st.dataframe(top_stocks_display)
            
            # Simple alert: Price above MA50
            alert_stocks = top_stocks[top_stocks['Close'] > top_stocks['MA50']]
            if not alert_stocks.empty:
                st.warning("⚠️ Stocks above MA50: " + ", ".join(alert_stocks['Ticker'].tolist()))
        
        # Sleep for next update
        time.sleep(update_interval)
    
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        time.sleep(update_interval)
