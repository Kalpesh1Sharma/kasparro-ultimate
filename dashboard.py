import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Kasparro Crypto Sentinel", layout="wide")

st.title("🚀 Kasparro Crypto Dashboard")

try:
    df = pd.read_csv('crypto_data.csv')
    
    # Check if required columns exist to avoid KeyErrors
    required_cols = ['price_usd', 'volume_24h', 'timestamp']
    if all(col in df.columns for col in required_cols):
        col1, col2 = st.columns(2)
        col1.metric("Latest Price (USD)", f"${df['price_usd'].iloc[-1]:,.2f}")
        col2.metric("24h Volume", f"${df['volume_24h'].iloc[-1]:,.0f}")

        st.subheader("Price History")
        fig = px.line(df, x='timestamp', y='price_usd', title="BTC Price USD")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.error(f"Missing columns. Found: {df.columns.tolist()}")
        
except FileNotFoundError:
    st.info("Waiting for ETL to generate initial data...")
