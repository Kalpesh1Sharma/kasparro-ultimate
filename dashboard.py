import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Kasparro Analytics", layout="wide")
st.title("🚀 Kasparro Crypto Sentinel")

try:
    df = pd.read_csv('crypto_data.csv')
    
    # Define and check for the required schema
    required_schema = ['price_usd', 'volume_24h', 'timestamp']
    if all(col in df.columns for col in required_schema):
        # Create interactive layout
        m1, m2 = st.columns(2)
        m1.metric("Latest Price (USD)", f"${df['price_usd'].iloc[-1]:,.2f}")
        m2.metric("24h Volume", f"${df['volume_24h'].iloc[-1]:,.0f}")

        st.subheader("Market Trend Analysis")
        fig = px.line(df, x='timestamp', y='price_usd', title="BTC Price USD")
        st.plotly_chart(fig, use_container_width=True)
    else:
        # Graceful error handling for missing data
        st.error(f"Schema Mismatch! Expected: {required_schema}. Found: {df.columns.tolist()}")
        st.info("Run 'python generate_csv.py' to reset your data structure.")
        
except FileNotFoundError:
    st.warning("Data source not found. Run the ingestion script to begin tracking.")