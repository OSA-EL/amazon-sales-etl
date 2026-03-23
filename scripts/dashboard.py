import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Connection to your Postgres
engine = create_engine('postgresql://admin:password123@localhost:5432/amazon_sales')

st.title("Amazon Sales Insights Dashboard")

# Load data from the table we created
df = pd.read_sql("SELECT * FROM analytics_amazon_sales", engine)

# Simple Metric
st.metric("Total Products", len(df))

# Chart 1: Top Categories by Discount
st.subheader("Top Categories by Discount %")
cat_discount = df.groupby('category')['discount_percentage'].mean().sort_values(ascending=False).head(10)
st.bar_chart(cat_discount)

# Chart 2: Rating vs Price
st.subheader("Relation between Price and Rating")
st.scatter_chart(df[['actual_price', 'rating']])