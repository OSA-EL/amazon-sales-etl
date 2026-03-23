import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# --- Database Connection ---
engine = create_engine('postgresql://admin:password123@localhost:5432/amazon_sales')

# --- Page Config ---
st.set_page_config(page_title="Amazon Data Insights", layout="wide")
st.title("🛒 Amazon Sales Analytics Dashboard")
st.markdown("---")

# --- Load Data ---
@st.cache_data 
def get_data():
    return pd.read_sql("SELECT * FROM analytics_amazon_sales", engine)

df = get_data()

# --- Top Metrics Row ---
col1, col2, col3 = st.columns(3)
col1.metric("Total Unique Products", len(df))
col2.metric("Avg Discount %", f"{round(df['discount_percentage'].mean() * 100, 2)}%")
col3.metric("Avg Rating", f"{round(df['rating'].mean(), 2)} ")

st.markdown("---")

# --- First Row of Charts ---
row1_col1, row1_col2 = st.columns(2)

with row1_col1:
    st.subheader("1. Rating Distribution")
    # Histogram 
    fig_rating = px.histogram(df, x="rating", nbins=20, 
                              title="How customers rate products",
                              color_discrete_sequence=['#ff9900'])
    st.plotly_chart(fig_rating, use_container_width=True)

with row1_col2:
    st.subheader("2. Top 10 Categories by Volume")
    # Bar Chart For Top Categories (assuming 'category' column has pipe-separated categories)
    cat_counts = df['category'].str.split('|').str[0].value_counts().head(10)
    fig_cat = px.bar(cat_counts, x=cat_counts.index, y=cat_counts.values, 
                     labels={'y': 'Number of Products', 'index': 'Category'},
                     color=cat_counts.values)
    st.plotly_chart(fig_cat, use_container_width=True)

# --- Second Row of Charts ---
st.markdown("---")
row2_col1, row2_col2 = st.columns(2)

with row2_col1:
    st.subheader("3. Price vs Discounted Price")
    # Scatter plot For Price vs Discounted Price with size by discount percentage and color by rating
    fig_price = px.scatter(df, x="actual_price", y="discounted_price", 
                           size="discount_percentage", color="rating",
                           hover_name="product_name", title="Pricing Gap Analysis")
    st.plotly_chart(fig_price, use_container_width=True)

with row2_col2:
    st.subheader("4. Best Deals (High Rating & High Discount)")
    # Filtering for products with rating >= 4 and discount > 50%
    best_deals = df[(df['rating'] >= 4) & (df['discount_percentage'] > 0.5)].sort_values(by='discount_percentage', ascending=False).head(10)
    st.write(best_deals[['product_name', 'discount_percentage', 'rating']])

# --- Footer ---
st.info(" Pro Tip: Use the sidebar or charts to filter and find insights about specific products.")