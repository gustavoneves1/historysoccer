import pandas as pd
import sqlite3
import streamlit as st

@st.cache_data
def load_data(limit, offset):
    conn = sqlite3.connect("football.db")
    query = f"SELECT * FROM games LIMIT {limit} OFFSET {offset}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Define the number of rows per page
rows_per_page = 1000

# Get the total number of rows in the table
conn = sqlite3.connect("football.db")
total_rows = pd.read_sql("SELECT COUNT(*) FROM games", conn).iloc[0, 0]
conn.close()

# Calculate the total number of pages
total_pages = (total_rows // rows_per_page) + 1

# Create a selectbox for the page number
page_number = st.sidebar.selectbox('Page number', range(1, total_pages + 1))

# Calculate the offset for the SQL query
offset = (page_number - 1) * rows_per_page

# Load the data for the selected page
df = load_data(rows_per_page, offset)

# Display the data
st.write(f"Displaying page {page_number} of {total_pages}")
st.table(df)
