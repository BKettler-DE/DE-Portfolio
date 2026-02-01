"""
Data Warehouse Analytics Dashboard

Interactive dashboard showcasing data from the warehouse.
Demonstrates end-to-end data engineering: ingestion → transformation → visualization.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# PAGE CONFIGURATION
st.set_page_config(
    page_title="Data Warehouse Analytics",
    page_icon="📊",
    layout="wide"
)

# DATABASE CONNECTION
@st.cache_resource
def get_database_connection():
    """Connect to DuckDB warehouse"""
    return duckdb.connect('warehouse.duckdb', read_only=True)

def main():
    st.title("📊 Data Warehouse Analytics Dashboard")
    st.markdown("**Real-time analytics from your production data warehouse**")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select View",
        ["🏠 Overview", "🌡️ Sensor Analytics", "📦 Product Analytics", "✅ Data Quality"]
    )
    
    conn = get_database_connection()
    
    if page == "🏠 Overview":
        show_overview(conn)
    elif page == "🌡️ Sensor Analytics":
        show_sensor_analytics(conn)
    elif page == "📦 Product Analytics":
        show_product_analytics(conn)
    elif page == "✅ Data Quality":
        show_data_quality(conn)

def show_overview(conn):
    """Overview dashboard with key metrics"""
    st.header("System Overview")
    
    # KEY METRICS ROW
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_sensors = conn.execute("""
            SELECT COUNT(DISTINCT sensor_id) 
            FROM main_marts.fct_sensor_readings
        """).fetchone()[0]
        st.metric("Active Sensors", f"{total_sensors:,}")
    
    with col2:
        total_readings = conn.execute("""
            SELECT COUNT(*) 
            FROM main_marts.fct_sensor_readings
        """).fetchone()[0]
        st.metric("Total Readings", f"{total_readings:,}")
    
    with col3:
        total_products = conn.execute("""
            SELECT COUNT(*) 
            FROM main_marts.dim_products
        """).fetchone()[0]
        st.metric("Products", f"{total_products:,}")
    
    with col4:
        data_quality = conn.execute("""
            SELECT 
                ROUND(100.0 * SUM(CASE WHEN is_valid_reading THEN 1 ELSE 0 END) / COUNT(*), 1)
            FROM main_marts.fct_sensor_readings
        """).fetchone()[0]
        st.metric("Data Quality", f"{data_quality}%")
    
    st.divider()
    
    # RECENT ACTIVITY
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Latest Sensor Readings")
        latest_readings = conn.execute("""
            SELECT 
                sensor_id,
                reading_timestamp,
                temperature,
                humidity,
                temperature_category
            FROM main_marts.fct_sensor_readings
            ORDER BY reading_timestamp DESC
            LIMIT 10
        """).df()
        st.dataframe(latest_readings, width='stretch', hide_index=True)
    
    with col2:
        st.subheader("Temperature Distribution")
        temp_dist = conn.execute("""
            SELECT 
                temperature_category,
                COUNT(*) as count
            FROM main_marts.fct_sensor_readings
            GROUP BY temperature_category
            ORDER BY 
                CASE temperature_category
                    WHEN 'freezing' THEN 1
                    WHEN 'cold' THEN 2
                    WHEN 'comfortable' THEN 3
                    WHEN 'warm' THEN 4
                    WHEN 'hot' THEN 5
                END
        """).df()
        
        fig = px.bar(
            temp_dist,
            x='temperature_category',
            y='count',
            title='Readings by Temperature Category',
            labels={'temperature_category': 'Category', 'count': 'Count'}
        )
        st.plotly_chart(fig, width='stretch')

def show_sensor_analytics(conn):
    """Sensor analytics dashboard"""
    st.header("🌡️ Sensor Analytics")
    
    # SENSOR SELECTOR
    sensors = conn.execute("""
        SELECT DISTINCT sensor_id 
        FROM main_marts.fct_sensor_readings 
        ORDER BY sensor_id
    """).df()['sensor_id'].tolist()
    
    selected_sensor = st.selectbox("Select Sensor", ["All Sensors"] + sensors)
    
    # METRICS ROW
    if selected_sensor == "All Sensors":
        where_clause = ""
    else:
        where_clause = f"WHERE sensor_id = '{selected_sensor}'"
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        reading_count = conn.execute(f"""
            SELECT COUNT(*) 
            FROM main_marts.fct_sensor_readings 
            {where_clause}
        """).fetchone()[0]
        st.metric("Total Readings", f"{reading_count:,}")
    
    with col2:
        avg_temp = conn.execute(f"""
            SELECT ROUND(AVG(temperature), 1)
            FROM main_marts.fct_sensor_readings 
            {where_clause}
        """).fetchone()[0]
        st.metric("Avg Temperature", f"{avg_temp}°C" if avg_temp else "N/A")
    
    with col3:
        avg_humidity = conn.execute(f"""
            SELECT ROUND(AVG(humidity), 1)
            FROM main_marts.fct_sensor_readings 
            {where_clause}
        """).fetchone()[0]
        st.metric("Avg Humidity", f"{avg_humidity}%" if avg_humidity else "N/A")
    
    with col4:
        anomaly_count = conn.execute(f"""
            SELECT COUNT(*) 
            FROM (select * from main_marts.fct_sensor_readings where is_anomaly = TRUE) a 
            {where_clause}
        """).fetchone()[0]
        st.metric("Anomalies", f"{anomaly_count:,}")
    
    st.divider()
    
    # TIME SERIES CHART
    st.subheader("Temperature Over Time")
    
    time_series = conn.execute(f"""
        SELECT 
            reading_timestamp,
            temperature,
            humidity,
            sensor_id
        FROM main_marts.fct_sensor_readings
        {where_clause}
        ORDER BY reading_timestamp
    """).df()
    
    if not time_series.empty:
        fig = px.line(
            time_series,
            x='reading_timestamp',
            y='temperature',
            color='sensor_id' if selected_sensor == "All Sensors" else None,
            title='Temperature Readings',
            labels={'reading_timestamp': 'Time', 'temperature': 'Temperature (°C)'}
        )
        st.plotly_chart(fig, width='stretch')
    
    # HOURLY PATTERNS
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Hourly Temperature Pattern")
        hourly_temp = conn.execute(f"""
            SELECT 
                reading_hour,
                ROUND(AVG(temperature), 1) as avg_temp
            FROM main_marts.fct_sensor_readings
            {where_clause}
            GROUP BY reading_hour
            ORDER BY reading_hour
        """).df()
        
        if not hourly_temp.empty:
            fig = px.line(
                hourly_temp,
                x='reading_hour',
                y='avg_temp',
                title='Average Temperature by Hour',
                labels={'reading_hour': 'Hour of Day', 'avg_temp': 'Avg Temp (°C)'}
            )
            st.plotly_chart(fig, width='stretch')
    
    with col2:
        st.subheader("Location Distribution")
        location_dist = conn.execute(f"""
            SELECT 
                location,
                COUNT(*) as count
            FROM main_marts.fct_sensor_readings
            {where_clause}
            GROUP BY location
            ORDER BY count DESC
        """).df()
        
        if not location_dist.empty:
            fig = px.pie(
                location_dist,
                values='count',
                names='location',
                title='Readings by Location'
            )
            st.plotly_chart(fig, width='stretch')

def show_product_analytics(conn):
    """Product analytics dashboard"""
    st.header("📦 Product Analytics")
    
    # METRICS ROW
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_products = conn.execute("""
            SELECT COUNT(*) 
            FROM main_marts.dim_products
        """).fetchone()[0]
        st.metric("Total Products", f"{total_products:,}")
    
    with col2:
        avg_price = conn.execute("""
            SELECT ROUND(AVG(price), 2)
            FROM main_marts.dim_products
        """).fetchone()[0]
        st.metric("Avg Price", f"${avg_price:,.2f}")
    
    with col3:
        total_stock = conn.execute("""
            SELECT SUM(stock_quantity)
            FROM main_marts.dim_products
        """).fetchone()[0]
        st.metric("Total Stock", f"{total_stock:,}")
    
    with col4:
        low_stock_count = conn.execute("""
            SELECT COUNT(*)
            FROM main_marts.dim_products
            WHERE stock_status IN ('low_stock', 'out_of_stock')
        """).fetchone()[0]
        st.metric("Low/Out of Stock", f"{low_stock_count:,}")
    
    st.divider()
    
    # PRICE TIER ANALYSIS
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Products by Price Tier")
        price_tier = conn.execute("""
            SELECT 
                price_tier,
                COUNT(*) as count,
                ROUND(AVG(price), 2) as avg_price
            FROM main_marts.dim_products
            GROUP BY price_tier
            ORDER BY 
                CASE price_tier
                    WHEN 'budget' THEN 1
                    WHEN 'mid-range' THEN 2
                    WHEN 'premium' THEN 3
                    WHEN 'luxury' THEN 4
                END
        """).df()
        
        fig = px.bar(
            price_tier,
            x='price_tier',
            y='count',
            title='Product Distribution by Price Tier',
            labels={'price_tier': 'Price Tier', 'count': 'Product Count'}
        )
        st.plotly_chart(fig, width='stretch')
    
    with col2:
        st.subheader("Stock Status")
        stock_status = conn.execute("""
            SELECT 
                stock_status,
                COUNT(*) as count
            FROM main_marts.dim_products
            GROUP BY stock_status
            ORDER BY 
                CASE stock_status
                    WHEN 'out_of_stock' THEN 1
                    WHEN 'low_stock' THEN 2
                    WHEN 'normal_stock' THEN 3
                    WHEN 'high_stock' THEN 4
                END
        """).df()
        
        fig = px.pie(
            stock_status,
            values='count',
            names='stock_status',
            title='Products by Stock Status'
        )
        st.plotly_chart(fig, width='stretch')
    
    # CATEGORY ANALYSIS
    st.subheader("Category Breakdown")
    category_data = conn.execute("""
        SELECT 
            category,
            COUNT(*) as product_count,
            ROUND(AVG(price), 2) as avg_price,
            SUM(stock_quantity) as total_stock
        FROM main_marts.dim_products
        GROUP BY category
        ORDER BY product_count DESC
    """).df()
    
    st.dataframe(category_data, width='stretch', hide_index=True)

def show_data_quality(conn):
    """Data quality monitoring dashboard"""
    st.header("✅ Data Quality Monitoring")
    
    # OVERALL QUALITY METRICS
    col1, col2, col3, col4 = st.columns(4)
    
    quality_stats = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN is_valid_reading THEN 1 ELSE 0 END) as valid,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies,
            SUM(CASE WHEN has_missing_data THEN 1 ELSE 0 END) as missing
        FROM main_marts.fct_sensor_readings
    """).fetchone()
    
    total, valid, anomalies, missing = quality_stats
    
    with col1:
        st.metric("Total Readings", f"{total:,}")
    
    with col2:
        quality_pct = round(100.0 * valid / total, 1) if total > 0 else 0
        st.metric("Valid Readings", f"{quality_pct}%")
    
    with col3:
        st.metric("Anomalies", f"{anomalies:,}")
    
    with col4:
        st.metric("Missing Data", f"{missing:,}")
    
    st.divider()
    
    # DAILY QUALITY TREND
    st.subheader("Daily Data Quality Trend")
    
    daily_quality = conn.execute("""
        SELECT 
            reading_date,
            ROUND(100 * SUM(CASE WHEN is_valid_reading THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_pct
        FROM main_marts.fct_sensor_readings
        GROUP BY reading_date
        ORDER BY reading_date
    """).df()
    
    if not daily_quality.empty:
        fig = px.line(
            daily_quality,
            x='reading_date',
            y='quality_pct',
            title='Data Quality Percentage by Day',
            labels={'reading_date': 'Date', 'quality_pct': 'Quality (%)'}
        )
        fig.add_hline(y=90, line_dash="dash", line_color="green", annotation_text="Target: 95%")
        st.plotly_chart(fig, width='stretch')
    
    # ANOMALY BREAKDOWN
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Anomalies by Sensor")
        anomaly_by_sensor = conn.execute("""
            SELECT 
                sensor_id,
                COUNT(*) as anomaly_count
            FROM main_marts.fct_sensor_readings
            WHERE is_anomaly = TRUE
            GROUP BY sensor_id
            ORDER BY anomaly_count DESC
        """).df()
        
        if not anomaly_by_sensor.empty:
            fig = px.bar(
                anomaly_by_sensor,
                x='sensor_id',
                y='anomaly_count',
                title='Anomalies by Sensor',
                labels={'sensor_id': 'Sensor', 'anomaly_count': 'Anomaly Count'}
            )
            st.plotly_chart(fig, width='stretch')
        else:
            st.success("✅ No anomalies detected!")
    
    with col2:
        st.subheader("Data Completeness")
        completeness = conn.execute("""
            SELECT 
                CASE 
                    WHEN has_missing_data THEN 'Incomplete'
                    ELSE 'Complete'
                END as completeness,
                COUNT(*) as count
            FROM main_marts.fct_sensor_readings
            GROUP BY 
                CASE 
                    WHEN has_missing_data THEN 'Incomplete'
                    ELSE 'Complete'
                END
        """).df()
        
        if not completeness.empty:
            fig = px.pie(
                completeness,
                values='count',
                names='completeness',
                title='Data Completeness'
            )
            st.plotly_chart(fig, width='stretch')

if __name__ == "__main__":
    main()