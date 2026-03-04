import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

# Database Connection
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "earthquakes")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

@st.cache_resource
def get_engine():
    url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(url)

@st.cache_data(ttl=600)
def load_data(query: str):
    engine = get_engine()
    return pd.read_sql(query, engine)

def main():
    st.set_page_config(page_title="Earthquake Analytics", page_icon="🌍", layout="wide")
    st.title("🌍 Global Earthquake Analytics Dashboard")
    st.markdown("This dashboard displays recent earthquake data extracted from USGS and transformed via dbt.")
    
    # Check if table exists before querying
    try:
        # Load core fact table
        fact_query = """
        SELECT 
            f.earthquake_id, f.magnitude, f.earthquake_time, 
            f.longitude, f.latitude, f.depth_km, f.magnitude_type, 
            l.place, l.region
        FROM fact_earthquake f
        JOIN dim_location l ON f.location_id = l.location_id
        """
        df = load_data(fact_query)
    except Exception as e:
        st.error(f"Could not load data from database. Has the pipeline run? Details: {e}")
        return

    # Sidebar filters
    st.sidebar.header("Filters")
    min_mag = st.sidebar.slider("Minimum Magnitude", min_value=-1.0, max_value=10.0, value=2.0, step=0.1)
    
    min_date = df["earthquake_time"].min().date()
    max_date = df["earthquake_time"].max().date()
    
    date_range = st.sidebar.date_input("Date Range", [min_date, max_date], min_value=min_date, max_value=max_date)
    
    # Filter Data
    filtered_df = df[df["magnitude"] >= min_mag]
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df["earthquake_time"].dt.date >= start_date) & 
            (filtered_df["earthquake_time"].dt.date <= end_date)
        ]

    # KPI Cards
    st.markdown("### Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)
    
    total_events = len(filtered_df)
    max_mag = filtered_df["magnitude"].max() if total_events > 0 else 0
    avg_depth = filtered_df["depth_km"].mean() if total_events > 0 else 0
    significant_events = len(filtered_df[filtered_df["magnitude"] >= 5.0])
    
    col1.metric("Total Earthquakes", f"{total_events:,}")
    col2.metric("Max Magnitude", f"{max_mag:.1f}")
    col3.metric("Avg Depth (km)", f"{avg_depth:.1f}")
    col4.metric("Significant Events (Mag >= 5.0)", f"{significant_events:,}")
    
    st.markdown("---")
    
    # Charts Row
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.markdown("### Time Series of Earthquakes (Count by Day)")
        if not filtered_df.empty:
            daily_counts = filtered_df.groupby(filtered_df["earthquake_time"].dt.date).size().reset_index(name="count")
            fig_time = px.line(daily_counts, x="earthquake_time", y="count", markers=True)
            st.plotly_chart(fig_time, use_container_width=True)
        else:
            st.info("No data available for the selected filters.")

    with chart_col2:
        st.markdown("### Top Regions by Earthquake Count")
        if not filtered_df.empty:
            top_regions = filtered_df["region"].value_counts().head(10).reset_index()
            top_regions.columns = ["Region", "Count"]
            fig_bar = px.bar(top_regions, x="Count", y="Region", orientation="h")
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No data available for the selected filters.")
            
    # Map
    st.markdown("### Map of Events")
    if not filtered_df.empty:
        # Avoid maps crashing or throwing warnings when some magnitude coordinates might be NaN
        map_df = filtered_df.dropna(subset=['latitude', 'longitude', 'magnitude'])
        
        # Ensure magnitude is positive for size parameter
        map_df['plot_size'] = map_df['magnitude'].apply(lambda x: max(0.1, x))
        
        fig_map = px.scatter_mapbox(
            map_df, lat="latitude", lon="longitude", 
            color="magnitude", size="plot_size",
            hover_name="place", hover_data=["magnitude", "depth_km"],
            color_continuous_scale=px.colors.sequential.YlOrRd, size_max=15, zoom=1,
            mapbox_style="carto-positron"
        )
        st.plotly_chart(fig_map, use_container_width=True)

    # Data Table Pagination
    st.markdown("### Raw Data (Sample)")
    if not filtered_df.empty:
        st.dataframe(filtered_df.head(100).sort_values(by="earthquake_time", ascending=False), use_container_width=True)

if __name__ == "__main__":
    main()
