import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
import requests

# Configuration de la page
st.set_page_config(page_title="Taxi Dashboard", layout="wide")

st.title("🚖 NYC Taxi Trip Dashboard")

# Connexion à la base de données
# J'utilise le hostname Docker 'postgres' car l'app tourne dans un conteneur.
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'password')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'warehouse')

@st.cache_resource
def get_connection():
    return create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

try:
    engine = get_connection()
    with engine.connect() as conn:
        st.success("Connected to Data Warehouse")
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    st.info("Ensure you are running this with docker-compose so it can reach the 'postgres' container.")
    st.stop()

# --- Barre latérale : Prédiction ---
st.sidebar.header("🤖 Fare Predictor (ML)")
st.sidebar.markdown("Estimation du prix via mon service ML.")

with st.sidebar.form("prediction_form"):
    distance = st.sidebar.number_input("Trip Distance (miles)", min_value=0.1, max_value=100.0, value=2.0)
    pu_id = st.sidebar.number_input("Pickup Location ID", min_value=1, max_value=265, value=132)
    do_id = st.sidebar.number_input("Dropoff Location ID", min_value=1, max_value=265, value=138)
    passengers = st.sidebar.slider("Passengers", 1, 6, 1)
    
    predict_btn = st.form_submit_button("Predict Fare")

if predict_btn:
    payload = {
        "trip_distance": distance,
        "pulocation_id": int(pu_id),
        "dolocation_id": int(do_id),
        "passenger_count": passengers
    }
    
    try:
        # ML Service running in docker-compose as "ml-prediction"
        response = requests.post("http://ml-prediction:8000/predict", json=payload)
        response.raise_for_status()
        prediction = response.json()
        fare = prediction.get("predicted_fare", 0)
        st.sidebar.success(f"Estimated Fare: **${fare:.2f}**")
    except Exception as e:
        st.sidebar.error(f"Prediction Service Error: {e}")


# --- Chargement des données ---

@st.cache_data(ttl=600)
def load_data():
    queries = {
        "metrics": "SELECT COUNT(*) as total_trips, AVG(fare_amount) as avg_fare, SUM(total_amount) as total_revenue FROM fact_trips",
        "boroughs": """
            SELECT l.borough, COUNT(f.trip_id) as trip_count 
            FROM fact_trips f 
            JOIN dim_location l ON f.pulocation_id = l.location_id 
            WHERE l.borough IS NOT NULL 
            GROUP BY l.borough 
            ORDER BY trip_count DESC
        """,
        "payments": """
            SELECT p.payment_type_name, AVG(f.fare_amount) as avg_fare 
            FROM fact_trips f 
            JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id 
            GROUP BY p.payment_type_name
        """,
        "distance": "SELECT trip_distance FROM fact_trips WHERE trip_distance < 50 AND trip_distance > 0 LIMIT 10000",
        "timeline": """
            SELECT DATE(tpep_pickup_datetime) as trip_date, COUNT(*) as trip_count 
            FROM fact_trips 
            WHERE tpep_pickup_datetime IS NOT NULL 
            GROUP BY DATE(tpep_pickup_datetime) 
            ORDER BY trip_date
        """
    }
    
    data = {}
    with engine.connect() as conn:
        for key, sql in queries.items():
            data[key] = pd.read_sql(sql, conn)
            
    return data

data = load_data()

# --- Indicateurs Clés ---
col1, col2, col3 = st.columns(3)
col1.metric("Total Trips", f"{data['metrics'].iloc[0]['total_trips']:,}")
col2.metric("Average Fare", f"${data['metrics'].iloc[0]['avg_fare']:.2f}")
col3.metric("Total Revenue", f"${data['metrics'].iloc[0]['total_revenue']:,.2f}")

st.markdown("---")

# --- CHARTS ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Trips by Borough")
    fig_boro = px.bar(
        data['boroughs'], 
        x='borough', 
        y='trip_count',
        color='trip_count',
        labels={'borough': 'Borough', 'trip_count': 'Trips'},
        template="plotly_dark"
    )
    st.plotly_chart(fig_boro, use_container_width=True)

    st.subheader("Trip Distance Distribution (Sample)")
    fig_dist = px.histogram(
        data['distance'], 
        x='trip_distance', 
        nbins=50,
        labels={'trip_distance': 'Distance (miles)'},
        template="plotly_dark",
        title="Distances < 50 miles"
    )
    st.plotly_chart(fig_dist, use_container_width=True)

with col_right:
    st.subheader("Avg Fare by Payment Type")
    fig_pay = px.bar(
        data['payments'], 
        x='payment_type_name', 
        y='avg_fare',
        color='avg_fare',
        labels={'payment_type_name': 'Payment Type', 'avg_fare': 'Average Fare ($)'},
        template="plotly_dark"
    )
    st.plotly_chart(fig_pay, use_container_width=True)

    st.subheader("Trips Over Time")
    fig_time = px.line(
        data['timeline'], 
        x='trip_date', 
        y='trip_count',
        template="plotly_dark"
    )
    st.plotly_chart(fig_time, use_container_width=True)

# --- REFRESH BUTTON ---
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()
