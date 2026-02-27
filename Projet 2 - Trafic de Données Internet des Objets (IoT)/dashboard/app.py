"""
Dashboard - Surveillance Environnementale IoT
---------------------------------------------
Visualisation temps réel, tendances et alertes.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATASET_CSV, DATA_PROCESSED, THRESHOLDS
from etl.extract import extract_from_csv
from etl.transform import transform

# Configuration de la page
st.set_page_config(
    page_title="Surveillance Environnementale IoT",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Style cohérent
st.markdown("""
<style>
    .main-header { font-size: 2rem; font-weight: 700; color: #1f77b4; margin-bottom: 1rem; }
    .metric-card { background: #f0f2f6; padding: 1rem; border-radius: 8px; text-align: center; }
    .alert-critical { color: #d32f2f; font-weight: bold; }
    .alert-warning { color: #f57c00; font-weight: bold; }
    .stMetric { background: #fafafa; padding: 0.5rem; border-radius: 6px; }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-header">🌍 Surveillance Environnementale en Temps Réel</p>', unsafe_allow_html=True)
st.caption("Pipeline ETL | Data Lake | Apache Kafka | Apache Airflow")

# Chargement des données
@st.cache_data
def load_data():
    df = extract_from_csv()
    return transform(df)

df = load_data()

# Sidebar - Filtres
st.sidebar.header("Filtres")
zones = sorted(df["Location"].unique().tolist())
selected_zones = st.sidebar.multiselect("Zones", zones, default=zones[:3] if len(zones) > 3 else zones)
date_range = st.sidebar.date_input("Période", value=(df["date"].min(), df["date"].max()))

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_d, end_d = date_range
else:
    start_d = end_d = date_range

mask = (df["Location"].isin(selected_zones)) & (df["date"] >= start_d) & (df["date"] <= end_d)
df_filtered = df[mask]

# KPI - Métriques principales
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Enregistrements", f"{len(df_filtered):,}", f"{len(df_filtered) - len(df)} vs total")
with col2:
    st.metric("Temp. moyenne", f"{df_filtered['Temperature'].mean():.1f}°C", f"Seuil: {THRESHOLDS['temperature']['min']}–{THRESHOLDS['temperature']['max']}°C")
with col3:
    st.metric("Humidité moyenne", f"{df_filtered['Humidity'].mean():.1f}%", f"Seuil: {THRESHOLDS['humidity']['min']}–{THRESHOLDS['humidity']['max']}%")
with col4:
    alert_count = int(df_filtered["has_alert"].sum())
    st.metric("Alertes critiques", alert_count, "Seuils dépassés" if alert_count > 0 else "Aucune")
with col5:
    pollution_crit = int(df_filtered["pollution_critical"].sum())
    st.metric("Pollution critique", pollution_crit, f"Niveau ≥ {THRESHOLDS['pollution']['critical']}")

# Section Alertes
st.subheader("⚠️ Alertes sur les seuils critiques")
alerts = df_filtered[df_filtered["has_alert"]]
if len(alerts) > 0:
    st.markdown(f'<p class="alert-critical">🔴 {len(alerts)} événements hors seuils détectés</p>', unsafe_allow_html=True)
    alert_sample = alerts[["Timestamp", "Location", "Temperature", "Humidity", "Pollution_Level", "temp_alert", "humidity_alert", "pollution_critical"]].head(20)
    st.dataframe(alert_sample, use_container_width=True, hide_index=True)
else:
    st.success("✅ Aucune alerte critique sur la période sélectionnée.")

# Graphiques
st.subheader("📊 Tendances et visualisations")

tab1, tab2, tab3, tab4 = st.tabs(["Température", "Humidité", "Pollution", "Comparaison par zone"])

with tab1:
    fig_temp = px.line(df_filtered, x="Timestamp", y="Temperature", color="Location",
                       title="Évolution de la température par zone")
    fig_temp.add_hline(y=THRESHOLDS["temperature"]["max"], line_dash="dash", line_color="red", annotation_text="Seuil max")
    fig_temp.add_hline(y=THRESHOLDS["temperature"]["min"], line_dash="dash", line_color="blue", annotation_text="Seuil min")
    fig_temp.update_layout(height=400)
    st.plotly_chart(fig_temp, use_container_width=True)

with tab2:
    fig_hum = px.line(df_filtered, x="Timestamp", y="Humidity", color="Location",
                      title="Évolution de l'humidité par zone")
    fig_hum.add_hline(y=THRESHOLDS["humidity"]["max"], line_dash="dash", line_color="red", annotation_text="Seuil max")
    fig_hum.add_hline(y=THRESHOLDS["humidity"]["min"], line_dash="dash", line_color="blue", annotation_text="Seuil min")
    fig_hum.update_layout(height=400)
    st.plotly_chart(fig_hum, use_container_width=True)

with tab3:
    fig_pol = px.bar(df_filtered.groupby("Location")["Pollution_Level"].mean().reset_index(),
                     x="Location", y="Pollution_Level", color="Pollution_Level",
                     title="Niveau de pollution moyen par zone",
                     color_continuous_scale="YlOrRd")
    fig_pol.add_hline(y=THRESHOLDS["pollution"]["critical"], line_dash="dash", line_color="red", annotation_text="Critique")
    fig_pol.add_hline(y=THRESHOLDS["pollution"]["warning"], line_dash="dot", line_color="orange", annotation_text="Avertissement")
    fig_pol.update_layout(height=400)
    st.plotly_chart(fig_pol, use_container_width=True)

with tab4:
    agg = df_filtered.groupby("Location").agg({
        "Temperature": "mean",
        "Humidity": "mean",
        "Pollution_Level": "mean",
    }).reset_index()
    fig_radar = go.Figure()
    for _, row in agg.iterrows():
        fig_radar.add_trace(go.Scatterpolar(
            r=[row["Temperature"], row["Humidity"], row["Pollution_Level"] * 10],
            theta=["Température", "Humidity", "Pollution (x10)"],
            name=row["Location"],
            fill="toself",
        ))
    fig_radar.update_layout(polar=dict(radialaxis=dict(visible=True)), height=450, title="Comparaison par zone")
    st.plotly_chart(fig_radar, use_container_width=True)

# Heatmap - Distribution temporelle
st.subheader("🗺️ Heatmap - Température par heure et par zone")
pivot_temp = df_filtered.pivot_table(values="Temperature", index="Location", columns="hour", aggfunc="mean")
fig_heat = px.imshow(pivot_temp, title="Température moyenne (°C) - Heure x Zone",
                     color_continuous_scale="RdYlBu_r", aspect="auto")
st.plotly_chart(fig_heat, use_container_width=True)

# Call to Action
st.divider()
st.markdown("""
**📬 Souhaitez-vous en savoir plus ?**

Ce projet démontre un pipeline complet de données IoT : ETL, Data Lake, Kafka et Airflow.  
Je serais ravi d'échanger sur l'architecture, les choix techniques ou les évolutions possibles.

*👉 N'hésitez pas à me contacter pour discuter de ce projet ou d'autres cas d'usage Data Engineering.*
""")
