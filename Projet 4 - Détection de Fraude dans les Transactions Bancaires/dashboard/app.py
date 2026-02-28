"""
Tableau de bord interactif Streamlit — Détection de Fraude Bancaire.
Affiche en temps réel les alertes de fraude, les tendances et les KPIs.

Lancer :  streamlit run dashboard/app.py
"""
import os
import sys
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import FRAUD_RESULTS_PATH, RAW_DATA_PATH

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION PAGE
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

COLORS = {
    "CRITICAL": "#FF1744",
    "HIGH": "#FF9100",
    "MEDIUM": "#FFD600",
    "LOW": "#00E676",
    "bg_dark": "#0E1117",
    "card_bg": "#1E1E2E",
    "accent": "#6C63FF",
}

st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(90deg, #6C63FF, #FF6584);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0;
    }
    .sub-header {
        color: #888;
        font-size: 1rem;
        margin-top: -10px;
        margin-bottom: 20px;
    }
    .metric-card {
        background: #1E1E2E;
        border-radius: 12px;
        padding: 20px;
        border-left: 4px solid;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    }
    .alert-critical { border-left-color: #FF1744 !important; }
    .alert-high { border-left-color: #FF9100 !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 8px 20px;
    }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
#  CHARGEMENT DES DONNÉES
# ═══════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_data():
    """Charge les résultats de détection ou exécute le pipeline si nécessaire."""
    if os.path.exists(FRAUD_RESULTS_PATH):
        df = pd.read_csv(FRAUD_RESULTS_PATH, parse_dates=["Transaction_Time"])
        return df

    st.warning("Fichier de résultats non trouvé. Exécution du pipeline...")
    from src.data_preprocessing import run_preprocessing_pipeline
    from src.fraud_detection import run_fraud_detection
    run_preprocessing_pipeline()
    df = run_fraud_detection()
    return df


df = load_data()

if df is None or df.empty:
    st.error("Aucune donnée disponible. Veuillez exécuter le pipeline d'abord.")
    st.stop()


# ═══════════════════════════════════════════════════════════════════════════
#  SIDEBAR — FILTRES
# ═══════════════════════════════════════════════════════════════════════════

st.sidebar.markdown("## 🔍 Filtres")

risk_filter = st.sidebar.multiselect(
    "Niveau de risque",
    options=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
    default=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
)

type_filter = st.sidebar.multiselect(
    "Type de transaction",
    options=sorted(df["Transaction_Type"].unique()),
    default=sorted(df["Transaction_Type"].unique()),
)

location_filter = st.sidebar.multiselect(
    "Localisation",
    options=sorted(df["Location"].unique()),
    default=sorted(df["Location"].unique()),
)

amount_range = st.sidebar.slider(
    "Montant (€)",
    min_value=float(df["Amount"].min()),
    max_value=float(df["Amount"].max()),
    value=(float(df["Amount"].min()), float(df["Amount"].max())),
    step=50.0,
)

if "Transaction_Time" in df.columns and df["Transaction_Time"].notna().any():
    min_date = df["Transaction_Time"].min().date()
    max_date = df["Transaction_Time"].max().date()
    date_range = st.sidebar.date_input(
        "Période",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
else:
    date_range = None

# Application des filtres
filtered = df[
    (df["risk_level"].isin(risk_filter))
    & (df["Transaction_Type"].isin(type_filter))
    & (df["Location"].isin(location_filter))
    & (df["Amount"].between(amount_range[0], amount_range[1]))
].copy()

if date_range and len(date_range) == 2:
    filtered = filtered[
        (filtered["Transaction_Time"].dt.date >= date_range[0])
        & (filtered["Transaction_Time"].dt.date <= date_range[1])
    ]

st.sidebar.markdown("---")
st.sidebar.metric("Transactions filtrées", f"{len(filtered):,}")


# ═══════════════════════════════════════════════════════════════════════════
#  HEADER
# ═══════════════════════════════════════════════════════════════════════════

st.markdown('<p class="main-header">Détection de Fraude — Transactions Bancaires</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Tableau de bord temps réel • Pipeline Data Engineering</p>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
#  KPIs — LIGNE PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════

total = len(filtered)
suspected = int(filtered["is_suspected_fraud"].sum()) if "is_suspected_fraud" in filtered.columns else 0
critical = int((filtered["risk_level"] == "CRITICAL").sum()) if "risk_level" in filtered.columns else 0
actual_fraud = int((filtered["Is_Fraud"] == "YES").sum()) if "Is_Fraud" in filtered.columns else 0
avg_risk = filtered["risk_score"].mean() if "risk_score" in filtered.columns else 0

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Transactions", f"{total:,}")
with col2:
    st.metric("Suspectes", f"{suspected:,}", delta=f"{suspected/max(total,1)*100:.1f}%")
with col3:
    st.metric("Critiques", f"{critical:,}", delta=f"{critical/max(total,1)*100:.1f}%", delta_color="inverse")
with col4:
    st.metric("Fraudes Confirmées", f"{actual_fraud:,}")
with col5:
    st.metric("Score Risque Moyen", f"{avg_risk:.1f}/100")

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
#  ONGLETS
# ═══════════════════════════════════════════════════════════════════════════

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Vue d'ensemble",
    "🚨 Alertes",
    "📈 Tendances",
    "👤 Analyse Utilisateurs",
    "🔬 Détail Règles",
])


# ──────────── TAB 1 : Vue d'ensemble ────────────
with tab1:
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        risk_counts = filtered["risk_level"].value_counts().reindex(
            ["CRITICAL", "HIGH", "MEDIUM", "LOW"], fill_value=0
        )
        fig_risk = go.Figure(go.Bar(
            x=risk_counts.index,
            y=risk_counts.values,
            marker_color=[COLORS.get(r, "#666") for r in risk_counts.index],
            text=risk_counts.values,
            textposition="outside",
        ))
        fig_risk.update_layout(
            title="Répartition par Niveau de Risque",
            xaxis_title="Niveau",
            yaxis_title="Nombre",
            template="plotly_dark",
            height=400,
        )
        st.plotly_chart(fig_risk, width="stretch")

    with row1_col2:
        fig_pie = px.pie(
            filtered,
            names="Transaction_Type",
            title="Répartition par Type de Transaction",
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.4,
        )
        fig_pie.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig_pie, width="stretch")

    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        fig_loc = px.histogram(
            filtered,
            x="Location",
            color="risk_level",
            barmode="group",
            title="Risque par Localisation",
            color_discrete_map=COLORS,
            category_orders={"risk_level": ["CRITICAL", "HIGH", "MEDIUM", "LOW"]},
        )
        fig_loc.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig_loc, width="stretch")

    with row2_col2:
        fig_amount = px.histogram(
            filtered,
            x="Amount",
            nbins=50,
            color="is_suspected_fraud",
            title="Distribution des Montants (suspect vs normal)",
            color_discrete_map={0: "#00E676", 1: "#FF1744"},
            labels={"is_suspected_fraud": "Suspect"},
        )
        fig_amount.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig_amount, width="stretch")

    # Scatter : Montant vs Score de Risque
    fig_scatter = px.scatter(
        filtered.sample(min(5000, len(filtered)), random_state=42),
        x="Amount",
        y="risk_score",
        color="risk_level",
        size="risk_score",
        hover_data=["User_ID", "Transaction_Type", "Location"],
        title="Montant vs Score de Risque",
        color_discrete_map=COLORS,
        category_orders={"risk_level": ["CRITICAL", "HIGH", "MEDIUM", "LOW"]},
    )
    fig_scatter.update_layout(template="plotly_dark", height=500)
    st.plotly_chart(fig_scatter, width="stretch")


# ──────────── TAB 2 : Alertes ────────────
with tab2:
    st.subheader("Transactions à Risque Élevé / Critique")

    alerts = filtered[filtered["risk_level"].isin(["CRITICAL", "HIGH"])].sort_values(
        "risk_score", ascending=False
    )

    if alerts.empty:
        st.info("Aucune alerte avec les filtres actuels.")
    else:
        st.metric("Alertes actives", f"{len(alerts):,}")

        alert_cols = [
            "User_ID", "Transaction_Time", "Amount", "Transaction_Type",
            "Location", "Status", "risk_score", "risk_level", "Is_Fraud",
        ]
        available_cols = [c for c in alert_cols if c in alerts.columns]

        def color_risk(val):
            if val == "CRITICAL":
                return "background-color: #FF1744; color: white; font-weight: bold"
            elif val == "HIGH":
                return "background-color: #FF9100; color: white; font-weight: bold"
            return ""

        styled = alerts[available_cols].head(100).style.map(
            color_risk, subset=["risk_level"]
        )
        st.dataframe(styled, width="stretch", height=500)

        csv = alerts[available_cols].to_csv(index=False)
        st.download_button(
            "📥 Télécharger les alertes (CSV)",
            csv,
            "fraud_alerts.csv",
            "text/csv",
        )


# ──────────── TAB 3 : Tendances ────────────
with tab3:
    t3_col1, t3_col2 = st.columns(2)

    with t3_col1:
        hourly = filtered.groupby("Hour").agg(
            total=("Amount", "count"),
            suspicious=("is_suspected_fraud", "sum"),
            avg_amount=("Amount", "mean"),
        ).reset_index()
        hourly["fraud_rate"] = hourly["suspicious"] / hourly["total"] * 100

        fig_hourly = make_subplots(specs=[[{"secondary_y": True}]])
        fig_hourly.add_trace(
            go.Bar(x=hourly["Hour"], y=hourly["total"], name="Total", marker_color="#6C63FF", opacity=0.6),
            secondary_y=False,
        )
        fig_hourly.add_trace(
            go.Scatter(x=hourly["Hour"], y=hourly["fraud_rate"], name="Taux fraude (%)",
                       line=dict(color="#FF1744", width=3)),
            secondary_y=True,
        )
        fig_hourly.update_layout(
            title="Activité par Heure du Jour",
            template="plotly_dark",
            height=400,
        )
        fig_hourly.update_yaxes(title_text="Transactions", secondary_y=False)
        fig_hourly.update_yaxes(title_text="Taux de fraude (%)", secondary_y=True)
        st.plotly_chart(fig_hourly, width="stretch")

    with t3_col2:
        if "DayOfWeek" in filtered.columns:
            day_names = {0: "Lun", 1: "Mar", 2: "Mer", 3: "Jeu", 4: "Ven", 5: "Sam", 6: "Dim"}
            daily = filtered.groupby("DayOfWeek").agg(
                total=("Amount", "count"),
                suspicious=("is_suspected_fraud", "sum"),
            ).reset_index()
            daily["day_name"] = daily["DayOfWeek"].map(day_names)
            daily["fraud_rate"] = daily["suspicious"] / daily["total"] * 100

            fig_daily = go.Figure()
            fig_daily.add_trace(go.Bar(
                x=daily["day_name"], y=daily["total"],
                name="Total", marker_color="#6C63FF", opacity=0.6,
            ))
            fig_daily.add_trace(go.Bar(
                x=daily["day_name"], y=daily["suspicious"],
                name="Suspectes", marker_color="#FF1744",
            ))
            fig_daily.update_layout(
                title="Activité par Jour de la Semaine",
                barmode="group",
                template="plotly_dark",
                height=400,
            )
            st.plotly_chart(fig_daily, width="stretch")

    # Tendance mensuelle
    if "Month" in filtered.columns:
        monthly = filtered.groupby("Month").agg(
            total=("Amount", "count"),
            suspicious=("is_suspected_fraud", "sum"),
            total_amount=("Amount", "sum"),
        ).reset_index()
        monthly["fraud_rate"] = monthly["suspicious"] / monthly["total"] * 100

        fig_monthly = make_subplots(specs=[[{"secondary_y": True}]])
        fig_monthly.add_trace(
            go.Bar(x=monthly["Month"], y=monthly["total"],
                   name="Transactions", marker_color="#6C63FF", opacity=0.6),
            secondary_y=False,
        )
        fig_monthly.add_trace(
            go.Scatter(x=monthly["Month"], y=monthly["fraud_rate"],
                       name="Taux fraude (%)", line=dict(color="#FF1744", width=3)),
            secondary_y=True,
        )
        fig_monthly.update_layout(
            title="Tendance Mensuelle des Transactions",
            template="plotly_dark",
            height=400,
        )
        st.plotly_chart(fig_monthly, width="stretch")


# ──────────── TAB 4 : Analyse Utilisateurs ────────────
with tab4:
    st.subheader("Top Utilisateurs Suspects")

    if "is_suspected_fraud" in filtered.columns:
        user_risk = filtered.groupby("User_ID").agg(
            tx_count=("Amount", "count"),
            suspicious_count=("is_suspected_fraud", "sum"),
            total_amount=("Amount", "sum"),
            avg_risk=("risk_score", "mean"),
            max_risk=("risk_score", "max"),
        ).reset_index()
        user_risk["suspicion_rate"] = user_risk["suspicious_count"] / user_risk["tx_count"] * 100
        top_users = user_risk.sort_values("avg_risk", ascending=False).head(20)

        fig_users = px.bar(
            top_users,
            x="User_ID",
            y="avg_risk",
            color="suspicious_count",
            title="Top 20 Utilisateurs par Score de Risque Moyen",
            color_continuous_scale="Reds",
            hover_data=["tx_count", "total_amount", "suspicion_rate"],
        )
        fig_users.update_layout(
            template="plotly_dark",
            height=450,
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig_users, width="stretch")

        t4_col1, t4_col2 = st.columns(2)

        with t4_col1:
            fig_user_scatter = px.scatter(
                user_risk,
                x="tx_count",
                y="avg_risk",
                size="total_amount",
                color="suspicion_rate",
                hover_data=["User_ID"],
                title="Fréquence vs Risque par Utilisateur",
                color_continuous_scale="RdYlGn_r",
            )
            fig_user_scatter.update_layout(template="plotly_dark", height=400)
            st.plotly_chart(fig_user_scatter, width="stretch")

        with t4_col2:
            fig_user_dist = px.histogram(
                user_risk,
                x="avg_risk",
                nbins=30,
                title="Distribution du Score Moyen par Utilisateur",
                color_discrete_sequence=["#6C63FF"],
            )
            fig_user_dist.update_layout(template="plotly_dark", height=400)
            st.plotly_chart(fig_user_dist, width="stretch")

        # Recherche utilisateur
        st.markdown("---")
        user_search = st.text_input("🔎 Rechercher un utilisateur (ex: User_123)")
        if user_search and user_search in filtered["User_ID"].values:
            user_data = filtered[filtered["User_ID"] == user_search]
            st.write(f"**{len(user_data)} transactions** pour {user_search}")

            u_col1, u_col2, u_col3 = st.columns(3)
            with u_col1:
                st.metric("Montant total", f"{user_data['Amount'].sum():,.2f} €")
            with u_col2:
                st.metric("Score risque moyen", f"{user_data['risk_score'].mean():.1f}")
            with u_col3:
                st.metric("Transactions suspectes", f"{int(user_data['is_suspected_fraud'].sum())}")

            st.dataframe(
                user_data[["Transaction_Time", "Amount", "Transaction_Type",
                           "Location", "Status", "risk_score", "risk_level"]].sort_values(
                    "Transaction_Time", ascending=False
                ),
                width="stretch",
            )
        elif user_search:
            st.warning(f"Utilisateur '{user_search}' non trouvé dans les données filtrées.")


# ──────────── TAB 5 : Détail Règles ────────────
with tab5:
    st.subheader("Performance des Règles de Détection")

    flag_cols = {
        "flag_high_amount": "Montant Élevé (>8000€)",
        "flag_very_high_amount": "Montant Très Élevé (>9500€)",
        "flag_night": "Transaction Nocturne (00h-05h)",
        "flag_online_high": "En Ligne + Montant Élevé (>7000€)",
        "flag_anomaly": "Anomalie Statistique (Z-score)",
        "flag_high_freq": "Fréquence Élevée (>5/jour)",
    }

    available_flags = {k: v for k, v in flag_cols.items() if k in filtered.columns}

    if available_flags:
        rule_stats = []
        for col, label in available_flags.items():
            flagged = filtered[filtered[col] == 1]
            true_pos = len(flagged[flagged["Is_Fraud"] == "YES"]) if "Is_Fraud" in flagged.columns else 0
            rule_stats.append({
                "Règle": label,
                "Déclenchements": int(filtered[col].sum()),
                "% du total": f"{filtered[col].sum() / max(len(filtered), 1) * 100:.1f}%",
                "Vrais Positifs": true_pos,
            })

        rules_df = pd.DataFrame(rule_stats)
        st.dataframe(rules_df, width="stretch", hide_index=True)

        fig_rules = go.Figure(go.Bar(
            x=[s["Déclenchements"] for s in rule_stats],
            y=[s["Règle"] for s in rule_stats],
            orientation="h",
            marker_color=["#FF1744", "#FF5252", "#FF9100", "#FFD600", "#6C63FF", "#00BCD4"],
            text=[s["Déclenchements"] for s in rule_stats],
            textposition="outside",
        ))
        fig_rules.update_layout(
            title="Nombre de Déclenchements par Règle",
            template="plotly_dark",
            height=400,
            xaxis_title="Déclenchements",
        )
        st.plotly_chart(fig_rules, width="stretch")

        # Heatmap corrélation des flags
        if len(available_flags) > 1:
            corr = filtered[list(available_flags.keys())].corr()
            fig_corr = px.imshow(
                corr,
                x=[available_flags[c] for c in corr.columns],
                y=[available_flags[c] for c in corr.index],
                title="Corrélation entre les Règles de Détection",
                color_continuous_scale="RdBu_r",
                zmin=-1, zmax=1,
            )
            fig_corr.update_layout(template="plotly_dark", height=500)
            st.plotly_chart(fig_corr, width="stretch")


# ═══════════════════════════════════════════════════════════════════════════
#  FOOTER
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666; font-size: 0.85rem;'>"
    "Fraud Detection Pipeline • Data Engineering Project • "
    f"Dernière mise à jour des données : {df['Transaction_Time'].max().strftime('%Y-%m-%d %H:%M') if 'Transaction_Time' in df.columns else 'N/A'}"
    "</div>",
    unsafe_allow_html=True,
)
