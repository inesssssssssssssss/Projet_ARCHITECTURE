"""
Sales Dashboard - AnyCompany Marketing Analytics
Dashboard interactif pour l'analyse de l'évolution des ventes

Basé sur les analyses SQL de phase2_2.sql (Axe 1 : Évolution des ventes)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="Sales Dashboard - AnyCompany",
    page_icon="📈",
    layout="wide"
)

# Titre principal
st.title("📈 Sales Performance Dashboard")
st.markdown("**AnyCompany Food & Beverage** - Analyse des ventes et tendances")
st.markdown("---")

# ============================================================================
# CONNEXION SNOWFLAKE
# ============================================================================

@st.cache_resource
def init_connection():
    """Initialise la connexion à Snowflake"""
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database="ANYCOMPANY_LAB",
        schema="SILVER"
    )

@st.cache_data(ttl=600)
def run_query(query):
    """Exécute une requête et retourne un DataFrame"""
    with init_connection() as conn:
        return pd.read_sql(query, conn)

# ============================================================================
# SIDEBAR - FILTRES
# ============================================================================

st.sidebar.header("🎯 Filtres")

# Sélection de la période
period_option = st.sidebar.selectbox(
    "Période d'analyse",
    ["Mensuelle", "Trimestrielle", "Annuelle"]
)

# Sélection de la région (optionnel)
show_region_filter = st.sidebar.checkbox("Filtrer par région", value=False)

if show_region_filter:
    regions_query = """
    SELECT DISTINCT region 
    FROM FINANCIAL_TRANSACTIONS_CLEAN 
    ORDER BY region
    """
    regions_df = run_query(regions_query)
    selected_region = st.sidebar.selectbox(
        "Région",
        options=["Toutes"] + regions_df['REGION'].tolist()
    )
else:
    selected_region = "Toutes"

st.sidebar.markdown("---")
st.sidebar.info("💡 **Astuce**: Survolez les graphiques pour plus de détails")

# ============================================================================
# KPIs PRINCIPAUX
# ============================================================================

st.header("📊 KPIs Clés")

col1, col2, col3, col4 = st.columns(4)

# Requête KPIs globaux
kpi_query = """
SELECT 
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    COUNT(DISTINCT entity) AS unique_customers
FROM FINANCIAL_TRANSACTIONS_CLEAN
"""

if selected_region != "Toutes":
    kpi_query += f" WHERE region = '{selected_region}'"

kpis = run_query(kpi_query)

with col1:
    st.metric(
        label="💰 Revenu Total",
        value=f"{kpis['TOTAL_REVENUE'].iloc[0]:,.0f} €",
        delta=None
    )

with col2:
    st.metric(
        label="🛒 Transactions",
        value=f"{kpis['TOTAL_TRANSACTIONS'].iloc[0]:,.0f}",
        delta=None
    )

with col3:
    st.metric(
        label="💵 Panier Moyen",
        value=f"{kpis['AVG_TRANSACTION_VALUE'].iloc[0]:,.2f} €",
        delta=None
    )

with col4:
    st.metric(
        label="👥 Clients Uniques",
        value=f"{kpis['UNIQUE_CUSTOMERS'].iloc[0]:,.0f}",
        delta=None
    )

st.markdown("---")

# ============================================================================
# ÉVOLUTION TEMPORELLE DES VENTES
# ============================================================================

st.header("📅 Évolution des Ventes dans le Temps")

# Requête selon la période sélectionnée
if period_option == "Mensuelle":
    time_query = """
    SELECT 
        DATE_TRUNC('month', transaction_date) AS period,
        COUNT(*) AS nb_transactions,
        ROUND(SUM(amount), 2) AS total_revenue,
        ROUND(AVG(amount), 2) AS avg_transaction_value
    FROM FINANCIAL_TRANSACTIONS_CLEAN
    """
    if selected_region != "Toutes":
        time_query += f" WHERE region = '{selected_region}'"
    time_query += """
    GROUP BY period
    ORDER BY period
    """
    
elif period_option == "Trimestrielle":
    time_query = """
    SELECT 
        DATE_TRUNC('quarter', transaction_date) AS period,
        COUNT(*) AS nb_transactions,
        ROUND(SUM(amount), 2) AS total_revenue,
        ROUND(AVG(amount), 2) AS avg_transaction_value
    FROM FINANCIAL_TRANSACTIONS_CLEAN
    """
    if selected_region != "Toutes":
        time_query += f" WHERE region = '{selected_region}'"
    time_query += """
    GROUP BY period
    ORDER BY period
    """
    
else:  # Annuelle
    time_query = """
    SELECT 
        YEAR(transaction_date) AS period,
        COUNT(*) AS nb_transactions,
        ROUND(SUM(amount), 2) AS total_revenue,
        ROUND(AVG(amount), 2) AS avg_transaction_value
    FROM FINANCIAL_TRANSACTIONS_CLEAN
    """
    if selected_region != "Toutes":
        time_query += f" WHERE region = '{selected_region}'"
    time_query += """
    GROUP BY period
    ORDER BY period
    """

time_df = run_query(time_query)

# Graphique double axe : Revenus et Transactions
fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(
    go.Scatter(
        x=time_df['PERIOD'],
        y=time_df['TOTAL_REVENUE'],
        name="Revenu",
        line=dict(color='#1f77b4', width=3),
        mode='lines+markers'
    ),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(
        x=time_df['PERIOD'],
        y=time_df['NB_TRANSACTIONS'],
        name="Nb Transactions",
        line=dict(color='#ff7f0e', width=2, dash='dot'),
        mode='lines+markers'
    ),
    secondary_y=True,
)

fig.update_xaxes(title_text="Période")
fig.update_yaxes(title_text="<b>Revenu (€)</b>", secondary_y=False)
fig.update_yaxes(title_text="<b>Nombre de Transactions</b>", secondary_y=True)

fig.update_layout(
    title=f"Évolution {period_option} des Ventes",
    hovermode="x unified",
    height=500
)

st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# CROISSANCE MONTH-OVER-MONTH
# ============================================================================

st.header("📈 Taux de Croissance")

growth_query = """
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', transaction_date) AS month,
        ROUND(SUM(amount), 2) AS revenue
    FROM FINANCIAL_TRANSACTIONS_CLEAN
"""
if selected_region != "Toutes":
    growth_query += f" WHERE region = '{selected_region}'"

growth_query += """
    GROUP BY month
)
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) AS previous_month_revenue,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) / LAG(revenue) OVER (ORDER BY month) * 100, 2) AS growth_percentage
FROM monthly_sales
ORDER BY month
"""

growth_df = run_query(growth_query)

fig_growth = go.Figure()

fig_growth.add_trace(go.Bar(
    x=growth_df['MONTH'],
    y=growth_df['GROWTH_PERCENTAGE'],
    marker_color=['red' if val < 0 else 'green' for val in growth_df['GROWTH_PERCENTAGE']],
    name='Croissance %',
    text=growth_df['GROWTH_PERCENTAGE'],
    texttemplate='%{text:.1f}%',
    textposition='outside'
))

fig_growth.update_layout(
    title="Croissance Mensuelle (Month-over-Month)",
    xaxis_title="Mois",
    yaxis_title="Croissance (%)",
    hovermode="x",
    height=400
)

st.plotly_chart(fig_growth, use_container_width=True)

# ============================================================================
# SAISONNALITÉ
# ============================================================================

st.header("🌡️ Analyse de Saisonnalité")

col1, col2 = st.columns(2)

with col1:
    # Jour de la semaine
    weekday_query = """
    SELECT 
        DAYNAME(transaction_date) AS day_of_week,
        COUNT(*) AS nb_transactions,
        ROUND(SUM(amount), 2) AS total_revenue
    FROM FINANCIAL_TRANSACTIONS_CLEAN
    """
    if selected_region != "Toutes":
        weekday_query += f" WHERE region = '{selected_region}'"
    
    weekday_query += """
    GROUP BY day_of_week
    ORDER BY 
        CASE day_of_week
            WHEN 'Monday' THEN 1
            WHEN 'Tuesday' THEN 2
            WHEN 'Wednesday' THEN 3
            WHEN 'Thursday' THEN 4
            WHEN 'Friday' THEN 5
            WHEN 'Saturday' THEN 6
            WHEN 'Sunday' THEN 7
        END
    """
    
    weekday_df = run_query(weekday_query)
    
    fig_weekday = px.bar(
        weekday_df,
        x='DAY_OF_WEEK',
        y='TOTAL_REVENUE',
        title="Revenu par Jour de la Semaine",
        labels={'DAY_OF_WEEK': 'Jour', 'TOTAL_REVENUE': 'Revenu (€)'},
        color='TOTAL_REVENUE',
        color_continuous_scale='Blues'
    )
    
    st.plotly_chart(fig_weekday, use_container_width=True)

with col2:
    # Mois de l'année
    month_query = """
    SELECT 
        MONTHNAME(transaction_date) AS month_name,
        MONTH(transaction_date) AS month_num,
        ROUND(SUM(amount), 2) AS total_revenue
    FROM FINANCIAL_TRANSACTIONS_CLEAN
    """
    if selected_region != "Toutes":
        month_query += f" WHERE region = '{selected_region}'"
    
    month_query += """
    GROUP BY month_name, month_num
    ORDER BY month_num
    """
    
    month_df = run_query(month_query)
    
    fig_month = px.line(
        month_df,
        x='MONTH_NAME',
        y='TOTAL_REVENUE',
        title="Revenu par Mois de l'Année",
        labels={'MONTH_NAME': 'Mois', 'TOTAL_REVENUE': 'Revenu (€)'},
        markers=True
    )
    
    st.plotly_chart(fig_month, use_container_width=True)

# ============================================================================
# PERFORMANCE PAR RÉGION
# ============================================================================

st.header("🌍 Performance par Région")

region_query = """
SELECT 
    region,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount
FROM FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY region
ORDER BY total_amount DESC
"""

region_df = run_query(region_query)

col1, col2 = st.columns(2)

with col1:
    fig_region_pie = px.pie(
        region_df,
        values='TOTAL_AMOUNT',
        names='REGION',
        title="Répartition du Revenu par Région",
        hole=0.4
    )
    st.plotly_chart(fig_region_pie, use_container_width=True)

with col2:
    fig_region_bar = px.bar(
        region_df,
        x='REGION',
        y='TRANSACTION_COUNT',
        title="Nombre de Transactions par Région",
        labels={'REGION': 'Région', 'TRANSACTION_COUNT': 'Transactions'},
        color='TRANSACTION_COUNT',
        color_continuous_scale='Viridis'
    )
    st.plotly_chart(fig_region_bar, use_container_width=True)

# ============================================================================
# TABLEAU DE DONNÉES
# ============================================================================

st.header("📋 Données Détaillées")

with st.expander("Voir les données brutes"):
    st.dataframe(region_df, use_container_width=True)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p style='color: gray;'>Dashboard créé avec Streamlit | Données : Snowflake | AnyCompany Marketing Analytics</p>
</div>
""", unsafe_allow_html=True)
