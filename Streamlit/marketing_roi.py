"""
Marketing ROI Dashboard - AnyCompany Marketing Analytics
Dashboard interactif pour analyser le ROI des campagnes marketing

Basé sur les analyses SQL de phase2_3.sql (Thème 2 : Marketing et Performance)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import numpy as np

# Configuration de la page
st.set_page_config(
    page_title="Marketing ROI - AnyCompany",
    page_icon="💼",
    layout="wide"
)

# Titre principal
st.title("💼 Marketing ROI & Campaign Performance")
st.markdown("**AnyCompany Food & Beverage** - Analyse du retour sur investissement marketing")
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

# Sélection du type de campagne
campaign_types_query = """
SELECT DISTINCT campaign_type 
FROM MARKETING_CAMPAIGNS_CLEAN 
ORDER BY campaign_type
"""
campaign_types_df = run_query(campaign_types_query)
selected_campaign_type = st.sidebar.selectbox(
    "Type de Campagne",
    options=["Tous"] + campaign_types_df['CAMPAIGN_TYPE'].tolist()
)

# Sélection de l'audience cible
audiences_query = """
SELECT DISTINCT target_audience 
FROM MARKETING_CAMPAIGNS_CLEAN 
ORDER BY target_audience
"""
audiences_df = run_query(audiences_query)
selected_audience = st.sidebar.selectbox(
    "Audience Cible",
    options=["Toutes"] + audiences_df['TARGET_AUDIENCE'].tolist()
)

# Sélection de région
regions_query = """
SELECT DISTINCT region 
FROM MARKETING_CAMPAIGNS_CLEAN 
ORDER BY region
"""
regions_df = run_query(regions_query)
selected_region = st.sidebar.selectbox(
    "Région",
    options=["Toutes"] + regions_df['REGION'].tolist()
)

st.sidebar.markdown("---")
st.sidebar.info("💡 **Astuce**: Analysez le ROI pour optimiser les budgets futurs")

# ============================================================================
# KPIs MARKETING GLOBAUX
# ============================================================================

st.header("📊 Vue d'Ensemble Marketing")

# Construction de la clause WHERE
where_clauses = []
if selected_campaign_type != "Tous":
    where_clauses.append(f"campaign_type = '{selected_campaign_type}'")
if selected_audience != "Toutes":
    where_clauses.append(f"target_audience = '{selected_audience}'")
if selected_region != "Toutes":
    where_clauses.append(f"region = '{selected_region}'")

where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

kpi_query = f"""
SELECT 
    COUNT(DISTINCT campaign_id) AS total_campaigns,
    ROUND(SUM(budget), 2) AS total_budget,
    SUM(reach) AS total_reach,
    ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_rate
FROM MARKETING_CAMPAIGNS_CLEAN
WHERE {where_clause}
"""

kpis = run_query(kpi_query)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="📢 Campagnes Totales",
        value=f"{kpis['TOTAL_CAMPAIGNS'].iloc[0]:,.0f}"
    )

with col2:
    st.metric(
        label="💰 Budget Total",
        value=f"{kpis['TOTAL_BUDGET'].iloc[0]:,.0f} €"
    )

with col3:
    st.metric(
        label="👥 Reach Total",
        value=f"{kpis['TOTAL_REACH'].iloc[0]:,.0f}"
    )

with col4:
    st.metric(
        label="📈 Taux de Conversion Moyen",
        value=f"{kpis['AVG_CONVERSION_RATE'].iloc[0]:.2f}%"
    )

st.markdown("---")

# ============================================================================
# LIEN ENTRE CAMPAGNES ET VENTES
# ============================================================================

st.header("🔗 Impact des Campagnes sur les Ventes")

campaign_sales_query = f"""
WITH campaign_performance AS (
    SELECT 
        mc.campaign_id,
        mc.campaign_name,
        mc.campaign_type,
        mc.region,
        mc.budget,
        mc.reach,
        mc.conversion_rate,
        mc.start_date,
        mc.end_date,
        COUNT(DISTINCT ft.transaction_id) AS transactions_during_campaign,
        ROUND(SUM(ft.amount), 2) AS revenue_during_campaign
    FROM MARKETING_CAMPAIGNS_CLEAN mc
    LEFT JOIN FINANCIAL_TRANSACTIONS_CLEAN ft 
        ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
        AND ft.region = mc.region
    WHERE {where_clause}
    GROUP BY mc.campaign_id, mc.campaign_name, mc.campaign_type, mc.region, 
             mc.budget, mc.reach, mc.conversion_rate, mc.start_date, mc.end_date
)
SELECT 
    campaign_id,
    campaign_name,
    campaign_type,
    region,
    budget,
    reach,
    ROUND(conversion_rate * 100, 2) AS conversion_rate_pct,
    transactions_during_campaign,
    revenue_during_campaign,
    ROUND(revenue_during_campaign / NULLIF(budget, 0), 2) AS roi,
    ROUND(revenue_during_campaign / NULLIF(reach, 0), 4) AS revenue_per_reach
FROM campaign_performance
ORDER BY roi DESC NULLS LAST
LIMIT 20
"""

campaign_sales_df = run_query(campaign_sales_query)

# Graphique ROI par campagne
fig_roi = px.bar(
    campaign_sales_df.head(15),
    x='CAMPAIGN_NAME',
    y='ROI',
    title="Top 15 Campagnes par ROI (Revenu / Budget)",
    labels={'CAMPAIGN_NAME': 'Campagne', 'ROI': 'ROI (€ revenus par € dépensé)'},
    color='ROI',
    color_continuous_scale='RdYlGn',
    text='ROI'
)

fig_roi.update_traces(texttemplate='%{text:.2f}x', textposition='outside')
fig_roi.update_xaxes(tickangle=45)
fig_roi.update_layout(height=500)

st.plotly_chart(fig_roi, use_container_width=True)

# ============================================================================
# PERFORMANCE PAR TYPE DE CAMPAGNE
# ============================================================================

st.header("📊 Performance par Type de Campagne")

campaign_type_query = f"""
SELECT 
    campaign_type,
    COUNT(*) AS campaign_count,
    ROUND(SUM(budget), 2) AS total_budget,
    ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_rate,
    ROUND(AVG(reach), 0) AS avg_reach,
    ROUND(AVG(budget), 2) AS avg_budget
FROM MARKETING_CAMPAIGNS_CLEAN
WHERE {where_clause}
GROUP BY campaign_type
ORDER BY total_budget DESC
"""

campaign_type_df = run_query(campaign_type_query)

col1, col2 = st.columns(2)

with col1:
    # Budget par type
    fig_budget = px.pie(
        campaign_type_df,
        values='TOTAL_BUDGET',
        names='CAMPAIGN_TYPE',
        title="Répartition du Budget par Type de Campagne",
        hole=0.4
    )
    fig_budget.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig_budget, use_container_width=True)

with col2:
    # Conversion par type
    fig_conversion = px.bar(
        campaign_type_df,
        x='CAMPAIGN_TYPE',
        y='AVG_CONVERSION_RATE',
        title="Taux de Conversion Moyen par Type",
        labels={'CAMPAIGN_TYPE': 'Type de Campagne', 'AVG_CONVERSION_RATE': 'Conversion (%)'},
        color='AVG_CONVERSION_RATE',
        color_continuous_scale='Viridis',
        text='AVG_CONVERSION_RATE'
    )
    fig_conversion.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
    fig_conversion.update_xaxes(tickangle=45)
    st.plotly_chart(fig_conversion, use_container_width=True)

# ============================================================================
# REACH VS CONVERSION
# ============================================================================

st.header("🎯 Reach vs Conversion Rate")

reach_conversion_query = f"""
SELECT 
    campaign_name,
    campaign_type,
    reach,
    ROUND(conversion_rate * 100, 2) AS conversion_rate_pct,
    budget,
    region
FROM MARKETING_CAMPAIGNS_CLEAN
WHERE {where_clause}
ORDER BY reach DESC
LIMIT 50
"""

reach_conversion_df = run_query(reach_conversion_query)

fig_scatter = px.scatter(
    reach_conversion_df,
    x='REACH',
    y='CONVERSION_RATE_PCT',
    size='BUDGET',
    color='CAMPAIGN_TYPE',
    hover_name='CAMPAIGN_NAME',
    title="Reach vs Taux de Conversion (taille = budget)",
    labels={
        'REACH': 'Reach (nombre de personnes)',
        'CONVERSION_RATE_PCT': 'Taux de Conversion (%)',
        'CAMPAIGN_TYPE': 'Type de Campagne'
    }
)

fig_scatter.update_layout(height=500)
st.plotly_chart(fig_scatter, use_container_width=True)

# ============================================================================
# PERFORMANCE PAR AUDIENCE CIBLE
# ============================================================================

st.header("👥 Performance par Audience Cible")

audience_query = f"""
SELECT 
    target_audience,
    COUNT(*) AS campaign_count,
    ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_rate,
    ROUND(SUM(budget), 2) AS total_budget,
    SUM(reach) AS total_reach
FROM MARKETING_CAMPAIGNS_CLEAN
WHERE {where_clause}
GROUP BY target_audience
ORDER BY avg_conversion_rate DESC
"""

audience_df = run_query(audience_query)

col1, col2 = st.columns(2)

with col1:
    fig_audience_conv = px.bar(
        audience_df,
        x='TARGET_AUDIENCE',
        y='AVG_CONVERSION_RATE',
        title="Taux de Conversion par Audience",
        labels={'TARGET_AUDIENCE': 'Audience', 'AVG_CONVERSION_RATE': 'Conversion (%)'},
        color='AVG_CONVERSION_RATE',
        color_continuous_scale='Blues',
        text='AVG_CONVERSION_RATE'
    )
    fig_audience_conv.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
    fig_audience_conv.update_xaxes(tickangle=45)
    st.plotly_chart(fig_audience_conv, use_container_width=True)

with col2:
    fig_audience_budget = px.bar(
        audience_df,
        x='TARGET_AUDIENCE',
        y='TOTAL_BUDGET',
        title="Budget Total par Audience",
        labels={'TARGET_AUDIENCE': 'Audience', 'TOTAL_BUDGET': 'Budget (€)'},
        color='CAMPAIGN_COUNT',
        color_continuous_scale='Oranges',
        text='TOTAL_BUDGET'
    )
    fig_audience_budget.update_traces(texttemplate='%{text:,.0f}€', textposition='outside')
    fig_audience_budget.update_xaxes(tickangle=45)
    st.plotly_chart(fig_audience_budget, use_container_width=True)

# ============================================================================
# PERFORMANCE PAR RÉGION
# ============================================================================

st.header("🌍 Performance par Région")

region_performance_query = f"""
SELECT 
    mc.region,
    COUNT(DISTINCT mc.campaign_id) AS campaign_count,
    ROUND(SUM(mc.budget), 2) AS total_budget,
    ROUND(AVG(mc.conversion_rate) * 100, 2) AS avg_conversion_rate,
    SUM(mc.reach) AS total_reach,
    COUNT(DISTINCT ft.transaction_id) AS total_transactions,
    ROUND(SUM(ft.amount), 2) AS total_revenue
FROM MARKETING_CAMPAIGNS_CLEAN mc
LEFT JOIN FINANCIAL_TRANSACTIONS_CLEAN ft 
    ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
    AND ft.region = mc.region
WHERE {where_clause}
GROUP BY mc.region
ORDER BY total_revenue DESC
"""

region_perf_df = run_query(region_performance_query)

# Calcul du ROI régional
region_perf_df['ROI'] = region_perf_df['TOTAL_REVENUE'] / region_perf_df['TOTAL_BUDGET']

fig_region = px.bar(
    region_perf_df,
    x='REGION',
    y='ROI',
    title="ROI Marketing par Région",
    labels={'REGION': 'Région', 'ROI': 'ROI (€ revenus / € budget)'},
    color='ROI',
    color_continuous_scale='RdYlGn',
    text='ROI',
    hover_data=['TOTAL_BUDGET', 'TOTAL_REVENUE', 'CAMPAIGN_COUNT']
)

fig_region.update_traces(texttemplate='%{text:.2f}x', textposition='outside')
fig_region.update_xaxes(tickangle=45)
fig_region.update_layout(height=500)

st.plotly_chart(fig_region, use_container_width=True)

# ============================================================================
# TOP CAMPAGNES
# ============================================================================

st.header("🏆 Top 10 Campagnes les Plus Performantes")

top_campaigns_query = f"""
WITH campaign_performance AS (
    SELECT 
        mc.campaign_id,
        mc.campaign_name,
        mc.campaign_type,
        mc.budget,
        mc.reach,
        ROUND(mc.conversion_rate * 100, 2) AS conversion_rate_pct,
        COUNT(DISTINCT ft.transaction_id) AS transactions,
        ROUND(SUM(ft.amount), 2) AS revenue
    FROM MARKETING_CAMPAIGNS_CLEAN mc
    LEFT JOIN FINANCIAL_TRANSACTIONS_CLEAN ft 
        ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
        AND ft.region = mc.region
    WHERE {where_clause}
    GROUP BY mc.campaign_id, mc.campaign_name, mc.campaign_type, 
             mc.budget, mc.reach, mc.conversion_rate
)
SELECT 
    campaign_name,
    campaign_type,
    budget,
    reach,
    conversion_rate_pct,
    transactions,
    revenue,
    ROUND(revenue / NULLIF(budget, 0), 2) AS roi
FROM campaign_performance
ORDER BY roi DESC NULLS LAST
LIMIT 10
"""

top_campaigns_df = run_query(top_campaigns_query)

st.dataframe(
    top_campaigns_df.style.format({
        'BUDGET': '{:,.2f}€',
        'REACH': '{:,.0f}',
        'CONVERSION_RATE_PCT': '{:.2f}%',
        'REVENUE': '{:,.2f}€',
        'ROI': '{:.2f}x'
    }),
    use_container_width=True
)

# ============================================================================
# ANALYSE TEMPORELLE
# ============================================================================

st.header("📅 Évolution Temporelle des Campagnes")

temporal_query = f"""
SELECT 
    DATE_TRUNC('month', start_date) AS month,
    COUNT(*) AS campaign_count,
    ROUND(SUM(budget), 2) AS monthly_budget,
    ROUND(AVG(conversion_rate) * 100, 2) AS avg_conversion_rate
FROM MARKETING_CAMPAIGNS_CLEAN
WHERE {where_clause}
GROUP BY month
ORDER BY month
"""

temporal_df = run_query(temporal_query)

fig_temporal = make_subplots(specs=[[{"secondary_y": True}]])

fig_temporal.add_trace(
    go.Bar(
        x=temporal_df['MONTH'],
        y=temporal_df['MONTHLY_BUDGET'],
        name="Budget Mensuel",
        marker_color='#3498db'
    ),
    secondary_y=False,
)

fig_temporal.add_trace(
    go.Scatter(
        x=temporal_df['MONTH'],
        y=temporal_df['AVG_CONVERSION_RATE'],
        name="Taux de Conversion",
        mode='lines+markers',
        marker=dict(size=10, color='#e74c3c'),
        line=dict(width=3)
    ),
    secondary_y=True,
)

fig_temporal.update_xaxes(title_text="Mois")
fig_temporal.update_yaxes(title_text="<b>Budget (€)</b>", secondary_y=False)
fig_temporal.update_yaxes(title_text="<b>Taux de Conversion (%)</b>", secondary_y=True)
fig_temporal.update_layout(
    title="Évolution du Budget et de la Conversion dans le Temps",
    hovermode="x unified",
    height=500
)

st.plotly_chart(fig_temporal, use_container_width=True)

# ============================================================================
# RECOMMANDATIONS STRATÉGIQUES
# ============================================================================

st.header("💡 Recommandations Stratégiques")

col1, col2, col3 = st.columns(3)

with col1:
    st.success("**✅ Types de Campagnes Efficaces**")
    top_types = campaign_type_df.nlargest(3, 'AVG_CONVERSION_RATE')
    for idx, row in top_types.iterrows():
        st.write(f"• **{row['CAMPAIGN_TYPE']}** : {row['AVG_CONVERSION_RATE']:.2f}% conversion")

with col2:
    st.info("**🎯 Audiences les Plus Réceptives**")
    top_audiences = audience_df.nlargest(3, 'AVG_CONVERSION_RATE')
    for idx, row in top_audiences.iterrows():
        st.write(f"• **{row['TARGET_AUDIENCE']}** : {row['AVG_CONVERSION_RATE']:.2f}% conversion")

with col3:
    st.warning("**💰 Optimisation Budget**")
    # Identifier les campagnes à faible ROI
    low_roi = campaign_sales_df.nsmallest(3, 'ROI')
    if len(low_roi) > 0:
        st.write("**Types à optimiser :**")
        for idx, row in low_roi.iterrows():
            if pd.notna(row['ROI']):
                st.write(f"• {row['CAMPAIGN_TYPE']}: ROI {row['ROI']:.2f}x")

# ============================================================================
# EXPORT DE DONNÉES
# ============================================================================

st.header("📥 Export de Données")

with st.expander("Télécharger les données complètes"):
    csv = campaign_sales_df.to_csv(index=False)
    st.download_button(
        label="📄 Télécharger CSV (Top Campagnes)",
        data=csv,
        file_name="marketing_campaigns_performance.csv",
        mime="text/csv"
    )

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p style='color: gray;'>Marketing ROI Dashboard | AnyCompany Marketing Analytics</p>
</div>
""", unsafe_allow_html=True)
