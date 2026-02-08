"""
Promotion Analysis Dashboard - AnyCompany Marketing Analytics
Dashboard interactif pour analyser l'impact des promotions sur les ventes

Basé sur les analyses SQL de phase2_3.sql (Thème 1 : Ventes et Promotions)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector

# Configuration de la page
st.set_page_config(
    page_title="Promotion Analysis - AnyCompany",
    page_icon="🎯",
    layout="wide"
)

# Titre principal
st.title("🎯 Promotion Impact Analysis")
st.markdown("**AnyCompany Food & Beverage** - Analyse de l'efficacité des promotions")
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

# Sélection de catégorie
categories_query = """
SELECT DISTINCT product_category 
FROM PROMOTIONS_CLEAN 
ORDER BY product_category
"""
categories_df = run_query(categories_query)
selected_category = st.sidebar.selectbox(
    "Catégorie de Produit",
    options=["Toutes"] + categories_df['PRODUCT_CATEGORY'].tolist()
)

# Sélection de région
regions_query = """
SELECT DISTINCT region 
FROM PROMOTIONS_CLEAN 
ORDER BY region
"""
regions_df = run_query(regions_query)
selected_region = st.sidebar.selectbox(
    "Région",
    options=["Toutes"] + regions_df['REGION'].tolist()
)

st.sidebar.markdown("---")
st.sidebar.info("💡 **Astuce**: Comparez les ventes avec et sans promotion")

# ============================================================================
# KPIs PROMOTIONS
# ============================================================================

st.header("📊 Vue d'Ensemble des Promotions")

col1, col2, col3, col4 = st.columns(4)

# KPIs Promotions
promo_kpi_query = """
SELECT 
    COUNT(DISTINCT promotion_id) AS total_promos,
    ROUND(AVG(discount_percentage) * 100, 1) AS avg_discount,
    COUNT(DISTINCT product_category) AS categories_with_promos,
    COUNT(DISTINCT region) AS regions_with_promos
FROM PROMOTIONS_CLEAN
WHERE 1=1
"""

if selected_category != "Toutes":
    promo_kpi_query += f" AND product_category = '{selected_category}'"
if selected_region != "Toutes":
    promo_kpi_query += f" AND region = '{selected_region}'"

promo_kpis = run_query(promo_kpi_query)

with col1:
    st.metric(
        label="🎁 Promotions Totales",
        value=f"{promo_kpis['TOTAL_PROMOS'].iloc[0]:,.0f}"
    )

with col2:
    st.metric(
        label="💸 Réduction Moyenne",
        value=f"{promo_kpis['AVG_DISCOUNT'].iloc[0]:.1f}%"
    )

with col3:
    st.metric(
        label="📦 Catégories Concernées",
        value=f"{promo_kpis['CATEGORIES_WITH_PROMOS'].iloc[0]:,.0f}"
    )

with col4:
    st.metric(
        label="🌍 Régions Actives",
        value=f"{promo_kpis['REGIONS_WITH_PROMOS'].iloc[0]:,.0f}"
    )

st.markdown("---")

# ============================================================================
# COMPARAISON AVEC / SANS PROMOTION
# ============================================================================

st.header("🔍 Impact des Promotions sur les Ventes")

comparison_query = """
WITH sales_with_promo_flag AS (
    SELECT 
        ft.transaction_id,
        ft.transaction_date,
        ft.amount,
        ft.region,
        CASE 
            WHEN p.promotion_id IS NOT NULL THEN 'Avec promotion'
            ELSE 'Sans promotion'
        END AS promo_status,
        p.product_category,
        p.discount_percentage
    FROM FINANCIAL_TRANSACTIONS_CLEAN ft
    LEFT JOIN PROMOTIONS_CLEAN p 
        ON ft.transaction_date BETWEEN p.start_date AND p.end_date
        AND ft.region = p.region
"""

if selected_category != "Toutes":
    comparison_query += f" WHERE p.product_category = '{selected_category}' OR p.product_category IS NULL"

comparison_query += """
)
SELECT 
    promo_status,
    COUNT(*) AS nb_transactions,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_of_transactions,
    ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER(), 2) AS percentage_of_revenue
FROM sales_with_promo_flag
GROUP BY promo_status
ORDER BY total_revenue DESC
"""

comparison_df = run_query(comparison_query)

col1, col2 = st.columns(2)

with col1:
    fig_revenue = px.bar(
        comparison_df,
        x='PROMO_STATUS',
        y='TOTAL_REVENUE',
        title="Revenu Total : Avec vs Sans Promotion",
        labels={'PROMO_STATUS': 'Statut', 'TOTAL_REVENUE': 'Revenu (€)'},
        color='PROMO_STATUS',
        color_discrete_map={
            'Avec promotion': '#2ecc71',
            'Sans promotion': '#e74c3c'
        },
        text='TOTAL_REVENUE'
    )
    fig_revenue.update_traces(texttemplate='%{text:,.0f}€', textposition='outside')
    st.plotly_chart(fig_revenue, use_container_width=True)

with col2:
    fig_avg = px.bar(
        comparison_df,
        x='PROMO_STATUS',
        y='AVG_TRANSACTION_VALUE',
        title="Panier Moyen : Avec vs Sans Promotion",
        labels={'PROMO_STATUS': 'Statut', 'AVG_TRANSACTION_VALUE': 'Panier Moyen (€)'},
        color='PROMO_STATUS',
        color_discrete_map={
            'Avec promotion': '#2ecc71',
            'Sans promotion': '#e74c3c'
        },
        text='AVG_TRANSACTION_VALUE'
    )
    fig_avg.update_traces(texttemplate='%{text:,.2f}€', textposition='outside')
    st.plotly_chart(fig_avg, use_container_width=True)

# Affichage du tableau comparatif
st.subheader("📊 Tableau Comparatif")
st.dataframe(comparison_df.style.format({
    'TOTAL_REVENUE': '{:,.2f}€',
    'AVG_TRANSACTION_VALUE': '{:,.2f}€',
    'PERCENTAGE_OF_TRANSACTIONS': '{:.1f}%',
    'PERCENTAGE_OF_REVENUE': '{:.1f}%'
}), use_container_width=True)

# ============================================================================
# IMPACT PAR NIVEAU DE RÉDUCTION
# ============================================================================

st.header("💰 Impact par Niveau de Réduction")

discount_query = """
WITH promo_sales AS (
    SELECT 
        ft.transaction_id,
        ft.amount,
        p.discount_percentage * 100 AS discount_pct,
        p.product_category,
        CASE 
            WHEN p.discount_percentage * 100 < 10 THEN '< 10%'
            WHEN p.discount_percentage * 100 BETWEEN 10 AND 20 THEN '10-20%'
            WHEN p.discount_percentage * 100 BETWEEN 21 AND 30 THEN '21-30%'
            WHEN p.discount_percentage * 100 BETWEEN 31 AND 50 THEN '31-50%'
            ELSE '> 50%'
        END AS discount_range
    FROM FINANCIAL_TRANSACTIONS_CLEAN ft
    INNER JOIN PROMOTIONS_CLEAN p 
        ON ft.transaction_date BETWEEN p.start_date AND p.end_date
        AND ft.region = p.region
"""

if selected_category != "Toutes":
    discount_query += f" WHERE p.product_category = '{selected_category}'"

discount_query += """
)
SELECT 
    discount_range,
    COUNT(*) AS nb_transactions,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct
FROM promo_sales
GROUP BY discount_range
ORDER BY 
    CASE discount_range
        WHEN '< 10%' THEN 1
        WHEN '10-20%' THEN 2
        WHEN '21-30%' THEN 3
        WHEN '31-50%' THEN 4
        ELSE 5
    END
"""

discount_df = run_query(discount_query)

fig_discount = make_subplots(specs=[[{"secondary_y": True}]])

fig_discount.add_trace(
    go.Bar(
        x=discount_df['DISCOUNT_RANGE'],
        y=discount_df['TOTAL_REVENUE'],
        name="Revenu",
        marker_color='#3498db'
    ),
    secondary_y=False,
)

fig_discount.add_trace(
    go.Scatter(
        x=discount_df['DISCOUNT_RANGE'],
        y=discount_df['NB_TRANSACTIONS'],
        name="Nb Transactions",
        mode='lines+markers',
        marker=dict(size=10, color='#e74c3c'),
        line=dict(width=3)
    ),
    secondary_y=True,
)

fig_discount.update_xaxes(title_text="Tranche de Réduction")
fig_discount.update_yaxes(title_text="<b>Revenu (€)</b>", secondary_y=False)
fig_discount.update_yaxes(title_text="<b>Nombre de Transactions</b>", secondary_y=True)
fig_discount.update_layout(
    title="Performance par Niveau de Réduction",
    hovermode="x unified",
    height=500
)

st.plotly_chart(fig_discount, use_container_width=True)

# ============================================================================
# SENSIBILITÉ DES CATÉGORIES AUX PROMOTIONS
# ============================================================================

st.header("📦 Sensibilité des Catégories aux Promotions")

sensitivity_query = """
WITH category_baseline AS (
    SELECT 
        i.product_category,
        COUNT(DISTINCT ft.transaction_id) AS transactions_no_promo,
        ROUND(AVG(ft.amount), 2) AS avg_value_no_promo
    FROM FINANCIAL_TRANSACTIONS_CLEAN ft
    CROSS JOIN INVENTORY_CLEAN i
    LEFT JOIN PROMOTIONS_CLEAN p 
        ON ft.transaction_date BETWEEN p.start_date AND p.end_date
        AND ft.region = p.region
        AND i.product_category = p.product_category
    WHERE p.promotion_id IS NULL
    GROUP BY i.product_category
),
category_with_promo AS (
    SELECT 
        p.product_category,
        COUNT(DISTINCT ft.transaction_id) AS transactions_with_promo,
        ROUND(AVG(ft.amount), 2) AS avg_value_with_promo,
        ROUND(AVG(p.discount_percentage) * 100, 2) AS avg_discount
    FROM FINANCIAL_TRANSACTIONS_CLEAN ft
    INNER JOIN PROMOTIONS_CLEAN p 
        ON ft.transaction_date BETWEEN p.start_date AND p.end_date
        AND ft.region = p.region
    GROUP BY p.product_category
)
SELECT 
    COALESCE(cb.product_category, cp.product_category) AS category,
    COALESCE(cb.transactions_no_promo, 0) AS transactions_no_promo,
    COALESCE(cb.avg_value_no_promo, 0) AS avg_value_no_promo,
    COALESCE(cp.transactions_with_promo, 0) AS transactions_with_promo,
    COALESCE(cp.avg_value_with_promo, 0) AS avg_value_with_promo,
    COALESCE(cp.avg_discount, 0) AS avg_discount,
    ROUND((COALESCE(cp.avg_value_with_promo, 0) - COALESCE(cb.avg_value_no_promo, 0)) / NULLIF(cb.avg_value_no_promo, 1) * 100, 2) AS lift_percentage,
    CASE 
        WHEN (COALESCE(cp.avg_value_with_promo, 0) - COALESCE(cb.avg_value_no_promo, 0)) / NULLIF(cb.avg_value_no_promo, 1) * 100 > 20 THEN 'Très sensible'
        WHEN (COALESCE(cp.avg_value_with_promo, 0) - COALESCE(cb.avg_value_no_promo, 0)) / NULLIF(cb.avg_value_no_promo, 1) * 100 BETWEEN 5 AND 20 THEN 'Sensible'
        WHEN (COALESCE(cp.avg_value_with_promo, 0) - COALESCE(cb.avg_value_no_promo, 0)) / NULLIF(cb.avg_value_no_promo, 1) * 100 BETWEEN -5 AND 5 THEN 'Neutre'
        ELSE 'Peu sensible'
    END AS sensitivity_level
FROM category_baseline cb
FULL OUTER JOIN category_with_promo cp ON cb.product_category = cp.product_category
WHERE COALESCE(cb.product_category, cp.product_category) IS NOT NULL
ORDER BY lift_percentage DESC NULLS LAST
LIMIT 15
"""

sensitivity_df = run_query(sensitivity_query)

# Graphique de sensibilité
fig_sensitivity = px.bar(
    sensitivity_df,
    x='CATEGORY',
    y='LIFT_PERCENTAGE',
    title="Lift des Ventes par Catégorie (% d'augmentation avec promotion)",
    labels={'CATEGORY': 'Catégorie', 'LIFT_PERCENTAGE': 'Lift (%)'},
    color='SENSITIVITY_LEVEL',
    color_discrete_map={
        'Très sensible': '#27ae60',
        'Sensible': '#f39c12',
        'Neutre': '#95a5a6',
        'Peu sensible': '#e74c3c'
    },
    text='LIFT_PERCENTAGE'
)

fig_sensitivity.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
fig_sensitivity.update_xaxes(tickangle=45)
fig_sensitivity.update_layout(height=500)

st.plotly_chart(fig_sensitivity, use_container_width=True)

# ============================================================================
# ROI DES PROMOTIONS PAR CATÉGORIE
# ============================================================================

st.header("💎 ROI des Promotions par Catégorie")

roi_query = """
SELECT 
    p.product_category,
    COUNT(DISTINCT p.promotion_id) AS promo_count,
    ROUND(AVG(p.discount_percentage) * 100, 2) AS avg_discount,
    COUNT(DISTINCT ft.transaction_id) AS transactions_during_promo,
    ROUND(SUM(ft.amount), 2) AS revenue_during_promo,
    ROUND(AVG(ft.amount), 2) AS avg_transaction_value
FROM PROMOTIONS_CLEAN p
LEFT JOIN FINANCIAL_TRANSACTIONS_CLEAN ft 
    ON ft.transaction_date BETWEEN p.start_date AND p.end_date
    AND ft.region = p.region
"""

if selected_region != "Toutes":
    roi_query += f" WHERE p.region = '{selected_region}'"

roi_query += """
GROUP BY p.product_category
ORDER BY revenue_during_promo DESC
LIMIT 10
"""

roi_df = run_query(roi_query)

fig_roi = px.scatter(
    roi_df,
    x='AVG_DISCOUNT',
    y='REVENUE_DURING_PROMO',
    size='TRANSACTIONS_DURING_PROMO',
    color='PRODUCT_CATEGORY',
    title="ROI : Réduction Moyenne vs Revenu Généré",
    labels={
        'AVG_DISCOUNT': 'Réduction Moyenne (%)',
        'REVENUE_DURING_PROMO': 'Revenu Pendant Promo (€)',
        'PRODUCT_CATEGORY': 'Catégorie'
    },
    hover_data=['PROMO_COUNT', 'TRANSACTIONS_DURING_PROMO']
)

fig_roi.update_layout(height=500)
st.plotly_chart(fig_roi, use_container_width=True)

# ============================================================================
# DURÉE OPTIMALE DES PROMOTIONS
# ============================================================================

st.header("⏱️ Durée Optimale des Promotions")

duration_query = """
SELECT 
    DATEDIFF(day, start_date, end_date) AS promo_duration_days,
    COUNT(*) AS promo_count,
    ROUND(AVG(discount_percentage) * 100, 2) AS avg_discount,
    COUNT(DISTINCT product_category) AS categories_count
FROM PROMOTIONS_CLEAN
GROUP BY promo_duration_days
HAVING promo_count >= 5
ORDER BY promo_duration_days
"""

duration_df = run_query(duration_query)

fig_duration = px.bar(
    duration_df,
    x='PROMO_DURATION_DAYS',
    y='PROMO_COUNT',
    title="Distribution des Promotions par Durée",
    labels={'PROMO_DURATION_DAYS': 'Durée (jours)', 'PROMO_COUNT': 'Nombre de Promotions'},
    color='AVG_DISCOUNT',
    color_continuous_scale='RdYlGn_r',
    text='PROMO_COUNT'
)

fig_duration.update_traces(textposition='outside')
st.plotly_chart(fig_duration, use_container_width=True)

# ============================================================================
# RECOMMANDATIONS
# ============================================================================

st.header("💡 Recommandations Stratégiques")

col1, col2 = st.columns(2)

with col1:
    st.success("**✅ Catégories à Promouvoir**")
    top_categories = sensitivity_df.nlargest(3, 'LIFT_PERCENTAGE')
    for idx, row in top_categories.iterrows():
        st.write(f"• **{row['CATEGORY']}** : +{row['LIFT_PERCENTAGE']:.1f}% de lift")

with col2:
    st.warning("**⚠️ Optimisations Possibles**")
    low_categories = sensitivity_df.nsmallest(3, 'LIFT_PERCENTAGE')
    for idx, row in low_categories.iterrows():
        st.write(f"• **{row['CATEGORY']}** : Revoir stratégie promo")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p style='color: gray;'>Promotion Analysis Dashboard | AnyCompany Marketing Analytics</p>
</div>
""", unsafe_allow_html=True)
