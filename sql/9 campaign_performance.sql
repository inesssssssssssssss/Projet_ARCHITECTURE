-- THÈME 2 : MARKETING ET PERFORMANCE COMMERCIALE



-- 2.1 Lien entre campagnes marketing et ventes
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
    FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN mc
    LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft 
        ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
        AND ft.region = mc.region
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
    conversion_rate,
    transactions_during_campaign,
    revenue_during_campaign,
    ROUND(revenue_during_campaign / NULLIF(budget, 0), 2) AS revenue_per_euro_spent,
    ROUND(revenue_during_campaign / NULLIF(reach, 0), 2) AS revenue_per_person_reached,
    ROUND(budget / NULLIF(transactions_during_campaign, 0), 2) AS cost_per_transaction
FROM campaign_performance
ORDER BY revenue_during_campaign DESC;

-- 2.2 Campagnes les plus efficaces par type
SELECT 
    campaign_type,
    COUNT(*) AS campaign_count,
    ROUND(AVG(budget), 2) AS avg_budget,
    ROUND(AVG(reach), 0) AS avg_reach,
    ROUND(AVG(conversion_rate), 2) AS avg_conversion_rate,
    ROUND(SUM(budget), 2) AS total_budget_invested
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
GROUP BY campaign_type
ORDER BY avg_conversion_rate DESC;

-- 2.3 ROI marketing par région
WITH marketing_roi AS (
    SELECT 
        mc.region,
        ROUND(SUM(mc.budget), 2) AS total_marketing_budget,
        SUM(mc.reach) AS total_reach,
        ROUND(AVG(mc.conversion_rate), 2) AS avg_conversion_rate,
        ROUND(SUM(ft.amount), 2) AS total_revenue
    FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN mc
    LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft 
        ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
        AND ft.region = mc.region
    GROUP BY mc.region
)
SELECT 
    region,
    total_marketing_budget,
    total_reach,
    avg_conversion_rate,
    total_revenue,
    ROUND((total_revenue - total_marketing_budget) / NULLIF(total_marketing_budget, 0) * 100, 2) AS roi_percentage,
    ROUND(total_revenue / NULLIF(total_marketing_budget, 0), 2) AS revenue_multiplier
FROM marketing_roi
ORDER BY roi_percentage DESC;

-- 2.4 Performance des campagnes par audience cible
SELECT 
    target_audience,
    COUNT(*) AS campaign_count,
    ROUND(AVG(budget), 2) AS avg_budget,
    ROUND(AVG(reach), 0) AS avg_reach,
    ROUND(AVG(conversion_rate), 2) AS avg_conversion_rate,
    COUNT(DISTINCT region) AS regions_targeted,
    COUNT(DISTINCT product_category) AS categories_promoted
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
GROUP BY target_audience
ORDER BY avg_conversion_rate DESC;

-- 2.5 Corrélation budget marketing vs revenus par mois
WITH monthly_marketing AS (
    SELECT 
        DATE_TRUNC('month', start_date) AS month,
        ROUND(SUM(budget), 2) AS monthly_marketing_budget,
        SUM(reach) AS monthly_reach
    FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
    GROUP BY month
),
monthly_revenue AS (
    SELECT 
        DATE_TRUNC('month', transaction_date) AS month,
        ROUND(SUM(amount), 2) AS monthly_revenue,
        COUNT(*) AS monthly_transactions
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
    GROUP BY month
)
SELECT 
    COALESCE(mm.month, mr.month) AS month,
    mm.monthly_marketing_budget,
    mm.monthly_reach,
    mr.monthly_revenue,
    mr.monthly_transactions,
    ROUND(mr.monthly_revenue / NULLIF(mm.monthly_marketing_budget, 0), 2) AS revenue_per_marketing_euro
FROM monthly_marketing mm
FULL OUTER JOIN monthly_revenue mr ON mm.month = mr.month
ORDER BY month;

-- 2.6 Top 10 campagnes les plus rentables
WITH campaign_revenue AS (
    SELECT 
        mc.campaign_id,
        mc.campaign_name,
        mc.campaign_type,
        mc.budget,
        mc.reach,
        mc.conversion_rate,
        ROUND(SUM(ft.amount), 2) AS revenue_generated
    FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN mc
    LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft 
        ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
        AND ft.region = mc.region
    GROUP BY mc.campaign_id, mc.campaign_name, mc.campaign_type, mc.budget, mc.reach, mc.conversion_rate
)
SELECT 
    campaign_id,
    campaign_name,
    campaign_type,
    budget,
    reach,
    conversion_rate,
    revenue_generated,
    ROUND((revenue_generated - budget) / NULLIF(budget, 0) * 100, 2) AS roi_percentage,
    ROUND(revenue_generated / NULLIF(budget, 0), 2) AS revenue_multiplier
FROM campaign_revenue
WHERE budget > 0
ORDER BY roi_percentage DESC
LIMIT 10;

-- 2.7 Efficacité marketing par catégorie de produit
SELECT 
    mc.product_category,
    COUNT(DISTINCT mc.campaign_id) AS campaign_count,
    ROUND(SUM(mc.budget), 2) AS total_budget,
    ROUND(AVG(mc.conversion_rate), 2) AS avg_conversion_rate,
    SUM(mc.reach) AS total_reach,
    COUNT(DISTINCT pr.review_id) AS review_count,
    ROUND(AVG(pr.rating), 2) AS avg_product_rating
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN mc
LEFT JOIN ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN pr ON mc.product_category = pr.product_category
GROUP BY mc.product_category
ORDER BY total_budget DESC;

