-- PHASE 2.3 – ANALYSES BUSINESS TRANSVERSES
-- Analyses cross-fonctionnelles pour insights marketing et opérationnels


--USE SCHEMA SILVER;


-- THÈME 1 : VENTES ET PROMOTIONS


-- 1.1 Comparaison des ventes avec/sans promotion par catégorie
-- On considère qu'une vente est "avec promo" si elle a lieu pendant une période promotionnelle active
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
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft
    LEFT JOIN ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p 
        ON ft.transaction_date BETWEEN p.start_date AND p.end_date
        AND ft.region = p.region
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
ORDER BY total_revenue DESC;

-- 1.2 Impact des promotions par niveau de réduction
WITH promo_sales AS (
    SELECT 
        ft.transaction_id,
        ft.amount,
        p.discount_percentage,
        p.product_category,
        CASE 
            WHEN p.discount_percentage < 10 THEN '< 10%'
            WHEN p.discount_percentage BETWEEN 10 AND 20 THEN '10-20%'
            WHEN p.discount_percentage BETWEEN 21 AND 30 THEN '21-30%'
            WHEN p.discount_percentage BETWEEN 31 AND 50 THEN '31-50%'
            ELSE '> 50%'
        END AS discount_range
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft
    INNER JOIN ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p 
        ON ft.transaction_date BETWEEN p.start_date AND p.end_date
        AND ft.region = p.region
)
SELECT 
    discount_range,
    COUNT(*) AS nb_transactions,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    ROUND(AVG(discount_percentage), 2) AS avg_discount_pct
FROM promo_sales
GROUP BY discount_range
ORDER BY discount_range;

-- 1.3 Sensibilité des catégories aux promotions
-- Analyse de la performance des catégories avec promotions vs baseline
WITH category_baseline AS (
    -- Revenus sans promotion par catégorie
    SELECT 
        i.product_category,
        COUNT(DISTINCT ft.transaction_id) AS transactions_no_promo,
        ROUND(AVG(ft.amount), 2) AS avg_value_no_promo
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft
    CROSS JOIN ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i
    LEFT JOIN ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p 
        ON ft.transaction_date BETWEEN p.start_date AND p.end_date
        AND ft.region = p.region
        AND i.product_category = p.product_category
    WHERE p.promotion_id IS NULL
    GROUP BY i.product_category
),
category_with_promo AS (
    -- Revenus avec promotion par catégorie
    SELECT 
        p.product_category,
        COUNT(DISTINCT ft.transaction_id) AS transactions_with_promo,
        ROUND(AVG(ft.amount), 2) AS avg_value_with_promo,
        ROUND(AVG(p.discount_percentage), 2) AS avg_discount
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft
    INNER JOIN ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p 
        ON ft.transaction_date BETWEEN p.start_date AND p.end_date
        AND ft.region = p.region
    GROUP BY p.product_category
)
SELECT 
    COALESCE(cb.product_category, cp.product_category) AS category,
    cb.transactions_no_promo,
    cb.avg_value_no_promo,
    cp.transactions_with_promo,
    cp.avg_value_with_promo,
    cp.avg_discount,
    ROUND((cp.avg_value_with_promo - cb.avg_value_no_promo) / NULLIF(cb.avg_value_no_promo, 0) * 100, 2) AS lift_percentage,
    CASE 
        WHEN (cp.avg_value_with_promo - cb.avg_value_no_promo) / NULLIF(cb.avg_value_no_promo, 0) * 100 > 20 THEN 'Très sensible'
        WHEN (cp.avg_value_with_promo - cb.avg_value_no_promo) / NULLIF(cb.avg_value_no_promo, 0) * 100 BETWEEN 5 AND 20 THEN 'Sensible'
        WHEN (cp.avg_value_with_promo - cb.avg_value_no_promo) / NULLIF(cb.avg_value_no_promo, 0) * 100 BETWEEN -5 AND 5 THEN 'Neutre'
        ELSE 'Peu sensible'
    END AS sensitivity_level
FROM category_baseline cb
FULL OUTER JOIN category_with_promo cp ON cb.product_category = cp.product_category
ORDER BY lift_percentage DESC NULLS LAST;

-- 1.4 ROI des promotions par catégorie
SELECT 
    p.product_category,
    COUNT(DISTINCT p.promotion_id) AS promo_count,
    ROUND(AVG(p.discount_percentage), 2) AS avg_discount,
    COUNT(DISTINCT ft.transaction_id) AS transactions_during_promo,
    ROUND(SUM(ft.amount), 2) AS revenue_during_promo,
    ROUND(AVG(ft.amount), 2) AS avg_transaction_value
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft 
    ON ft.transaction_date BETWEEN p.start_date AND p.end_date
    AND ft.region = p.region
GROUP BY p.product_category
ORDER BY revenue_during_promo DESC;

-- 1.5 Timing optimal des promotions (jour de la semaine)
SELECT 
    DAYNAME(ft.transaction_date) AS day_of_week,
    COUNT(DISTINCT CASE WHEN p.promotion_id IS NOT NULL THEN ft.transaction_id END) AS transactions_with_promo,
    COUNT(DISTINCT CASE WHEN p.promotion_id IS NULL THEN ft.transaction_id END) AS transactions_without_promo,
    ROUND(SUM(CASE WHEN p.promotion_id IS NOT NULL THEN ft.amount ELSE 0 END), 2) AS revenue_with_promo,
    ROUND(SUM(CASE WHEN p.promotion_id IS NULL THEN ft.amount ELSE 0 END), 2) AS revenue_without_promo
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft
LEFT JOIN ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p 
    ON ft.transaction_date BETWEEN p.start_date AND p.end_date
    AND ft.region = p.region
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
    END;

-- 1.6 Durée optimale des promotions
SELECT 
    DATEDIFF(day, start_date, end_date) AS promo_duration_days,
    COUNT(*) AS promo_count,
    ROUND(AVG(discount_percentage), 2) AS avg_discount,
    COUNT(DISTINCT product_category) AS categories_count
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN
GROUP BY promo_duration_days
HAVING promo_count >= 5
ORDER BY promo_duration_days;
