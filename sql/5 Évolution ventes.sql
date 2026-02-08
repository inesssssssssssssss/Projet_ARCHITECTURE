-- Partie 1 : ANALYSE DE L'ÉVOLUTION DES VENTES DANS LE TEMPS

-- 1.1 Évolution mensuelle du chiffre d'affaires
SELECT 
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS nb_transactions,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    COUNT(DISTINCT entity) AS unique_customers
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY month
ORDER BY month;

-- 1.2 Évolution trimestrielle du CA
SELECT 
    DATE_TRUNC('quarter', transaction_date) AS quarter,
    COUNT(*) AS nb_transactions,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_transaction_value
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY quarter
ORDER BY quarter;

-- 1.3 Évolution annuelle du CA
SELECT 
    YEAR(transaction_date) AS year,
    COUNT(*) AS nb_transactions,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_transaction_value
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY year
ORDER BY year;

-- 1.4 Croissance mensuelle (MoM - Month over Month)
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', transaction_date) AS month,
        ROUND(SUM(amount), 2) AS revenue
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
    GROUP BY month
)
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) AS previous_month_revenue,
    ROUND(revenue - LAG(revenue) OVER (ORDER BY month), 2) AS revenue_change,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) / LAG(revenue) OVER (ORDER BY month) * 100, 2) AS growth_percentage
FROM monthly_sales
ORDER BY month;

-- 1.5 Croissance trimestrielle (QoQ - Quarter over Quarter)
WITH quarterly_sales AS (
    SELECT 
        DATE_TRUNC('quarter', transaction_date) AS quarter,
        ROUND(SUM(amount), 2) AS revenue
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
    GROUP BY quarter
)
SELECT 
    quarter,
    revenue,
    LAG(revenue) OVER (ORDER BY quarter) AS previous_quarter_revenue,
    ROUND(revenue - LAG(revenue) OVER (ORDER BY quarter), 2) AS revenue_change,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY quarter)) / LAG(revenue) OVER (ORDER BY quarter) * 100, 2) AS growth_percentage
FROM quarterly_sales
ORDER BY quarter;

-- 1.6 Saisonnalité : analyse par jour de la semaine
SELECT 
    DAYNAME(transaction_date) AS day_of_week,
    COUNT(*) AS nb_transactions,
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    ROUND(SUM(amount), 2) AS total_revenue
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
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

-- 1.7 Saisonnalité : analyse par mois de l'année
SELECT 
    MONTHNAME(transaction_date) AS month_name,
    COUNT(*) AS nb_transactions,
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    ROUND(SUM(amount), 2) AS total_revenue
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY month_name, MONTH(transaction_date)
ORDER BY MONTH(transaction_date);

-- 1.8 Analyse des tendances : moyenne mobile sur 3 mois
WITH monthly_revenue AS (
    SELECT 
        DATE_TRUNC('month', transaction_date) AS month,
        ROUND(SUM(amount), 2) AS revenue
    FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
    GROUP BY month
)
SELECT 
    month,
    revenue,
    ROUND(AVG(revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3months
FROM monthly_revenue
ORDER BY month;



-- 1.9 Peak hours/days : Meilleurs jours de vente
SELECT 
    transaction_date,
    COUNT(*) AS nb_transactions,
    ROUND(SUM(amount), 2) AS daily_revenue
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY transaction_date
ORDER BY daily_revenue DESC
LIMIT 20;
