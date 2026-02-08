-- Partie 3 : RÉPARTITION DES CLIENTS PAR SEGMENTS DÉMOGRAPHIQUES


-- 3.1 Répartition par genre
SELECT 
    gender,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(annual_income), 2) AS avg_income
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
GROUP BY gender
ORDER BY customer_count DESC;

-- 3.2 Répartition par tranche d'âge
SELECT 
    CASE 
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN '< 18'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 18 AND 25 THEN '18-25'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 26 AND 35 THEN '26-35'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 36 AND 50 THEN '36-50'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(annual_income), 2) AS avg_income
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
WHERE date_of_birth IS NOT NULL
GROUP BY CASE 
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN '< 18'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 18 AND 25 THEN '18-25'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 26 AND 35 THEN '26-35'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 36 AND 50 THEN '36-50'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END 
ORDER BY age_group;

-- 3.3 Répartition par statut marital
SELECT 
    marital_status,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(annual_income), 2) AS avg_income
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
GROUP BY marital_status
ORDER BY customer_count DESC;

-- 3.4 Répartition par tranche de revenus
SELECT 
    CASE 
        WHEN annual_income < 20000 THEN '< 20K'
        WHEN annual_income BETWEEN 20000 AND 40000 THEN '20K-40K'
        WHEN annual_income BETWEEN 40001 AND 60000 THEN '40K-60K'
        WHEN annual_income BETWEEN 60001 AND 80000 THEN '60K-80K'
        WHEN annual_income BETWEEN 80001 AND 100000 THEN '80K-100K'
        ELSE '> 100K'
    END AS income_bracket,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
WHERE annual_income IS NOT NULL
GROUP BY income_bracket
ORDER BY income_bracket;

-- 3.5 Répartition géographique : Top 20 pays
SELECT 
    country,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(annual_income), 2) AS avg_income
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
GROUP BY country
ORDER BY customer_count DESC
LIMIT 20;

-- 3.6 Répartition par région
SELECT 
    region,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(annual_income), 2) AS avg_income,
    COUNT(DISTINCT country) AS country_count
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
GROUP BY region
ORDER BY customer_count DESC;

-- 3.7 Segmentation RFM simplifiée (basée sur les interactions service client)
-- Recency, Frequency, Monetary (simulation)
WITH customer_activity AS (
    SELECT 
        cd.customer_id,
        cd.gender,
        cd.annual_income,
        DATEDIFF(year, cd.date_of_birth, CURRENT_DATE()) AS age,
        cd.marital_status,
        cd.region,
        cd.country
    FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN cd
)
SELECT 
    CASE 
        WHEN age < 25 THEN 'Young'
        WHEN age BETWEEN 25 AND 45 THEN 'Adult'
        WHEN age BETWEEN 46 AND 65 THEN 'Middle-aged'
        ELSE 'Senior'
    END AS age_segment,
    CASE 
        WHEN annual_income < 40000 THEN 'Low income'
        WHEN annual_income BETWEEN 40000 AND 80000 THEN 'Medium income'
        ELSE 'High income'
    END AS income_segment,
    gender,
    COUNT(*) AS customer_count,
    ROUND(AVG(annual_income), 2) AS avg_income
FROM customer_activity
WHERE age IS NOT NULL AND annual_income IS NOT NULL
GROUP BY age_segment, income_segment, gender
ORDER BY customer_count DESC;

-- 3.8 Profil type par région (persona)
SELECT 
    region,
    COUNT(*) AS customer_count,
    ROUND(AVG(DATEDIFF(year, date_of_birth, CURRENT_DATE())), 0) AS avg_age,
    ROUND(AVG(annual_income), 2) AS avg_income,
    MODE(gender) AS dominant_gender,
    MODE(marital_status) AS dominant_marital_status,
    COUNT(DISTINCT country) AS country_diversity
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
WHERE date_of_birth IS NOT NULL
GROUP BY region
ORDER BY customer_count DESC;

-- 3.9 Matrice de segmentation : Âge x Revenu
SELECT 
    CASE 
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 25 THEN 'Young'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 25 AND 45 THEN 'Adult'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 46 AND 65 THEN 'Middle-aged'
        ELSE 'Senior'
    END AS age_segment,
    CASE 
        WHEN annual_income < 40000 THEN 'Low'
        WHEN annual_income BETWEEN 40000 AND 80000 THEN 'Medium'
        ELSE 'High'
    END AS income_segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_of_total
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
WHERE date_of_birth IS NOT NULL AND annual_income IS NOT NULL
GROUP BY age_segment, income_segment
ORDER BY customer_count DESC;

-- 3.10 Diversité démographique par pays
SELECT 
    country,
    COUNT(*) AS customer_count,
    COUNT(DISTINCT gender) AS gender_diversity,
    ROUND(AVG(annual_income), 2) AS avg_income,
    ROUND(STDDEV(annual_income), 2) AS income_std_dev,
    MIN(DATEDIFF(year, date_of_birth, CURRENT_DATE())) AS youngest_age,
    MAX(DATEDIFF(year, date_of_birth, CURRENT_DATE())) AS oldest_age
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
WHERE date_of_birth IS NOT NULL
GROUP BY country
HAVING COUNT(*) >= 10
ORDER BY customer_count DESC
LIMIT 20;



-- ANALYSES CROISÉES AVANCÉES

-- 4.1 Corrélation ventes par région et campagnes marketing
SELECT 
    ft.region,
    ROUND(SUM(ft.amount), 2) AS total_revenue,
    COUNT(DISTINCT mc.campaign_id) AS campaign_count,
    ROUND(SUM(mc.budget), 2) AS marketing_budget,
    ROUND(SUM(ft.amount) / NULLIF(SUM(mc.budget), 0), 2) AS revenue_per_marketing_euro
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft
LEFT JOIN ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN mc ON ft.region = mc.region
GROUP BY ft.region
ORDER BY total_revenue DESC;



-- 4.2 Performance logistique par région
SELECT 
    destination_region,
    COUNT(*) AS shipment_count,
    ROUND(AVG(shipping_cost), 2) AS avg_shipping_cost,
    ROUND(AVG(DATEDIFF(day, ship_date, estimated_delivery)), 1) AS avg_delivery_days,
    COUNT(DISTINCT carrier) AS carrier_count,
    MODE(status) AS most_common_status
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
GROUP BY destination_region
ORDER BY shipment_count DESC;

