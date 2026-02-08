

USE SCHEMA silver;


-- 1. CUSTOMER_DEMOGRAPHICS – Données démographiques clients

-- Périmètre métier : Profil et segmentation client
-- Colonnes clés : customer_id (PK), name, date_of_birth, gender, region, country, city, marital_status, annual_income

-- Période couverte (dates de naissance)
SELECT 'Période dates de naissance' AS metric, 
       MIN(date_of_birth) AS date_min, 
       MAX(date_of_birth) AS date_max 
from ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
WHERE date_of_birth IS NOT NULL;


-- Distribution par genre
SELECT gender, COUNT(*) AS count, 
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
from ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
GROUP BY gender
ORDER BY count DESC;

-- Distribution par tranche d'âge
SELECT 
    CASE 
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN '< 18'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 18 AND 25 THEN '18-25'   
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 26 AND 35 THEN '26-35'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 36 AND 50 THEN '36-50'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END AS age_group,
    COUNT(*) AS count
from ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
WHERE date_of_birth IS NOT NULL
GROUP BY  CASE 
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN '< 18'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 18 AND 25 THEN '18-25'   
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 26 AND 35 THEN '26-35'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 36 AND 50 THEN '36-50'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END
ORDER BY age_group;



-- Top 10 régions
SELECT region, COUNT(*) AS customer_count
from ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
GROUP BY region
ORDER BY customer_count DESC
LIMIT 10;

-- Top 10 pays
SELECT country, COUNT(*) AS customer_count
from ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
GROUP BY country
ORDER BY customer_count DESC
LIMIT 10;



-- Distribution des revenus par tranches
SELECT 
    CASE 
        WHEN annual_income < 20000 THEN '< 20K'
        WHEN annual_income BETWEEN 20000 AND 40000 THEN '20K-40K'
        WHEN annual_income BETWEEN 40001 AND 60000 THEN '40K-60K'
        WHEN annual_income BETWEEN 60001 AND 80000 THEN '60K-80K'
        WHEN annual_income BETWEEN 80001 AND 100000 THEN '80K-100K'
        ELSE '> 100K'
    END AS income_bracket,
    COUNT(*) AS count
from ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN
WHERE annual_income IS NOT NULL
GROUP BY income_bracket
ORDER BY income_bracket;
 



-- 2. CUSTOMER_SERVICE_INTERACTIONS – Interactions service client


-- Périmètre métier : Support client et satisfaction
-- Colonnes clés : interaction_id (PK), interaction_date, interaction_type, issue_category, description, duration_minutes, resolution_status, follow_up_required, customer_satisfaction



-- Période couverte
SELECT 'Période' AS metric, 
       MIN(interaction_date) AS date_min, 
       MAX(interaction_date) AS date_max 
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN;


-- Distribution par type d'interaction
SELECT interaction_type, COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
GROUP BY interaction_type
ORDER BY count DESC;



-- Distribution par statut de résolution
SELECT resolution_status, COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
GROUP BY resolution_status
ORDER BY count DESC;

-- Satisfaction moyenne par catégorie de problème
SELECT issue_category, 
       ROUND(AVG(customer_satisfaction), 2) AS avg_satisfaction,
       COUNT(*) AS total_interactions,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_interactions 

FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
WHERE customer_satisfaction IS NOT NULL
GROUP BY issue_category
ORDER BY avg_satisfaction DESC
LIMIT 10;


-- Durée moyenne d'interaction par type
SELECT interaction_type,
       ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
       COUNT(*) AS count
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
WHERE duration_minutes IS NOT NULL
GROUP BY interaction_type
ORDER BY avg_duration_min DESC;

-- Taux de follow-up requis
SELECT follow_up_required,
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
GROUP BY follow_up_required;

-- Tendance mensuelle des interactions
SELECT 
    DATE_TRUNC('month', interaction_date) AS month,
    COUNT(*) AS interaction_count,
    ROUND(AVG(customer_satisfaction), 2) AS avg_satisfaction
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
GROUP BY month
ORDER BY month;



-- 3. FINANCIAL_TRANSACTIONS – Transactions de ventes


-- Périmètre métier : Ventes, revenus et analyse financière
-- Colonnes clés : transaction_id (PK), transaction_date, transaction_type, amount, payment_method, entity, region, account_code


-- Volume total

-- Période couverte
SELECT 'Période' AS metric, 
       MIN(transaction_date) AS date_min, 
       MAX(transaction_date) AS date_max 
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN;


-- Distribution par type de transaction
SELECT transaction_type, 
       COUNT(*) AS transaction_count,
       ROUND(SUM(amount), 2) AS total_amount,
       ROUND(AVG(amount), 2) AS avg_amount
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY transaction_type
ORDER BY total_amount DESC;

-- Distribution par méthode de paiement
SELECT payment_method, 
       COUNT(*) AS transaction_count,
       ROUND(SUM(amount), 2) AS total_amount,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY payment_method
ORDER BY total_amount DESC;

-- Top 10 entités par montant
SELECT entity, 
       COUNT(*) AS nb_transactions,
       ROUND(SUM(amount), 2) AS total_amount
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY entity
ORDER BY total_amount DESC
LIMIT 10;
-- Montant total par région
SELECT region,
       COUNT(*) AS transaction_count,
       ROUND(SUM(amount), 2) AS total_amount
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY region
ORDER BY total_amount DESC;

-- Tendance mensuelle du CA
SELECT 
    DATE_TRUNC('month', transaction_date) AS month,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS monthly_revenue
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY month
ORDER BY month;




-- 4. PRODUCT_REVIEWS – Avis et notes produits


-- Périmètre métier : Satisfaction produit et feedback client
-- Colonnes clés : review_id (PK), product_id, reviewer_id, reviewer_name, rating, review_date, review_title, review_text, product_category



-- Période couverte
SELECT 'Période' AS metric, 
       MIN(review_datetime) AS date_min, 
       MAX(review_datetime) AS date_max 
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN;


-- Distribution des notes
SELECT rating, 
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN
GROUP BY rating
ORDER BY rating DESC;

-- Note moyenne globale
SELECT 'Note moyenne' AS metric, 
       ROUND(AVG(rating), 2) AS value
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN;


-- Distribution par catégorie
SELECT PRODUCT_CATEGORY, 
       COUNT(*) AS review_count,
       ROUND(AVG(rating), 2) AS avg_rating
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN
GROUP BY PRODUCT_CATEGORY
ORDER BY review_count DESC
LIMIT 10;


-- Top reviewers (les plus actifs)
SELECT reviewer_name,
       COUNT(*) AS review_count,
       ROUND(AVG(rating), 2) AS avg_rating_given
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN
GROUP BY reviewer_name
ORDER BY review_count DESC
LIMIT 10;



-- 5. INVENTORY – Niveaux de stock


-- Périmètre métier : Gestion des stocks
-- Colonnes clés : product_id, product_category, region, country, warehouse, current_stock, reorder_point, lead_time, last_restock_date


-- Période du dernier restock
SELECT 'Période restockage' AS metric, 
       MIN(last_restock_date) AS date_min, 
       MAX(last_restock_date) AS date_max 
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN;


-- Stock total par catégorie
SELECT product_category, 
       SUM(current_stock) AS total_stock,
       COUNT(DISTINCT product_id) AS product_count,
       ROUND(AVG(current_stock), 0) AS avg_stock_per_product
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN
GROUP BY product_category
ORDER BY total_stock DESC
LIMIT 10;



-- Distribution par pays (top 10)
SELECT country,
       COUNT(DISTINCT warehouse) AS warehouse_count,
       SUM(current_stock) AS total_stock
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN
GROUP BY country
ORDER BY total_stock DESC
LIMIT 10;

-- Lead time moyen par région
SELECT country,
       ROUND(AVG(lead_time), 1) AS avg_lead_time_days,
       ROUND(MIN(lead_time), 0) AS min_lead_time,
       ROUND(MAX(lead_time), 0) AS max_lead_time
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN
GROUP BY country
ORDER BY avg_lead_time_days;



-- 6. STORE_LOCATIONS – Informations géographiques des magasins


-- Périmètre métier : Réseau de distribution physique
-- Colonnes clés : store_id (PK), store_name, store_type, region, country, city, address, postal_code, square_footage, employee_count


-- Distribution par type de magasin
SELECT store_type, 
       COUNT(*) AS store_count,
       ROUND(AVG(square_footage), 2) AS avg_square_footage,
       ROUND(AVG(employee_count), 0) AS avg_employees,
       SUM(employee_count) AS total_employees
FROM ANYCOMPANY_LAB.SILVER.STORE_LOCATIONS_CLEAN
GROUP BY store_type
ORDER BY store_count DESC;

-- Distribution par région
SELECT region, 
       COUNT(*) AS store_count,
       SUM(employee_count) AS total_employees,
       ROUND(SUM(square_footage), 2) AS total_square_footage
FROM ANYCOMPANY_LAB.SILVER.STORE_LOCATIONS_CLEAN
GROUP BY region
ORDER BY store_count DESC;

-- Top 10 pays par nombre de magasins
SELECT country, 
       COUNT(*) AS store_count,
       SUM(employee_count) AS total_employees
FROM ANYCOMPANY_LAB.SILVER.STORE_LOCATIONS_CLEAN
GROUP BY country
ORDER BY store_count DESC
LIMIT 10;



-- Effectifs totaux
SELECT 
    SUM(employee_count) AS total_employees,
    ROUND(AVG(employee_count), 0) AS avg_employees_per_store
FROM ANYCOMPANY_LAB.SILVER.STORE_LOCATIONS_CLEAN;



-- 7. SUPPLIER_INFORMATION – Informations fournisseurs


-- Périmètre métier : Chaîne d'approvisionnement
-- Colonnes clés : supplier_id (PK), supplier_name, product_category, region, country, city, lead_time, reliability_score, quality_rating



-- Distribution par pays (top 10)
SELECT country, 
       COUNT(*) AS supplier_count,
       ROUND(AVG(reliability_score), 2) AS avg_reliability,
    ROUND(AVG(lead_time), 1) AS avg_lead_time

FROM ANYCOMPANY_LAB.SILVER.SUPPLIER_INFORMATION_CLEAN
GROUP BY country
ORDER BY supplier_count DESC
LIMIT 10;


-- Lead time moyen par catégorie produit
SELECT product_category,
       ROUND(AVG(lead_time), 1) AS avg_lead_time_days,
       COUNT(*) AS supplier_count
FROM ANYCOMPANY_LAB.SILVER.SUPPLIER_INFORMATION_CLEAN
GROUP BY product_category
ORDER BY avg_lead_time_days DESC
LIMIT 10;

-- Fournisseurs les plus fiables (score > 0.85)
SELECT 'Fournisseurs très fiables (>0.85)' AS metric,
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ANYCOMPANY_LAB.SILVER.SUPPLIER_INFORMATION_CLEAN), 2) AS percentage
FROM ANYCOMPANY_LAB.SILVER.SUPPLIER_INFORMATION_CLEAN
WHERE reliability_score > 0.85;

-- Distribution des scores de fiabilité
SELECT 
    CASE 
        WHEN reliability_score < 0.5 THEN '< 0.5 (Faible)'
        WHEN reliability_score BETWEEN 0.5 AND 0.7 THEN '0.5-0.7 (Moyen)'
        WHEN reliability_score BETWEEN 0.71 AND 0.85 THEN '0.71-0.85 (Bon)'
        WHEN reliability_score BETWEEN 0.86 AND 0.95 THEN '0.86-0.95 (Très bon)'
        ELSE '> 0.95 (Excellent)'
    END AS reliability_category,
    COUNT(*) AS count
FROM ANYCOMPANY_LAB.SILVER.SUPPLIER_INFORMATION_CLEAN
GROUP BY reliability_category
ORDER BY reliability_category;



-- 8. PROMOTIONS_DATA – Données de promotions


-- Périmètre métier : Campagnes promotionnelles et réductions
-- Colonnes clés : promotion_id (PK), product_category, promotion_type,discount_percentage, start_date, end_date, region


-- Période couverte
SELECT 'Période' AS metric, 
       MIN(start_date) AS date_min, 
       MAX(end_date) AS date_max 
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN;

-- Distribution par catégorie produit
SELECT product_category, 
       COUNT(*) AS promo_count,
       ROUND(AVG(discount_percentage), 2) AS avg_discount
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN
GROUP BY product_category
ORDER BY promo_count DESC
LIMIT 10;



-- Distribution des réductions par tranches
SELECT 
    CASE 
        WHEN discount_percentage < 10 THEN '< 10%'
        WHEN discount_percentage BETWEEN 10 AND 20 THEN '10-20%'
        WHEN discount_percentage BETWEEN 21 AND 30 THEN '21-30%'
        WHEN discount_percentage BETWEEN 31 AND 50 THEN '31-50%'
        ELSE '> 50%'
    END AS discount_range,
    COUNT(*) AS count
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN
GROUP BY discount_range
ORDER BY discount_range;

-- Promotions actives actuellement
SELECT 'Promotions actives' AS metric,
       COUNT(*) AS count
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN
WHERE CURRENT_DATE() BETWEEN start_date AND end_date;

-- Durée moyenne des promotions
SELECT 
    ROUND(AVG(DATEDIFF(day, start_date, end_date)), 1) AS avg_duration_days,
    ROUND(MIN(DATEDIFF(day, start_date, end_date)), 0) AS min_duration,
    ROUND(MAX(DATEDIFF(day, start_date, end_date)), 0) AS max_duration
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN;



-- 9. MARKETING_CAMPAIGNS – Campagnes marketing


-- Périmètre métier : Actions marketing et ROI
-- Colonnes clés : campaign_id (PK), campaign_name, campaign_type, product_category,target_audience, start_date, end_date, region, budget, reach, conversion_rate


-- Période couverte
SELECT 'Période' AS metric, 
       MIN(start_date) AS date_min, 
       MAX(end_date) AS date_max 
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN;


-- Distribution par type de campagne
SELECT campaign_type, 
       COUNT(*) AS campaign_count,
       ROUND(SUM(budget), 2) AS total_budget,
       ROUND(AVG(conversion_rate), 2) AS avg_conversion_rate
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
GROUP BY campaign_type
ORDER BY total_budget DESC;

-- Distribution par région
SELECT region,
       COUNT(*) AS campaign_count,
       ROUND(SUM(budget), 2) AS total_budget
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
GROUP BY region
ORDER BY total_budget DESC;



-- Performance moyenne par type de campagne
SELECT campaign_type,
       COUNT(*) AS campaign_count,
       ROUND(AVG(reach), 0) AS avg_reach,
       ROUND(AVG(conversion_rate), 2) AS avg_conversion_rate,
       ROUND(AVG(budget), 2) AS avg_budget
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
GROUP BY campaign_type
ORDER BY avg_conversion_rate DESC;

-- Top 10 campagnes par reach
SELECT campaign_name, 
       campaign_type,
       reach,
       conversion_rate,
       budget
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
ORDER BY reach DESC
LIMIT 10;


-- Distribution par audience cible
SELECT target_audience,
       COUNT(*) AS campaign_count,
       ROUND(AVG(conversion_rate), 2) AS avg_conversion_rate
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
GROUP BY target_audience
ORDER BY campaign_count DESC;

-- Campagnes actives actuellement
SELECT 'Campagnes actives' AS metric,
       COUNT(*) AS count
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
WHERE CURRENT_DATE() BETWEEN start_date AND end_date;



-- 10. LOGISTICS_AND_SHIPPING – Données logistiques


-- Périmètre métier : Livraisons et transport
-- Colonnes clés : shipment_id (PK), order_id, ship_date, estimated_delivery, shipping_method, status, shipping_cost, destination_region, destination_country, carrier


-- Période couverte
SELECT 'Période' AS metric, 
       MIN(ship_date) AS date_min, 
       MAX(estimated_delivery) AS date_max 
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN;


-- Distribution par transporteur
SELECT carrier, 
       COUNT(*) AS shipment_count,
       ROUND(SUM(shipping_cost), 2) AS total_cost,
       ROUND(AVG(shipping_cost), 2) AS avg_cost
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
GROUP BY carrier
ORDER BY shipment_count DESC;

-- Distribution par statut de livraison
SELECT status, 
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
GROUP BY status
ORDER BY count DESC;

-- Distribution par méthode d'expédition
SELECT shipping_method,
       COUNT(*) AS shipment_count,
       ROUND(AVG(shipping_cost), 2) AS avg_cost
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
GROUP BY shipping_method
ORDER BY shipment_count DESC;



-- Top 10 pays de destination
SELECT destination_country,
       COUNT(*) AS shipment_count,
       ROUND(AVG(shipping_cost), 2) AS avg_cost
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
GROUP BY destination_country
ORDER BY shipment_count DESC
LIMIT 10;

-- Délai moyen de livraison par transporteur
SELECT carrier,
       ROUND(AVG(DATEDIFF(day, ship_date, estimated_delivery)), 1) AS avg_delivery_days,
       COUNT(*) AS shipment_count
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
WHERE estimated_delivery IS NOT NULL
GROUP BY carrier
ORDER BY avg_delivery_days desc;

-- Tendance mensuelle des expéditions
SELECT 
    DATE_TRUNC('month', ship_date) AS month,
    COUNT(*) AS shipment_count,
    ROUND(SUM(shipping_cost), 2) AS total_cost
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
GROUP BY month
ORDER BY month;



-- 11. EMPLOYEE_RECORDS – Données organisationnelles


-- Périmètre métier : Ressources humaines et organisation
-- Colonnes clés : employee_id (PK), name, date_of_birth, hire_date, department, job_title, salary, region, country, email


-- Période couverte
SELECT 'Période embauches' AS metric, 
       MIN(hire_date) AS date_min, 
       MAX(hire_date) AS date_max 
FROM ANYCOMPANY_LAB.SILVER.EMPLOYEE_RECORDS_CLEAN;

-- Distribution par département
SELECT department, 
       COUNT(*) AS employee_count,
       ROUND(AVG(salary), 2) AS avg_salary,
       ROUND(SUM(salary), 2) AS total_salary
FROM ANYCOMPANY_LAB.SILVER.EMPLOYEE_RECORDS_CLEAN
GROUP BY department
ORDER BY employee_count DESC;

-- Distribution par poste
SELECT job_title, 
       COUNT(*) AS count,
       ROUND(AVG(salary), 2) AS avg_salary
FROM ANYCOMPANY_LAB.SILVER.EMPLOYEE_RECORDS_CLEAN
GROUP BY job_title
ORDER BY count DESC
LIMIT 10;

-- Distribution par région
SELECT region,
       COUNT(*) AS employee_count,
       ROUND(AVG(salary), 2) AS avg_salary
FROM ANYCOMPANY_LAB.SILVER.EMPLOYEE_RECORDS_CLEAN
GROUP BY region
ORDER BY employee_count DESC;

-- Top 10 pays par effectifs
SELECT country,
       COUNT(*) AS employee_count,
       ROUND(AVG(salary), 2) AS avg_salary
FROM ANYCOMPANY_LAB.SILVER.EMPLOYEE_RECORDS_CLEAN
GROUP BY country
ORDER BY employee_count DESC
LIMIT 10;


-- Distribution salariale par tranches
SELECT 
    CASE 
        WHEN salary < 30000 THEN '< 30K'
        WHEN salary BETWEEN 30000 AND 50000 THEN '30K-50K'
        WHEN salary BETWEEN 50001 AND 70000 THEN '50K-70K'
        WHEN salary BETWEEN 70001 AND 100000 THEN '70K-100K'
        ELSE '> 100K'
    END AS salary_bracket,
    COUNT(*) AS count
FROM ANYCOMPANY_LAB.SILVER.EMPLOYEE_RECORDS_CLEAN
GROUP BY salary_bracket
ORDER BY salary_bracket;

-- Ancienneté moyenne
SELECT 
    ROUND(AVG(DATEDIFF(year, hire_date, CURRENT_DATE())), 1) AS avg_years_tenure,
    ROUND(MIN(DATEDIFF(year, hire_date, CURRENT_DATE())), 0) AS min_years,
    ROUND(MAX(DATEDIFF(year, hire_date, CURRENT_DATE())), 0) AS max_years
FROM ANYCOMPANY_LAB.SILVER.EMPLOYEE_RECORDS_CLEAN;

-- Distribution par tranche d'âge
SELECT 
    CASE 
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 25 THEN '< 25'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 25 AND 35 THEN '25-35'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 36 AND 45 THEN '36-45'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 46 AND 55 THEN '46-55'
        ELSE '> 55'
    END AS age_group,
    COUNT(*) AS count
FROM ANYCOMPANY_LAB.SILVER.EMPLOYEE_RECORDS_CLEAN
WHERE date_of_birth IS NOT NULL
GROUP BY age_group
ORDER BY age_group;

-- Tendance des embauches par année
SELECT 
    YEAR(hire_date) AS hire_year,
    COUNT(*) AS new_hires
FROM ANYCOMPANY_LAB.SILVER.EMPLOYEE_RECORDS_CLEAN
GROUP BY hire_year
ORDER BY hire_year DESC
LIMIT 10;


