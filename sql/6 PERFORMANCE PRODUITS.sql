
-- Partie 2 : PERFORMANCE PAR PRODUIT, CATÉGORIE ET RÉGION


-- 2.1 ANALYSE PAR PRODUIT (via ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN + ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN)
-- Top 20 produits par nombre d'avis
SELECT 
    pr.product_id,
    COUNT(*) AS review_count,
    ROUND(AVG(pr.rating), 2) AS avg_rating,
    SUM(pr.helpful_votes) AS total_helpful_votes,
    SUM(pr.total_votes) AS total_votes
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN pr
LEFT JOIN ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i ON pr.product_id = i.product_id
GROUP BY pr.product_id
ORDER BY review_count DESC
LIMIT 20;

-- Produits les mieux notés (min 10 avis)
SELECT 
    pr.product_id,
    
    COUNT(*) AS review_count,
    ROUND(AVG(pr.rating), 2) AS avg_rating,
  
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN pr
LEFT JOIN ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i ON pr.product_id = i.product_id
GROUP BY pr.product_id
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC, review_count DESC
LIMIT 20;

-- Produits les moins bien notés (problématiques)
SELECT 
    pr.product_id,
    COUNT(*) AS review_count,
    ROUND(AVG(pr.rating), 2) AS avg_rating
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN pr
LEFT JOIN ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i ON pr.product_id = i.product_id
GROUP BY pr.product_id, i.product_category
HAVING COUNT(*) >= 5
ORDER BY avg_rating ASC
LIMIT 20;


-- 2.2 ANALYSE PAR CATÉGORIE DE PRODUITS

-- Performance des catégories par notes moyennes
SELECT 
    product_category AS category,
    COUNT(*) AS review_count,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(DISTINCT reviewer_id) AS unique_reviewers
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN
GROUP BY category
ORDER BY avg_rating DESC;

-- Catégories avec le plus de stock
SELECT 
    product_category,
    COUNT(DISTINCT product_id) AS product_count,
    SUM(current_stock) AS total_stock,
    ROUND(AVG(current_stock), 0) AS avg_stock_per_product,
    COUNT(DISTINCT warehouse) AS warehouse_count
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN
GROUP BY product_category
ORDER BY total_stock DESC;

-- Catégories en promotion
SELECT 
    product_category,
    COUNT(*) AS promo_count,
    ROUND(AVG(discount_percentage), 2) AS avg_discount,
    MIN(start_date) AS first_promo,
    MAX(end_date) AS last_promo
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN
GROUP BY product_category
ORDER BY promo_count DESC;

-- Catégories ciblées par les campagnes marketing
SELECT 
    product_category,
    COUNT(*) AS campaign_count,
    ROUND(SUM(budget), 2) AS total_budget,
    ROUND(AVG(conversion_rate), 2) AS avg_conversion_rate,
    ROUND(AVG(reach), 0) AS avg_reach
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
GROUP BY product_category
ORDER BY total_budget DESC;

-- Performance croisée : catégories avec notes + stock + promos
SELECT 
    i.product_category,
    COUNT(DISTINCT i.product_id) AS product_count,
    SUM(i.current_stock) AS total_stock
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i
LEFT JOIN ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN pr ON i.product_id = pr.product_id
LEFT JOIN ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p ON i.product_category = p.product_category
GROUP BY i.product_category
ORDER BY total_stock DESC;


-- 2.3 ANALYSE PAR RÉGION

-- Performance des ventes par région
SELECT 
    region,
    COUNT(*) AS nb_transactions,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    COUNT(DISTINCT entity) AS unique_customers
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN
GROUP BY region
ORDER BY total_revenue DESC;

-- Distribution du stock par région
SELECT 
    region,
    COUNT(DISTINCT warehouse) AS warehouse_count,
    COUNT(DISTINCT product_id) AS product_count,
    SUM(current_stock) AS total_stock,
    ROUND(AVG(current_stock), 0) AS avg_stock_per_product
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN
GROUP BY region
ORDER BY total_stock DESC;

-- Magasins par région
SELECT 
    region,
    COUNT(*) AS store_count,
    SUM(employee_count) AS total_employees,
    ROUND(SUM(square_footage), 2) AS total_square_footage,
    ROUND(AVG(employee_count), 0) AS avg_employees_per_store
FROM ANYCOMPANY_LAB.SILVER.STORE_LOCATIONS_CLEAN
GROUP BY region
ORDER BY store_count DESC;

-- Livraisons par région de destination
SELECT 
    destination_region,
    COUNT(*) AS shipment_count,
    ROUND(SUM(shipping_cost), 2) AS total_shipping_cost,
    ROUND(AVG(shipping_cost), 2) AS avg_shipping_cost,
    COUNT(DISTINCT carrier) AS carrier_count
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
GROUP BY destination_region
ORDER BY shipment_count DESC;

-- Promotions par région
SELECT 
    region,
    COUNT(*) AS promo_count,
    ROUND(AVG(discount_percentage), 2) AS avg_discount,
    COUNT(DISTINCT product_category) AS categories_promoted
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN
GROUP BY region
ORDER BY promo_count DESC;

-- Campagnes marketing par région
SELECT 
    region,
    COUNT(*) AS campaign_count,
    ROUND(SUM(budget), 2) AS total_budget,
    ROUND(AVG(conversion_rate), 2) AS avg_conversion_rate,
    SUM(reach) AS total_reach
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN
GROUP BY region
ORDER BY total_budget DESC;


