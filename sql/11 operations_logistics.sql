-- THÈME 4 : OPÉRATIONS ET LOGISTIQUE



-- 4.1 Analyse des ruptures de stock
SELECT 
    product_category,
    region,
    COUNT(*) AS total_products,
    COUNT(CASE WHEN current_stock < reorder_point THEN 1 END) AS products_below_reorder,
    COUNT(CASE WHEN current_stock = 0 THEN 1 END) AS products_out_of_stock,
    ROUND(COUNT(CASE WHEN current_stock < reorder_point THEN 1 END) * 100.0 / COUNT(*), 2) AS stockout_rate,
    ROUND(AVG(current_stock), 0) AS avg_stock_level,
    ROUND(AVG(reorder_point), 0) AS avg_reorder_point
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN
GROUP BY product_category, region
ORDER BY stockout_rate DESC;

-- 4.2 Produits critiques en rupture (avec beaucoup d'avis = populaires)
SELECT 
    i.product_id,
    i.product_category,
    i.region,
    i.warehouse,
    i.current_stock,
    i.reorder_point,
    i.lead_time,
    i.last_restock_date,
    COUNT(pr.review_id) AS review_count,
    ROUND(AVG(pr.rating), 2) AS avg_rating,
    DATEDIFF(day, i.last_restock_date, CURRENT_DATE()) AS days_since_restock
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i
LEFT JOIN ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN pr ON i.product_id = pr.product_id
WHERE i.current_stock < i.reorder_point
GROUP BY i.product_id, i.product_category, i.region, i.warehouse, 
         i.current_stock, i.reorder_point, i.lead_time, i.last_restock_date
HAVING COUNT(pr.review_id) >= 5  -- Produits populaires
ORDER BY review_count DESC, current_stock ASC
LIMIT 30;

-- 4.3 Impact des délais de livraison par transporteur
SELECT 
    carrier,
    shipping_method,
    COUNT(*) AS shipment_count,
    ROUND(AVG(DATEDIFF(day, ship_date, estimated_delivery)), 1) AS avg_delivery_days,
    ROUND(MIN(DATEDIFF(day, ship_date, estimated_delivery)), 0) AS min_delivery_days,
    ROUND(MAX(DATEDIFF(day, ship_date, estimated_delivery)), 0) AS max_delivery_days,
    ROUND(AVG(shipping_cost), 2) AS avg_shipping_cost,
    COUNT(CASE WHEN status = 'Delivered' THEN 1 END) AS delivered_count,
    ROUND(COUNT(CASE WHEN status = 'Delivered' THEN 1 END) * 100.0 / COUNT(*), 2) AS delivery_success_rate
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
WHERE ship_date IS NOT NULL AND estimated_delivery IS NOT NULL
GROUP BY carrier, shipping_method
ORDER BY avg_delivery_days;

-- 4.4 Délais de livraison par région de destination
SELECT 
    destination_region,
    destination_country,
    COUNT(*) AS shipment_count,
    ROUND(AVG(DATEDIFF(day, ship_date, estimated_delivery)), 1) AS avg_delivery_days,
    ROUND(AVG(shipping_cost), 2) AS avg_shipping_cost,
    COUNT(DISTINCT carrier) AS carrier_diversity,
    MODE(status) AS most_common_status
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
WHERE ship_date IS NOT NULL AND estimated_delivery IS NOT NULL
GROUP BY destination_region, destination_country
HAVING COUNT(*) >= 10
ORDER BY avg_delivery_days DESC
LIMIT 20;

-- 4.5 Corrélation délai livraison vs coût
WITH delivery_stats AS (
    SELECT 
        shipment_id,
        DATEDIFF(day, ship_date, estimated_delivery) AS delivery_days,
        shipping_cost,
        carrier,
        destination_region,
        CASE 
            WHEN DATEDIFF(day, ship_date, estimated_delivery) <= 2 THEN 'Express (≤2 jours)'
            WHEN DATEDIFF(day, ship_date, estimated_delivery) BETWEEN 3 AND 5 THEN 'Standard (3-5 jours)'
            WHEN DATEDIFF(day, ship_date, estimated_delivery) BETWEEN 6 AND 10 THEN 'Économique (6-10 jours)'
            ELSE 'Lent (>10 jours)'
        END AS delivery_speed_category
    FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
    WHERE ship_date IS NOT NULL AND estimated_delivery IS NOT NULL
)
SELECT 
    delivery_speed_category,
    COUNT(*) AS shipment_count,
    ROUND(AVG(delivery_days), 1) AS avg_days,
    ROUND(AVG(shipping_cost), 2) AS avg_cost,
    ROUND(MIN(shipping_cost), 2) AS min_cost,
    ROUND(MAX(shipping_cost), 2) AS max_cost,
    COUNT(DISTINCT carrier) AS carrier_count
FROM delivery_stats
GROUP BY delivery_speed_category
ORDER BY avg_days;

-- 4.6 Performance des entrepôts (stock vs lead time)
SELECT 
    warehouse,
    region,
    country,
    COUNT(DISTINCT product_id) AS product_count,
    SUM(current_stock) AS total_stock,
    ROUND(AVG(lead_time), 1) AS avg_lead_time_days,
    COUNT(CASE WHEN current_stock < reorder_point THEN 1 END) AS products_below_reorder,
    ROUND(COUNT(CASE WHEN current_stock < reorder_point THEN 1 END) * 100.0 / COUNT(*), 2) AS stockout_risk_rate,
    ROUND(AVG(DATEDIFF(day, last_restock_date, CURRENT_DATE())), 0) AS avg_days_since_restock
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN
GROUP BY warehouse, region, country
ORDER BY total_stock DESC;

-- 4.7 Fiabilité des fournisseurs vs ruptures de stock
SELECT 
    si.product_category,
    si.region,
    COUNT(DISTINCT si.supplier_id) AS supplier_count,
    ROUND(AVG(si.reliability_score), 2) AS avg_supplier_reliability,
    ROUND(AVG(si.lead_time), 1) AS avg_supplier_lead_time,
    COUNT(CASE WHEN si.quality_rating IN ('A', 'B') THEN 1 END) AS high_quality_suppliers,
    COUNT(DISTINCT i.product_id) AS products_in_category,
    COUNT(CASE WHEN i.current_stock < i.reorder_point THEN 1 END) AS products_low_stock,
    ROUND(COUNT(CASE WHEN i.current_stock < i.reorder_point THEN 1 END) * 100.0 / 
          NULLIF(COUNT(DISTINCT i.product_id), 0), 2) AS stockout_rate
FROM ANYCOMPANY_LAB.SILVER.SUPPLIER_INFORMATION_CLEAN si
LEFT JOIN ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i ON si.product_category = i.product_category AND si.region = i.region
GROUP BY si.product_category, si.region
ORDER BY stockout_rate DESC;

-- 4.8 Analyse des retards de livraison par statut
SELECT 
    status,
    COUNT(*) AS shipment_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(DATEDIFF(day, ship_date, estimated_delivery)), 1) AS avg_delivery_days,
    ROUND(AVG(shipping_cost), 2) AS avg_cost,
    COUNT(DISTINCT carrier) AS carriers_involved
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING_CLEAN
GROUP BY status
ORDER BY shipment_count DESC;

-- 4.9 Corrélation stock faible vs avis clients négatifs
SELECT 
    i.product_category,
    COUNT(DISTINCT i.product_id) AS total_products,
    COUNT(CASE WHEN i.current_stock < i.reorder_point THEN 1 END) AS products_low_stock,
    COUNT(DISTINCT pr.review_id) AS total_reviews,
    COUNT(CASE WHEN pr.rating <= 2 THEN 1 END) AS negative_reviews,
    ROUND(AVG(pr.rating), 2) AS avg_rating,
    ROUND(COUNT(CASE WHEN pr.rating <= 2 THEN 1 END) * 100.0 / 
          NULLIF(COUNT(DISTINCT pr.review_id), 0), 2) AS negative_review_rate
FROM ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i
LEFT JOIN ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN pr ON i.product_id = pr.product_id
GROUP BY i.product_category
ORDER BY products_low_stock DESC;



