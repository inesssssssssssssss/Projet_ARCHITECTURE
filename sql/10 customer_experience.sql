-- THÈME 3 : EXPÉRIENCE CLIENT



-- 3.1 Impact des avis produits sur les ventes (corrélation rating vs stock)
WITH product_performance AS (
    SELECT 
        pr.product_id,
        pr.product_category,
        COUNT(DISTINCT pr.review_id) AS review_count,
        ROUND(AVG(pr.rating), 2) AS avg_rating,
        i.current_stock,
        i.reorder_point,
        CASE 
            WHEN i.current_stock < i.reorder_point THEN 'Low stock'
            WHEN i.current_stock BETWEEN i.reorder_point AND i.reorder_point * 3 THEN 'Normal stock'
            ELSE 'High stock'
        END AS stock_level
    FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN pr
    LEFT JOIN ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i ON pr.product_id = i.product_id
    GROUP BY pr.product_id, pr.product_category, i.current_stock, i.reorder_point
)
SELECT 
    CASE 
        WHEN avg_rating >= 4.5 THEN '4.5-5 (Excellent)'
        WHEN avg_rating >= 4.0 THEN '4.0-4.5 (Bon)'
        WHEN avg_rating >= 3.0 THEN '3.0-4.0 (Moyen)'
        ELSE '< 3.0 (Faible)'
    END AS rating_category,
    COUNT(*) AS product_count,
    ROUND(AVG(review_count), 0) AS avg_reviews_per_product,
    ROUND(AVG(current_stock), 0) AS avg_stock_level,
    COUNT(CASE WHEN stock_level = 'Low stock' THEN 1 END) AS products_low_stock
FROM product_performance
WHERE avg_rating IS NOT NULL
GROUP BY rating_category
ORDER BY rating_category DESC;

-- 3.2 Produits populaires (beaucoup d'avis) vs disponibilité stock
SELECT 
    pr.product_id,
    PR.PRODUCT_CATEGORY,
    COUNT(*) AS review_count,
    ROUND(AVG(pr.rating), 2) AS avg_rating,
    i.current_stock,
    i.reorder_point,
    CASE 
        WHEN i.current_stock < i.reorder_point THEN '⚠️ Risque rupture'
        WHEN i.current_stock < i.reorder_point * 2 THEN '⚡ Stock faible'
        ELSE '✅ Stock OK'
    END AS stock_status,
    i.warehouse,
    i.region
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN pr
LEFT JOIN ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i ON pr.product_id = i.product_id
GROUP BY pr.product_id, PR.PRODUCT_CATEGORY, i.current_stock, i.reorder_point, i.warehouse, i.region
HAVING COUNT(*) >= 10  -- Produits avec au moins 10 avis
ORDER BY review_count DESC, avg_rating DESC
LIMIT 30;

-- 3.3 Influence des interactions service client sur la satisfaction
SELECT 
    csi.issue_category,
    COUNT(*) AS interaction_count,
    ROUND(AVG(csi.customer_satisfaction), 2) AS avg_satisfaction,
    ROUND(AVG(csi.duration_minutes), 1) AS avg_duration_minutes,
    COUNT(CASE WHEN csi.resolution_status = 'Resolved' THEN 1 END) AS resolved_count,
    ROUND(COUNT(CASE WHEN csi.resolution_status = 'Resolved' THEN 1 END) * 100.0 / COUNT(*), 2) AS resolution_rate,
    COUNT(CASE WHEN csi.follow_up_required = 'Yes' THEN 1 END) AS follow_ups_needed
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN csi
GROUP BY csi.issue_category
ORDER BY avg_satisfaction DESC;

-- 3.4 Corrélation type d'interaction vs satisfaction
SELECT 
    interaction_type,
    COUNT(*) AS interaction_count,
    ROUND(AVG(customer_satisfaction), 2) AS avg_satisfaction,
    ROUND(AVG(duration_minutes), 1) AS avg_duration,
    COUNT(CASE WHEN resolution_status = 'Resolved' THEN 1 END) AS resolved_count,
    ROUND(COUNT(CASE WHEN resolution_status = 'Resolved' THEN 1 END) * 100.0 / COUNT(*), 2) AS resolution_rate
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
GROUP BY interaction_type
ORDER BY avg_satisfaction DESC;

-- 3.5 Impact des avis négatifs sur les catégories
SELECT 
    product_category,
    COUNT(CASE WHEN rating <= 2 THEN 1 END) AS negative_reviews,
    COUNT(CASE WHEN rating >= 4 THEN 1 END) AS positive_reviews,
    COUNT(*) AS total_reviews,
    ROUND(COUNT(CASE WHEN rating <= 2 THEN 1 END) * 100.0 / COUNT(*), 2) AS negative_review_rate,
    ROUND(AVG(rating), 2) AS avg_rating
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN
GROUP BY product_category
ORDER BY negative_review_rate DESC;



-- 3.7 Temps de résolution moyen par catégorie de problème
SELECT 
    issue_category,
    COUNT(*) AS interaction_count,
    ROUND(AVG(duration_minutes), 1) AS avg_resolution_time_minutes,
    ROUND(MIN(duration_minutes), 0) AS min_time,
    ROUND(MAX(duration_minutes), 0) AS max_time,
    ROUND(AVG(customer_satisfaction), 2) AS avg_satisfaction
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_SERVICE_INTERACTIONS_CLEAN
WHERE resolution_status = 'Resolved'
GROUP BY issue_category
ORDER BY avg_resolution_time_minutes;


