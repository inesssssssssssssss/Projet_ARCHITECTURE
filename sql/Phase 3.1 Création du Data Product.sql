-- ============================================================================
-- PHASE 3.1 – DATA PRODUCT ANALYTICS
-- Tables analytiques pour le projet AnyCompany
-- MBA ESG Data & IA - 2026
-- ============================================================================

-- Création du schéma pour nos tables analytics
CREATE OR REPLACE SCHEMA ANYCOMPANY_LAB.ANALYTICS;
USE SCHEMA ANYCOMPANY_LAB.ANALYTICS;


-- ============================================================================
-- TABLE 1 : VENTES_ENRICHIES 
-- On met toutes les ventes avec les infos des promos et campagnes
-- ============================================================================

CREATE OR REPLACE TABLE VENTES_ENRICHIES AS
SELECT 
    -- Infos de base de la transaction
    ft.transaction_id,
    ft.transaction_date,
    ft.amount,
    ft.transaction_type,
    ft.payment_method,
    ft.entity as client,
    ft.region,
    
    -- Infos temporelles - pour analyser quand les gens achètent
    YEAR(ft.transaction_date) as annee,
    MONTH(ft.transaction_date) as mois,
    MONTHNAME(ft.transaction_date) as nom_mois,
    DAYNAME(ft.transaction_date) as jour_semaine,
    CASE WHEN DAYOFWEEK(ft.transaction_date) IN (6,7) THEN 'Weekend' ELSE 'Semaine' END as type_jour,
    
    -- Infos sur les promos - est-ce qu'il y avait une promo active pendant l'achat ?
    p.promotion_id,
    p.product_category as categorie_promo,
    p.discount_percentage as taux_reduction,
    CASE WHEN p.promotion_id IS NOT NULL THEN 'Oui' ELSE 'Non' END as avec_promo,
    
    -- Infos sur les campagnes marketing
    mc.campaign_id,
    mc.campaign_name as nom_campagne,
    mc.campaign_type as type_campagne,
    mc.budget as budget_campagne,
    CASE WHEN mc.campaign_id IS NOT NULL THEN 'Oui' ELSE 'Non' END as avec_campagne,
    
    -- Calculs pour mesurer l'impact des promos
    CASE 
        WHEN p.promotion_id IS NOT NULL THEN ft.amount / (1 - p.discount_percentage/100)
        ELSE ft.amount
    END as montant_sans_promo,
    
    CASE 
        WHEN p.promotion_id IS NOT NULL THEN ft.amount * p.discount_percentage / 100
        ELSE 0
    END as montant_reduction

FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS_CLEAN ft

LEFT JOIN ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p 
    ON ft.transaction_date BETWEEN p.start_date AND p.end_date
    AND ft.region = p.region

LEFT JOIN ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN mc 
    ON ft.transaction_date BETWEEN mc.start_date AND mc.end_date
    AND ft.region = mc.region;


-- ===== VERIFICATIONS TABLE 1 =====

-- Combien de lignes on a ?
SELECT 'Nombre total de ventes' as info, COUNT(*) as valeur FROM VENTES_ENRICHIES;

-- CA total et répartition promo/sans promo
SELECT 
    'CA total' as metric,
    ROUND(SUM(amount), 2) as montant
FROM VENTES_ENRICHIES
UNION ALL
SELECT 
    'CA avec promo',
    ROUND(SUM(amount), 2)
FROM VENTES_ENRICHIES
WHERE avec_promo = 'Oui'
UNION ALL
SELECT 
    'CA sans promo',
    ROUND(SUM(amount), 2)
FROM VENTES_ENRICHIES
WHERE avec_promo = 'Non';

-- Répartition par jour de la semaine
SELECT 
    jour_semaine,
    COUNT(*) as nb_ventes,
    ROUND(SUM(amount), 2) as ca
FROM VENTES_ENRICHIES
GROUP BY jour_semaine
ORDER BY 
    CASE jour_semaine
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END;

-- Un aperçu des données
SELECT * FROM VENTES_ENRICHIES LIMIT 10;




-- ============================================================================
-- TABLE 2 : ANALYSE_PROMOTIONS
-- Analyse de performance de chaque promotion
-- ============================================================================

CREATE OR REPLACE TABLE ANALYSE_PROMOTIONS AS
SELECT 
    p.promotion_id,
    p.product_category,
    p.promotion_type,
    p.discount_percentage,
    p.start_date,
    p.end_date,
    p.region,
    DATEDIFF(day, p.start_date, p.end_date) as duree_jours,
    
    -- Statut actuel de la promo
    CASE 
        WHEN CURRENT_DATE() < p.start_date THEN 'A venir'
        WHEN CURRENT_DATE() BETWEEN p.start_date AND p.end_date THEN 'En cours'
        ELSE 'Terminée'
    END as statut,
    
    -- Résultats de la promo
    COUNT(v.transaction_id) as nb_ventes,
    ROUND(SUM(v.amount), 2) as ca_genere,
    ROUND(AVG(v.amount), 2) as panier_moyen,
    
    -- Combien on a donné en réduction
    ROUND(SUM(v.montant_reduction), 2) as total_reductions,
    
    -- CA théorique sans promo
    ROUND(SUM(v.montant_sans_promo), 2) as ca_sans_promo,
    
    -- ROI de la promo
    CASE 
        WHEN SUM(v.montant_reduction) > 0 
        THEN ROUND((SUM(v.amount) / SUM(v.montant_reduction) - 1) * 100, 2)
        ELSE NULL
    END as roi_promo_pct

FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN VENTES_ENRICHIES v ON p.promotion_id = v.promotion_id
GROUP BY p.promotion_id, p.product_category, p.promotion_type, 
         p.discount_percentage, p.start_date, p.end_date, p.region;


-- ===== VERIFICATIONS TABLE 2 =====

-- Nombre de promos
SELECT 'Nombre de promotions' as info, COUNT(*) as valeur FROM ANALYSE_PROMOTIONS;

-- Répartition par statut
SELECT 
    statut,
    COUNT(*) as nb_promos,
    ROUND(AVG(discount_percentage), 2) as taux_reduction_moyen
FROM ANALYSE_PROMOTIONS
GROUP BY statut;

-- Top 10 des meilleures promos (ROI le plus élevé)
SELECT 
    promotion_id,
    product_category,
    discount_percentage,
    roi_promo_pct,
    ca_genere,
    nb_ventes
FROM ANALYSE_PROMOTIONS
WHERE statut = 'Terminée' AND nb_ventes > 0
ORDER BY roi_promo_pct DESC
LIMIT 10;

-- Les promos qui ont le moins marché
SELECT 
    promotion_id,
    product_category,
    discount_percentage,
    roi_promo_pct,
    ca_genere,
    nb_ventes
FROM ANALYSE_PROMOTIONS
WHERE statut = 'Terminée' AND nb_ventes > 0
ORDER BY roi_promo_pct ASC
LIMIT 10;

-- Aperçu
SELECT * FROM ANALYSE_PROMOTIONS LIMIT 10;




-- ============================================================================
-- TABLE 3 : PROFIL_CLIENTS
-- Vue 360 de chaque client
-- ============================================================================

CREATE OR REPLACE TABLE PROFIL_CLIENTS AS
WITH comportement_achat AS (
    SELECT 
        client,
        COUNT(*) as nb_achats,
        ROUND(SUM(amount), 2) as total_depense,
        ROUND(AVG(amount), 2) as panier_moyen,
        MIN(transaction_date) as premier_achat,
        MAX(transaction_date) as dernier_achat,
        COUNT(CASE WHEN avec_promo = 'Oui' THEN 1 END) as achats_avec_promo
    FROM VENTES_ENRICHIES
    GROUP BY client
)
SELECT 
    -- Identité
    ca.client,
    cd.name,
    cd.gender,
    cd.age,
    cd.marital_status,
    cd.annual_income,
    cd.region,
    cd.country,
    
    -- Comportement d'achat
    ca.nb_achats,
    ca.total_depense,
    ca.panier_moyen,
    ca.premier_achat,
    ca.dernier_achat,
    ca.achats_avec_promo,
    
    -- Récence - depuis combien de temps pas d'achat ?
    DATEDIFF(day, ca.dernier_achat, CURRENT_DATE()) as jours_depuis_dernier_achat,
    
    -- % d'achats avec promo
    ROUND(ca.achats_avec_promo * 100.0 / ca.nb_achats, 1) as pct_achats_promo,
    
    -- Segmentation par valeur
    CASE 
        WHEN ca.total_depense >= 50000 THEN 'VIP'
        WHEN ca.total_depense >= 20000 THEN 'Gros client'
        WHEN ca.total_depense >= 5000 THEN 'Client moyen'
        ELSE 'Petit client'
    END as segment_valeur,
    
    -- Segmentation par activité
    CASE 
        WHEN DATEDIFF(day, ca.dernier_achat, CURRENT_DATE()) <= 30 THEN 'Actif'
        WHEN DATEDIFF(day, ca.dernier_achat, CURRENT_DATE()) <= 90 THEN 'A risque'
        WHEN DATEDIFF(day, ca.dernier_achat, CURRENT_DATE()) <= 180 THEN 'Inactif'
        ELSE 'Perdu'
    END as segment_activite,
    
    -- Scores RFM (1 à 5)
    NTILE(5) OVER (ORDER BY ca.dernier_achat DESC) as score_recence,
    NTILE(5) OVER (ORDER BY ca.nb_achats) as score_frequence,
    NTILE(5) OVER (ORDER BY ca.total_depense) as score_monetaire

FROM comportement_achat ca
LEFT JOIN ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS_CLEAN cd 
    ON ca.client = cd.name;


-- ===== VERIFICATIONS TABLE 3 =====

-- Nombre de clients
SELECT 'Nombre de clients' as info, COUNT(*) as valeur FROM PROFIL_CLIENTS;

-- Répartition par segment de valeur
SELECT 
    segment_valeur,
    COUNT(*) as nb_clients,
    ROUND(AVG(total_depense), 2) as depense_moyenne,
    ROUND(SUM(total_depense), 2) as ca_total
FROM PROFIL_CLIENTS
GROUP BY segment_valeur
ORDER BY ca_total DESC;

-- Répartition par segment d'activité
SELECT 
    segment_activite,
    COUNT(*) as nb_clients,
    ROUND(AVG(jours_depuis_dernier_achat), 0) as jours_moy_depuis_achat
FROM PROFIL_CLIENTS
GROUP BY segment_activite
ORDER BY 
    CASE segment_activite
        WHEN 'Actif' THEN 1
        WHEN 'A risque' THEN 2
        WHEN 'Inactif' THEN 3
        WHEN 'Perdu' THEN 4
    END;

-- Matrice valeur x activité
SELECT 
    segment_valeur,
    segment_activite,
    COUNT(*) as nb_clients
FROM PROFIL_CLIENTS
GROUP BY segment_valeur, segment_activite
ORDER BY segment_valeur, segment_activite;

-- Les meilleurs clients (RFM 555)
SELECT 
    name,
    total_depense,
    nb_achats,
    jours_depuis_dernier_achat,
    score_recence,
    score_frequence,
    score_monetaire
FROM PROFIL_CLIENTS
WHERE score_recence = 5 AND score_frequence = 5 AND score_monetaire = 5
ORDER BY total_depense DESC
LIMIT 20;

-- Clients VIP à risque (gros clients qui n'ont pas acheté récemment)
SELECT 
    name,
    total_depense,
    nb_achats,
    jours_depuis_dernier_achat,
    segment_valeur,
    segment_activite
FROM PROFIL_CLIENTS
WHERE segment_valeur IN ('VIP', 'Gros client')
  AND segment_activite IN ('A risque', 'Inactif')
ORDER BY total_depense DESC
LIMIT 20;

-- Aperçu
SELECT * FROM PROFIL_CLIENTS LIMIT 10;




-- ============================================================================
-- TABLE 4 : PERF_PRODUITS
-- Performance produit (avis + stock)
-- ============================================================================

CREATE OR REPLACE TABLE PERF_PRODUITS AS
WITH avis_produits AS (
    SELECT 
        product_id,
        product_category,
        COUNT(*) as nb_avis,
        ROUND(AVG(rating), 2) as note_moyenne,
        COUNT(CASE WHEN rating >= 4 THEN 1 END) as avis_positifs,
        COUNT(CASE WHEN rating <= 2 THEN 1 END) as avis_negatifs
    FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS_CLEAN
    GROUP BY product_id, product_category
)
SELECT 
    COALESCE(a.product_id, i.product_id) as product_id,
    COALESCE(a.product_category, i.product_category) as categorie,
    
    -- Infos avis clients
    COALESCE(a.nb_avis, 0) as nb_avis,
    a.note_moyenne,
    a.avis_positifs,
    a.avis_negatifs,
    
    -- Évaluation qualité
    CASE 
        WHEN a.nb_avis >= 10 AND a.note_moyenne >= 4.0 THEN 'Excellent produit'
        WHEN a.nb_avis >= 5 AND a.note_moyenne >= 3.5 THEN 'Bon produit'
        WHEN a.nb_avis >= 5 AND a.note_moyenne < 3.0 THEN 'Problème qualité'
        ELSE 'Pas assez d''avis'
    END as qualite,
    
    -- Infos stock
    i.region,
    i.warehouse,
    i.current_stock,
    i.reorder_point,
    i.lead_time,
    DATEDIFF(day, i.last_restock_date, CURRENT_DATE()) as jours_depuis_restock,
    
    -- Niveau de stock
    CASE 
        WHEN i.current_stock = 0 THEN 'RUPTURE'
        WHEN i.current_stock < i.reorder_point THEN 'Stock faible'
        WHEN i.current_stock < i.reorder_point * 2 THEN 'Stock correct'
        ELSE 'Stock élevé'
    END as niveau_stock,
    
    -- Alertes
    CASE 
        WHEN a.note_moyenne >= 4.0 AND a.nb_avis >= 10 AND i.current_stock < i.reorder_point 
        THEN 'URGENT : Réappro nécessaire'
        WHEN a.note_moyenne < 3.0 AND a.nb_avis >= 10 AND i.current_stock > i.reorder_point * 5
        THEN 'Surstock produit mal noté'
        WHEN a.nb_avis = 0 OR a.nb_avis IS NULL
        THEN 'Pas d''avis - lancer campagne review'
        ELSE 'OK'
    END as alerte

FROM avis_produits a
FULL OUTER JOIN ANYCOMPANY_LAB.SILVER.INVENTORY_CLEAN i 
    ON a.product_id = i.product_id;


-- ===== VERIFICATIONS TABLE 4 =====

-- Nombre de produits
SELECT 'Nombre de produits' as info, COUNT(*) as valeur FROM PERF_PRODUITS;

-- Répartition par qualité
SELECT 
    qualite,
    COUNT(*) as nb_produits,
    ROUND(AVG(note_moyenne), 2) as note_moy
FROM PERF_PRODUITS
GROUP BY qualite
ORDER BY 
    CASE qualite
        WHEN 'Excellent produit' THEN 1
        WHEN 'Bon produit' THEN 2
        WHEN 'Problème qualité' THEN 3
        WHEN 'Pas assez d''avis' THEN 4
    END;

-- Répartition par niveau de stock
SELECT 
    niveau_stock,
    COUNT(*) as nb_produits
FROM PERF_PRODUITS
GROUP BY niveau_stock
ORDER BY 
    CASE niveau_stock
        WHEN 'RUPTURE' THEN 1
        WHEN 'Stock faible' THEN 2
        WHEN 'Stock correct' THEN 3
        WHEN 'Stock élevé' THEN 4
    END;

-- Produits avec alertes
SELECT 
    alerte,
    COUNT(*) as nb_produits
FROM PERF_PRODUITS
GROUP BY alerte;

-- Produits nécessitant une action urgente
SELECT 
    product_id,
    categorie,
    note_moyenne,
    nb_avis,
    niveau_stock,
    current_stock,
    reorder_point,
    alerte
FROM PERF_PRODUITS
WHERE alerte != 'OK'
ORDER BY 
    CASE alerte
        WHEN 'URGENT : Réappro nécessaire' THEN 1
        WHEN 'Surstock produit mal noté' THEN 2
        WHEN 'Pas d''avis - lancer campagne review' THEN 3
    END,
    nb_avis DESC
LIMIT 30;

-- Top produits par note
SELECT 
    product_id,
    categorie,
    note_moyenne,
    nb_avis,
    niveau_stock
FROM PERF_PRODUITS
WHERE nb_avis >= 10
ORDER BY note_moyenne DESC, nb_avis DESC
LIMIT 20;

-- Aperçu
SELECT * FROM PERF_PRODUITS LIMIT 10;




-- ============================================================================
-- TABLE 5 : PERF_CAMPAGNES
-- Performance et ROI des campagnes marketing
-- ============================================================================

CREATE OR REPLACE TABLE PERF_CAMPAGNES AS
SELECT 
    mc.campaign_id,
    mc.campaign_name,
    mc.campaign_type,
    mc.product_category,
    mc.target_audience,
    mc.region,
    mc.start_date,
    mc.end_date,
    DATEDIFF(day, mc.start_date, mc.end_date) as duree_jours,
    mc.budget,
    mc.reach,
    mc.conversion_rate as taux_conversion_prevu,
    
    -- Résultats réels
    COUNT(v.transaction_id) as nb_ventes,
    COUNT(DISTINCT v.client) as nb_clients,
    ROUND(SUM(v.amount), 2) as ca_genere,
    ROUND(AVG(v.amount), 2) as panier_moyen,
    
    -- Métriques de performance
    CASE 
        WHEN COUNT(v.transaction_id) > 0 
        THEN ROUND(mc.budget / COUNT(v.transaction_id), 2)
        ELSE NULL
    END as cout_par_vente,
    
    CASE 
        WHEN COUNT(DISTINCT v.client) > 0 
        THEN ROUND(mc.budget / COUNT(DISTINCT v.client), 2)
        ELSE NULL
    END as cout_par_client,
    
    -- ROI
    CASE 
        WHEN mc.budget > 0 
        THEN ROUND(SUM(v.amount) / mc.budget, 2)
        ELSE NULL
    END as roi,
    
    CASE 
        WHEN mc.budget > 0 
        THEN ROUND((SUM(v.amount) - mc.budget) / mc.budget * 100, 2)
        ELSE NULL
    END as roi_pct,
    
    -- Taux de conversion réel
    CASE 
        WHEN mc.reach > 0 
        THEN ROUND(COUNT(v.transaction_id) * 100.0 / mc.reach, 4)
        ELSE NULL
    END as taux_conversion_reel,
    
    -- Évaluation
    CASE 
        WHEN mc.budget > 0 AND (SUM(v.amount) - mc.budget) / mc.budget * 100 >= 100 THEN 'Très bon'
        WHEN mc.budget > 0 AND (SUM(v.amount) - mc.budget) / mc.budget * 100 >= 50 THEN 'Bon'
        WHEN mc.budget > 0 AND (SUM(v.amount) - mc.budget) / mc.budget * 100 >= 0 THEN 'Rentable'
        WHEN mc.budget > 0 THEN 'Perte'
        ELSE 'N/A'
    END as performance

FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS_CLEAN mc
LEFT JOIN VENTES_ENRICHIES v ON mc.campaign_id = v.campaign_id
GROUP BY mc.campaign_id, mc.campaign_name, mc.campaign_type, mc.product_category,
         mc.target_audience, mc.region, mc.start_date, mc.end_date, 
         mc.budget, mc.reach, mc.conversion_rate;


-- ===== VERIFICATIONS TABLE 5 =====

-- Nombre de campagnes
SELECT 'Nombre de campagnes' as info, COUNT(*) as valeur FROM PERF_CAMPAGNES;

-- Répartition par performance
SELECT 
    performance,
    COUNT(*) as nb_campagnes,
    ROUND(AVG(roi_pct), 2) as roi_moyen,
    ROUND(SUM(budget), 2) as budget_total,
    ROUND(SUM(ca_genere), 2) as ca_total
FROM PERF_CAMPAGNES
GROUP BY performance
ORDER BY 
    CASE performance
        WHEN 'Très bon' THEN 1
        WHEN 'Bon' THEN 2
        WHEN 'Rentable' THEN 3
        WHEN 'Perte' THEN 4
        WHEN 'N/A' THEN 5
    END;

-- Répartition par type de campagne
SELECT 
    campaign_type,
    COUNT(*) as nb_campagnes,
    ROUND(AVG(roi_pct), 2) as roi_moyen,
    ROUND(AVG(taux_conversion_reel), 4) as taux_conv_moyen
FROM PERF_CAMPAGNES
WHERE performance != 'N/A'
GROUP BY campaign_type
ORDER BY roi_moyen DESC;

-- Top 15 campagnes par ROI
SELECT 
    campaign_name,
    campaign_type,
    target_audience,
    budget,
    ca_genere,
    roi_pct,
    performance
FROM PERF_CAMPAGNES
WHERE performance != 'N/A'
ORDER BY roi_pct DESC
LIMIT 15;

-- Les pires campagnes
SELECT 
    campaign_name,
    campaign_type,
    budget,
    ca_genere,
    roi_pct,
    performance
FROM PERF_CAMPAGNES
WHERE performance IN ('Perte', 'Rentable')
ORDER BY roi_pct ASC
LIMIT 15;

-- Performance par audience cible
SELECT 
    target_audience,
    COUNT(*) as nb_campagnes,
    ROUND(AVG(roi_pct), 2) as roi_moyen,
    ROUND(AVG(budget), 2) as budget_moyen
FROM PERF_CAMPAGNES
WHERE performance != 'N/A'
GROUP BY target_audience
ORDER BY roi_moyen DESC;

-- Aperçu
SELECT * FROM PERF_CAMPAGNES LIMIT 10;




-- ============================================================================
-- SYNTHESE GLOBALE - Vue d'ensemble des 5 tables
-- ============================================================================

SELECT '========== RÉSUMÉ DES TABLES ANALYTICS ==========' as info;

-- Nombre de lignes par table
SELECT 'VENTES_ENRICHIES' as table_name, COUNT(*) as nb_lignes FROM VENTES_ENRICHIES
UNION ALL
SELECT 'ANALYSE_PROMOTIONS', COUNT(*) FROM ANALYSE_PROMOTIONS
UNION ALL
SELECT 'PROFIL_CLIENTS', COUNT(*) FROM PROFIL_CLIENTS
UNION ALL
SELECT 'PERF_PRODUITS', COUNT(*) FROM PERF_PRODUITS
UNION ALL
SELECT 'PERF_CAMPAGNES', COUNT(*) FROM PERF_CAMPAGNES
ORDER BY table_name;


-- Quelques KPIs globaux
SELECT 
    'CA total' as kpi,
    ROUND(SUM(amount), 2) as valeur,
    'EUR' as unite
FROM VENTES_ENRICHIES

UNION ALL

SELECT 
    'CA avec promo',
    ROUND(SUM(amount), 2),
    'EUR'
FROM VENTES_ENRICHIES
WHERE avec_promo = 'Oui'

UNION ALL

SELECT 
    'Nombre de clients',
    COUNT(*)::FLOAT,
    'clients'
FROM PROFIL_CLIENTS

UNION ALL

SELECT 
    'Nombre de clients VIP',
    COUNT(*)::FLOAT,
    'clients'
FROM PROFIL_CLIENTS
WHERE segment_valeur = 'VIP'

UNION ALL

SELECT 
    'ROI moyen des promos',
    ROUND(AVG(roi_promo_pct), 2),
    '%'
FROM ANALYSE_PROMOTIONS
WHERE roi_promo_pct IS NOT NULL

UNION ALL

SELECT 
    'ROI moyen des campagnes',
    ROUND(AVG(roi_pct), 2),
    '%'
FROM PERF_CAMPAGNES
WHERE performance != 'N/A';


-- ============================================================================
-- FIN PHASE 3.1
-- ============================================================================

