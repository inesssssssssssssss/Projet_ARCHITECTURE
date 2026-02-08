-- FEATURE ENGINEERING

-- On travaille toujours dans le schéma ANALYTICS
USE SCHEMA ANYCOMPANY_LAB.ANALYTICS;



-- TABLE 1 : FEATURES_CLIENTS


CREATE OR REPLACE TABLE FEATURES_CLIENTS AS
SELECT 
    -- Identifiant
    client,
    name,
    
    --  Features RFM (déjà calculées) 
    score_recence,
    score_frequence,
    score_monetaire,
    
    -- Score RFM combiné (somme des 3 scores)
    score_recence + score_frequence + score_monetaire as score_rfm_total,
    
    -- Concaténation RFM (ex: "555" pour un champion)
    CONCAT(score_recence, score_frequence, score_monetaire) as rfm_segment,
    
    --  Features comportementales 
    nb_achats,
    total_depense,
    panier_moyen,
    jours_depuis_dernier_achat,
    pct_achats_promo,
    
    -- Fréquence d'achat (achats par mois)
    CASE 
        WHEN DATEDIFF(day, premier_achat, dernier_achat) > 0 
        THEN ROUND(nb_achats * 30.0 / DATEDIFF(day, premier_achat, dernier_achat), 2)
        ELSE nb_achats
    END as frequence_mensuelle,
    
    -- Durée de vie client (en jours)
    DATEDIFF(day, premier_achat, dernier_achat) as duree_vie_client_jours,
    
    -- Durée de vie en mois
    ROUND(DATEDIFF(day, premier_achat, dernier_achat) / 30.0, 1) as duree_vie_client_mois,
    
    --  Features démographiques 
    age,
    gender,
    annual_income,
    marital_status,
    region,
    country,
    
    -- Tranche d'âge (pour le ML)
    CASE 
        WHEN age < 25 THEN '18-25'
        WHEN age BETWEEN 25 AND 35 THEN '25-35'
        WHEN age BETWEEN 36 AND 50 THEN '36-50'
        WHEN age BETWEEN 51 AND 65 THEN '51-65'
        ELSE '65+'
    END as tranche_age,
    
    -- Tranche de revenu
    CASE 
        WHEN annual_income < 30000 THEN 'Bas'
        WHEN annual_income BETWEEN 30000 AND 70000 THEN 'Moyen'
        ELSE 'Élevé'
    END as tranche_revenu,
    
    --  Features calculées avancées 
    
    -- Valeur vie client (Customer Lifetime Value approximative)
    CASE 
        WHEN DATEDIFF(day, premier_achat, dernier_achat) > 0
        THEN ROUND(total_depense / (DATEDIFF(day, premier_achat, dernier_achat) / 365.0), 2)
        ELSE total_depense
    END as clv_annuelle_estimee,
    
    -- Tendance d'achat (les achats augmentent ou diminuent ?)
    -- On compare les 3 derniers mois vs les 3 mois d'avant
    -- (approximation simplifiée)
    CASE 
        WHEN jours_depuis_dernier_achat <= 90 THEN 'Actif récent'
        WHEN jours_depuis_dernier_achat BETWEEN 91 AND 180 THEN 'Ralenti'
        ELSE 'Inactif'
    END as tendance_achat,
    
    -- Ratio dépenses/revenus (en %)
    CASE 
        WHEN annual_income > 0 
        THEN ROUND(total_depense * 100.0 / annual_income, 2)
        ELSE NULL
    END as ratio_depense_revenu,
    
    -- Flag client premium (dépense élevée + achats fréquents)
    CASE 
        WHEN total_depense >= 30000 AND nb_achats >= 15 THEN 1
        ELSE 0
    END as flag_client_premium,
    
    -- Flag risque churn (inactif + était actif avant)
    CASE 
        WHEN jours_depuis_dernier_achat > 90 AND nb_achats >= 3 THEN 1
        ELSE 0
    END as flag_risque_churn,
    
    -- Flag sensible aux promos
    CASE 
        WHEN pct_achats_promo >= 50 THEN 1
        ELSE 0
    END as flag_sensible_promo,
    
    --  Segments prêts pour le ML 
    segment_valeur,
    segment_activite,
    
    -- Encodage numérique des segments (pour certains algos ML)
    CASE segment_valeur
        WHEN 'VIP' THEN 4
        WHEN 'Gros client' THEN 3
        WHEN 'Client moyen' THEN 2
        WHEN 'Petit client' THEN 1
    END as segment_valeur_num,
    
    CASE segment_activite
        WHEN 'Actif' THEN 4
        WHEN 'A risque' THEN 3
        WHEN 'Inactif' THEN 2
        WHEN 'Perdu' THEN 1
    END as segment_activite_num

FROM PROFIL_CLIENTS;


--  VERIFICATIONS TABLE 1 

SELECT 'Nombre de clients avec features' as info, COUNT(*) as valeur FROM FEATURES_CLIENTS;

-- Distribution des scores RFM totaux
SELECT 
    score_rfm_total,
    COUNT(*) as nb_clients
FROM FEATURES_CLIENTS
GROUP BY score_rfm_total
ORDER BY score_rfm_total DESC
LIMIT 15;

-- Top segments RFM
SELECT 
    rfm_segment,
    COUNT(*) as nb_clients,
    ROUND(AVG(total_depense), 2) as depense_moy
FROM FEATURES_CLIENTS
GROUP BY rfm_segment
ORDER BY nb_clients DESC
LIMIT 10;

-- Stats sur les flags
SELECT 
    SUM(flag_client_premium) as nb_clients_premium,
    SUM(flag_risque_churn) as nb_clients_risque_churn,
    SUM(flag_sensible_promo) as nb_clients_sensibles_promo
FROM FEATURES_CLIENTS;

-- Distribution des tranches d'âge
SELECT 
    tranche_age,
    COUNT(*) as nb_clients,
    ROUND(AVG(panier_moyen), 2) as panier_moy
FROM FEATURES_CLIENTS
GROUP BY tranche_age
ORDER BY 
    CASE tranche_age
        WHEN '18-25' THEN 1
        WHEN '25-35' THEN 2
        WHEN '36-50' THEN 3
        WHEN '51-65' THEN 4
        WHEN '65+' THEN 5
    END;

-- Aperçu
SELECT * FROM FEATURES_CLIENTS LIMIT 10;




 
--  FEATURES_VENTES

CREATE OR REPLACE TABLE FEATURES_VENTES AS
WITH ventes_agregees AS (
    -- On agrège par jour/région/catégorie pour avoir des patterns
    SELECT 
        transaction_date,
        region,
        categorie_promo,
        
        -- Indicateurs de vente
        COUNT(*) as nb_ventes_jour,
        ROUND(SUM(amount), 2) as ca_jour,
        ROUND(AVG(amount), 2) as panier_moyen_jour,
        
        -- Indicateurs promo
        COUNT(CASE WHEN avec_promo = 'Oui' THEN 1 END) as nb_ventes_avec_promo,
        AVG(CASE WHEN avec_promo = 'Oui' THEN taux_reduction END) as taux_reduction_moyen,
        
        -- Indicateurs campagne
        COUNT(CASE WHEN avec_campagne = 'Oui' THEN 1 END) as nb_ventes_avec_campagne
        
    FROM VENTES_ENRICHIES
    GROUP BY transaction_date, region, categorie_promo
)
SELECT 
    transaction_date,
    region,
    categorie_promo,
    
    --  Features de base 
    nb_ventes_jour,
    ca_jour,
    panier_moyen_jour,
    nb_ventes_avec_promo,
    nb_ventes_avec_campagne,
    taux_reduction_moyen,
    
    --  Features temporelles 
    YEAR(transaction_date) as annee,
    MONTH(transaction_date) as mois,
    DAYOFMONTH(transaction_date) as jour_du_mois,
    DAYOFWEEK(transaction_date) as jour_semaine_num,
    DAYNAME(transaction_date) as jour_semaine,
    
    -- Flag weekend
    CASE WHEN DAYOFWEEK(transaction_date) IN (6,7) THEN 1 ELSE 0 END as is_weekend,
    
    -- Flag début/milieu/fin de mois
    CASE 
        WHEN DAYOFMONTH(transaction_date) <= 10 THEN 'Début'
        WHEN DAYOFMONTH(transaction_date) <= 20 THEN 'Milieu'
        ELSE 'Fin'
    END as periode_mois,
    
    -- Saison
    CASE 
        WHEN MONTH(transaction_date) IN (12, 1, 2) THEN 'Hiver'
        WHEN MONTH(transaction_date) IN (3, 4, 5) THEN 'Printemps'
        WHEN MONTH(transaction_date) IN (6, 7, 8) THEN 'Été'
        ELSE 'Automne'
    END as saison,
    
    --  Features calculées 
    
    -- % de ventes avec promo
    CASE 
        WHEN nb_ventes_jour > 0 
        THEN ROUND(nb_ventes_avec_promo * 100.0 / nb_ventes_jour, 2)
        ELSE 0
    END as pct_ventes_avec_promo,
    
    -- % de ventes avec campagne
    CASE 
        WHEN nb_ventes_jour > 0 
        THEN ROUND(nb_ventes_avec_campagne * 100.0 / nb_ventes_jour, 2)
        ELSE 0
    END as pct_ventes_avec_campagne,
    
    -- Moyenne mobile 7 jours (tendance)
    AVG(ca_jour) OVER (
        PARTITION BY region, categorie_promo 
        ORDER BY transaction_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as ca_moyenne_7j,
    
    -- CA du jour vs moyenne 7j (indicateur de performance)
    ROUND(ca_jour - AVG(ca_jour) OVER (
        PARTITION BY region, categorie_promo 
        ORDER BY transaction_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as ecart_vs_moyenne_7j,
    
    -- Flag jour exceptionnel (CA > 150% de la moyenne)
    CASE 
        WHEN ca_jour > 1.5 * AVG(ca_jour) OVER (
            PARTITION BY region, categorie_promo 
            ORDER BY transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) THEN 1
        ELSE 0
    END as flag_jour_exceptionnel

FROM ventes_agregees;


--  VERIFICATIONS TABLE 2 

SELECT 'Nombre de jours avec ventes' as info, COUNT(*) as valeur FROM FEATURES_VENTES;

-- CA par jour de la semaine
SELECT 
    jour_semaine,
    COUNT(*) as nb_jours,
    ROUND(AVG(ca_jour), 2) as ca_moyen_par_jour,
    ROUND(SUM(ca_jour), 2) as ca_total
FROM FEATURES_VENTES
GROUP BY jour_semaine, jour_semaine_num
ORDER BY jour_semaine_num;

-- CA par saison
SELECT 
    saison,
    COUNT(*) as nb_jours,
    ROUND(AVG(ca_jour), 2) as ca_moyen,
    ROUND(SUM(ca_jour), 2) as ca_total
FROM FEATURES_VENTES
GROUP BY saison
ORDER BY 
    CASE saison
        WHEN 'Hiver' THEN 1
        WHEN 'Printemps' THEN 2
        WHEN 'Été' THEN 3
        WHEN 'Automne' THEN 4
    END;

-- Impact des promos
SELECT 
    CASE WHEN pct_ventes_avec_promo >= 50 THEN 'Beaucoup de promos'
         WHEN pct_ventes_avec_promo >= 20 THEN 'Promos moyennes'
         ELSE 'Peu de promos'
    END as niveau_promo,
    COUNT(*) as nb_jours,
    ROUND(AVG(ca_jour), 2) as ca_moyen
FROM FEATURES_VENTES
GROUP BY niveau_promo
ORDER BY ca_moyen DESC;

-- Jours exceptionnels
SELECT 
    transaction_date,
    region,
    ca_jour,
    ca_moyenne_7j,
    ecart_vs_moyenne_7j
FROM FEATURES_VENTES
WHERE flag_jour_exceptionnel = 1
ORDER BY ca_jour DESC
LIMIT 20;

-- Aperçu
SELECT * FROM FEATURES_VENTES LIMIT 10;





--  FEATURES_PRODUITS
-- Features pour prédire la performance produit

CREATE OR REPLACE TABLE FEATURES_PRODUITS AS
SELECT 
    product_id,
    categorie,
    region,
    
    --  Features avis 
    nb_avis,
    note_moyenne,
    avis_positifs,
    avis_negatifs,
    
    -- Ratio avis positifs/négatifs
    CASE 
        WHEN avis_negatifs > 0 
        THEN ROUND(avis_positifs::FLOAT / avis_negatifs, 2)
        ELSE NULL
    END as ratio_positif_negatif,
    
    -- % d'avis positifs
    CASE 
        WHEN nb_avis > 0 
        THEN ROUND(avis_positifs * 100.0 / nb_avis, 2)
        ELSE NULL
    END as pct_avis_positifs,
    
    -- Score de popularité (nb avis × note moyenne)
    CASE 
        WHEN nb_avis > 0 AND note_moyenne IS NOT NULL
        THEN ROUND(nb_avis * note_moyenne, 2)
        ELSE 0
    END as score_popularite,
    
    --  Features stock 
    current_stock,
    reorder_point,
    lead_time,
    jours_depuis_restock,
    
    -- Ratio stock actuel / seuil
    CASE 
        WHEN reorder_point > 0 
        THEN ROUND(current_stock::FLOAT / reorder_point, 2)
        ELSE NULL
    END as ratio_stock_seuil,
    
    -- Couverture stock (en jours approximatifs)
    -- On suppose une vente moyenne par jour
    CASE 
        WHEN current_stock > 0 
        THEN current_stock * lead_time
        ELSE 0
    END as couverture_stock_approx,
    
    --  Encodage qualité 
    qualite,
    
    CASE qualite
        WHEN 'Excellent produit' THEN 4
        WHEN 'Bon produit' THEN 3
        WHEN 'Problème qualité' THEN 1
        ELSE 2
    END as qualite_num,
    
    --  Encodage niveau stock 
    niveau_stock,
    
    CASE niveau_stock
        WHEN 'RUPTURE' THEN 0
        WHEN 'Stock faible' THEN 1
        WHEN 'Stock correct' THEN 2
        WHEN 'Stock élevé' THEN 3
    END as niveau_stock_num,
    
    --  Flags 
    
    -- Flag produit populaire
    CASE WHEN nb_avis >= 20 AND note_moyenne >= 4.0 THEN 1 ELSE 0 END as flag_populaire,
    
    -- Flag besoin urgent de stock
    CASE 
        WHEN current_stock < reorder_point AND note_moyenne >= 4.0 AND nb_avis >= 10 
        THEN 1 ELSE 0 
    END as flag_urgent_reappro,
    
    -- Flag produit à problème
    CASE WHEN note_moyenne < 3.0 AND nb_avis >= 10 THEN 1 ELSE 0 END as flag_probleme,
    
    -- Flag surstock
    CASE 
        WHEN current_stock > reorder_point * 5 
        THEN 1 ELSE 0 
    END as flag_surstock,
    
    --  Recommandation action 
    alerte

FROM PERF_PRODUITS;


--  VERIFICATIONS TABLE 3 

SELECT 'Nombre de produits avec features' as info, COUNT(*) as valeur FROM FEATURES_PRODUITS;

-- Distribution des scores de popularité
SELECT 
    CASE 
        WHEN score_popularite >= 100 THEN 'Très populaire'
        WHEN score_popularite >= 50 THEN 'Populaire'
        WHEN score_popularite >= 20 THEN 'Moyennement populaire'
        ELSE 'Peu populaire'
    END as categorie_popularite,
    COUNT(*) as nb_produits,
    ROUND(AVG(note_moyenne), 2) as note_moy
FROM FEATURES_PRODUITS
WHERE nb_avis > 0
GROUP BY categorie_popularite
ORDER BY 
    CASE 
        WHEN score_popularite >= 100 THEN 1
        WHEN score_popularite >= 50 THEN 2
        WHEN score_popularite >= 20 THEN 3
        ELSE 4
    END;

-- Répartition des flags
SELECT 
    SUM(flag_populaire) as nb_populaires,
    SUM(flag_urgent_reappro) as nb_urgents_reappro,
    SUM(flag_probleme) as nb_produits_probleme,
    SUM(flag_surstock) as nb_surstock
FROM FEATURES_PRODUITS;

-- Produits qui nécessitent une action
SELECT 
    product_id,
    categorie,
    score_popularite,
    niveau_stock,
    flag_urgent_reappro,
    flag_probleme,
    alerte
FROM FEATURES_PRODUITS
WHERE flag_urgent_reappro = 1 OR flag_probleme = 1
ORDER BY score_popularite DESC
LIMIT 20;

-- Aperçu
SELECT * FROM FEATURES_PRODUITS LIMIT 10;





-- FEATURES_PROMOTIONS
-- Features pour prédire l'efficacité des promotions


CREATE OR REPLACE TABLE FEATURES_PROMOTIONS AS
SELECT 
    promotion_id,
    product_category,
    promotion_type,
    region,
    
    --  Features de base 
    discount_percentage,
    duree_jours,
    nb_ventes,
    ca_genere,
    panier_moyen,
    roi_promo_pct,
    
    --  Features temporelles 
    start_date,
    end_date,
    
    MONTH(start_date) as mois_debut,
    DAYOFWEEK(start_date) as jour_semaine_debut,
    
    -- Saison de la promo
    CASE 
        WHEN MONTH(start_date) IN (12, 1, 2) THEN 'Hiver'
        WHEN MONTH(start_date) IN (3, 4, 5) THEN 'Printemps'
        WHEN MONTH(start_date) IN (6, 7, 8) THEN 'Été'
        ELSE 'Automne'
    END as saison_promo,
    
    --  Features calculées 
    
    -- Catégorie de réduction
    CASE 
        WHEN discount_percentage < 10 THEN 'Faible (<10%)'
        WHEN discount_percentage BETWEEN 10 AND 20 THEN 'Moyenne (10-20%)'
        WHEN discount_percentage BETWEEN 21 AND 30 THEN 'Forte (21-30%)'
        ELSE 'Très forte (>30%)'
    END as categorie_reduction,
    
    -- Catégorie durée
    CASE 
        WHEN duree_jours <= 7 THEN 'Courte (≤7j)'
        WHEN duree_jours BETWEEN 8 AND 14 THEN 'Moyenne (8-14j)'
        WHEN duree_jours BETWEEN 15 AND 30 THEN 'Longue (15-30j)'
        ELSE 'Très longue (>30j)'
    END as categorie_duree,
    
    -- Ventes par jour de promo
    CASE 
        WHEN duree_jours > 0 
        THEN ROUND(nb_ventes::FLOAT / duree_jours, 2)
        ELSE nb_ventes
    END as ventes_par_jour,
    
    -- CA par jour
    CASE 
        WHEN duree_jours > 0 
        THEN ROUND(ca_genere / duree_jours, 2)
        ELSE ca_genere
    END as ca_par_jour,
    
    -- Efficacité (ROI par point de réduction)
    CASE 
        WHEN discount_percentage > 0 
        THEN ROUND(roi_promo_pct / discount_percentage, 2)
        ELSE NULL
    END as efficacite_par_pct_reduction,
    
    --  Encodage pour ML 
    
    -- Encodage type promo
    promotion_type,
    
    -- Encodage région
    region,
    
    --  Flags 
    
    -- Flag promo réussie
    CASE WHEN roi_promo_pct > 50 THEN 1 ELSE 0 END as flag_promo_reussie,
    
    -- Flag promo très performante
    CASE WHEN roi_promo_pct > 100 THEN 1 ELSE 0 END as flag_promo_excellente,
    
    -- Flag promo longue durée
    CASE WHEN duree_jours > 20 THEN 1 ELSE 0 END as flag_longue_duree,
    
    -- Flag forte réduction
    CASE WHEN discount_percentage >= 25 THEN 1 ELSE 0 END as flag_forte_reduction,
    
    --  Statut 
    statut

FROM ANALYSE_PROMOTIONS
WHERE nb_ventes > 0;  -- On garde que les promos qui ont eu des ventes


--  VERIFICATIONS TABLE 4 

SELECT 'Nombre de promos avec features' as info, COUNT(*) as valeur FROM FEATURES_PROMOTIONS;

-- Performance par catégorie de réduction
SELECT 
    categorie_reduction,
    COUNT(*) as nb_promos,
    ROUND(AVG(roi_promo_pct), 2) as roi_moyen,
    ROUND(AVG(ventes_par_jour), 2) as ventes_jour_moy
FROM FEATURES_PROMOTIONS
GROUP BY categorie_reduction
ORDER BY 
    CASE categorie_reduction
        WHEN 'Faible (<10%)' THEN 1
        WHEN 'Moyenne (10-20%)' THEN 2
        WHEN 'Forte (21-30%)' THEN 3
        WHEN 'Très forte (>30%)' THEN 4
    END;

-- Performance par durée
SELECT 
    categorie_duree,
    COUNT(*) as nb_promos,
    ROUND(AVG(roi_promo_pct), 2) as roi_moyen,
    ROUND(AVG(ca_par_jour), 2) as ca_jour_moy
FROM FEATURES_PROMOTIONS
GROUP BY categorie_duree
ORDER BY 
    CASE categorie_duree
        WHEN 'Courte (≤7j)' THEN 1
        WHEN 'Moyenne (8-14j)' THEN 2
        WHEN 'Longue (15-30j)' THEN 3
        WHEN 'Très longue (>30j)' THEN 4
    END;

-- Performance par saison
SELECT 
    saison_promo,
    COUNT(*) as nb_promos,
    ROUND(AVG(roi_promo_pct), 2) as roi_moyen,
    ROUND(AVG(discount_percentage), 2) as reduction_moy
FROM FEATURES_PROMOTIONS
GROUP BY saison_promo;

-- Répartition des flags
SELECT 
    SUM(flag_promo_reussie) as nb_promos_reussies,
    SUM(flag_promo_excellente) as nb_promos_excellentes,
    SUM(flag_longue_duree) as nb_promos_longues,
    SUM(flag_forte_reduction) as nb_fortes_reductions
FROM FEATURES_PROMOTIONS;

-- Meilleures combos réduction × durée
SELECT 
    categorie_reduction,
    categorie_duree,
    COUNT(*) as nb_promos,
    ROUND(AVG(roi_promo_pct), 2) as roi_moyen
FROM FEATURES_PROMOTIONS
GROUP BY categorie_reduction, categorie_duree
HAVING COUNT(*) >= 3
ORDER BY roi_moyen DESC
LIMIT 15;

-- Aperçu
SELECT * FROM FEATURES_PROMOTIONS LIMIT 10;





--FEATURES_CAMPAGNES
-- Features pour optimiser le budget marketing


CREATE OR REPLACE TABLE FEATURES_CAMPAGNES AS
SELECT 
    campaign_id,
    campaign_name,
    campaign_type,
    product_category,
    target_audience,
    region,
    
    --  Features de base 
    budget,
    reach,
    taux_conversion_prevu,
    taux_conversion_reel,
    nb_ventes,
    ca_genere,
    roi,
    roi_pct,
    
    --  Features temporelles 
    start_date,
    end_date,
    duree_jours,
    
    MONTH(start_date) as mois_debut,
    
    CASE 
        WHEN MONTH(start_date) IN (12, 1, 2) THEN 'Hiver'
        WHEN MONTH(start_date) IN (3, 4, 5) THEN 'Printemps'
        WHEN MONTH(start_date) IN (6, 7, 8) THEN 'Été'
        ELSE 'Automne'
    END as saison,
    
    --  Features calculées 
    
    -- Coût par vente
    cout_par_vente,
    cout_par_client,
    
    -- Budget par jour
    CASE 
        WHEN duree_jours > 0 
        THEN ROUND(budget / duree_jours, 2)
        ELSE budget
    END as budget_par_jour,
    
    -- Ventes par jour
    CASE 
        WHEN duree_jours > 0 
        THEN ROUND(nb_ventes::FLOAT / duree_jours, 2)
        ELSE nb_ventes
    END as ventes_par_jour,
    
    -- CA par jour
    CASE 
        WHEN duree_jours > 0 
        THEN ROUND(ca_genere / duree_jours, 2)
        ELSE ca_genere
    END as ca_par_jour,
    
    -- Reach par euro investi
    CASE 
        WHEN budget > 0 
        THEN ROUND(reach / budget, 2)
        ELSE NULL
    END as reach_par_euro,
    
    -- Écart conversion prévu vs réel
    CASE 
        WHEN taux_conversion_prevu > 0 
        THEN ROUND((taux_conversion_reel - taux_conversion_prevu) / taux_conversion_prevu * 100, 2)
        ELSE NULL
    END as ecart_conversion_pct,
    
    -- Catégorie de budget
    CASE 
        WHEN budget < 100000 THEN 'Petit budget'
        WHEN budget BETWEEN 100000 AND 300000 THEN 'Budget moyen'
        ELSE 'Gros budget'
    END as categorie_budget,
    
    -- Catégorie de reach
    CASE 
        WHEN reach < 300000 THEN 'Faible reach'
        WHEN reach BETWEEN 300000 AND 700000 THEN 'Reach moyen'
        ELSE 'Fort reach'
    END as categorie_reach,
    
    --  Flags 
    
    -- Flag campagne réussie
    CASE WHEN roi_pct > 50 THEN 1 ELSE 0 END as flag_campagne_reussie,
    
    -- Flag campagne excellente
    CASE WHEN roi_pct > 100 THEN 1 ELSE 0 END as flag_campagne_excellente,
    
    -- Flag dépassement conversion
    CASE 
        WHEN taux_conversion_reel > taux_conversion_prevu 
        THEN 1 ELSE 0 
    END as flag_depasse_objectif,
    
    -- Flag gros investissement
    CASE WHEN budget > 400000 THEN 1 ELSE 0 END as flag_gros_investissement,
    
    --  Performance 
    performance

FROM PERF_CAMPAGNES
WHERE performance != 'N/A';


--  VERIFICATIONS TABLE 5 

SELECT 'Nombre de campagnes avec features' as info, COUNT(*) as valeur FROM FEATURES_CAMPAGNES;

-- Performance par type de campagne
SELECT 
    campaign_type,
    COUNT(*) as nb_campagnes,
    ROUND(AVG(roi_pct), 2) as roi_moyen,
    ROUND(AVG(reach_par_euro), 2) as reach_par_euro_moy
FROM FEATURES_CAMPAGNES
GROUP BY campaign_type
ORDER BY roi_moyen DESC;

-- Performance par audience
SELECT 
    target_audience,
    COUNT(*) as nb_campagnes,
    ROUND(AVG(roi_pct), 2) as roi_moyen,
    ROUND(AVG(taux_conversion_reel), 4) as taux_conv_moy
FROM FEATURES_CAMPAGNES
GROUP BY target_audience
ORDER BY roi_moyen DESC;

-- Performance par catégorie de budget
SELECT 
    categorie_budget,
    COUNT(*) as nb_campagnes,
    ROUND(AVG(roi_pct), 2) as roi_moyen,
    ROUND(SUM(budget), 2) as budget_total,
    ROUND(SUM(ca_genere), 2) as ca_total
FROM FEATURES_CAMPAGNES
GROUP BY categorie_budget
ORDER BY 
    CASE categorie_budget
        WHEN 'Petit budget' THEN 1
        WHEN 'Budget moyen' THEN 2
        WHEN 'Gros budget' THEN 3
    END;

-- Performance par saison
SELECT 
    saison,
    COUNT(*) as nb_campagnes,
    ROUND(AVG(roi_pct), 2) as roi_moyen
FROM FEATURES_CAMPAGNES
GROUP BY saison;

-- Répartition des flags
SELECT 
    SUM(flag_campagne_reussie) as nb_campagnes_reussies,
    SUM(flag_campagne_excellente) as nb_campagnes_excellentes,
    SUM(flag_depasse_objectif) as nb_depassent_objectif,
    SUM(flag_gros_investissement) as nb_gros_investissements
FROM FEATURES_CAMPAGNES;

-- Meilleures combos type × audience
SELECT 
    campaign_type,
    target_audience,
    COUNT(*) as nb_campagnes,
    ROUND(AVG(roi_pct), 2) as roi_moyen
FROM FEATURES_CAMPAGNES
GROUP BY campaign_type, target_audience
HAVING COUNT(*) >= 2
ORDER BY roi_moyen DESC
LIMIT 15;

-- Aperçu
SELECT * FROM FEATURES_CAMPAGNES LIMIT 10;





-- Stats globales sur les features clients
SELECT 
    'Clients premium' as metric,
    SUM(flag_client_premium)::FLOAT as valeur
FROM FEATURES_CLIENTS

UNION ALL

SELECT 
    'Clients à risque de churn',
    SUM(flag_risque_churn)::FLOAT
FROM FEATURES_CLIENTS

UNION ALL

SELECT 
    'Clients sensibles aux promos',
    SUM(flag_sensible_promo)::FLOAT
FROM FEATURES_CLIENTS

UNION ALL

SELECT 
    'Produits populaires',
    SUM(flag_populaire)::FLOAT
FROM FEATURES_PRODUITS

UNION ALL

SELECT 
    'Promos réussies',
    SUM(flag_promo_reussie)::FLOAT
FROM FEATURES_PROMOTIONS

UNION ALL

SELECT 
    'Campagnes réussies',
    SUM(flag_campagne_reussie)::FLOAT
FROM FEATURES_CAMPAGNES;


