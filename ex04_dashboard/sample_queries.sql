-- ========================================
-- REQUÊTES SQL POUR POWER BI
-- Base de données: warehouse (PostgreSQL)
-- ========================================

-- 1. VUE AGRÉGÉE: Trips par Borough (Pickup)
-- Utilisez cette requête comme source de données personnalisée
SELECT 
    l.borough,
    COUNT(f.trip_id) as trip_count,
    AVG(f.fare_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance
FROM 
    fact_trips f
JOIN 
    dim_location l ON f.pulocation_id = l.location_id
WHERE 
    l.borough IS NOT NULL
GROUP BY 
    l.borough
ORDER BY 
    trip_count DESC;


-- 2. VUE AGRÉGÉE: Performance par Payment Type
SELECT 
    p.payment_type_name,
    COUNT(f.trip_id) as trip_count,
    AVG(f.fare_amount) as avg_fare,
    AVG(f.total_amount) as avg_total,
    AVG(f.tip_amount) as avg_tip
FROM 
    fact_trips f
JOIN 
    dim_payment_type p ON f.payment_type_id = p.payment_type_id
GROUP BY 
    p.payment_type_name
ORDER BY 
    trip_count DESC;


-- 3. VUE AGRÉGÉE: Statistiques par Vendor
SELECT 
    v.vendor_name,
    COUNT(f.trip_id) as trip_count,
    AVG(f.fare_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance,
    SUM(f.total_amount) as total_revenue
FROM 
    fact_trips f
JOIN 
    dim_vendor v ON f.vendor_id = v.vendor_id
GROUP BY 
    v.vendor_name;


-- 4. VUE DÉTAILLÉE: Top 20 zones de pickup
SELECT 
    l.borough,
    l.zone,
    COUNT(f.trip_id) as trip_count,
    AVG(f.fare_amount) as avg_fare
FROM 
    fact_trips f
JOIN 
    dim_location l ON f.pulocation_id = l.location_id
WHERE 
    l.zone IS NOT NULL
GROUP BY 
    l.borough, l.zone
ORDER BY 
    trip_count DESC
LIMIT 20;


-- 5. VUE TEMPORELLE: Trips par jour
-- Utile pour les line charts temporels
SELECT 
    DATE(f.tpep_pickup_datetime) as trip_date,
    COUNT(f.trip_id) as trip_count,
    AVG(f.fare_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance
FROM 
    fact_trips f
WHERE 
    f.tpep_pickup_datetime IS NOT NULL
GROUP BY 
    DATE(f.tpep_pickup_datetime)
ORDER BY 
    trip_date;


-- 6. VUE TEMPORELLE: Trips par heure de la journée
SELECT 
    EXTRACT(HOUR FROM f.tpep_pickup_datetime) as pickup_hour,
    COUNT(f.trip_id) as trip_count,
    AVG(f.fare_amount) as avg_fare
FROM 
    fact_trips f
WHERE 
    f.tpep_pickup_datetime IS NOT NULL
GROUP BY 
    EXTRACT(HOUR FROM f.tpep_pickup_datetime)
ORDER BY 
    pickup_hour;


-- 7. VUE DÉTAILLÉE: Distribution des distances (binned)
-- Pour histogrammes
SELECT 
    FLOOR(f.trip_distance) as distance_mile,
    COUNT(f.trip_id) as trip_count
FROM 
    fact_trips f
WHERE 
    f.trip_distance < 50  -- Exclure les outliers
    AND f.trip_distance > 0
GROUP BY 
    FLOOR(f.trip_distance)
ORDER BY 
    distance_mile;


-- 8. VUE DÉTAILLÉE: Routes populaires (Pickup → Dropoff)
SELECT 
    pickup_loc.borough as pickup_borough,
    dropoff_loc.borough as dropoff_borough,
    pickup_loc.zone as pickup_zone,
    dropoff_loc.zone as dropoff_zone,
    COUNT(f.trip_id) as trip_count,
    AVG(f.fare_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance
FROM 
    fact_trips f
JOIN 
    dim_location pickup_loc ON f.pulocation_id = pickup_loc.location_id
JOIN 
    dim_location dropoff_loc ON f.dolocation_id = dropoff_loc.location_id
GROUP BY 
    pickup_loc.borough, dropoff_loc.borough, 
    pickup_loc.zone, dropoff_loc.zone
HAVING 
    COUNT(f.trip_id) > 100  -- Filtrer les routes peu fréquentes
ORDER BY 
    trip_count DESC
LIMIT 50;


-- 9. VUE AGRÉGÉE: Statistiques par Rate Code
SELECT 
    r.rate_code_name,
    COUNT(f.trip_id) as trip_count,
    AVG(f.fare_amount) as avg_fare,
    AVG(f.trip_distance) as avg_distance
FROM 
    fact_trips f
JOIN 
    dim_rate_code r ON f.rate_code_id = r.rate_code_id
GROUP BY 
    r.rate_code_name
ORDER BY 
    trip_count DESC;


-- 10. VUE COMPLÈTE: Échantillon de données pour exploration
-- ATTENTION: Limitée à 10,000 lignes pour les performances
SELECT 
    f.trip_id,
    f.tpep_pickup_datetime,
    f.tpep_dropoff_datetime,
    v.vendor_name,
    pickup_loc.borough as pickup_borough,
    pickup_loc.zone as pickup_zone,
    dropoff_loc.borough as dropoff_borough,
    dropoff_loc.zone as dropoff_zone,
    f.passenger_count,
    f.trip_distance,
    r.rate_code_name,
    p.payment_type_name,
    f.fare_amount,
    f.tip_amount,
    f.total_amount
FROM 
    fact_trips f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
JOIN dim_location pickup_loc ON f.pulocation_id = pickup_loc.location_id
JOIN dim_location dropoff_loc ON f.dolocation_id = dropoff_loc.location_id
JOIN dim_rate_code r ON f.rate_code_id = r.rate_code_id
JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
ORDER BY 
    f.tpep_pickup_datetime DESC
LIMIT 10000;


-- ========================================
-- NOTES D'UTILISATION
-- ========================================
-- 1. Pour utiliser ces requêtes dans Power BI:
--    - Obtenir des données → PostgreSQL
--    - Options avancées → Copier/coller la requête
--
-- 2. Les vues 1-9 sont optimisées (agrégées)
--    pour de meilleures performances
--
-- 3. La vue 10 est pour l'exploration détaillée
--    mais limitée à 10,000 lignes
--
-- 4. Pour une vue complète sans limite:
--    Importez les tables directement et
--    laissez Power BI gérer les relations
-- ========================================
