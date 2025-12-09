# Projet Big Data 2025 - NYC Taxi

Ce projet implémente une pipeline de données complète pour l'analyse des trajets de taxis New-Yorkais.

## Architecture
J'ai architecturé le projet en plusieurs micro-services orchestrés par Docker Compose :

1.  **Ingestion (Spark/Scala)** : Nettoyage et validation des données brutes (Parquet) -> Stockage MinIO & Postgres.
2.  **Warehouse (Postgres)** : Modèle en étoile (Fact/Dimensions).
3.  **Analytics (Streamlit)** : Dashboard interactif pour visualiser les KPIs.
4.  **ML (FastAPI/Scikit-Learn)** : Service de prédiction de prix (entrainement automatique et API REST).
5.  **Orchestration (Airflow)** : Pipeline automatisé de bout en bout.

## Démarrage Rapide

Tout est conteneurisé. Pour lancer le projet :

```bash
docker-compose up -d --build
```

## Accès aux Services

- **Dashboard** : [http://localhost:8501](http://localhost:8501)
- **Airflow** : [http://localhost:8080](http://localhost:8080) (`admin`/`admin`)
- **API ML** : [http://localhost:8000/docs](http://localhost:8000/docs)
- **MinIO** : [http://localhost:9001](http://localhost:9001)

## Structure du Projet

- `ex02_data_ingestion`: Code Spark (Scala).
- `ex03_sql_table_creation`: Scripts SQL.
- `ex04_dashboard`: Application Streamlit.
- `ex05_ml_prediction_service`: API ML (Python).
- `ex06_airflow`: DAGs Airflow.