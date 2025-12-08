# Exercice 6 : Automatisation Airflow

J'ai mis en place un pipeline Airflow complet (`project_pipeline`) pour orchestrer l'ensemble du projet.

## Le Pipeline
1. **Création DB** : Initialisation des tables Postgres (via `creation.sql`).
2. **Setup** : Insertion des données de référence (via `insertion.sql`).
3. **Ingestion** : Lancement du job Spark `DataValidation` (via DockerOperator).
4. **ML** : Déclenchement de l'entrainement du modèle via l'API.

## Accès
- **UI** : [http://localhost:8080](http://localhost:8080)
- **Login** : `admin` / `admin`