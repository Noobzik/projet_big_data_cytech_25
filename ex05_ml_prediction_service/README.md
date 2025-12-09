# Exercice 5 : Service de Prédiction (ML)

J'ai créé un microservice FastAPI qui prédit le prix d'une course de taxi en utilisant un modèle Random Forest entrainé sur mes données.

## Stack Technique
- **Gestion des paquets** : `uv` (FastAPI, Scikit-Learn, Pandas).
- **Conteneur** : Dockerfile multi-stage.

## Utilisation

### 1. Entrainement
Le modèle s'entraine automatiquement au démarrage.
Pour forcer un nouvel entrainement :
```bash
docker exec -it ml-prediction python train_model.py
```

### 2. Faire une prédiction (API)
Le service écoute sur le port **8000**.

**Endpoint** : `POST /predict`

**Exemple** :
```bash
curl -X POST "http://localhost:8000/predict" \
     -H "Content-Type: application/json" \
     -d '{
           "trip_distance": 2.5,
           "pulocation_id": 161,
           "dolocation_id": 162,
           "passenger_count": 1
         }'
```

### 3. Documentation
J'ai laissé l'interface Swagger activée sur [http://localhost:8000/docs](http://localhost:8000/docs).