# Guide Dashboard Streamlit

## Mon Dashboard
J'ai mis en place une application Streamlit connectée directement à la base de données PostgreSQL.

### Comment le lancer
Le service est géré par Docker Compose :
```bash
docker-compose up -d --build streamlit
```

### Accès
Ouvrir : **[http://localhost:8501](http://localhost:8501)**

Fonctionnalités :
- **Metriques** : Total des courses, revenus, prix moyen.
- **Graphiques** : Analyse par quartier, moyen de paiement, etc.
- **Prédiction (ML)** : J'ai ajouté une barre latérale pour tester mon modèle de prédiction via l'API.
