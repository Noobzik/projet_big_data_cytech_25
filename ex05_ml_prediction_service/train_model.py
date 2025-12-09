import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import joblib
import os
import time

# Connexion DB
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'password')
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'warehouse')

def get_connection():
    return create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def train():
    print("Connexion à la base...")
    engine = get_connection()
    
    # Récupération d'un sous-ensemble pour l'entrainement
    query = """
    SELECT 
        trip_distance,
        pulocation_id,
        dolocation_id,
        passenger_count,
        fare_amount
    FROM fact_trips
    WHERE fare_amount > 0 
      AND trip_distance > 0
    LIMIT 100000;
    """
    
    try:
        print("Chargement des données...")
        df = pd.read_sql(query, engine)
        
        # Nettoyage rapide
        df = df.dropna()
        
        X = df[['trip_distance', 'pulocation_id', 'dolocation_id', 'passenger_count']]
        y = df['fare_amount']
        
        print(f"Entrainement sur {len(df)} lignes...")
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Modèle
        model = RandomForestRegressor(n_estimators=50, max_depth=10, random_state=42, n_jobs=-1)
        model.fit(X_train, y_train)
        
        # Evaluation MAE
        predictions = model.predict(X_test)
        mae = mean_absolute_error(y_test, predictions)
        print(f"Modèle entrainé. MAE: ${mae:.2f}")
        
        # Sauvegarde
        joblib.dump(model, 'model.joblib')
        print("Modèle sauvegardé : model.joblib")
        
    except Exception as e:
        print(f"Error during training: {e}")

if __name__ == "__main__":
    # Wait for DB to be potentially ready if running in compose
    time.sleep(5)
    train()
