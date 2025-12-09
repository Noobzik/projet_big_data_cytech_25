from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
import os

app = FastAPI(title="Taxi Fare Prediction Service")

model = None

class TripInput(BaseModel):
    trip_distance: float
    pulocation_id: int
    dolocation_id: int
    passenger_count: int = 1

@app.on_event("startup")
def load_model():
    global model
    model_path = "model.joblib"
    if os.path.exists(model_path):
        model = joblib.load(model_path)
        print("Model loaded successfully.")
    else:
        print("Model file not found. Please train the model first.")

@app.post("/predict")
def predict_fare(trip: TripInput):
    if not model:
        raise HTTPException(status_code=503, detail="Model not loaded. Service isn't ready.")
    
    # Préparation du dataframe d'entrée
    input_df = pd.DataFrame([{
        "trip_distance": trip.trip_distance,
        "pulocation_id": trip.pulocation_id,
        "dolocation_id": trip.dolocation_id,
        "passenger_count": trip.passenger_count
    }])
    
    prediction = model.predict(input_df)
    return {"predicted_fare": float(prediction[0])}

@app.post("/train")
def trigger_training():
    from train_model import train
    try:
        train()
        # Rechargement du modèle
        load_model()
        return {"status": "training completed", "model_reloaded": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Training failed: {e}")

@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None}
