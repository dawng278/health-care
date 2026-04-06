import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
import os
import json

def train_tensorflow_model():
    print("Starting TensorFlow training for Healthcare Cost Prediction...")
    file_path = "d:/health-care/hadoop/data/raw/healthcare_dataset.csv"
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    df = pd.read_csv(file_path)
    
    le_gender = LabelEncoder()
    le_condition = LabelEncoder()
    le_admission = LabelEncoder()
    
    df['Gender'] = le_gender.fit_transform(df['Gender'])
    df['Medical Condition'] = le_condition.fit_transform(df['Medical Condition'])
    df['Admission Type'] = le_admission.fit_transform(df['Admission Type'])
    
    X = df[['Age', 'Gender', 'Medical Condition', 'Admission Type']].values
    y = df['Billing Amount'].values
    
    scaler = StandardScaler()
    X = scaler.fit_transform(X)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = keras.Sequential([
        layers.Dense(64, activation='relu', input_shape=(X_train.shape[1],)),
        layers.Dropout(0.2),
        layers.Dense(32, activation='relu'),
        layers.Dense(1) 
    ])
    
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    
    print("Training Deep Learning model...")
    model.fit(X_train, y_train, epochs=5, batch_size=128, validation_split=0.2, verbose=1)
    
    model_path = "d:/health-care/ai_models/tf_cost_model"
    os.makedirs(model_path, exist_ok=True)
    # Using legacy H5 format for better compatibility
    model.save(os.path.join(model_path, "model.h5"))
    
    metadata = {
        "gender_classes": le_gender.classes_.tolist(),
        "condition_classes": le_condition.classes_.tolist(),
        "admission_classes": le_admission.classes_.tolist(),
        "engine": "TensorFlow 2.x (Distributed Simulation)"
    }
    
    with open(os.path.join(model_path, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=4)

    print(f"TensorFlow model saved successfully at {model_path}/model.h5")

if __name__ == "__main__":
    train_tensorflow_model()
