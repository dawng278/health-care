# -----------------------------------------------------------------------------
# Script: train_model.py (Member 3 - ML Engineer)
# -----------------------------------------------------------------------------

import os
import json
import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping

# PATH CONFIG
# Note: Dùng đường dẫn tương đối từ vị trí file script trong thư mục ml/
INPUT_DATA = os.path.join("..", "output", "cleaned_data.csv")
ML_DIR = os.path.dirname(os.path.abspath(__file__)) 
CHART_DIR = os.path.join("..", "output", "charts")

os.makedirs(CHART_DIR, exist_ok=True)

def train_pipeline():
    print("[Step 1] Loading cleaned data from Hadoop output...")
    if not os.path.exists(INPUT_DATA):
        print(f"Error: {INPUT_DATA} not found! Run Hadoop pipeline first.")
        # Nếu chưa có file cleaned_data.csv, ta sẽ thử dùng healthcare_dataset.csv gốc để demo
        INPUT_DATA_FALLBACK = os.path.join("..", "hadoop", "data", "raw", "healthcare_dataset.csv")
        if os.path.exists(INPUT_DATA_FALLBACK):
            print(f"Sử dụng dữ liệu gốc tại {INPUT_DATA_FALLBACK} để huấn luyện thử nghiệm...")
            df = pd.read_csv(INPUT_DATA_FALLBACK)
            # Giả lập cột Length_of_Stay nếu chưa có
            if 'Length_of_Stay' not in df.columns and 'Date of Admission' in df.columns and 'Discharge Date' in df.columns:
                df['Date of Admission'] = pd.to_datetime(df['Date of Admission'])
                df['Discharge Date'] = pd.to_datetime(df['Discharge Date'])
                df['Length_of_Stay'] = (df['Discharge Date'] - df['Date of Admission']).dt.days
        else:
            return
    else:
        df = pd.read_csv(INPUT_DATA)

    # 1. TIỀN XỬ LÝ (PREPROCESSING)
    print("[Step 2] Preprocessing features...")
    drop_cols = ['Name', 'Doctor', 'Date of Admission', 'Discharge Date']
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    categorical_cols = ['Gender', 'Blood Type', 'Medical Condition', 'Insurance Provider', 'Admission Type']
    numerical_cols = ['Age', 'Room Number', 'Length_of_Stay']
    target_col = 'Billing Amount'

    # Drop missing values
    df = df.dropna(subset=[target_col] + categorical_cols + numerical_cols)

    # Label Encoding cho Categorical
    encoders = {}
    for col in categorical_cols:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        encoders[col] = le

    # Chọn Features & Target
    X = df[categorical_cols + numerical_cols].values
    y = df[target_col].values

    # Standard Scaling
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Lưu Encoders & Scaler
    with open(os.path.join(ML_DIR, 'encoders.pkl'), 'wb') as f:
        pickle.dump(encoders, f)
    with open(os.path.join(ML_DIR, 'scaler.pkl'), 'wb') as f:
        pickle.dump(scaler, f)

    # Split data 80/20
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

    # 2. XÂY DỰNG MODEL (DEEP LEARNING)
    print("[Step 3] Building TensorFlow Model...")
    model = Sequential([
        Dense(128, activation='relu', input_shape=(X_train.shape[1],)),
        Dropout(0.3),
        Dense(64, activation='relu'),
        Dropout(0.2),
        Dense(32, activation='relu'),
        Dense(1, activation='linear')
    ])

    optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
    model.compile(optimizer=optimizer, loss='mean_squared_error', metrics=['mae'])

    # Callbacks
    early_stop = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

    # Training
    print("🔨 [Step 4] Fast training started (5 epochs for demo)...")
    history = model.fit(
        X_train, y_train,
        epochs=5,
        batch_size=64,
        validation_split=0.1,
        callbacks=[early_stop],
        verbose=1
    )

    # 3. ĐÁNH GIÁ (EVALUATION)
    print("[Step 5] Evaluating model performance...")
    y_pred = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    print("-" * 30)
    print(f"MAE  (USD): ${mae:.2f}")
    print(f"RMSE (USD): ${rmse:.2f}")
    print(f"R2 Score  : {r2:.4f}")
    print("-" * 30)

    # 4. OUTPUT & CHARTS
    model.save(os.path.join(ML_DIR, 'billing_model.keras'))
    
    with open(os.path.join(ML_DIR, 'training_history.json'), 'w') as f:
        # Convert values to list for JSON serialization
        history_dict = {k: [float(x) for x in v] for k, v in history.history.items()}
        json.dump(history_dict, f)

    plt.figure(figsize=(10, 6))
    plt.plot(history.history['loss'], label='Train Loss')
    plt.plot(history.history['val_loss'], label='Val Loss')
    plt.title('Model Loss Curve (MSE)')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(CHART_DIR, 'loss_curve.png'))
    print(f"Training completed. Outputs saved in {ML_DIR}/")

if __name__ == "__main__":
    train_pipeline()
