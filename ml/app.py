# -----------------------------------------------------------------------------
# Script: app.py (Member 3 - High Speed Edition)
# Dùng Scikit-learn để huấn luyện tức thì (Real data - Instant Mode)
# -----------------------------------------------------------------------------

import sys
if sys.stdout.encoding != 'utf-8':
    try: sys.stdout.reconfigure(encoding='utf-8')
    except: pass

import os
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify
from flask_cors import CORS
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import LabelEncoder, StandardScaler

app = Flask(__name__)
CORS(app)

# Load CSV and Train AI model on Startup (Instant)
CSV_PATH = os.path.join(os.path.dirname(__file__), '..', 'hadoop', 'data', 'raw', 'healthcare_dataset.csv')

model = None
encoders = {}
scaler = StandardScaler()
features_cols = ['Gender', 'Blood Type', 'Medical Condition', 'Insurance Provider', 'Admission Type', 'Age', 'Length_of_Stay']

def train_instant_ai():
    global model, encoders, scaler
    print("⏳ [AI] Đang huấn luyện mô hình thực từ dữ liệu CSV (Siêu tốc)...")
    
    if not os.path.exists(CSV_PATH):
        print(f"❌ Error: {CSV_PATH} không tồn tại.")
        return False
        
    df = pd.read_csv(CSV_PATH)
    
    # Precompute Length_of_Stay
    df['Date of Admission'] = pd.to_datetime(df['Date of Admission'])
    df['Discharge Date'] = pd.to_datetime(df['Discharge Date'])
    df['Length_of_Stay'] = (df['Discharge Date'] - df['Date of Admission']).dt.days
    
    # Select columns
    categorical_cols = ['Gender', 'Blood Type', 'Medical Condition', 'Insurance Provider', 'Admission Type']
    
    # Process Categorical
    for col in categorical_cols:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        encoders[col] = le
    
    X = df[features_cols]
    y = df['Billing Amount']
    
    # Scale & Fit (LinearRegression fits in < 0.1s)
    X_scaled = scaler.fit_transform(X)
    model = LinearRegression()
    model.fit(X_scaled, y)
    
    print(f"✅ [AI] Mô hình thực đã sẵn sàng phục vụ! (Số bệnh nhân: {len(df)})")
    return True

# Initialize AI
train_success = train_instant_ai()

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok", 
        "mode": "production_high_speed",
        "ready": train_success
    })

@app.route('/predict', methods=['POST'])
def predict():
    try:
        data = request.json
        print(f"📥 Received Request: {data}")
        
        if not data:
            return jsonify({"status": "error", "message": "Empty payload"}), 400

        # Feature extraction
        input_data = []
        for col in features_cols:
            val = data.get(col)
            if col in encoders:
                try:
                    # Chuyển đổi categorical
                    encoded_val = encoders[col].transform([str(val)])[0]
                    input_data.append(encoded_val)
                except Exception as ve:
                    print(f"⚠️ Encoder error for {col} (val={val}): {ve}")
                    input_data.append(0) 
            else:
                try:
                    input_data.append(float(val))
                except Exception as fe:
                    print(f"⚠️ Float conversion error for {col} (val={val}): {fe}")
                    input_data.append(0.0)
        
        print(f"🔢 Input Vector: {input_data}")

        # Predict
        input_scaled = scaler.transform([input_data])
        pred = model.predict(input_scaled)[0]
        
        print(f"🎯 Prediction: {pred}")

        return jsonify({
            "status": "success",
            "predicted_billing": round(max(0, float(pred)), 2),
            "currency": "USD",
            "confidence_note": "AI thực tế được huấn luyện từ 55,500 hồ sơ bệnh nhân."
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500 # Trả về 500 để dễ debug

if __name__ == '__main__':
    print("🚀 API Production High-speed starting on port 5005...")
    app.run(host='0.0.0.0', port=5005)
