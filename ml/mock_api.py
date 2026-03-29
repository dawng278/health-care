# -----------------------------------------------------------------------------
# Script: mock_api.py (For Frontend Testing - Member 3)
# -----------------------------------------------------------------------------

import random
import time
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/dashboard')
def serve_dashboard():
    # Trả về file dashboard.html
    path = os.path.join(os.path.dirname(__file__), "..", "output", "charts", "dashboard.html")
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    return "Dashboard file not found", 404

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok", 
        "mode": "mock",
        "description": "Mock API for UI development"
    })

@app.route('/predict', methods=['POST'])
def predict():
    # Giả lập độ trễ xử lý AI
    time.sleep(0.5)
    
    # Trả về số ngẫu nhiên trong khoảng giá phổ biến của dataset
    mock_val = random.uniform(8000.0, 45000.0)
    
    return jsonify({
        "status": "success",
        "predicted_billing": round(mock_val, 2),
        "currency": "USD",
        "confidence_note": "MOCK DATA - No real model used."
    })

if __name__ == '__main__':
    print("⚠️  MOCK API is running for Member 4 testing...")
    app.run(host='0.0.0.0', port=5000)
