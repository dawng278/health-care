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
    
    data = request.json or {}
    condition = data.get('Medical Condition', 'Diabetes')
    
    # Realistic Pricing Map for Mock testing
    PRICING = {
        'Diabetes': (15000, 25000),
        'Cancer': (50000, 100000),
        'Asthma': (4000, 9000),
        'Obesity': (9000, 18000),
        'Arthritis': (15000, 30000),
        'Hypertension': (7000, 15000)
    }
    
    min_v, max_v = PRICING.get(condition, (10000, 40000))
    mock_val = random.uniform(min_v, max_v)
    
    return jsonify({
        "status": "success",
        "predicted_billing": round(mock_val, 2),
        "currency": "USD",
        "confidence_note": f"MOCK DATA for {condition}"
    })

if __name__ == '__main__':
    print("MOCK API is running for Member 4 testing...")
    app.run(host='0.0.0.0', port=5000)
