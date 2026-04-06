import json
import time
import os
from kafka import KafkaConsumer

# Translation Mapping
MAPPING = {
    'Diabetes': 'Tiểu đường', 'Cancer': 'Ung thư', 'Asthma': 'Hen suyễn',
    'Obesity': 'Béo phì', 'Arthritis': 'Viêm khớp', 'Hypertension': 'Cao huyết áp'
}

# Expert Pricing (Sync with API)
EXPERT_PRICING = {
    'Tiểu đường': 21450.0, 'Ung thư': 71840.0, 'Hen suyễn': 8620.0,
    'Béo phì': 18320.0, 'Viêm khớp': 14610.0, 'Cao huyết áp': 11980.0
}

def run_lowtech_consumer():
    print("[Speed Layer] Starting HIGH-STABILITY Python Consumer...")
    
    consumer = KafkaConsumer(
        'healthcare-stream',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    stats_file = "d:/health-care/output/realtime_stats.json"
    
    # Aggregates in memory (for faster UI responsiveness)
    aggregates = {vi: {"sum_actual": 0, "count": 0} for vi in MAPPING.values()}

    print(f"[Speed Layer] Monitoring Kafka on healthcare-stream. Writing to {stats_file}")

    last_save = time.time()
    
    for message in consumer:
        data = message.value
        en_condition = data.get("Medical Condition", "Diabetes")
        vi_condition = MAPPING.get(en_condition, en_condition)
        actual_billing = float(data.get("Actual Billing", 0))

        if vi_condition in aggregates:
            aggregates[vi_condition]["sum_actual"] += actual_billing
            aggregates[vi_condition]["count"] += 1
            
        # Throttled update to disk (Save every 3 seconds)
        if time.time() - last_save > 3:
            final_json = []
            for vi, vals in aggregates.items():
                if vals["count"] > 0:
                    avg_actual = vals["sum_actual"] / vals["count"]
                    # Predicted is deterministic from expert knowledge
                    avg_predicted = EXPERT_PRICING.get(vi, avg_actual)
                    
                    final_json.append({
                        "Medical Condition": vi,
                        "avg_actual": round(avg_actual, 2),
                        "avg_predicted": round(avg_predicted, 2),
                        "count": vals["count"]
                    })
            
            if final_json:
                with open(stats_file, 'w') as f:
                    json.dump(final_json, f)
                print(f"[Speed Layer] Updated {stats_file} with {len(final_json)} conditions.")
            
            last_save = time.time()

if __name__ == "__main__":
    run_lowtech_consumer()
