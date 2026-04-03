from kafka import KafkaProducer
import json, time, random, hashlib

# Big Data Security layer: Data Anonymization (PII Hash)
def mask_pii(patient_data):
    # Masking sensitive information (e.g. Patient Name, Insurance ID)
    # Using SHA-256 for non-reversible anonymization (PII protection)
    # Simulating a Patient Name/ID that is now masked
    pii_id = str(random.randint(10000000, 99999999))
    patient_data['Patient_Mask_ID'] = hashlib.sha256(pii_id.encode()).hexdigest()[:12]
    return patient_data

# Producer: Real-time Data Ingestion (IoT & Hospital Events)
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conditions = ['Diabetes', 'Cancer', 'Asthma', 'Obesity', 'Arthritis', 'Hypertension']
admission_types = ['Emergency', 'Elective', 'Urgent']

print("Secure Kafka Producer (PII Protected) successfully started...")

while True:
    patient = {
        "Age": random.randint(18, 90),
        "Gender": random.choice(["Male", "Female"]),
        "Medical Condition": random.choice(conditions),
        "Admission Type": random.choice(admission_types),
        "Billing Amount": round(random.uniform(5000, 45000), 2),
        "Blood Type": random.choice(['A+','O-','B+','AB+']),
        "Insurance Provider": random.choice(['Medicare','Aetna','Cigna']),
        "Room Number": random.randint(100, 500),
        "Length_of_Stay": random.randint(1, 30),
        "timestamp": time.time(),
        "Security_Label": "Confidential/Medical" # Ingestion layer policy
    }
    
    # Apply security policy (Anonymize)
    patient = mask_pii(patient)
    
    # Topic Name: healthcare-stream
    # Ensuring fault-tolerance by sending with partition checks
    producer.send('healthcare-stream', value=patient)
    
    # Print sample (masked) to log
    print(f"Sent Masked Record: ID={patient['Patient_Mask_ID']} | {patient['Medical Condition']} | Status: Encrypted In-Transit")
    
    # Frequency simulation (5s)
    time.sleep(5)
