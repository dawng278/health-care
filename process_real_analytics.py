import pandas as pd
import json
import os

# Đường dẫn dữ liệu
CSV_PATH = 'hadoop/data/raw/healthcare_dataset.csv'
OUTPUT_DIR = 'output/json/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🔍 Đang đọc dữ liệu thực từ healthcare_dataset.csv...")
df = pd.read_csv(CSV_PATH)

# Giả lập Length_of_Stay nếu chưa tính trong CSV
df['Date of Admission'] = pd.to_datetime(df['Date of Admission'])
df['Discharge Date'] = pd.to_datetime(df['Discharge Date'])
df['Length_of_Stay'] = (df['Discharge Date'] - df['Date of Admission']).dt.days

# 1. Billing Stats
billing_stats = {
    "total_patients": int(len(df)),
    "avg": float(df['Billing Amount'].mean()),
    "min": float(df['Billing Amount'].min()),
    "max": float(df['Billing Amount'].max()),
    "median": float(df['Billing Amount'].median())
}

# 2. Age Distribution
age_bins = [0, 18, 35, 50, 65, 120]
age_labels = ['0-18', '19-35', '36-50', '51-65', '65+']
df['AgeGroup'] = pd.cut(df['Age'], bins=age_bins, labels=age_labels)
age_counts = df['AgeGroup'].value_counts().sort_index()
age_distribution = {
    "labels": age_labels,
    "values": [int(v) for v in age_counts.values]
}

# 3. Top Diseases
top_diseases_df = df.groupby('Medical Condition').agg({'Billing Amount': ['count', 'mean']}).reset_index()
top_diseases_df.columns = ['condition', 'count', 'avg_billing']
top_diseases_df = top_diseases_df.sort_values('count', ascending=False).head(10)

top_diseases = {
    "labels": top_diseases_df['condition'].tolist(),
    "counts": [int(v) for v in top_diseases_df['count'].tolist()],
    "avg_billing": [float(v) for v in top_diseases_df['avg_billing'].tolist()]
}

# 4. Admission Stats
adm_counts = df['Admission Type'].value_counts()
admission_stats = {
    "labels": adm_counts.index.tolist(),
    "counts": [int(v) for v in adm_counts.values],
    "percentages": [round(v/len(df)*100, 1) for v in adm_counts.values]
}

# Tổng hợp
all_data = {
    "billing_stats": billing_stats,
    "age_distribution": age_distribution,
    "top_diseases": top_diseases,
    "admission_stats": admission_stats
}

# Lưu ra các file JSON
with open(os.path.join(OUTPUT_DIR, 'analytics_results.json'), 'w') as f:
    json.dump(all_data, f, indent=4)

for key, val in all_data.items():
    with open(os.path.join(OUTPUT_DIR, f'{key}.json'), 'w') as f:
        json.dump(val, f, indent=4)

print("✅ Đã trích xuất số liệu thực thành công vào thư mục output/json/")
