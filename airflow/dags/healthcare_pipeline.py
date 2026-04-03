from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'healthcare-admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'healthcare_analytics_distributed_pipeline',
    default_args=default_args,
    description='Pipeline phân tích y tế phân tán trên Hadoop, Hive, Spark và MLlib',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['healthcare', 'distributed', 'bigdata'],
) as dag:

    # 1. Upload dữ liệu lên HDFS
    upload_hdfs = BashOperator(
        task_id='ingest_to_hdfs',
        bash_command='docker exec namenode hadoop fs -put -f /hadoop/data/raw/healthcare_dataset.csv /healthcare/raw/',
    )

    # 2. Tạo bảng Hive
    hive_tables = BashOperator(
        task_id='hive_create_tables',
        bash_command='docker exec hive-server hive -f /hive-queries/create_tables.hql',
    )

    # 3. Chạy các Hive queries song song
    # Airflow sẽ tự chạy song song nếu có nguồn lực
    hive_age = BashOperator(task_id='hive_age_dist', bash_command='docker exec hive-server hive -f /hive-queries/query_age_dist.hql')
    hive_disease = BashOperator(task_id='hive_top_diseases', bash_command='docker exec hive-server hive -f /hive-queries/query_top_diseases.hql')
    hive_billing = BashOperator(task_id='hive_billing_stats', bash_command='docker exec hive-server hive -f /hive-queries/query_billing.hql')
    hive_admission = BashOperator(task_id='hive_admission_stats', bash_command='docker exec hive-server hive -f /hive-queries/query_admission.hql')

    # 4. Spark Analytics (Thế chỗ Pandas)
    spark_analytics = BashOperator(
        task_id='spark_distributed_analytics',
        bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /app/spark/spark_analytics.py',
    )

    # 5. Distributed ML Training
    spark_ml_training = BashOperator(
        task_id='spark_mllib_training',
        bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /app/ml/train_distributed.py',
    )

    # === Dependency Graph ===
    upload_hdfs >> hive_tables
    hive_tables >> [hive_age, hive_disease, hive_billing, hive_admission]
    [hive_age, hive_disease, hive_billing, hive_admission] >> spark_analytics
    spark_analytics >> spark_ml_training
