from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F
import json, os

def main():
    spark = SparkSession.builder \
        .appName("Healthcare ML - Distributed Training") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()

    # 1. Đọc dữ liệu từ HDFS
    hdfs_path = "hdfs://namenode:9000/healthcare/raw/healthcare_dataset.csv"
    print(f"🚀 Loading data for training: {hdfs_path}")
    df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

    # 2. Tiền xử lý dữ liệu (Phân tán)
    # Tính Length_of_Stay
    df = df.withColumn("Length_of_Stay",
        F.datediff(F.col("Discharge Date"), F.col("Date of Admission"))
    )
    
    # Loại bỏ các cột không dùng cho học máy
    drop_cols = ["Name", "Doctor", "Date of Admission", "Discharge Date"]
    df = df.drop(*drop_cols)
    
    # Loại bỏ dòng thiếu dữ liệu
    df = df.na.drop()

    # 3. Pipeline Pipeline: Indexers -> Assembler -> Scaler -> GBT
    categorical_cols = ['Gender', 'Blood Type', 'Medical Condition', 'Insurance Provider', 'Admission Type']
    numerical_cols = ['Age', 'Room Number', 'Length_of_Stay']

    # Chuyển đổi Categorical (String) -> Index (Số)
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in categorical_cols
    ]

    # Gộp tất cả features vào 1 vector
    assembler = VectorAssembler(
        inputCols=[f"{c}_idx" for c in categorical_cols] + numerical_cols,
        outputCol="features_raw"
    )

    # Chuẩn hoá dữ liệu (Standardization)
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withStd=True, withMean=True
    )

    # Mô hình Gradient Boosted Trees (mạnh mẽ hơn Linear Regression đơn giản)
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="Billing Amount",
        maxIter=20,
        maxDepth=5,
        seed=42
    )

    pipeline = Pipeline(stages=indexers + [assembler, scaler, gbt])

    # 4. Chia dữ liệu Train / Test (phân tán)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"📊 Training records: {train_df.count()}, Test records: {test_df.count()}")

    # 5. Huấn luyện (Song song trên cluster)
    print("🔥 Starting distributed training (Spark MLlib GBT)...")
    model_pipeline = pipeline.fit(train_df)

    # 6. Đánh giá mô hình
    predictions = model_pipeline.transform(test_df)
    evaluator_mae = RegressionEvaluator(labelCol="Billing Amount", predictionCol="prediction", metricName="mae")
    evaluator_rmse = RegressionEvaluator(labelCol="Billing Amount", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="Billing Amount", predictionCol="prediction", metricName="r2")

    mae = evaluator_mae.evaluate(predictions)
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print("-" * 30)
    print(f"MAE  (USD): ${mae:.2f}")
    print(f"RMSE (USD): ${rmse:.2f}")
    print(f"R2 Score  : {r2:.4f}")
    print("-" * 30)

    # 7. Lưu mô hình lên HDFS (để Flask API có thể dùng)
    model_path = "hdfs://namenode:9000/healthcare/models/gbt_pipeline_model"
    model_pipeline.write().overwrite().save(model_path)
    
    # Lưu metrics local để báo cáo
    results_path = "/app/output/json/spark_ml_metrics.json"
    with open(results_path, "w") as f:
        json.dump({"mae": mae, "rmse": rmse, "r2": r2, "status": "distributed_training_success"}, f, indent=4)

    print(f"✅ Model saved to HDFS: {model_path}")
    spark.stop()

if __name__ == "__main__":
    main()
