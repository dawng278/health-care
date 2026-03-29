#!/bin/bash
# -----------------------------------------------------------------------------
# Script: upload_to_hdfs.sh
# Mô tả: Khởi tạo cấu trúc thư mục HDFS và upload dữ liệu thô.
# -----------------------------------------------------------------------------

# 1. Kiểm tra biến môi trường
HADOOP_BIN="${HADOOP_HOME}/bin/hadoop"
if [ -z "$HADOOP_HOME" ]; then
    echo "[ERROR] HADOOP_HOME chưa được định nghĩa!"
    exit 1
fi

HDFS_PATH="${HDFS_HOST:-hdfs://localhost:9000}"
RAW_DIR="/healthcare/raw"
OUT_DIR="/healthcare/output"
LOCAL_FILE="hadoop/data/raw/healthcare_dataset.csv"

echo "[INFO] Đang sử dụng HDFS Host: $HDFS_PATH"

# 2. Tạo thư mục nếu chưa có
echo "[INFO] Đang kiểm tra cấu trúc thư mục HDFS..."
$HADOOP_BIN fs -mkdir -p $RAW_DIR $OUT_DIR
if [ $? -eq 0 ]; then
    echo "[OK] Thư mục HDFS đã sẵn sàng."
else
    echo "[ERROR] Không thể tạo thư mục trên HDFS."
    exit 1
fi

# 3. Kiểm tra file local
if [ ! -f "$LOCAL_FILE" ]; then
    echo "[ERROR] Không tìm thấy file local tại: $LOCAL_FILE"
    exit 1
fi

# 4. Kiểm tra file tồn tại trên HDFS để hỏi Overwrite
$HADOOP_BIN fs -test -e $RAW_DIR/healthcare_dataset.csv
if [ $? -eq 0 ]; then
    echo "[WARNING] File đã tồn tại trên HDFS."
    read -p "Bạn có muốn ghi đè (Overwrite) không? [y/N]: " choice
    if [[ ! $choice =~ ^[Yy]$ ]]; then
        echo "[INFO] Đã hủy bỏ quá trình upload."
        exit 0
    fi
fi

# 5. Thực hiện Upload
echo "[INFO] Đang upload $LOCAL_FILE lên $RAW_DIR..."
$HADOOP_BIN fs -put -f $LOCAL_FILE $RAW_DIR/
if [ $? -eq 0 ]; then
    echo "[OK] Upload thành công!"
else
    echo "[ERROR] Upload thất bại."
    exit 1
fi

$HADOOP_BIN fs -ls $RAW_DIR
