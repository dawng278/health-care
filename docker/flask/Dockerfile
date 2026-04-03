FROM python:3.9

WORKDIR /app

# Cài đặt các thư viện cần thiết
COPY ml/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY . .

# Flask server port
EXPOSE 5005

# Khởi chạy app.py (Member 3)
CMD ["python", "ml/app.py"]
