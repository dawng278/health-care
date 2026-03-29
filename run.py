import subprocess
import time
import sys
import os

# Fix Unicode console output
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# -----------------------------------------------------------------------------
# Script: run.py 
# Một bộ khởi chạy duy nhất cho cả AI Server (Port 5000) và Dashboard (Port 8080).
# -----------------------------------------------------------------------------

def start_pipeline():
    print("="*60)
    print("🏥 [HEALTHCARE ANALYTICS PIPELINE] - PRODUCTION DEMO")
    print("="*60)
    
    # Dọn dẹp các port nếu bị kẹt (Windows)
    print("🧹 Kiểm tra và dọn dẹp port 5005 & 8080...")
    for port in [5005, 8080]:
        try:
            # Tìm PID đang dùng port
            cmd = f'netstat -ano | findstr :{port}'
            res = subprocess.check_output(cmd, shell=True).decode()
            if res:
                pid = res.split()[-1]
                if int(pid) != os.getpid():
                    subprocess.run(["taskkill", "/F", "/PID", pid, "/T"], capture_output=True)
        except:
            pass

    # 1. Bật AI Server (Port 5005) - Real Data Mode
    print("\n📦 [1/2] Đang khởi động AI Server (Port 5005)...")
    ai_process = subprocess.Popen([sys.executable, "ml/app.py"], 
                                 stdout=subprocess.PIPE, 
                                 stderr=subprocess.STDOUT, 
                                 text=True,
                                 encoding='utf-8')

    import threading
    def log_forwarder(pipe, prefix):
        for line in iter(pipe.readline, ""):
            print(f"   [{prefix}] {line.strip()}")

    # Đợi AI server báo sẵn sàng (Startup loop remains same for sync)
    print("   🚀 Đang kiểm tra trạng thái khởi động...")
    for line in iter(ai_process.stdout.readline, ""):
        print(f"   [AI] {line.strip()}")
        if "Mô hình thực đã sẵn sàng phục vụ" in line or "Running on" in line:
            break
            
    # Bắt đầu luồng chuyển tiếp log nền
    threading.Thread(target=log_forwarder, args=(ai_process.stdout, "AI"), daemon=True).start()

    # 2. Bật Web Server (Port 8080) cho Dashboard
    print("\n🌐 [2/2] Đang khởi động Dashboard Server (Port 8080)...")
    web_process = subprocess.Popen([sys.executable, "-m", "http.server", "8080", "--directory", "."],
                                  stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL)
    
    dashboard_url = "http://localhost:8080/output/charts/dashboard.html"
    print(f"\n✅ HỆ THỐNG ĐÃ SẴN SÀNG!")
    print(f"👉 Truy cập Dashboard tại: {dashboard_url}")
    print("="*60)
    print("Nhấn CTRL+C để tắt toàn bộ hệ thống.")

    try:
        while True:
            time.sleep(1)
            if ai_process.poll() is not None:
                print("⚠️ AI Server đã dừng bất thường!")
                break
    except KeyboardInterrupt:
        print("\n🛑 Đang dừng hệ thống...")
        ai_process.terminate()
        web_process.terminate()
        print("👋 Hẹn gặp lại!")

if __name__ == '__main__':
    start_pipeline()
