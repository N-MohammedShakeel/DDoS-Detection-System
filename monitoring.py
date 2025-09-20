# monitoring.py
import time
import pandas as pd
import os
from database import init_db, log_requests_bulk, update_prediction, get_logs_in_window
from model import DDoSModel
from utils import compute_features

def start_monitoring(window_size=30):
    init_db()
    model = DDoSModel()
    print("✅ Monitoring started...")

    os.makedirs("logs", exist_ok=True)
    log_file = "./logs/access.log"
    
    try:
        with open(log_file, 'r') as f:
            f.seek(0, 2)
            last_pos = f.tell()
    except FileNotFoundError:
        last_pos = 0

    while True:
        try:
            with open(log_file, 'r') as f:
                f.seek(last_pos)
                new_lines = f.readlines()
                last_pos = f.tell()

            if not new_lines:
                time.sleep(1)
                continue

            print(f"Read {len(new_lines)} new log entries.")
            
            parsed_logs_for_df = []
            parsed_logs_for_db = [] # A list of tuples for bulk insert
            for line in new_lines:
                try:
                    ip = line.split("IP: ")[1].split(",")[0]
                    url = line.split("URL: ")[1].split(",")[0]
                    timestamp_str = line.split("Time: ")[1].strip()
                    timestamp = pd.to_datetime(timestamp_str)
                    
                    parsed_logs_for_df.append({'ip': ip, 'url': url, 'timestamp': timestamp})
                    parsed_logs_for_db.append((ip, timestamp.isoformat(), url))

                except (IndexError, ValueError):
                    continue

            if not parsed_logs_for_df:
                continue
            
            # --- MODIFIED LOGIC ---
            # Insert all new logs in one go. Much faster!
            log_requests_bulk(parsed_logs_for_db)
                
            df_new_logs = pd.DataFrame(parsed_logs_for_df)
            unique_ips_in_batch = df_new_logs['ip'].unique()
            current_time = pd.Timestamp.now()
            
            for ip in unique_ips_in_batch:
                window_start_time = current_time - pd.Timedelta(seconds=window_size)
                df_window = get_logs_in_window(ip, window_start_time)
                
                if df_window.empty:
                    continue

                request_rate, unique_urls_proxy = compute_features(df_window, window_size)
                ip_encoded = model.encode_ip(ip)

                features = pd.DataFrame(
                    [[ip_encoded, request_rate, unique_urls_proxy]],
                    columns=['src_ip_encoded', 'request_rate', 'unique_urls_proxy']
                )
                
                prediction = int(model.predict(features)[0])
                
                update_prediction(ip, request_rate, unique_urls_proxy, prediction, window_start_time)

                if prediction == 1:
                    print(f"🚨 DDoS Attack Detected: IP={ip}, Rate={request_rate:.2f} req/s, Unique URLs={unique_urls_proxy:.0f}")
                else:
                    print(f"✅ No DDoS Detected: IP={ip}, Rate={request_rate:.2f} req/s, Unique URLs={unique_urls_proxy:.0f}")

        except Exception as e:
            print(f"An error occurred in the monitoring loop: {e}")
        
        print("Waiting for 5 seconds before next check...")
        time.sleep(5)

if __name__ == "__main__":
    start_monitoring()