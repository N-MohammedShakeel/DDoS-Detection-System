# create_training_data.py
import pandas as pd
from utils import compute_features
import re
import os

print("Creating new training dataset from access.log...")

LOG_FILE = './logs/access.log'
OUTPUT_PATH = './data/training_from_logs.csv'

if not os.path.exists(LOG_FILE):
    raise FileNotFoundError(f"Log file not found at {LOG_FILE}. Please generate it first.")

# Read the raw log file
with open(LOG_FILE, 'r') as f:
    lines = f.readlines()

# Parse the log file into a DataFrame
parsed_logs = []
for line in lines:
    try:
        ip = re.search(r"IP: ([\d.]+),", line).group(1)
        timestamp_str = re.search(r"Time: (.*)", line).group(1)
        url = re.search(r"URL: ([^,]+),", line).group(1)
        timestamp = pd.to_datetime(timestamp_str)
        parsed_logs.append({'timestamp': timestamp, 'ip': ip, 'url': url})
    except AttributeError:
        # Skip malformed lines
        continue

if not parsed_logs:
    raise ValueError("Could not parse any valid log entries. The log file might be empty or in the wrong format.")

df = pd.DataFrame(parsed_logs)
df = df.sort_values('timestamp').reset_index(drop=True)

# --- Feature Engineering & Labeling ---
window_size = 30 # seconds
records = []

for i in range(len(df)):
    current_time = df.loc[i, 'timestamp']
    window_start_time = current_time - pd.Timedelta(seconds=window_size)
    
    # Get all logs within the sliding window
    window_df = df[(df['timestamp'] <= current_time) & (df['timestamp'] > window_start_time)]
    
    request_rate, unique_urls_proxy = compute_features(window_df, window_size)
    
    # Simple rule for labeling: if rate is high, it's an attack.
    # This threshold may need tuning depending on your machine's speed.
    # A rate of 10 means 10*30=300 requests in the 30s window.
    label = 1 if request_rate > 10.0 else 0 
    
    records.append({
        'src_ip': df.loc[i, 'ip'],
        'request_rate': request_rate,
        'unique_urls_proxy': unique_urls_proxy,
        'label': label
    })

df_features = pd.DataFrame(records)

# Ensure the data directory exists
os.makedirs('./data', exist_ok=True)
df_features.to_csv(OUTPUT_PATH, index=False)

print(f"✅ New training set saved to {OUTPUT_PATH}")
print("Label distribution:\n", df_features['label'].value_counts())