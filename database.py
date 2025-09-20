# database.py
import sqlite3
import pandas as pd
import threading

db_lock = threading.Lock()

def init_db():
    with db_lock:
        conn = sqlite3.connect("logs/requests.db", check_same_thread=False, timeout=10)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS requests
                     (ip TEXT, timestamp TEXT, url TEXT, request_rate REAL, 
                      unique_urls_proxy REAL, prediction INTEGER)''')
        conn.commit()
        conn.close()

def log_requests_bulk(log_entries: list):
    """
    Inserts a list of log entries in a single, efficient transaction.
    log_entries should be a list of tuples: [(ip, timestamp, url), ...]
    """
    with db_lock:
        conn = sqlite3.connect("logs/requests.db", check_same_thread=False, timeout=10)
        c = conn.cursor()
        try:
            # Use executemany for a massive performance boost
            c.executemany("INSERT INTO requests (ip, timestamp, url, request_rate, unique_urls_proxy, prediction) "
                          "VALUES (?, ?, ?, 0.0, 0.0, 0)", log_entries)
            conn.commit()
        except sqlite3.Error as e:
            print(f"Database error in log_requests_bulk: {e}")
        finally:
            conn.close()

def get_logs_in_window(ip: str, window_start_time: pd.Timestamp):
    start_time_iso = window_start_time.isoformat()
    with db_lock:
        conn = sqlite3.connect("logs/requests.db", check_same_thread=False, timeout=10)
        query = "SELECT * FROM requests WHERE ip = ? AND timestamp >= ?"
        try:
            df = pd.read_sql_query(query, conn, params=(ip, start_time_iso))
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        except sqlite3.Error as e:
            print(f"Database error in get_logs_in_window: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

def update_prediction(ip: str, request_rate: float, unique_urls_proxy: float, prediction: int, window_start_time: pd.Timestamp):
    start_time_iso = window_start_time.isoformat()
    with db_lock:
        conn = sqlite3.connect("logs/requests.db", check_same_thread=False, timeout=10)
        c = conn.cursor()
        try:
            c.execute("UPDATE requests SET request_rate = ?, unique_urls_proxy = ?, prediction = ? "
                      "WHERE ip = ? AND timestamp >= ?",
                      (request_rate, unique_urls_proxy, prediction, ip, start_time_iso))
            conn.commit()
            print(f"Updated prediction for IP {ip}: Rate={request_rate:.2f}, URLs={unique_urls_proxy:.0f}, Prediction={prediction}, Rows affected={c.rowcount}")
        except sqlite3.Error as e:
            print(f"Database error in update_prediction: {e}")
        finally:
            conn.close()

def get_recent_logs(limit=1000):
    with db_lock:
        conn = sqlite3.connect("logs/requests.db", check_same_thread=False, timeout=10)
        query = f"SELECT * FROM requests ORDER BY timestamp DESC LIMIT {limit}"
        try:
            df = pd.read_sql_query(query, conn)
        except Exception as e:
            print(f"Database error in get_recent_logs: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        return df