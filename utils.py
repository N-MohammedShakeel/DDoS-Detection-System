# utils.py
import pandas as pd

def compute_features(df: pd.DataFrame, window_size: int = 30):
    """
    Computes request rate and unique URL count from a DataFrame of logs.
    """
    if df.empty:
        return 0.0, 0.0

    # Request rate is the total number of requests divided by the window size in seconds.
    # This provides a stable metric.
    request_rate = len(df) / float(window_size)
    
    # Unique URLs is the count of distinct URLs in the window.
    unique_urls_proxy = df['url'].nunique()
    
    return request_rate, unique_urls_proxy