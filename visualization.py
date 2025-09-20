# visualization.py
import pandas as pd
import plotly.express as px

def plot_request_rates(df: pd.DataFrame):
    """Creates a line chart of request rates over time."""
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # --- MODIFIED LINE ---
    # Resample data to get requests per second ('s' is the new standard)
    requests_per_second = df.set_index('timestamp').resample('1s').size().rename('requests_per_second').reset_index()
    
    fig = px.line(
        requests_per_second, 
        x='timestamp', 
        y='requests_per_second',
        title='Requests per Second',
        labels={'timestamp': 'Time', 'requests_per_second': 'Number of Requests'}
    )
    fig.update_layout(xaxis_title='Time', yaxis_title='Requests/sec')
    return fig

def plot_top_ips(df: pd.DataFrame):
    """Creates a bar chart of the top 10 IPs by request count."""
    top_ips = df['ip'].value_counts().nlargest(10).reset_index()
    top_ips.columns = ['ip', 'count']
    
    fig = px.bar(
        top_ips,
        x='ip',
        y='count',
        title='Top 10 Source IPs by Request Count',
        labels={'ip': 'Source IP Address', 'count': 'Total Requests'}
    )
    fig.update_layout(xaxis_title='IP Address', yaxis_title='Request Count')
    return fig

def plot_prediction_pie(df: pd.DataFrame):
    """Creates a pie chart showing the breakdown of predictions."""
    prediction_counts = df['prediction'].value_counts().reset_index()
    prediction_counts.columns = ['prediction', 'count']
    
    prediction_counts['label'] = prediction_counts['prediction'].map({0: 'Benign', 1: 'DDoS'})
    
    fig = px.pie(
        prediction_counts,
        names='label',
        values='count',
        title='Prediction Breakdown',
        color='label',
        color_discrete_map={'Benign': 'green', 'DDoS': 'red'}
    )
    return fig