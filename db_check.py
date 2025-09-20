import sqlite3
import pandas as pd
conn = sqlite3.connect("logs/requests.db")
df = pd.read_sql_query("SELECT * FROM requests", conn)
print(df)
conn.close()