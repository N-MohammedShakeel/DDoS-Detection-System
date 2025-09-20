# model.py
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
import joblib
import os

class DDoSModel:
    def __init__(self, model_path="./models/rf_model.pkl"):
        self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        self.le = LabelEncoder()
        self.model_path = model_path

        if os.path.exists(model_path):
            self.load_model()
        else:
            # The model will now train on our custom log data by default
            self.train_model()

    # --- MODIFIED LINE ---
    def train_model(self, data_path="./data/training_from_logs.csv"):
        print(f"Training model with custom data: {data_path}")
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Training data not found at {data_path}. Please run create_training_data.py first.")
            
        df = pd.read_csv(data_path)
        print(f"Dataset loaded: {len(df)} rows")
        print(f"Sample data:\n{df[['src_ip', 'request_rate', 'unique_urls_proxy', 'label']].head()}")
        print(f"Label distribution:\n{df['label'].value_counts()}")

        self.le.fit(df['src_ip'])
        df['src_ip_encoded'] = self.le.transform(df['src_ip'])

        X = df[['src_ip_encoded', 'request_rate', 'unique_urls_proxy']]
        y = df['label']

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        self.model.fit(X_train, y_train)

        os.makedirs("models", exist_ok=True)
        joblib.dump(self.model, self.model_path)
        joblib.dump(self.le, "./models/label_encoder.pkl")

        accuracy = self.model.score(X_test, y_test)
        print(f"✅ Model trained and saved. Test accuracy: {accuracy:.2f}")

    def load_model(self):
        self.model = joblib.load(self.model_path)
        self.le = joblib.load("./models/label_encoder.pkl")
        print("✅ Model and label encoder loaded.")

    def predict(self, features):
        return self.model.predict(features)

    def encode_ip(self, ip):
        try:
            return self.le.transform([ip])[0]
        except ValueError:
            # This is now expected behavior for any IPs not in our small training set
            return -1 # Assign a consistent "unknown" value