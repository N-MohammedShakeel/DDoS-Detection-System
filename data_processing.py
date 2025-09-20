import pandas as pd
import numpy as np
import glob
import os
from pathlib import Path


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace, lowercase and convert spaces to underscore for column names."""
    mapping = {c: c.strip().lower().replace(" ", "_") for c in df.columns}
    return df.rename(columns=mapping)


def _find_first_existing(df_cols, candidates):
    """Return first normalized candidate found in df_cols or None."""
    df_set = set(df_cols)
    for c in candidates:
        key = c.strip().lower().replace(" ", "_")
        if key in df_set:
            return key
    return None


def load_and_preprocess_data(data_path="./data/DNS-testing.csv", sample_frac=0.01, sample_limit_files=2):
    """
    Loads and preprocesses CIC-DDoS-style CSV(s) into a DataFrame with columns:
      - src_ip (string)
      - request_rate (float)
      - unique_urls_proxy (float)
      - label (0 for BENIGN, 1 for attack)

    If data_path does not exist, this will try to combine CSVs under ./data/ (limited by sample_limit_files).
    """
    p = Path(data_path)
    # Ensure data/ folder exists
    p.parent.mkdir(parents=True, exist_ok=True)

    # If combined file doesn't exist, create it from CSV files in data/
    if not p.exists():
        csv_files = sorted(glob.glob("data/*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in 'data/' and {data_path} does not exist.")
        dfs = []
        for file in csv_files[:sample_limit_files]:
            # Read full file but sample a fraction to keep memory low during testing
            df_tmp = pd.read_csv(file, low_memory=True)
            if sample_frac is not None and 0 < sample_frac < 1:
                df_tmp = df_tmp.sample(frac=sample_frac, random_state=42)
            dfs.append(df_tmp)
        df = pd.concat(dfs, ignore_index=True)
        df.to_csv(p, index=False)
        print(f"Created combined sample file: {p} (from {len(dfs)} files)")

    # Read the combined file
    df = pd.read_csv(p, low_memory=True)
    # Normalize column names
    df = _normalize_columns(df)
    cols = df.columns.tolist()

    # Candidate names for requested canonical fields
    src_candidates = ["source_ip", "src_ip", "src ip", "sip", "flow_source_ip"]
    request_rate_candidates = [
        "flow_packets/s", "flow_packets_s", "flow_packets_per_second",
        "packets_per_second", "flow_packets_ps", "flow_packetsps", "flow_packets"
    ]
    unique_candidates = [
        "packet_length_variance", "unique_urls_proxy", "unique_urls", "unique_urls_count",
        "unique_host_count", "unique_destination_ip"
    ]
    label_candidates = ["label", "attack_label", "class", "traffic_type", "flow_label"]

    # Find columns
    src_col = _find_first_existing(cols, src_candidates)
    req_col = _find_first_existing(cols, request_rate_candidates)
    uniq_col = _find_first_existing(cols, unique_candidates)
    lab_col = _find_first_existing(cols, label_candidates)

    # Debug print
    print("Detected columns ->", {"src": src_col, "request_rate": req_col, "unique": uniq_col, "label": lab_col})

    # src_ip: synthesize if missing
    if src_col:
        df["src_ip"] = df[src_col].astype(str)
    else:
        df["src_ip"] = ["10.0.0." + str(i % 255) for i in range(len(df))]

    # request_rate
    if req_col:
        df["request_rate"] = pd.to_numeric(df[req_col], errors="coerce").fillna(0.0)
    else:
        flow_packets_col = _find_first_existing(cols, ["total_fwd_packets", "total_backward_packets", "packets"])
        if flow_packets_col:
            df["request_rate"] = pd.to_numeric(df[flow_packets_col], errors="coerce").fillna(0.0)
        else:
            df["request_rate"] = df.groupby("src_ip")["src_ip"].transform("count").astype(float)

    # unique_urls_proxy
    if uniq_col:
        df["unique_urls_proxy"] = pd.to_numeric(df[uniq_col], errors="coerce").fillna(0.0)
    else:
        pkt_len_var_col = _find_first_existing(cols, ["packet_length_variance", "packet_length_var"])
        if pkt_len_var_col:
            df["unique_urls_proxy"] = pd.to_numeric(df[pkt_len_var_col], errors="coerce").fillna(0.0)
        else:
            df["unique_urls_proxy"] = 0.0

    # label
    if lab_col:
        df["label_raw"] = df[lab_col].astype(str)
    else:
        print("Warning: no label column found. Defaulting all labels to 'BENIGN'.")
        df["label_raw"] = "BENIGN"

    df["label"] = df["label_raw"].apply(lambda x: 0 if str(x).strip().upper() == "BENIGN" else 1)

    # Keep only required columns
    out = df[["src_ip", "request_rate", "unique_urls_proxy", "label"]].copy()

    # Final dtype cleanup
    out["request_rate"] = pd.to_numeric(out["request_rate"], errors="coerce").fillna(0.0)
    out["unique_urls_proxy"] = pd.to_numeric(out["unique_urls_proxy"], errors="coerce").fillna(0.0)
    out["label"] = out["label"].astype(int)

    print(f"Processed dataset: {len(out)} rows, Label distribution:\n{out['label'].value_counts()}")
    return out


# ======================= MAIN EXECUTION =======================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load & preprocess CIC-DDoS-style CSV(s).")
    parser.add_argument("--data-path", default="./data/DNS-testing.csv",
                        help="Combined CSV output path (default: ./data/DNS-testing.csv)")
    parser.add_argument("--sample-frac", type=float, default=0.01,
                        help="Fraction to sample from each source CSV (0-1). Use 1 for full file.")
    parser.add_argument("--sample-files", type=int, default=2,
                        help="Max number of CSV source files to sample/concatenate from ./data/")
    args = parser.parse_args()

    try:
        df = load_and_preprocess_data(
            data_path=args.data_path,
            sample_frac=args.sample_frac,
            sample_limit_files=args.sample_files
        )
        print("\n=== Head of processed dataframe ===")
        print(df.head(10).to_string(index=False))
        print("\n=== Summary ===")
        print(df.describe(include="all"))
    except Exception as e:
        import traceback
        print("Error while running load_and_preprocess_data():", str(e))
        traceback.print_exc()
