import pandas as pd
import os

def run_filter():
    input_path = "data/transactions.csv"
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Missing source file: {input_path}")

    df = pd.read_csv(input_path)
    
    # Logic: Separate valid transactions from those requiring audit
    good_data = df[df['amount'] > 0]
    bad_data = df[df['amount'] <= 0]
    
    good_data.to_csv("data/good_transactions.csv", index=False)
    bad_data.to_csv("data/bad_transactions.csv", index=False)
    
    print(f"✅ Validation Complete.")
    print(f"   - Silver Layer (Clean): {len(good_data)} records")
    print(f"   - Quarantine Layer (Audit): {len(bad_data)} records")

if __name__ == "__main__":
    run_filter()
