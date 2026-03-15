import pandas as pd
from faker import Faker
import random

fake=Faker()

def generate_bulk_data(num_rows=1000):
    data=[]
    for _ in range(num_rows):
        data.append({
            'txn_id':random.randint(100,500),
            'amount':round(random.uniform(-50,1000),2),
            'desc':fake.bs(),
            'txn_date':fake.date_this_year()
        })
    df=pd.DataFrame(data)
    df.to_csv('Transactions.csv',index=False)
    print(f"Generated {num_rows} transactions with potential duplicates and errors.")

if __name__ == "__main__":
    generate_bulk_data(1000)
