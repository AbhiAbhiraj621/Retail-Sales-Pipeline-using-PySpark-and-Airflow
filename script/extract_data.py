#Python extract
import os
import pandas as pd
RAW_DIR = 'raw_data/data'
MERGED_FN = os.path.join(RAW_DIR, 'orders_customers.csv')
def extract_data():
os.makedirs(RAW_DIR, exist_ok=True)
print('Looking for required CSVs in data/raw/')
orders_path = os.path.join(RAW_DIR, 'olist_orders_dataset.csv')
customers_path = os.path.join(RAW_DIR, 'olist_customers_dataset.csv')
payments_path = os.path.join(RAW_DIR, 'olist_order_payments_dataset.csv')

for p in [orders_path, customers_path, payments_path]:
if not os.path.exists(p):
raise FileNotFoundError(f"Required file not found: {p}")

orders = pd.read_csv(orders_path, parse_dates=['order_purchase_timestamp'], low_memory=False)
customers = pd.read_csv(customers_path, low_memory=False)
payments = pd.read_csv(payments_path, low_memory=False)

merged = orders.merge(customers, on='customer_id', how='left')

payments_agg = payments.groupby('order_id', as_index=False)['payment_value'].sum()
merged = merged.merge(payments_agg, on='order_id', how='left')

merged.to_csv(MERGED_FN, index=False)
print(f'Wrote merged raw file to: {MERGED_FN}')