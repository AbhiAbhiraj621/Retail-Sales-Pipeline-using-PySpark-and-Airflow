#Load
import os
import glob
import pandas as pd
from scripts.utils import get_postgres_engine

PROCESSED_MONTHLY_DIR = 'data/processed/monthly_sales'
def load_data():

files = glob.glob(os.path.join(PROCESSED_MONTHLY_DIR, '*.csv'))
if not files:
raise FileNotFoundError('No processed CSV found. Run transform first.')
df = pd.read_csv(files[0])

engine = get_postgres_engine()
print('Loading to PostgreSQL table: monthly_sales')
df.to_sql('monthly_sales', con=engine, if_exists='replace', index=False)
print('Load complete.')