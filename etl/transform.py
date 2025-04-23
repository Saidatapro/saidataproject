
# Cleans and transforms Kafka or CSV input

import pandas as pd

df = pd.read_csv('../data/raw_transactions.csv')
df['total'] = df['quantity'] * df['price']
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Save cleaned version
df.to_csv('../data/cleaned_transactions.csv', index=False)
print("✅ Data transformed.")
