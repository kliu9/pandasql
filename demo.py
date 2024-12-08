import pandas as pd

df1 = pd.read_csv('data/A.csv')
df2 = pd.read_csv('data/B.csv')

merged_df = pd.merge(df1, df2, on='key1', how='inner')

print(merged_df)