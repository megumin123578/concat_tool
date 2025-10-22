import pandas as pd
df = pd.read_excel(r'log_data\temp.xlsx')
print(df.columns.tolist())