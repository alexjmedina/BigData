import pandas as pd
import yfinance as yf
import numpy as np

# Download historical data for the NASDAQ index from 2000 to 2023
data = yf.download('^IXIC', start='2000-01-01', end='2023-09-30')

# Create a DataFrame with the required columns
df = pd.DataFrame(index=data.index)
df['date'] = data.index
df['cost_of_sales'] = data['Close'] * 0.75  # Assuming cost is 75% of closing price

# Add random noise to 'cost_of_sales'
noise = np.random.normal(0, 100, len(df))
df['cost_of_sales'] = df['cost_of_sales'] + noise

df['selling_price'] = data['Close']
df['net_profit'] = df['selling_price'] - df['cost_of_sales']

# Round the values to two decimal places
df = df.round(2)

# Add additional columns for 'project_category' and 'project_name'
df['project_category'] = np.random.choice(['Category1', 'Category2', 'Category3'], len(df))
df['project_name'] = np.random.choice(['Proj1', 'Proj2', 'Proj3','Proj4', 'Proj5', 'Proj6', 'Proj7','Proj8', 'Proj9'], len(df))

# Save the DataFrame as a CSV file
df.to_csv('NASDAQ_data.csv', index=False)
