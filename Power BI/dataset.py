import pandas as pd
import yfinance as yf
import numpy as np

# Download historical data for the NASDAQ index from 2000 to 2023
data = yf.download('^IXIC', start='2000-01-01', end='2023-09-30')

# Create a DataFrame with the required columns
df = pd.DataFrame(index=data.index)
df['date'] = data.index
df['cost_of_sales'] = data['Close'] * 0.6  # Assuming cost is 60% of closing price

# Add random noise to 'cost_of_sales'
noise = np.random.normal(0, 1, len(df))
df['cost_of_sales'] = df['cost_of_sales'] + noise

df['selling_price'] = data['Close']
df['net_profit'] = df['selling_price'] - df['cost_of_sales']

# Round the values to two decimal places
df = df.round(2)

# Save the DataFrame as a CSV file
df.to_csv('NASDAQ_data.csv', index=False)