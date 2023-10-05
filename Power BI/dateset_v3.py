import pandas as pd
import yfinance as yf
import numpy as np

# Download historical data for the NASDAQ index from 2000 to 2023
data = yf.download('^IXIC', start='2000-01-01', end='2023-09-30')

# Create a DataFrame with the required columns
df = pd.DataFrame(index=data.index)
df['date'] = data.index
df['daily_sales'] = data['Close']
df['cost_of_sales'] = data['Close'] * 0.75  # Assuming cost is 75% of closing price

# Add random noise to 'cost_of_sales'
noise = np.random.normal(0, 200, len(df))
df['cost_of_sales'] = df['cost_of_sales'] + noise

# Calcilating Net Profit
df['net_profit'] = df['daily_sales'] - df['cost_of_sales']

# Round the values to two decimal places
df = df.round(2)

# Define project names and dimension table dataframes
project_names = ['Proj1', 'Proj2', 'Proj3','Proj4', 'Proj5', 'Proj6', 'Proj7','Proj8', 'Proj9']
project_categories = ['Category1', 'Category1', 'Category1','Category2', 'Category2', 'Category2', 'Category3','Category3', 'Category3']
project_status = ['In - Progress', 'In - Progress', 'In - Progress','In - Progress', 'On - hold', 'On - hold', 'On - hold','Completed', 'Completed']
project_complexity = ['High', 'Medium', 'Low','Low', 'High', 'Medium', 'Medium','Low', 'High']
project_type = ['Income Generation', 'Process Improvement', 'Cost Reduction','Working Capital Improvement', 'Income Generation', 'Process Improvement', 'Cost Reduction','Working Capital Improvement', 'Income Generation']
project_manager = ['Manager1', 'Manager2', 'Manager3','Manager1', 'Manager2', 'Manager3', 'Manager1', 'Manager2', 'Manager3']

# Assign project names, categories and project status to the DataFrame
df['project_name'] = np.random.choice(project_names, len(df))

# Adjust the cost of sales based on project name
cost_adjustments = {'Proj9': 2.5, 'Proj5': 1.8}
for project, adjustment in cost_adjustments.items():
    df.loc[df['project_name'] == project, 'cost_of_sales'] *= adjustment

# Adjust the selling price based on project name
selling_price_adjustments = {'Proj9': 1.7, 'Proj5': 1.2}
for project, adjustment in selling_price_adjustments.items():
    df.loc[df['project_name'] == project, 'daily_sales'] *= adjustment

# Defining Dimension Tables
df1 = pd.DataFrame(project_names,project_categories)
df2 = pd.DataFrame(project_names,project_status)
df3 = pd.DataFrame(project_names,project_complexity)
df4 = pd.DataFrame(project_names,project_type)
df5 = pd.DataFrame(project_names,project_manager)

# Save the DataFrame as a CSV file - Fact Table
df.to_csv('NASDAQ_data.csv', index=False)

# Save the DataFrame as a CSV file - Dimension Table: Project Category
df1.to_csv('projcategory.csv', index=False)

# Save the DataFrame as a CSV file - Fact Table - Dimension Table: Project Status
df2.to_csv('projstatus.csv', index=False)

# Save the DataFrame as a CSV file - Fact Table - Dimension Table: Project Complexity
df3.to_csv('projcomplexity.csv', index=False)

# Save the DataFrame as a CSV file - Fact Table - Dimension Table: Project Type
df4.to_csv('projtype.csv', index=False)

# Save the DataFrame as a CSV file - Fact Table - Dimension Table: Project Manager
df5.to_csv('projmanager.csv', index=False)