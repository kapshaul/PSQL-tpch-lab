import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

# Define the column to plot histogram
column_n = 2
# Define the number of rows to plot histogram
row_n = 1000000
# Define the file path
file_path = './lineitem.tbl'

# Load the TBL file with '|' delimiter and no header
data = pd.read_csv(file_path, delimiter='|', header=None)
# Select the column data up to the defined number of rows
column_c = data[column_n][:row_n]

# Function to determine if the entire column is numeric or not
def is_column_numeric(series):
    # Check if the first value is numeric; assume consistency for the whole column
    try:
        float(series.iloc[0])
        return True
    except ValueError:
        return False

# Determine if the column is numeric
if is_column_numeric(column_c):
    # Convert entire column to numeric
    hist_data = column_c.astype(float)
else:
    # Handle as string data
    hist_data = column_c
unique_values = len(set(hist_data))
plt.hist(hist_data, bins=unique_values, edgecolor='b')
plt.ylabel('Frequency')
plt.xlabel('Values')
plt.title('key')

plt.savefig('key.png')