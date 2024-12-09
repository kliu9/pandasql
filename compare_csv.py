import pandas as pd
import numpy as np

def compare_csvs(file1_path='data/merged.csv', file2_path='data/merged_A.csv'):
    """
    Compare two CSV files and return True if identical, False if different.
    """
    # Read the CSV files
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)
    
    # Quick checks for shape and columns
    if df1.shape != df2.shape or not all(df1.columns == df2.columns):
        return False
    
    # Compare actual content
    return df1.equals(df2)

if __name__ == "__main__":
    result = compare_csvs()
    print(result)