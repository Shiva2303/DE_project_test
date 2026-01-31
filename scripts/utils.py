"""
Utility functions for ETL
"""

import pandas as pd
import logging

def read_csv(file_path):
    """Read CSV file"""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Read CSV: {file_path} with {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Error reading CSV: {str(e)}")
        raise

def save_csv(df, file_path):
    """Save DataFrame to CSV"""
    try:
        df.to_csv(file_path, index=False)
        logging.info(f"Saved CSV: {file_path}")
    except Exception as e:
        logging.error(f"Error saving CSV: {str(e)}")
        raise

def validate_data(df, required_columns=None):
    """Validate data quality"""
    issues = []
    
    if df is None or len(df) == 0:
        issues.append("DataFrame is empty")
    
    if required_columns:
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
    
    return issues
