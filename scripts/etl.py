"""
Simple ETL Pipeline
Extract, Transform, Load data
"""

import pandas as pd
import os
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    filename='logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class SimpleETL:
    """Simple ETL pipeline"""
    
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        logger.info("ETL Pipeline initialized")
    
    def extract(self):
        """Extract data from source"""
        logger.info("Starting data extraction...")
        try:
            # Read CSV files from data folder
            files = [f for f in os.listdir(self.input_path) if f.endswith('.csv')]
            
            if not files:
                logger.warning("No CSV files found in input folder")
                return None
            
            dfs = []
            for file in files:
                df = pd.read_csv(os.path.join(self.input_path, file))
                dfs.append(df)
                logger.info(f"Extracted {file}: {len(df)} rows")
            
            # Combine all dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Total rows extracted: {len(combined_df)}")
            return combined_df
        
        except Exception as e:
            logger.error(f"Error during extraction: {str(e)}")
            raise
    
    def transform(self, df):
        """Transform data"""
        logger.info("Starting data transformation...")
        try:
            # Remove duplicates
            df = df.drop_duplicates()
            logger.info(f"Rows after removing duplicates: {len(df)}")
            
            # Handle missing values
            df = df.fillna(0)
            logger.info("Missing values handled")
            
            # Add timestamp
            df['load_timestamp'] = datetime.now()
            logger.info("Metadata column added")
            
            return df
        
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise
    
    def load(self, df):
        """Load data to output"""
        logger.info("Starting data load...")
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_path, f"output_{timestamp}.csv")
            
            os.makedirs(self.output_path, exist_ok=True)
            df.to_csv(output_file, index=False)
            
            logger.info(f"Data loaded to {output_file}")
            print(f"✓ Data successfully loaded to {output_file}")
        
        except Exception as e:
            logger.error(f"Error during load: {str(e)}")
            raise
    
    def run(self):
        """Run complete ETL pipeline"""
        logger.info("="*50)
        logger.info("ETL Pipeline Started")
        logger.info("="*50)
        
        try:
            # Extract
            df = self.extract()
            if df is None:
                logger.error("No data to process")
                return
            
            # Transform
            df = self.transform(df)
            
            # Load
            self.load(df)
            
            logger.info("="*50)
            logger.info("ETL Pipeline Completed Successfully")
            logger.info("="*50)
            print("\n✓ ETL Pipeline completed successfully!")
        
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}")
            print(f"\n✗ ETL Pipeline failed: {str(e)}")

if __name__ == "__main__":
    etl = SimpleETL(input_path="data/", output_path="output/")
    etl.run()
