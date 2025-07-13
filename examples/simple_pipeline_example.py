#!/usr/bin/env python3
"""
Simple Data Pipeline Example - POC Implementation

This example demonstrates a basic ETL pipeline that:
1. Extracts data from a CSV file
2. Transforms data (pass-through for POC)
3. Loads data into a MySQL database

Usage:
    python simple_pipeline_example.py

Requirements:
    - MySQL server running
    - .env file with database credentials
    - CSV file to process
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CSVExtractor:
    """Extracts data from CSV files."""
    
    def __init__(self):
        """Initialize CSV extractor."""
        pass
    
    def extract(self, file_path: str) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            pd.DataFrame: Extracted data
            
        Raises:
            FileNotFoundError: If CSV file doesn't exist
            Exception: If CSV parsing fails
        """
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found: {file_path}")
            
            logger.info(f"Extracting data from: {file_path}")
            df = pd.read_csv(file_path)
            logger.info(f"Successfully extracted {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract CSV data: {e}")
            raise


class PassthroughTransformer:
    """POC transformer that passes data through without modification."""
    
    def __init__(self):
        """Initialize passthrough transformer."""
        pass
    
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform data (passthrough for POC).
        
        Args:
            data (pd.DataFrame): Input data
            
        Returns:
            pd.DataFrame: Transformed data (unchanged for POC)
        """
        logger.info(f"Transforming {len(data)} rows (passthrough)")
        # Reason: POC implementation - no actual transformation
        return data.copy()


class MySQLLoader:
    """Loads data into MySQL database."""
    
    def __init__(self, connection_string: str):
        """
        Initialize MySQL loader.
        
        Args:
            connection_string (str): SQLAlchemy connection string
        """
        self.connection_string = connection_string
        self.engine = None
    
    def connect(self) -> None:
        """
        Establish database connection.
        
        Raises:
            SQLAlchemyError: If connection fails
        """
        try:
            logger.info("Connecting to MySQL database")
            self.engine = create_engine(self.connection_string)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def load(self, data: pd.DataFrame, table_name: str) -> None:
        """
        Load data into MySQL table.
        
        Args:
            data (pd.DataFrame): Data to load
            table_name (str): Target table name
            
        Raises:
            SQLAlchemyError: If loading fails
        """
        try:
            if self.engine is None:
                raise RuntimeError("Database not connected. Call connect() first.")
            
            logger.info(f"Loading {len(data)} rows into table: {table_name}")
            
            # Load data with replace mode (creates table if not exists)
            data.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='replace',  # Replace table for POC simplicity
                index=False,
                method='multi'
            )
            
            logger.info(f"Successfully loaded data into {table_name}")
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def close(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


class DataPipeline:
    """Main data pipeline orchestrator."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize data pipeline.
        
        Args:
            config (Dict[str, Any]): Pipeline configuration
        """
        self.config = config
        self.extractor = CSVExtractor()
        self.transformer = PassthroughTransformer()
        
        # Build connection string
        db_config = config['database']
        connection_string = (
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        )
        self.loader = MySQLLoader(connection_string)
    
    def run(self, csv_file: str, table_name: str) -> None:
        """
        Execute the complete pipeline.
        
        Args:
            csv_file (str): Path to input CSV file
            table_name (str): Target database table name
        """
        try:
            logger.info("Starting data pipeline execution")
            
            # Extract
            data = self.extractor.extract(csv_file)
            
            # Transform
            transformed_data = self.transformer.transform(data)
            
            # Load
            self.loader.connect()
            self.loader.load(transformed_data, table_name)
            
            logger.info("Pipeline execution completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            self.loader.close()


def load_config() -> Dict[str, Any]:
    """
    Load configuration from environment variables.
    
    Returns:
        Dict[str, Any]: Configuration dictionary
        
    Raises:
        ValueError: If required environment variables are missing
    """
    load_dotenv()
    
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    return {
        'database': {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT')),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'name': os.getenv('DB_NAME')
        }
    }


def main():
    """Main execution function."""
    try:
        # Load configuration
        config = load_config()
        
        # Initialize pipeline
        pipeline = DataPipeline(config)
        
        # Run pipeline with sample data
        current_dir = Path(__file__).parent
        csv_file = current_dir / "sample_data.csv"
        table_name = "users"
        
        pipeline.run(str(csv_file), table_name)
        
        print("✅ Pipeline execution completed successfully!")
        print(f"Data from {csv_file} has been loaded into table '{table_name}'")
        
    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()