#!/usr/bin/env python3
"""
Upload Products Data Script

This script demonstrates how to use the data_pipeline module to upload
the products sample data to a MySQL database.

Usage:
    python scripts/upload_products.py

Requirements:
    - MySQL server running
    - .env file with database credentials
    - Virtual environment activated (venv_linux)
"""

import sys
from pathlib import Path
import logging

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_pipeline import DataPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main execution function."""
    try:
        logger.info("=== Products Data Upload Script ===")
        
        # Define file paths
        products_csv = project_root / "data" / "products.csv"
        
        # Verify CSV file exists
        if not products_csv.exists():
            raise FileNotFoundError(f"Products CSV file not found: {products_csv}")
        
        logger.info(f"Products CSV file: {products_csv}")
        
        # Initialize data pipeline with environment configuration
        logger.info("Initializing data pipeline...")
        pipeline = DataPipeline()
        
        # Validate pipeline configuration
        logger.info("Validating pipeline configuration...")
        validation_results = pipeline.validate_pipeline(str(products_csv))
        
        if not validation_results['config_valid']:
            raise RuntimeError("Invalid pipeline configuration")
        
        if not validation_results['source_valid']:
            raise RuntimeError(f"Invalid source file: {products_csv}")
        
        if not validation_results['loader_connection']:
            raise RuntimeError("Cannot connect to database. Check your .env configuration.")
        
        logger.info("✅ Pipeline validation successful")
        
        # Execute the ETL pipeline
        logger.info("Starting ETL process for products data...")
        
        results = pipeline.run(
            source=str(products_csv),
            destination="products",
            extract_params={
                'encoding': 'utf-8'
                # Note: delimiter is already set to ',' by default in CSVExtractor
            },
            transform_params={
                'log_details': True
            },
            load_params={
                'if_exists': 'replace'  # Replace table if it exists
            }
        )
        
        # Display results
        logger.info("=== ETL Results ===")
        logger.info(f"Pipeline: {results['pipeline_name']}")
        logger.info(f"Source: {results['source']}")
        logger.info(f"Destination: {results['destination']}")
        logger.info(f"Success: {results['success']}")
        
        if results['success']:
            extract_stats = results['steps']['extract']
            transform_stats = results['steps']['transform']
            load_stats = results['steps']['load']
            
            logger.info(f"📊 Extract: {extract_stats['rows']} rows, {extract_stats['columns']} columns")
            logger.info(f"🔄 Transform: {transform_stats['input_rows']} → {transform_stats['output_rows']} rows")
            logger.info(f"💾 Load: {load_stats['rows_loaded']} rows loaded to '{load_stats['destination']}'")
            
            print("\n" + "="*50)
            print("🎉 SUCCESS: Products data uploaded successfully!")
            print("="*50)
            print(f"📁 Source File: {products_csv}")
            print(f"🗄️  Database Table: {load_stats['destination']}")
            print(f"📊 Records Processed: {load_stats['rows_loaded']}")
            print("="*50)
            
        else:
            error_msg = results.get('error_message', 'Unknown error')
            logger.error(f"❌ Pipeline execution failed: {error_msg}")
            print(f"\n❌ FAILED: {error_msg}")
            sys.exit(1)
            
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        print(f"\n❌ ERROR: {e}")
        print("💡 Make sure the products.csv file exists in the data/ directory")
        sys.exit(1)
        
    except RuntimeError as e:
        logger.error(f"Runtime error: {e}")
        print(f"\n❌ ERROR: {e}")
        if "database" in str(e).lower():
            print("💡 Check your .env file and ensure MySQL is running")
            print("💡 Verify database credentials and permissions")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        sys.exit(1)


def check_prerequisites():
    """Check that all prerequisites are met."""
    issues = []
    
    # Check if .env file exists
    env_file = project_root / ".env"
    if not env_file.exists():
        issues.append("Missing .env file - copy from examples/.env.example and configure")
    
    # Check if data file exists
    products_file = project_root / "data" / "products.csv"
    if not products_file.exists():
        issues.append("Missing data/products.csv file")
    
    # Check if we can import the data_pipeline module
    try:
        import data_pipeline
        _ = data_pipeline  # Use the import to avoid unused warning
    except ImportError as e:
        issues.append(f"Cannot import data_pipeline module: {e}")
    
    if issues:
        print("❌ Prerequisites check failed:")
        for issue in issues:
            print(f"  • {issue}")
        print("\n💡 Setup instructions:")
        print("  1. Activate virtual environment: source venv_linux/bin/activate")
        print("  2. Install dependencies: pip install -r examples/requirements.txt")
        print("  3. Configure database: cp examples/.env.example .env (then edit)")
        print("  4. Ensure MySQL is running and accessible")
        return False
    
    print("✅ Prerequisites check passed")
    return True


if __name__ == "__main__":
    print("🔍 Checking prerequisites...")
    if check_prerequisites():
        print()
        main()
    else:
        sys.exit(1)