#!/usr/bin/env python3
"""
Upload All Sample Data Script

This script uploads all sample data (users, products, orders) to MySQL
using the data_pipeline module in the correct order to maintain
referential integrity.

Usage:
    python scripts/upload_all_data.py

Requirements:
    - MySQL server running
    - .env file with database credentials
    - Virtual environment activated (venv_linux)
"""

import sys
import os
from pathlib import Path
import logging
import time

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


def upload_dataset(pipeline: DataPipeline, csv_file: Path, table_name: str, description: str) -> dict:
    """
    Upload a single dataset to the database.
    
    Args:
        pipeline: Initialized DataPipeline instance
        csv_file: Path to CSV file
        table_name: Target database table name
        description: Human-readable description for logging
        
    Returns:
        dict: Pipeline execution results
        
    Raises:
        RuntimeError: If upload fails
    """
    logger.info(f"📤 Uploading {description}...")
    logger.info(f"  Source: {csv_file}")
    logger.info(f"  Target: {table_name}")
    
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")
    
    # Execute ETL pipeline
    results = pipeline.run(
        source=str(csv_file),
        destination=table_name,
        extract_params={
            'encoding': 'utf-8'
            # Note: delimiter is already set to ',' by default in CSVExtractor
        },
        transform_params={
            'log_details': True
        },
        load_params={
            'if_exists': 'replace'  # Replace table if exists
        }
    )
    
    if not results['success']:
        error_msg = results.get('error_message', 'Unknown error')
        raise RuntimeError(f"Failed to upload {description}: {error_msg}")
    
    # Log success metrics
    load_stats = results['steps']['load']
    logger.info(f"  ✅ Successfully loaded {load_stats['rows_loaded']} rows")
    
    return results


def main():
    """Main execution function."""
    start_time = time.time()
    
    try:
        logger.info("=== Complete Sample Data Upload ===")
        
        # Define datasets in upload order (to maintain referential integrity)
        datasets = [
            {
                'file': 'users.csv',
                'table': 'users',
                'description': 'User Data (20 records)'
            },
            {
                'file': 'products.csv', 
                'table': 'products',
                'description': 'Product Data (20 records)'
            },
            {
                'file': 'orders.csv',
                'table': 'orders', 
                'description': 'Order Data (35 records)'
            }
        ]
        
        # Verify all CSV files exist
        data_dir = project_root / "data"
        missing_files = []
        
        for dataset in datasets:
            csv_file = data_dir / dataset['file']
            if not csv_file.exists():
                missing_files.append(str(csv_file))
        
        if missing_files:
            raise FileNotFoundError(f"Missing CSV files: {missing_files}")
        
        # Initialize data pipeline
        logger.info("🔧 Initializing data pipeline...")
        pipeline = DataPipeline()
        
        # Validate pipeline configuration once
        logger.info("🔍 Validating pipeline configuration...")
        sample_file = data_dir / datasets[0]['file']
        validation_results = pipeline.validate_pipeline(str(sample_file))
        
        if not validation_results['config_valid']:
            raise RuntimeError("Invalid pipeline configuration")
        
        if not validation_results['loader_connection']:
            raise RuntimeError("Cannot connect to database. Check your .env configuration.")
        
        logger.info("✅ Pipeline validation successful")
        
        # Upload each dataset
        upload_results = []
        total_records = 0
        
        for i, dataset in enumerate(datasets, 1):
            logger.info(f"\n--- Step {i}/{len(datasets)} ---")
            
            csv_file = data_dir / dataset['file']
            
            try:
                results = upload_dataset(
                    pipeline=pipeline,
                    csv_file=csv_file,
                    table_name=dataset['table'],
                    description=dataset['description']
                )
                
                upload_results.append({
                    'dataset': dataset,
                    'results': results,
                    'success': True
                })
                
                total_records += results['steps']['load']['rows_loaded']
                
            except Exception as e:
                upload_results.append({
                    'dataset': dataset,
                    'error': str(e),
                    'success': False
                })
                logger.error(f"❌ Failed to upload {dataset['description']}: {e}")
                raise
        
        # Summary report
        end_time = time.time()
        duration = end_time - start_time
        
        print("\n" + "="*60)
        print("🎉 ALL DATA UPLOADED SUCCESSFULLY!")
        print("="*60)
        
        for result in upload_results:
            dataset = result['dataset']
            if result['success']:
                rows = result['results']['steps']['load']['rows_loaded']
                print(f"✅ {dataset['description']:<25} → {dataset['table']:<10} ({rows:>2} rows)")
            else:
                print(f"❌ {dataset['description']:<25} → FAILED")
        
        print("-" * 60)
        print(f"📊 Total Records Uploaded: {total_records}")
        print(f"⏱️  Total Time: {duration:.2f} seconds")
        print(f"🗄️  Database Tables Created: {len([r for r in upload_results if r['success']])}")
        print("="*60)
        
        # Database connection info
        db_config = pipeline.config.get_database_config()
        if db_config:
            print(f"🔗 Database: {db_config['user']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
        
        print("\n💡 Next Steps:")
        print("  • Verify data in MySQL: SELECT COUNT(*) FROM users, products, orders;")
        print("  • Run analytics queries on the uploaded data")
        print("  • Test referential integrity between tables")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        print(f"\n❌ ERROR: {e}")
        print("💡 Make sure all CSV files exist in the data/ directory")
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


if __name__ == "__main__":
    main()