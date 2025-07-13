#!/usr/bin/env python3
"""
Verify Data Upload Script

Simple script to verify that data was uploaded correctly to MySQL.
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_pipeline.loaders.mysql_loader import MySQLLoader
from data_pipeline.pipeline.config import Config

def main():
    """Verify uploaded data."""
    try:
        # Load configuration
        config = Config()
        db_config = config.get_database_config()
        
        if not db_config:
            print("❌ No database configuration found")
            return
        
        # Connect to database
        loader = MySQLLoader(db_config)
        
        with loader:
            print("🔍 Checking uploaded data...\n")
            
            # Check products table
            result = loader.execute_query("SELECT COUNT(*) as count FROM products")
            if result is not None and len(result) > 0:
                count = result.iloc[0]['count']
                print(f"✅ Products table: {count} records")
                
                # Show sample data
                sample = loader.execute_query("SELECT product_name, category, price FROM products LIMIT 5")
                if sample is not None:
                    print("\n📋 Sample products:")
                    for _, row in sample.iterrows():
                        print(f"  • {row['product_name']} ({row['category']}) - ${row['price']}")
            else:
                print("❌ No products table found")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()