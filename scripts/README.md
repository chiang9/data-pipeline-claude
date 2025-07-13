# Data Upload Scripts

This directory contains scripts for uploading sample data to MySQL using the data_pipeline module.

## Scripts Overview

### 📦 upload_products.py
**Single dataset upload script for products data**

```bash
python scripts/upload_products.py
```

**Features:**
- Uploads `data/products.csv` to MySQL `products` table
- Comprehensive error handling and validation
- Detailed logging and progress reporting
- Prerequisites checking (files, dependencies, database connection)

**Output Example:**
```
🔍 Checking prerequisites...
✅ Prerequisites check passed

=== Products Data Upload Script ===
📊 Extract: 20 rows, 17 columns
🔄 Transform: 20 → 20 rows
💾 Load: 20 rows loaded to 'products'

🎉 SUCCESS: Products data uploaded successfully!
📁 Source File: data/products.csv
🗄️  Database Table: products
📊 Records Processed: 20
```

### 🗃️ upload_all_data.py
**Complete sample data upload script**

```bash
python scripts/upload_all_data.py
```

**Features:**
- Uploads all sample data in correct order (users → products → orders)
- Maintains referential integrity
- Batch processing with individual error handling
- Summary statistics and performance metrics
- Database connection verification

**Upload Order:**
1. **users.csv** → `users` table (20 records)
2. **products.csv** → `products` table (20 records)  
3. **orders.csv** → `orders` table (35 records)

**Output Example:**
```
🎉 ALL DATA UPLOADED SUCCESSFULLY!
✅ User Data (20 records)     → users     (20 rows)
✅ Product Data (20 records)  → products  (20 rows)
✅ Order Data (35 records)    → orders    (35 rows)
────────────────────────────────────────────────────
📊 Total Records Uploaded: 75
⏱️  Total Time: 2.45 seconds
🗄️  Database Tables Created: 3
```

## Prerequisites

### 1. Environment Setup
```bash
# Activate virtual environment
source venv_linux/bin/activate

# Install dependencies
pip install -r examples/requirements.txt
```

### 2. Database Configuration
```bash
# Copy environment template
cp examples/.env.example .env

# Edit with your MySQL credentials
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=your_database
```

### 3. MySQL Database Setup
```sql
-- Create database
CREATE DATABASE data_pipeline;

-- Create user (optional)
CREATE USER 'pipeline_user'@'localhost' IDENTIFIED BY 'secure_password';
GRANT ALL PRIVILEGES ON data_pipeline.* TO 'pipeline_user'@'localhost';
FLUSH PRIVILEGES;
```

## Usage Patterns

### Basic Upload
```bash
# Upload single dataset
python scripts/upload_products.py

# Upload all datasets
python scripts/upload_all_data.py
```

### With Custom Configuration
```bash
# Set custom database name
DB_NAME=test_pipeline python scripts/upload_all_data.py

# Use different table replacement strategy
# (Edit script to change if_exists parameter)
```

### Development Testing
```bash
# Quick test with products only
python scripts/upload_products.py

# Full integration test
python scripts/upload_all_data.py
```

## Error Handling

### Common Issues

**❌ Database Connection Failed**
```
💡 Check your .env file and ensure MySQL is running
💡 Verify database credentials and permissions
```

**❌ Missing CSV Files**
```
💡 Make sure all CSV files exist in the data/ directory
```

**❌ Import Errors**
```
💡 Activate virtual environment: source venv_linux/bin/activate
💡 Install dependencies: pip install -r examples/requirements.txt
```

### Debug Mode
Add detailed logging for troubleshooting:
```python
import logging
logging.getLogger('data_pipeline').setLevel(logging.DEBUG)
```

## Verification

### Check Upload Success
```sql
-- Verify table creation and row counts
SELECT 
    'users' as table_name, COUNT(*) as row_count FROM users
UNION ALL
SELECT 
    'products' as table_name, COUNT(*) as row_count FROM products  
UNION ALL
SELECT 
    'orders' as table_name, COUNT(*) as row_count FROM orders;
```

### Test Data Relationships
```sql
-- Verify referential integrity
SELECT 
    o.order_id,
    u.first_name,
    u.last_name,
    p.product_name,
    o.total_amount
FROM orders o
JOIN users u ON o.user_id = u.user_id
JOIN products p ON o.product_id = p.product_id
LIMIT 10;
```

### Sample Analytics Queries
```sql
-- Top customers by order value
SELECT 
    u.first_name,
    u.last_name,
    SUM(o.total_amount) as total_spent
FROM users u
JOIN orders o ON u.user_id = o.user_id
GROUP BY u.user_id
ORDER BY total_spent DESC
LIMIT 5;

-- Product sales performance
SELECT 
    p.product_name,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_revenue
FROM products p
JOIN orders o ON p.product_id = o.product_id
GROUP BY p.product_id
ORDER BY total_revenue DESC;
```

## Script Architecture

### Data Flow
```
CSV Files → DataPipeline → MySQL Tables
    ↓            ↓            ↓
Validation → Transform → Load & Verify
```

### Error Recovery
- **File validation** before processing
- **Database connectivity** checks
- **Individual dataset** error isolation
- **Detailed logging** for debugging
- **Graceful failure** with helpful messages

### Performance Features
- **Batch processing** for multiple datasets
- **Connection reuse** across uploads
- **Progress reporting** with metrics
- **Optimized table** creation (replace mode)

These scripts provide a complete solution for uploading and testing the sample data with the data pipeline module!