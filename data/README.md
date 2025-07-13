# Sample Data for Data Pipeline Testing

This folder contains realistic sample data for testing the data pipeline functionality. The data represents a typical e-commerce scenario with users, products, and orders.

## Files Overview

### 📋 users.csv
**20 user records** with comprehensive customer information
- **Fields**: user_id, first_name, last_name, email, phone, date_of_birth, address, city, state, zip_code, country, registration_date, last_login, account_status
- **Use Case**: Customer master data, user analytics, segmentation
- **Data Quality**: Mixed registration dates (2023-2024), realistic contact info, geographic distribution across US

### 📦 products.csv
**20 product records** covering multiple categories
- **Fields**: product_id, product_name, category, brand, price, cost, stock_quantity, weight_kg, dimensions_cm, color, material, description, supplier_id, sku, barcode, created_date, last_updated
- **Categories**: Electronics, Footwear, Clothing, Home & Kitchen, Accessories
- **Use Case**: Product catalog management, inventory analysis, pricing optimization
- **Data Quality**: Realistic pricing (price > cost), varied stock levels, complete product specifications

### 🛒 orders.csv
**35 order records** spanning multiple time periods and statuses
- **Fields**: order_id, user_id, product_id, quantity, unit_price, total_amount, order_date, order_status, payment_method, shipping_address, city, state, zip_code, country, shipping_method, tracking_number, estimated_delivery, actual_delivery, discount_amount, tax_amount
- **Order Statuses**: delivered, shipped, processing, pending, cancelled
- **Use Case**: Sales analytics, order fulfillment, customer behavior analysis
- **Data Quality**: Referential integrity (user_id → users, product_id → products), realistic order progression

## Data Relationships

```
users (1) ←→ (many) orders (many) ←→ (1) products
```

- **users.user_id** → **orders.user_id** (one user can have multiple orders)
- **products.product_id** → **orders.product_id** (one product can appear in multiple orders)

## Sample Pipeline Usage

### Basic CSV Import
```bash
# Import users
python examples/simple_pipeline_example.py data/users.csv users_table

# Import products  
python examples/simple_pipeline_example.py data/products.csv products_table

# Import orders
python examples/simple_pipeline_example.py data/orders.csv orders_table
```

### Using the Data Pipeline Module
```python
from data_pipeline import DataPipeline

# Initialize pipeline
pipeline = DataPipeline()

# Load each dataset
pipeline.run("data/users.csv", "users")
pipeline.run("data/products.csv", "products") 
pipeline.run("data/orders.csv", "orders")
```

## Data Statistics

| Dataset | Records | Columns | Date Range | File Size |
|---------|---------|---------|------------|-----------|
| Users | 20 | 14 | 2023-01-15 to 2024-07-01 | ~2.5KB |
| Products | 20 | 17 | 2023-01-10 to 2024-07-13 | ~3.5KB |
| Orders | 35 | 20 | 2024-01-15 to 2024-07-13 | ~4.2KB |

## Testing Scenarios

### 1. **Complete ETL Workflow**
- Extract all three datasets
- Verify referential integrity
- Load into separate database tables

### 2. **Data Quality Testing**
- Test handling of different data types (dates, decimals, text)
- Validate NULL handling (actual_delivery for pending orders)
- Test special characters in addresses and names

### 3. **Performance Testing**
- Measure pipeline performance on small datasets
- Test memory usage and processing time
- Validate row count accuracy

### 4. **Error Handling**
- Test with corrupted CSV files
- Validate database connection error handling
- Test duplicate key scenarios

## Business Intelligence Use Cases

### Customer Analytics
- Customer acquisition trends by registration_date
- Geographic distribution analysis
- Account status monitoring

### Product Analytics  
- Inventory levels by category
- Price vs. cost margin analysis
- Product performance by sales volume

### Sales Analytics
- Revenue trends over time
- Order status distribution
- Payment method preferences
- Shipping method analysis

## Data Generation Notes

- **Realistic Names**: Mix of common first/last names
- **Geographic Distribution**: Major US cities across different states
- **Price Points**: Range from $34.99 to $1,299.99 covering various product tiers
- **Temporal Distribution**: Orders span 6 months with realistic patterns
- **Data Consistency**: All calculations (taxes, totals) are mathematically correct

This sample data provides a solid foundation for testing data pipeline functionality while representing real-world e-commerce scenarios.