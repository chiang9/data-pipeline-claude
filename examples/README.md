# Data Pipeline Example

This example demonstrates a simple ETL (Extract, Transform, Load) pipeline that reads CSV data and loads it into a MySQL database.

## Setup

1. **Activate virtual environment:**
   ```bash
   source ../venv_linux/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Setup MySQL database:**
   - Ensure MySQL server is running
   - Create a database for the pipeline
   - Create a user with appropriate permissions

4. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

## Usage

Run the simple pipeline example:
```bash
python simple_pipeline_example.py
```

This will:
1. Extract data from `sample_data.csv`
2. Transform the data (pass-through for POC)
3. Load the data into MySQL table `users`

## Files

- `simple_pipeline_example.py` - Main pipeline implementation
- `sample_data.csv` - Sample CSV data for testing
- `requirements.txt` - Python dependencies
- `.env.example` - Environment variables template

## Expected Output

```
2025-07-13 10:30:15,123 - INFO - Starting data pipeline execution
2025-07-13 10:30:15,124 - INFO - Extracting data from: sample_data.csv
2025-07-13 10:30:15,125 - INFO - Successfully extracted 5 rows
2025-07-13 10:30:15,126 - INFO - Transforming 5 rows (passthrough)
2025-07-13 10:30:15,127 - INFO - Connecting to MySQL database
2025-07-13 10:30:15,130 - INFO - Database connection successful
2025-07-13 10:30:15,135 - INFO - Loading 5 rows into table: users
2025-07-13 10:30:15,145 - INFO - Successfully loaded data into users
2025-07-13 10:30:15,146 - INFO - Database connection closed
2025-07-13 10:30:15,147 - INFO - Pipeline execution completed successfully
✅ Pipeline execution completed successfully!
Data from sample_data.csv has been loaded into table 'users'
```