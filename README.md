# Data Pipeline Module - POC Implementation

A flexible ETL (Extract, Transform, Load) data pipeline module that demonstrates core functionality for ingesting CSV data, processing it through a transformation layer, and loading it into a MySQL database.

## 🚀 Quick Start

```bash
# 1. Clone and navigate to the project
cd data-pipeline-claude

# 2. Set up virtual environment
python3 -m venv venv_linux
source venv_linux/bin/activate

# 3. Install dependencies
pip install -r examples/requirements.txt

# 4. Configure database connection
cp examples/.env.example .env
# Edit .env with your MySQL credentials

# 5. Run the example pipeline
cd examples
python simple_pipeline_example.py
```

## 📋 Project Overview

This POC demonstrates a modular data pipeline architecture that:
- **Extracts** data from CSV files with robust error handling
- **Transforms** data through a configurable layer (passthrough for POC)
- **Loads** data into MySQL database with auto table creation
- **Orchestrates** the complete ETL workflow with comprehensive logging

## 🏗️ Architecture

### Core Components

```
data_pipeline/
├── extractors/          # Data extraction components
│   ├── base_extractor.py    # Abstract base class
│   └── csv_extractor.py     # CSV file extractor
├── transformers/        # Data transformation components
│   ├── base_transformer.py # Abstract base class
│   └── passthrough_transformer.py # POC pass-through
├── loaders/            # Data loading components
│   ├── base_loader.py      # Abstract base class
│   └── mysql_loader.py     # MySQL database loader
└── pipeline/           # Pipeline orchestration
    ├── pipeline.py         # Main pipeline class
    └── config.py          # Configuration management
```

### Design Patterns
- **Strategy Pattern**: Pluggable extractors, transformers, and loaders
- **Configuration Management**: Environment variables and Pydantic validation
- **Context Management**: Automatic connection handling
- **Comprehensive Logging**: Detailed operation tracking

## 🛠️ Setup Instructions

### Prerequisites
- Python 3.8+
- MySQL Server 5.7+ or 8.0+
- Git (for cloning)

### Installation Steps

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd data-pipeline-claude
   ```

2. **Create Virtual Environment**
   ```bash
   python3 -m venv venv_linux
   source venv_linux/bin/activate  # On Windows: venv_linux\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r examples/requirements.txt
   ```

4. **Set Up MySQL Database**
   ```sql
   -- Create database
   CREATE DATABASE data_pipeline;
   
   -- Create user (optional)
   CREATE USER 'pipeline_user'@'localhost' IDENTIFIED BY 'secure_password';
   GRANT ALL PRIVILEGES ON data_pipeline.* TO 'pipeline_user'@'localhost';
   FLUSH PRIVILEGES;
   ```

5. **Configure Environment Variables**
   ```bash
   cp examples/.env.example .env
   ```
   
   Edit `.env` with your MySQL credentials:
   ```env
   DB_HOST=localhost
   DB_PORT=3306
   DB_USER=pipeline_user
   DB_PASSWORD=secure_password
   DB_NAME=data_pipeline
   ```

### Verification

Run the example pipeline to verify setup:
```bash
cd examples
python simple_pipeline_example.py
```

Expected output:
```
2025-07-13 10:30:15,123 - INFO - Starting pipeline execution
2025-07-13 10:30:15,124 - INFO - Extracting data from: sample_data.csv
2025-07-13 10:30:15,125 - INFO - Successfully extracted 5 rows
2025-07-13 10:30:15,126 - INFO - Transforming 5 rows (passthrough)
2025-07-13 10:30:15,127 - INFO - Connecting to MySQL database
2025-07-13 10:30:15,135 - INFO - Loading 5 rows into table: users
✅ Pipeline execution completed successfully!
```

## 📖 Usage Examples

### Basic Usage

```python
from data_pipeline import DataPipeline

# Initialize pipeline with configuration
pipeline = DataPipeline()

# Run ETL process
results = pipeline.run(
    source="data/input.csv",
    destination="users_table"
)

print(f"Processed {results['steps']['extract']['rows']} rows")
```

### Custom Configuration

```python
from data_pipeline.pipeline.config import Config

# Configure via dictionary
config_dict = {
    'name': 'my_pipeline',
    'extractor': {
        'type': 'csv',
        'config': {'encoding': 'utf-8', 'delimiter': ';'}
    },
    'transformer': {
        'type': 'passthrough',
        'config': {'log_details': True}
    },
    'loader': {
        'type': 'mysql',
        'config': {'if_exists': 'replace'}
    },
    'database': {
        'host': 'localhost',
        'port': 3306,
        'user': 'user',
        'password': 'pass',
        'database': 'db'
    }
}

config = Config(config_dict)
pipeline = DataPipeline(config)
```

### Using Individual Components

```python
from data_pipeline.extractors import CSVExtractor
from data_pipeline.transformers import PassthroughTransformer
from data_pipeline.loaders import MySQLLoader

# Extract
extractor = CSVExtractor({'encoding': 'utf-8'})
data = extractor.extract('input.csv')

# Transform
transformer = PassthroughTransformer()
transformed_data = transformer.transform(data)

# Load
loader_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'user',
    'password': 'pass',
    'database': 'db'
}
loader = MySQLLoader(loader_config)

with loader:
    loader.load(transformed_data, 'output_table')
```

## 🧪 Testing

### Run Unit Tests
```bash
# Activate virtual environment
source venv_linux/bin/activate

# Run all tests
python -m pytest tests/ -v

# Run specific test module
python -m pytest tests/test_extractors/test_csv_extractor.py -v

# Run with coverage
python -m pytest tests/ --cov=data_pipeline --cov-report=html
```

### Test Coverage
- **70 unit tests** covering all components
- Tests for expected use cases, edge cases, and failure scenarios
- Mock-based testing for database interactions
- Pytest fixtures for consistent test data

## 📁 Project Structure

```
data-pipeline-claude/
├── data_pipeline/              # Main package
│   ├── extractors/            # Data extraction modules
│   ├── transformers/          # Data transformation modules
│   ├── loaders/              # Data loading modules
│   └── pipeline/             # Pipeline orchestration
├── examples/                  # Usage examples
│   ├── simple_pipeline_example.py
│   ├── sample_data.csv
│   ├── requirements.txt
│   └── README.md
├── tests/                    # Unit test suite
│   ├── test_extractors/
│   ├── test_transformers/
│   ├── test_loaders/
│   ├── test_pipeline/
│   └── conftest.py
├── PLANNING.md              # Project architecture
├── TASK.md                 # Task tracking
├── INITIAL.md             # Project requirements
├── CLAUDE.md              # Development guidelines
└── README.md              # This file
```

## 🔧 Configuration Options

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | MySQL host | `localhost` |
| `DB_PORT` | MySQL port | `3306` |
| `DB_USER` | MySQL username | - |
| `DB_PASSWORD` | MySQL password | - |
| `DB_NAME` | Database name | - |
| `PIPELINE_NAME` | Pipeline identifier | `data_pipeline` |
| `EXTRACTOR_TYPE` | Extractor type | `csv` |
| `TRANSFORMER_TYPE` | Transformer type | `passthrough` |
| `LOADER_TYPE` | Loader type | `mysql` |
| `CSV_ENCODING` | CSV file encoding | `utf-8` |
| `CSV_DELIMITER` | CSV delimiter | `,` |
| `CSV_SKIP_ROWS` | Rows to skip | `0` |
| `CSV_MAX_ROWS` | Max rows to read | `None` |

### Loader Options

| Option | Description | Values |
|--------|-------------|---------|
| `if_exists` | Table exists behavior | `fail`, `replace`, `append` |
| `charset` | Database charset | `utf8mb4` |

## 🚧 Current Limitations (POC Scope)

- **Data Sources**: CSV files only
- **Data Destinations**: MySQL only
- **Transformations**: Pass-through only
- **Performance**: Optimized for files up to 10,000 rows
- **Schema**: Basic type inference (VARCHAR default)

## 🎯 Future Enhancements

### Additional Data Sources
- JSON, XML, Parquet files
- API endpoints and web services
- Cloud storage (S3, GCS)
- Message queues (Kafka, RabbitMQ)

### Additional Data Destinations
- PostgreSQL, SQL Server
- NoSQL databases (MongoDB, Cassandra)
- Data warehouses (Snowflake, BigQuery)
- Data lakes and file systems

### Advanced Transformations
- Data cleaning and validation
- Type conversion and normalization
- Data enrichment and joining
- Aggregation and summarization

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Follow the development guidelines in `CLAUDE.md`
4. Write unit tests for new functionality
5. Ensure all tests pass (`pytest tests/`)
6. Commit changes (`git commit -m 'Add amazing feature'`)
7. Push to branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

### Common Issues

**Database Connection Errors**
- Verify MySQL server is running
- Check credentials in `.env` file
- Ensure database exists and user has permissions

**CSV Import Errors**
- Verify file exists and is readable
- Check file encoding (default: UTF-8)
- Ensure CSV format is valid

**Module Import Errors**
- Activate virtual environment: `source venv_linux/bin/activate`
- Install dependencies: `pip install -r examples/requirements.txt`
- Verify Python path includes project directory

### Getting Help

1. Check the [examples/](examples/) directory for usage patterns
2. Review test files in [tests/](tests/) for API examples
3. Consult the [PLANNING.md](PLANNING.md) for architecture details
4. Open an issue for bug reports or feature requests

## 📊 Project Status

- ✅ **Core ETL Pipeline**: Fully implemented
- ✅ **CSV Extraction**: Production-ready with error handling
- ✅ **MySQL Loading**: Connection pooling and table management
- ✅ **Configuration Management**: Environment and file-based config
- ✅ **Unit Testing**: Comprehensive test suite (70 tests)
- ✅ **Documentation**: Complete API documentation
- 🔄 **Performance Optimization**: Future enhancement
- 🔄 **Additional Data Sources**: Future enhancement
- 🔄 **Advanced Transformations**: Future enhancement