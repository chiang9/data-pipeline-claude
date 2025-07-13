"""
Pytest configuration and shared fixtures.

Contains common test fixtures and configurations used across test modules.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path


@pytest.fixture
def sample_dataframe():
    """
    Fixture providing a sample DataFrame for testing.
    
    Returns:
        pd.DataFrame: Sample test data
    """
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'city': ['New York', 'London', 'Tokyo', 'Paris', 'Berlin'],
        'score': [85.5, 92.0, 78.5, 88.0, 91.5]
    })


@pytest.fixture
def empty_dataframe():
    """
    Fixture providing an empty DataFrame for testing.
    
    Returns:
        pd.DataFrame: Empty DataFrame
    """
    return pd.DataFrame()


@pytest.fixture
def temp_csv_file(sample_dataframe):
    """
    Fixture providing a temporary CSV file with sample data.
    
    Args:
        sample_dataframe: Sample DataFrame fixture
        
    Returns:
        str: Path to temporary CSV file
    """
    temp_dir = tempfile.mkdtemp()
    csv_path = os.path.join(temp_dir, "test_data.csv")
    
    sample_dataframe.to_csv(csv_path, index=False)
    
    yield csv_path
    
    # Cleanup
    if os.path.exists(csv_path):
        os.remove(csv_path)
    os.rmdir(temp_dir)


@pytest.fixture
def temp_empty_csv_file():
    """
    Fixture providing a temporary empty CSV file.
    
    Returns:
        str: Path to temporary empty CSV file
    """
    temp_dir = tempfile.mkdtemp()
    csv_path = os.path.join(temp_dir, "empty.csv")
    
    with open(csv_path, 'w') as f:
        f.write("")
    
    yield csv_path
    
    # Cleanup
    if os.path.exists(csv_path):
        os.remove(csv_path)
    os.rmdir(temp_dir)


@pytest.fixture
def sample_database_config():
    """
    Fixture providing sample database configuration.
    
    Returns:
        dict: Sample database configuration
    """
    return {
        'host': 'localhost',
        'port': 3306,
        'user': 'test_user',
        'password': 'test_password',
        'database': 'test_db',
        'charset': 'utf8mb4'
    }


@pytest.fixture
def sample_pipeline_config():
    """
    Fixture providing sample pipeline configuration.
    
    Returns:
        dict: Sample pipeline configuration
    """
    return {
        'name': 'test_pipeline',
        'description': 'Test pipeline for unit tests',
        'extractor': {
            'type': 'csv',
            'config': {
                'encoding': 'utf-8',
                'delimiter': ','
            }
        },
        'transformer': {
            'type': 'passthrough',
            'config': {
                'log_details': True
            }
        },
        'loader': {
            'type': 'mysql',
            'config': {
                'if_exists': 'append'
            }
        },
        'database': {
            'host': 'localhost',
            'port': 3306,
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
    }


@pytest.fixture
def temp_env_file():
    """
    Fixture providing a temporary .env file with sample configuration.
    
    Returns:
        str: Path to temporary .env file
    """
    env_content = """DB_HOST=localhost
DB_PORT=3306
DB_USER=test_user
DB_PASSWORD=test_password
DB_NAME=test_db
PIPELINE_NAME=test_pipeline
EXTRACTOR_TYPE=csv
TRANSFORMER_TYPE=passthrough
LOADER_TYPE=mysql
CSV_ENCODING=utf-8
CSV_DELIMITER=,
"""
    
    temp_dir = tempfile.mkdtemp()
    env_path = os.path.join(temp_dir, ".env")
    
    with open(env_path, 'w') as f:
        f.write(env_content)
    
    yield env_path
    
    # Cleanup
    if os.path.exists(env_path):
        os.remove(env_path)
    os.rmdir(temp_dir)


# Configure pytest to capture logs
@pytest.fixture(autouse=True)
def configure_logging():
    """
    Auto-use fixture to configure logging for tests.
    """
    import logging
    
    # Set logging level to DEBUG for tests
    logging.getLogger('data_pipeline').setLevel(logging.DEBUG)
    
    # Create console handler with formatting
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    # Add handler if not already present
    logger = logging.getLogger('data_pipeline')
    if not logger.handlers:
        logger.addHandler(handler)
    
    yield
    
    # Cleanup handlers after tests
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)