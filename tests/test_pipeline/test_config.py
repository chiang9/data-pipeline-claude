"""
Unit tests for Configuration Management.

Tests cover configuration loading, validation, and various sources.
"""

import os
import tempfile
import pytest
from unittest.mock import patch
from pydantic import ValidationError
from data_pipeline.pipeline.config import Config, PipelineConfig, DatabaseConfig


class TestDatabaseConfig:
    """Test suite for DatabaseConfig."""
    
    def test_valid_database_config(self):
        """Test creation with valid configuration."""
        # Arrange
        config_data = {
            'host': 'localhost',
            'port': 3306,
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        
        # Act
        config = DatabaseConfig(**config_data)
        
        # Assert
        assert config.host == 'localhost'
        assert config.port == 3306
        assert config.user == 'test_user'
        assert config.database == 'test_db'
        assert config.charset == 'utf8mb4'  # default
    
    def test_invalid_port_failure(self):
        """Test failure with invalid port."""
        # Arrange
        config_data = {
            'host': 'localhost',
            'port': 70000,  # Invalid port
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        
        # Act & Assert
        with pytest.raises(ValidationError):
            DatabaseConfig(**config_data)
    
    def test_empty_host_failure(self):
        """Test failure with empty host."""
        # Arrange
        config_data = {
            'host': '   ',  # Empty/whitespace host
            'port': 3306,
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        
        # Act & Assert
        with pytest.raises(ValidationError, match="Host cannot be empty"):
            DatabaseConfig(**config_data)


class TestPipelineConfig:
    """Test suite for PipelineConfig."""
    
    def test_valid_pipeline_config(self):
        """Test creation with valid configuration."""
        # Arrange
        config_data = {
            'name': 'test_pipeline',
            'extractor': {'type': 'csv', 'config': {}},
            'transformer': {'type': 'passthrough', 'config': {}},
            'loader': {'type': 'mysql', 'config': {}}
        }
        
        # Act
        config = PipelineConfig(**config_data)
        
        # Assert
        assert config.name == 'test_pipeline'
        assert config.extractor.type == 'csv'
        assert config.transformer.type == 'passthrough'
        assert config.loader.type == 'mysql'
    
    def test_invalid_extractor_type_failure(self):
        """Test failure with invalid extractor type."""
        # Arrange
        config_data = {
            'name': 'test_pipeline',
            'extractor': {'type': 'invalid_type', 'config': {}},
            'transformer': {'type': 'passthrough', 'config': {}},
            'loader': {'type': 'mysql', 'config': {}}
        }
        
        # Act & Assert
        with pytest.raises(ValidationError, match="Extractor type must be one of"):
            PipelineConfig(**config_data)


class TestConfig:
    """Test suite for Config class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.test_env_vars = {
            'DB_HOST': 'localhost',
            'DB_PORT': '3306',
            'DB_USER': 'test_user',
            'DB_PASSWORD': 'test_password',
            'DB_NAME': 'test_db',
            'PIPELINE_NAME': 'test_pipeline',
            'EXTRACTOR_TYPE': 'csv',
            'TRANSFORMER_TYPE': 'passthrough',
            'LOADER_TYPE': 'mysql'
        }
    
    def test_load_from_dict_success(self):
        """Test loading configuration from dictionary."""
        # Arrange
        config_dict = {
            'name': 'test_pipeline',
            'extractor': {'type': 'csv', 'config': {'encoding': 'utf-8'}},
            'transformer': {'type': 'passthrough', 'config': {}},
            'loader': {'type': 'mysql', 'config': {}},
            'database': {
                'host': 'localhost',
                'port': 3306,
                'user': 'test_user',
                'password': 'test_password',
                'database': 'test_db'
            }
        }
        
        # Act
        config = Config(config_dict)
        
        # Assert
        assert config.pipeline_config.name == 'test_pipeline'
        assert config.get_database_config()['host'] == 'localhost'
        assert config.get_extractor_config()['encoding'] == 'utf-8'
    
    @patch.dict(os.environ, clear=True)
    def test_load_from_env_success(self):
        """Test loading configuration from environment variables."""
        # Arrange
        with patch.dict(os.environ, self.test_env_vars):
            # Act
            config = Config()
            
            # Assert
            assert config.pipeline_config.name == 'test_pipeline'
            db_config = config.get_database_config()
            assert db_config['host'] == 'localhost'
            assert db_config['port'] == 3306
    
    def test_load_from_env_missing_db_vars(self):
        """Test loading from environment with missing database variables."""
        # Arrange - explicitly clear DB environment variables
        db_vars_to_clear = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
        
        with patch.dict(os.environ, {var: '' for var in db_vars_to_clear}, clear=False):
            with patch('data_pipeline.pipeline.config.load_dotenv'):  # Prevent loading .env file
                # Act
                config = Config()
                
                # Assert
                assert config.get_database_config() is None
    
    def test_load_from_env_file_success(self):
        """Test loading configuration from .env file."""
        # Arrange
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            for key, value in self.test_env_vars.items():
                f.write(f"{key}={value}\n")
            env_file_path = f.name
        
        try:
            # Act
            config = Config(env_file_path)
            
            # Assert
            assert config.pipeline_config.name == 'test_pipeline'
            db_config = config.get_database_config()
            assert db_config['host'] == 'localhost'
            
        finally:
            os.unlink(env_file_path)
    
    def test_load_unsupported_file_type_failure(self):
        """Test failure with unsupported file type."""
        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported config file type"):
            Config('config.json')
    
    def test_load_unsupported_source_type_failure(self):
        """Test failure with unsupported source type."""
        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported config source type"):
            Config(123)
    
    def test_validate_success(self):
        """Test successful configuration validation."""
        # Arrange
        config_dict = {
            'name': 'test_pipeline',
            'extractor': {'type': 'csv', 'config': {}},
            'transformer': {'type': 'passthrough', 'config': {}},
            'loader': {'type': 'mysql', 'config': {}}
        }
        config = Config(config_dict)
        
        # Act
        result = config.validate()
        
        # Assert
        assert result is True
    
    def test_get_extractor_config(self):
        """Test extractor configuration retrieval."""
        # Arrange
        config_dict = {
            'name': 'test_pipeline',
            'extractor': {'type': 'csv', 'config': {'encoding': 'utf-8', 'delimiter': ';'}},
            'transformer': {'type': 'passthrough', 'config': {}},
            'loader': {'type': 'mysql', 'config': {}}
        }
        config = Config(config_dict)
        
        # Act
        extractor_config = config.get_extractor_config()
        
        # Assert
        assert extractor_config['type'] == 'csv'
        assert extractor_config['encoding'] == 'utf-8'
        assert extractor_config['delimiter'] == ';'
    
    def test_get_transformer_config(self):
        """Test transformer configuration retrieval."""
        # Arrange
        config_dict = {
            'name': 'test_pipeline',
            'extractor': {'type': 'csv', 'config': {}},
            'transformer': {'type': 'passthrough', 'config': {'log_details': False}},
            'loader': {'type': 'mysql', 'config': {}}
        }
        config = Config(config_dict)
        
        # Act
        transformer_config = config.get_transformer_config()
        
        # Assert
        assert transformer_config['type'] == 'passthrough'
        assert transformer_config['log_details'] is False
    
    def test_get_loader_config(self):
        """Test loader configuration retrieval."""
        # Arrange
        config_dict = {
            'name': 'test_pipeline',
            'extractor': {'type': 'csv', 'config': {}},
            'transformer': {'type': 'passthrough', 'config': {}},
            'loader': {'type': 'mysql', 'config': {'if_exists': 'replace'}}
        }
        config = Config(config_dict)
        
        # Act
        loader_config = config.get_loader_config()
        
        # Assert
        assert loader_config['type'] == 'mysql'
        assert loader_config['if_exists'] == 'replace'
    
    def test_to_dict(self):
        """Test configuration conversion to dictionary."""
        # Arrange
        config_dict = {
            'name': 'test_pipeline',
            'extractor': {'type': 'csv', 'config': {}},
            'transformer': {'type': 'passthrough', 'config': {}},
            'loader': {'type': 'mysql', 'config': {}}
        }
        config = Config(config_dict)
        
        # Act
        result_dict = config.to_dict()
        
        # Assert
        assert result_dict['name'] == 'test_pipeline'
        assert result_dict['extractor']['type'] == 'csv'
        assert result_dict['transformer']['type'] == 'passthrough'
        assert result_dict['loader']['type'] == 'mysql'
    
    def test_str_representation(self):
        """Test string representation of config."""
        # Arrange
        config_dict = {
            'name': 'test_pipeline',
            'extractor': {'type': 'csv', 'config': {}},
            'transformer': {'type': 'passthrough', 'config': {}},
            'loader': {'type': 'mysql', 'config': {}}
        }
        config = Config(config_dict)
        
        # Act
        str_repr = str(config)
        
        # Assert
        assert "Config" in str_repr
        assert "test_pipeline" in str_repr
        assert "csv" in str_repr
        assert "passthrough" in str_repr
        assert "mysql" in str_repr