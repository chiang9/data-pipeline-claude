"""
Configuration Management

Handles configuration loading and validation for the data pipeline.
Supports environment variables, configuration files, and direct configuration.
"""

import os
from typing import Any, Dict, Optional, Union
from pathlib import Path
import logging
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator


logger = logging.getLogger(__name__)


class DatabaseConfig(BaseModel):
    """Database connection configuration."""
    
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port", ge=1, le=65535)
    user: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    database: str = Field(..., description="Database name")
    charset: str = Field(default="utf8mb4", description="Character set")
    
    @validator('host')
    def host_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError('Host cannot be empty')
        return v.strip()
    
    @validator('user')
    def user_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError('User cannot be empty')
        return v.strip()
    
    @validator('database')
    def database_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError('Database name cannot be empty')
        return v.strip()


class ExtractorConfig(BaseModel):
    """Extractor configuration."""
    
    type: str = Field(..., description="Extractor type (e.g., 'csv')")
    config: Dict[str, Any] = Field(default_factory=dict, description="Extractor-specific config")
    
    @validator('type')
    def type_must_be_supported(cls, v):
        supported_types = ['csv']  # Add more as needed
        if v not in supported_types:
            raise ValueError(f'Extractor type must be one of: {supported_types}')
        return v


class TransformerConfig(BaseModel):
    """Transformer configuration."""
    
    type: str = Field(..., description="Transformer type (e.g., 'passthrough')")
    config: Dict[str, Any] = Field(default_factory=dict, description="Transformer-specific config")
    
    @validator('type')
    def type_must_be_supported(cls, v):
        supported_types = ['passthrough']  # Add more as needed
        if v not in supported_types:
            raise ValueError(f'Transformer type must be one of: {supported_types}')
        return v


class LoaderConfig(BaseModel):
    """Loader configuration."""
    
    type: str = Field(..., description="Loader type (e.g., 'mysql')")
    config: Dict[str, Any] = Field(default_factory=dict, description="Loader-specific config")
    
    @validator('type')
    def type_must_be_supported(cls, v):
        supported_types = ['mysql']  # Add more as needed
        if v not in supported_types:
            raise ValueError(f'Loader type must be one of: {supported_types}')
        return v


class PipelineConfig(BaseModel):
    """Main pipeline configuration."""
    
    name: str = Field(default="data_pipeline", description="Pipeline name")
    description: str = Field(default="", description="Pipeline description")
    extractor: ExtractorConfig = Field(..., description="Extractor configuration")
    transformer: TransformerConfig = Field(..., description="Transformer configuration")
    loader: LoaderConfig = Field(..., description="Loader configuration")
    database: Optional[DatabaseConfig] = Field(None, description="Database configuration")
    
    @validator('name')
    def name_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError('Pipeline name cannot be empty')
        return v.strip()


class Config:
    """Configuration manager for the data pipeline."""
    
    def __init__(self, config_source: Optional[Union[str, Dict[str, Any], Path]] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_source: Configuration source - can be:
                - None: Load from environment variables
                - str/Path: Path to .env file or config file
                - Dict: Direct configuration dictionary
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self._config: Optional[PipelineConfig] = None
        self._load_config(config_source)
    
    def _load_config(self, source: Optional[Union[str, Dict[str, Any], Path]]) -> None:
        """
        Load configuration from the specified source.
        
        Args:
            source: Configuration source
        """
        if source is None:
            self._load_from_env()
        elif isinstance(source, dict):
            self._load_from_dict(source)
        elif isinstance(source, (str, Path)):
            path = Path(source)
            if path.suffix == '.env':
                self._load_from_env_file(path)
            else:
                raise ValueError(f"Unsupported config file type: {path.suffix}")
        else:
            raise ValueError(f"Unsupported config source type: {type(source)}")
    
    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        self.logger.info("Loading configuration from environment variables")
        
        # Load .env file if it exists
        load_dotenv()
        
        # Build database config from environment
        db_config = None
        if all(os.getenv(key) for key in ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']):
            db_config = DatabaseConfig(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT')),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME'),
                charset=os.getenv('DB_CHARSET', 'utf8mb4')
            )
        
        # Build pipeline config
        config_dict = {
            'name': os.getenv('PIPELINE_NAME', 'data_pipeline'),
            'description': os.getenv('PIPELINE_DESCRIPTION', ''),
            'extractor': {
                'type': os.getenv('EXTRACTOR_TYPE', 'csv'),
                'config': self._parse_extractor_config()
            },
            'transformer': {
                'type': os.getenv('TRANSFORMER_TYPE', 'passthrough'),
                'config': self._parse_transformer_config()
            },
            'loader': {
                'type': os.getenv('LOADER_TYPE', 'mysql'),
                'config': self._parse_loader_config()
            }
        }
        
        if db_config:
            config_dict['database'] = db_config.dict()
        
        self._config = PipelineConfig(**config_dict)
    
    def _load_from_env_file(self, file_path: Path) -> None:
        """
        Load configuration from .env file.
        
        Args:
            file_path: Path to .env file
        """
        self.logger.info(f"Loading configuration from .env file: {file_path}")
        load_dotenv(file_path)
        self._load_from_env()
    
    def _load_from_dict(self, config_dict: Dict[str, Any]) -> None:
        """
        Load configuration from dictionary.
        
        Args:
            config_dict: Configuration dictionary
        """
        self.logger.info("Loading configuration from dictionary")
        self._config = PipelineConfig(**config_dict)
    
    def _parse_extractor_config(self) -> Dict[str, Any]:
        """Parse extractor configuration from environment variables."""
        config = {}
        
        # CSV extractor config
        if os.getenv('CSV_ENCODING'):
            config['encoding'] = os.getenv('CSV_ENCODING')
        if os.getenv('CSV_DELIMITER'):
            config['delimiter'] = os.getenv('CSV_DELIMITER')
        if os.getenv('CSV_SKIP_ROWS'):
            config['skip_rows'] = int(os.getenv('CSV_SKIP_ROWS'))
        if os.getenv('CSV_MAX_ROWS'):
            config['max_rows'] = int(os.getenv('CSV_MAX_ROWS'))
        
        return config
    
    def _parse_transformer_config(self) -> Dict[str, Any]:
        """Parse transformer configuration from environment variables."""
        config = {}
        
        # Passthrough transformer config
        if os.getenv('TRANSFORMER_LOG_DETAILS'):
            config['log_details'] = os.getenv('TRANSFORMER_LOG_DETAILS').lower() == 'true'
        
        return config
    
    def _parse_loader_config(self) -> Dict[str, Any]:
        """Parse loader configuration from environment variables."""
        config = {}
        
        # MySQL loader config
        if os.getenv('MYSQL_IF_EXISTS'):
            config['if_exists'] = os.getenv('MYSQL_IF_EXISTS')
        if os.getenv('MYSQL_CHARSET'):
            config['charset'] = os.getenv('MYSQL_CHARSET')
        
        return config
    
    @property
    def pipeline_config(self) -> PipelineConfig:
        """
        Get the pipeline configuration.
        
        Returns:
            PipelineConfig: Validated pipeline configuration
            
        Raises:
            RuntimeError: If configuration is not loaded
        """
        if self._config is None:
            raise RuntimeError("Configuration not loaded")
        return self._config
    
    def get_database_config(self) -> Optional[Dict[str, Any]]:
        """
        Get database configuration as dictionary.
        
        Returns:
            Optional[Dict[str, Any]]: Database configuration or None if not configured
        """
        if self._config and self._config.database:
            return self._config.database.dict()
        return None
    
    def get_extractor_config(self) -> Dict[str, Any]:
        """
        Get extractor configuration.
        
        Returns:
            Dict[str, Any]: Extractor configuration
        """
        return {
            'type': self._config.extractor.type,
            **self._config.extractor.config
        }
    
    def get_transformer_config(self) -> Dict[str, Any]:
        """
        Get transformer configuration.
        
        Returns:
            Dict[str, Any]: Transformer configuration
        """
        return {
            'type': self._config.transformer.type,
            **self._config.transformer.config
        }
    
    def get_loader_config(self) -> Dict[str, Any]:
        """
        Get loader configuration.
        
        Returns:
            Dict[str, Any]: Loader configuration
        """
        return {
            'type': self._config.loader.type,
            **self._config.loader.config
        }
    
    def validate(self) -> bool:
        """
        Validate the current configuration.
        
        Returns:
            bool: True if configuration is valid
        """
        try:
            if self._config is None:
                self.logger.error("No configuration loaded")
                return False
            
            # Configuration is automatically validated by Pydantic
            self.logger.info("Configuration validation successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Dict[str, Any]: Configuration as dictionary
        """
        if self._config is None:
            return {}
        return self._config.dict()
    
    def __str__(self) -> str:
        """String representation of the configuration."""
        if self._config is None:
            return "Config(not loaded)"
        return f"Config(name={self._config.name}, extractor={self._config.extractor.type}, transformer={self._config.transformer.type}, loader={self._config.loader.type})"