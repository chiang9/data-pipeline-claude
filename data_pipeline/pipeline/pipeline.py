"""
Data Pipeline Orchestrator

Main pipeline class that orchestrates the ETL process by coordinating
extractors, transformers, and loaders based on configuration.
"""

from typing import Any, Dict, Optional, Union
from pathlib import Path
import logging
import pandas as pd
from ..extractors import BaseExtractor, CSVExtractor
from ..transformers import BaseTransformer, PassthroughTransformer
from ..loaders import BaseLoader, MySQLLoader
from .config import Config


logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Custom exception for pipeline errors."""
    pass


class DataPipeline:
    """Main data pipeline orchestrator."""
    
    def __init__(self, config: Optional[Union[Config, Dict[str, Any], str, Path]] = None):
        """
        Initialize data pipeline.
        
        Args:
            config: Pipeline configuration - can be:
                - Config object
                - Dictionary with configuration
                - Path to configuration file
                - None (load from environment)
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Load configuration
        if isinstance(config, Config):
            self.config = config
        else:
            self.config = Config(config)
        
        # Initialize components
        self.extractor: Optional[BaseExtractor] = None
        self.transformer: Optional[BaseTransformer] = None
        self.loader: Optional[BaseLoader] = None
        
        # Pipeline state
        self._is_initialized = False
        self._execution_log = []
    
    def initialize(self) -> None:
        """
        Initialize pipeline components based on configuration.
        
        Raises:
            PipelineError: If initialization fails
        """
        try:
            self.logger.info("Initializing pipeline components")
            
            # Validate configuration
            if not self.config.validate():
                raise PipelineError("Invalid pipeline configuration")
            
            # Initialize extractor
            self.extractor = self._create_extractor()
            
            # Initialize transformer
            self.transformer = self._create_transformer()
            
            # Initialize loader
            self.loader = self._create_loader()
            
            self._is_initialized = True
            self.logger.info("Pipeline initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline initialization failed: {e}")
            raise PipelineError(f"Initialization failed: {e}")
    
    def _create_extractor(self) -> BaseExtractor:
        """
        Create extractor based on configuration.
        
        Returns:
            BaseExtractor: Configured extractor instance
            
        Raises:
            PipelineError: If extractor creation fails
        """
        extractor_config = self.config.get_extractor_config()
        extractor_type = extractor_config.pop('type')
        
        if extractor_type == 'csv':
            return CSVExtractor(extractor_config)
        else:
            raise PipelineError(f"Unsupported extractor type: {extractor_type}")
    
    def _create_transformer(self) -> BaseTransformer:
        """
        Create transformer based on configuration.
        
        Returns:
            BaseTransformer: Configured transformer instance
            
        Raises:
            PipelineError: If transformer creation fails
        """
        transformer_config = self.config.get_transformer_config()
        transformer_type = transformer_config.pop('type')
        
        if transformer_type == 'passthrough':
            return PassthroughTransformer(transformer_config)
        else:
            raise PipelineError(f"Unsupported transformer type: {transformer_type}")
    
    def _create_loader(self) -> BaseLoader:
        """
        Create loader based on configuration.
        
        Returns:
            BaseLoader: Configured loader instance
            
        Raises:
            PipelineError: If loader creation fails
        """
        loader_config = self.config.get_loader_config()
        loader_type = loader_config.pop('type')
        
        if loader_type == 'mysql':
            # Merge database config with loader config
            db_config = self.config.get_database_config()
            if db_config:
                loader_config.update(db_config)
            return MySQLLoader(loader_config)
        else:
            raise PipelineError(f"Unsupported loader type: {loader_type}")
    
    def run(self, 
            source: str, 
            destination: str, 
            **kwargs) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Args:
            source (str): Data source identifier (file path, URL, etc.)
            destination (str): Data destination identifier (table name, etc.)
            **kwargs: Additional execution parameters
            
        Returns:
            Dict[str, Any]: Execution results and statistics
            
        Raises:
            PipelineError: If pipeline execution fails
        """
        if not self._is_initialized:
            self.initialize()
        
        execution_stats = {
            'pipeline_name': self.config.pipeline_config.name,
            'source': source,
            'destination': destination,
            'success': False,
            'error_message': None,
            'steps': {}
        }
        
        try:
            self.logger.info(f"Starting pipeline execution: {source} -> {destination}")
            
            # Step 1: Extract
            self.logger.info("Step 1: Extracting data")
            extracted_data = self.extractor.extract(source, **kwargs.get('extract_params', {}))
            
            execution_stats['steps']['extract'] = {
                'success': True,
                'rows': len(extracted_data),
                'columns': len(extracted_data.columns)
            }
            
            # Step 2: Transform
            self.logger.info("Step 2: Transforming data")
            transformed_data = self.transformer.transform(extracted_data, **kwargs.get('transform_params', {}))
            
            execution_stats['steps']['transform'] = {
                'success': True,
                'input_rows': len(extracted_data),
                'output_rows': len(transformed_data),
                'input_columns': len(extracted_data.columns),
                'output_columns': len(transformed_data.columns)
            }
            
            # Step 3: Load
            self.logger.info("Step 3: Loading data")
            with self.loader as loader:
                loader.load(transformed_data, destination, **kwargs.get('load_params', {}))
            
            execution_stats['steps']['load'] = {
                'success': True,
                'rows_loaded': len(transformed_data),
                'destination': destination
            }
            
            execution_stats['success'] = True
            self.logger.info("Pipeline execution completed successfully")
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {e}"
            self.logger.error(error_msg)
            execution_stats['error_message'] = str(e)
            raise PipelineError(error_msg)
        
        finally:
            # Log execution
            self._execution_log.append(execution_stats)
        
        return execution_stats
    
    def validate_pipeline(self, source: str) -> Dict[str, Any]:
        """
        Validate pipeline configuration and connectivity.
        
        Args:
            source (str): Data source to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        if not self._is_initialized:
            self.initialize()
        
        validation_results = {
            'config_valid': False,
            'source_valid': False,
            'loader_connection': False,
            'errors': []
        }
        
        try:
            # Validate configuration
            validation_results['config_valid'] = self.config.validate()
            
            # Validate source
            validation_results['source_valid'] = self.extractor.validate_source(source)
            if not validation_results['source_valid']:
                validation_results['errors'].append(f"Invalid source: {source}")
            
            # Test loader connection
            try:
                self.loader.connect()
                validation_results['loader_connection'] = True
                self.loader.disconnect()
            except Exception as e:
                validation_results['errors'].append(f"Loader connection failed: {e}")
            
        except Exception as e:
            validation_results['errors'].append(f"Validation error: {e}")
        
        return validation_results
    
    def get_pipeline_info(self) -> Dict[str, Any]:
        """
        Get information about the pipeline configuration and state.
        
        Returns:
            Dict[str, Any]: Pipeline information
        """
        info = {
            'name': self.config.pipeline_config.name,
            'description': self.config.pipeline_config.description,
            'initialized': self._is_initialized,
            'executions_count': len(self._execution_log)
        }
        
        if self._is_initialized:
            info.update({
                'extractor': {
                    'type': type(self.extractor).__name__,
                    'config': self.extractor.config
                },
                'transformer': {
                    'type': type(self.transformer).__name__,
                    'config': self.transformer.config
                },
                'loader': {
                    'type': type(self.loader).__name__,
                    'config': self.loader.config
                }
            })
        
        return info
    
    def get_execution_log(self) -> list:
        """
        Get the pipeline execution log.
        
        Returns:
            list: List of execution records
        """
        return self._execution_log.copy()
    
    def clear_execution_log(self) -> None:
        """Clear the pipeline execution log."""
        self._execution_log.clear()
    
    def __str__(self) -> str:
        """String representation of the pipeline."""
        return f"DataPipeline(name={self.config.pipeline_config.name}, initialized={self._is_initialized})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the pipeline."""
        return self.__str__()