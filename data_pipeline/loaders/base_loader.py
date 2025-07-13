"""
Base Loader Class

Abstract base class for all data loaders in the pipeline.
Defines the interface that all loaders must implement.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """Abstract base class for data loaders."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize base loader.
        
        Args:
            config (Optional[Dict[str, Any]]): Loader configuration
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._is_connected = False
        self._load_log: List[Dict[str, Any]] = []
    
    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the target destination.
        
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement connect method")
    
    @abstractmethod
    def disconnect(self) -> None:
        """
        Close connection to the target destination.
        
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement disconnect method")
    
    @abstractmethod
    def load(self, data: pd.DataFrame, destination: str, **kwargs) -> None:
        """
        Load data to the specified destination.
        
        Args:
            data (pd.DataFrame): Data to load
            destination (str): Destination identifier (table name, file path, etc.)
            **kwargs: Additional loading parameters
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement load method")
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        Validate data before loading.
        
        Args:
            data (pd.DataFrame): Data to validate
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        if not isinstance(data, pd.DataFrame):
            self.logger.error("Data must be a pandas DataFrame")
            return False
        
        if data.empty:
            self.logger.warning("DataFrame is empty")
            return True  # Empty DataFrame is technically valid
        
        # Check for completely null columns
        null_columns = data.columns[data.isnull().all()].tolist()
        if null_columns:
            self.logger.warning(f"Columns with all null values: {null_columns}")
        
        return True
    
    def get_load_stats(self, data: pd.DataFrame, destination: str) -> Dict[str, Any]:
        """
        Get statistics about the load operation.
        
        Args:
            data (pd.DataFrame): Data being loaded
            destination (str): Destination identifier
            
        Returns:
            Dict[str, Any]: Load operation statistics
        """
        return {
            "destination": destination,
            "rows_loaded": len(data),
            "columns_loaded": len(data.columns),
            "data_size_bytes": data.memory_usage(deep=True).sum(),
            "loader_type": self.__class__.__name__
        }
    
    def log_load_operation(self, 
                          data: pd.DataFrame, 
                          destination: str,
                          operation: str = "load",
                          success: bool = True) -> None:
        """
        Log load operation details.
        
        Args:
            data (pd.DataFrame): Data that was loaded
            destination (str): Destination identifier
            operation (str): Description of the load operation
            success (bool): Whether the operation was successful
        """
        stats = self.get_load_stats(data, destination)
        
        log_entry = {
            "operation": operation,
            "timestamp": pd.Timestamp.now(),
            "success": success,
            **stats
        }
        
        self._load_log.append(log_entry)
        
        status = "SUCCESS" if success else "FAILED"
        self.logger.info(
            f"Load operation '{operation}' {status}: "
            f"{stats['rows_loaded']} rows to '{destination}'"
        )
    
    def get_load_log(self) -> List[Dict[str, Any]]:
        """
        Get the load operation log.
        
        Returns:
            List[Dict[str, Any]]: List of load log entries
        """
        return self._load_log.copy()
    
    def clear_load_log(self) -> None:
        """Clear the load operation log."""
        self._load_log.clear()
    
    @property
    def is_connected(self) -> bool:
        """
        Check if loader is connected to destination.
        
        Returns:
            bool: True if connected, False otherwise
        """
        return self._is_connected
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the loader.
        
        Returns:
            Dict[str, Any]: Loader metadata
        """
        return {
            "loader_type": self.__class__.__name__,
            "config": self.config,
            "is_connected": self._is_connected,
            "load_operations_count": len(self._load_log)
        }
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def __str__(self) -> str:
        """String representation of the loader."""
        return f"{self.__class__.__name__}(config={self.config})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the loader."""
        return self.__str__()