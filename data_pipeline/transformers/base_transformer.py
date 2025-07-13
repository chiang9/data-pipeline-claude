"""
Base Transformer Class

Abstract base class for all data transformers in the pipeline.
Defines the interface that all transformers must implement.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """Abstract base class for data transformers."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize base transformer.
        
        Args:
            config (Optional[Dict[str, Any]]): Transformer configuration
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._transformation_log: List[Dict[str, Any]] = []
    
    @abstractmethod
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform the input data.
        
        Args:
            data (pd.DataFrame): Input data to transform
            **kwargs: Additional transformation parameters
            
        Returns:
            pd.DataFrame: Transformed data
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement transform method")
    
    def validate_input(self, data: pd.DataFrame) -> bool:
        """
        Validate input data before transformation.
        
        Args:
            data (pd.DataFrame): Input data to validate
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        if not isinstance(data, pd.DataFrame):
            self.logger.error("Input data must be a pandas DataFrame")
            return False
        
        if data.empty:
            self.logger.warning("Input DataFrame is empty")
            return True  # Empty DataFrame is technically valid
        
        return True
    
    def get_transformation_stats(self, 
                                input_data: pd.DataFrame, 
                                output_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Get statistics about the transformation.
        
        Args:
            input_data (pd.DataFrame): Original input data
            output_data (pd.DataFrame): Transformed output data
            
        Returns:
            Dict[str, Any]: Transformation statistics
        """
        return {
            "input_rows": len(input_data),
            "output_rows": len(output_data),
            "input_columns": len(input_data.columns),
            "output_columns": len(output_data.columns),
            "rows_added": len(output_data) - len(input_data),
            "columns_added": len(output_data.columns) - len(input_data.columns),
            "transformer_type": self.__class__.__name__
        }
    
    def log_transformation(self, 
                          input_data: pd.DataFrame, 
                          output_data: pd.DataFrame,
                          operation: str = "transform") -> None:
        """
        Log transformation details for debugging and monitoring.
        
        Args:
            input_data (pd.DataFrame): Original input data
            output_data (pd.DataFrame): Transformed output data
            operation (str): Description of the transformation operation
        """
        stats = self.get_transformation_stats(input_data, output_data)
        
        log_entry = {
            "operation": operation,
            "timestamp": pd.Timestamp.now(),
            **stats
        }
        
        self._transformation_log.append(log_entry)
        
        self.logger.info(
            f"Transformation '{operation}': "
            f"{stats['input_rows']} → {stats['output_rows']} rows, "
            f"{stats['input_columns']} → {stats['output_columns']} columns"
        )
    
    def get_transformation_log(self) -> List[Dict[str, Any]]:
        """
        Get the transformation log.
        
        Returns:
            List[Dict[str, Any]]: List of transformation log entries
        """
        return self._transformation_log.copy()
    
    def clear_transformation_log(self) -> None:
        """Clear the transformation log."""
        self._transformation_log.clear()
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the transformer.
        
        Returns:
            Dict[str, Any]: Transformer metadata
        """
        return {
            "transformer_type": self.__class__.__name__,
            "config": self.config,
            "transformations_count": len(self._transformation_log)
        }
    
    def __str__(self) -> str:
        """String representation of the transformer."""
        return f"{self.__class__.__name__}(config={self.config})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the transformer."""
        return self.__str__()