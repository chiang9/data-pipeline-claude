"""
Passthrough Transformer

A transformer that passes data through without modification.
Used for POC and as a placeholder for future transformation logic.
"""

from typing import Any, Dict, Optional
import pandas as pd
from .base_transformer import BaseTransformer


class PassthroughTransformer(BaseTransformer):
    """Transformer that passes data through unchanged."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize passthrough transformer.
        
        Args:
            config (Optional[Dict[str, Any]]): Transformer configuration
                - log_details (bool): Whether to log detailed transformation info
        """
        super().__init__(config)
        self.log_details = self.config.get('log_details', True)
    
    def transform(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform data by passing it through unchanged.
        
        Args:
            data (pd.DataFrame): Input data
            **kwargs: Additional parameters (ignored for passthrough)
            
        Returns:
            pd.DataFrame: Unchanged copy of input data
            
        Raises:
            ValueError: If input data is invalid
        """
        if not self.validate_input(data):
            raise ValueError("Invalid input data for transformation")
        
        self.logger.info(f"Passthrough transformation: {len(data)} rows")
        
        # Reason: Create a copy to avoid modifying original data
        # and maintain consistent behavior with other transformers
        output_data = data.copy()
        
        if self.log_details:
            self.log_transformation(data, output_data, "passthrough")
        
        return output_data
    
    def validate_input(self, data: pd.DataFrame) -> bool:
        """
        Validate input data for passthrough transformation.
        
        Args:
            data (pd.DataFrame): Input data to validate
            
        Returns:
            bool: True if data is valid
        """
        if not super().validate_input(data):
            return False
        
        # Passthrough accepts any valid DataFrame
        self.logger.debug(f"Validated input: {len(data)} rows, {len(data.columns)} columns")
        return True
    
    def get_transformation_stats(self, 
                                input_data: pd.DataFrame, 
                                output_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Get transformation statistics for passthrough operation.
        
        Args:
            input_data (pd.DataFrame): Original input data
            output_data (pd.DataFrame): Transformed output data
            
        Returns:
            Dict[str, Any]: Transformation statistics
        """
        stats = super().get_transformation_stats(input_data, output_data)
        stats.update({
            "transformation_type": "passthrough",
            "data_modified": False,
            "copy_created": True
        })
        return stats