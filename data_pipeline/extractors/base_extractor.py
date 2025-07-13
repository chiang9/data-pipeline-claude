"""
Base Extractor Class

Abstract base class for all data extractors in the pipeline.
Defines the interface that all extractors must implement.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for data extractors."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize base extractor.
        
        Args:
            config (Optional[Dict[str, Any]]): Extractor configuration
        """
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def extract(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from the specified source.
        
        Args:
            source (str): Data source identifier (file path, URL, etc.)
            **kwargs: Additional extraction parameters
            
        Returns:
            pd.DataFrame: Extracted data
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement extract method")
    
    def validate_source(self, source: str) -> bool:
        """
        Validate that the source is accessible and valid.
        
        Args:
            source (str): Data source identifier
            
        Returns:
            bool: True if source is valid, False otherwise
        """
        # Default implementation - subclasses can override
        return bool(source)
    
    def get_metadata(self, source: str) -> Dict[str, Any]:
        """
        Get metadata about the data source.
        
        Args:
            source (str): Data source identifier
            
        Returns:
            Dict[str, Any]: Source metadata
        """
        return {
            "source": source,
            "extractor_type": self.__class__.__name__,
            "config": self.config
        }
    
    def __str__(self) -> str:
        """String representation of the extractor."""
        return f"{self.__class__.__name__}(config={self.config})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the extractor."""
        return self.__str__()