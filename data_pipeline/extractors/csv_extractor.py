"""
CSV Extractor

Extracts data from CSV (Comma Separated Values) files.
Provides robust error handling and configurable parsing options.
"""

import os
from typing import Any, Dict, Optional
import pandas as pd
from .base_extractor import BaseExtractor


class CSVExtractorError(Exception):
    """Custom exception for CSV extraction errors."""
    pass


class CSVExtractor(BaseExtractor):
    """Extractor for CSV files."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize CSV extractor.
        
        Args:
            config (Optional[Dict[str, Any]]): CSV-specific configuration
                - encoding (str): File encoding (default: 'utf-8')
                - delimiter (str): Column delimiter (default: ',')
                - skip_rows (int): Number of rows to skip (default: 0)
                - max_rows (int): Maximum rows to read (default: None)
        """
        super().__init__(config)
        
        # Default CSV configuration
        self.encoding = self.config.get('encoding', 'utf-8')
        self.delimiter = self.config.get('delimiter', ',')
        self.skip_rows = self.config.get('skip_rows', 0)
        self.max_rows = self.config.get('max_rows', None)
    
    def validate_source(self, source: str) -> bool:
        """
        Validate that the CSV file exists and is readable.
        
        Args:
            source (str): Path to CSV file
            
        Returns:
            bool: True if file is valid, False otherwise
        """
        try:
            if not source:
                return False
            
            if not os.path.exists(source):
                self.logger.error(f"CSV file does not exist: {source}")
                return False
            
            if not os.path.isfile(source):
                self.logger.error(f"Source is not a file: {source}")
                return False
            
            if not os.access(source, os.R_OK):
                self.logger.error(f"CSV file is not readable: {source}")
                return False
            
            # Check file extension
            if not source.lower().endswith('.csv'):
                self.logger.warning(f"File does not have .csv extension: {source}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating CSV file {source}: {e}")
            return False
    
    def extract(self, source: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            source (str): Path to CSV file
            **kwargs: Additional pandas.read_csv parameters
            
        Returns:
            pd.DataFrame: Extracted data
            
        Raises:
            CSVExtractorError: If extraction fails
        """
        try:
            if not self.validate_source(source):
                raise CSVExtractorError(f"Invalid CSV source: {source}")
            
            self.logger.info(f"Extracting data from CSV: {source}")
            
            # Build pandas read_csv parameters
            read_params = {
                'filepath_or_buffer': source,
                'encoding': self.encoding,
                'sep': self.delimiter,
                'skiprows': self.skip_rows,
            }
            
            # Add optional parameters
            if self.max_rows is not None:
                read_params['nrows'] = self.max_rows
            
            # Override with any kwargs
            read_params.update(kwargs)
            
            # Read CSV file
            df = pd.read_csv(**read_params)
            
            # Validate extracted data
            if df.empty:
                self.logger.warning(f"CSV file is empty: {source}")
            else:
                self.logger.info(f"Successfully extracted {len(df)} rows from {source}")
            
            return df
            
        except pd.errors.EmptyDataError:
            raise CSVExtractorError(f"CSV file is empty: {source}")
        
        except pd.errors.ParserError as e:
            raise CSVExtractorError(f"Error parsing CSV file {source}: {e}")
        
        except UnicodeDecodeError as e:
            raise CSVExtractorError(f"Encoding error reading CSV file {source}: {e}")
        
        except MemoryError:
            raise CSVExtractorError(f"File too large to load into memory: {source}")
        
        except Exception as e:
            raise CSVExtractorError(f"Unexpected error extracting from {source}: {e}")
    
    def get_metadata(self, source: str) -> Dict[str, Any]:
        """
        Get metadata about the CSV file.
        
        Args:
            source (str): Path to CSV file
            
        Returns:
            Dict[str, Any]: CSV file metadata
        """
        metadata = super().get_metadata(source)
        
        try:
            if os.path.exists(source):
                file_stats = os.stat(source)
                metadata.update({
                    "file_size_bytes": file_stats.st_size,
                    "file_modified": file_stats.st_mtime,
                    "encoding": self.encoding,
                    "delimiter": self.delimiter,
                    "skip_rows": self.skip_rows,
                    "max_rows": self.max_rows
                })
        except Exception as e:
            self.logger.warning(f"Could not get file metadata for {source}: {e}")
        
        return metadata