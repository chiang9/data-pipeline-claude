"""
Unit tests for CSV Extractor.

Tests cover expected use cases, edge cases, and failure scenarios.
"""

import os
import tempfile
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from data_pipeline.extractors.csv_extractor import CSVExtractor, CSVExtractorError


class TestCSVExtractor:
    """Test suite for CSVExtractor."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = CSVExtractor()
        
        # Create temporary CSV file for testing
        self.temp_dir = tempfile.mkdtemp()
        self.test_csv_path = os.path.join(self.temp_dir, "test.csv")
        
        # Sample CSV content
        self.csv_content = """id,name,age
1,John,25
2,Jane,30
3,Bob,35"""
        
        with open(self.test_csv_path, 'w') as f:
            f.write(self.csv_content)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_csv_path):
            os.remove(self.test_csv_path)
        os.rmdir(self.temp_dir)
    
    def test_extract_expected_use(self):
        """Test expected use case - successful CSV extraction."""
        # Act
        result = self.extractor.extract(self.test_csv_path)
        
        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ['id', 'name', 'age']
        assert result.iloc[0]['name'] == 'John'
        assert result.iloc[1]['age'] == 30
    
    def test_extract_with_custom_config(self):
        """Test extraction with custom configuration."""
        # Arrange
        config = {
            'delimiter': ';',
            'encoding': 'utf-8',
            'skip_rows': 1
        }
        extractor = CSVExtractor(config)
        
        # Create CSV with semicolon delimiter
        csv_content_semicolon = """id;name;age
1;John;25
2;Jane;30"""
        
        csv_path = os.path.join(self.temp_dir, "test_semicolon.csv")
        with open(csv_path, 'w') as f:
            f.write(csv_content_semicolon)
        
        # Act
        result = extractor.extract(csv_path)
        
        # Assert
        assert len(result) == 1  # One row due to skip_rows=1
        assert list(result.columns) == ['1', 'John', '25']  # Header skipped
        
        # Cleanup
        os.remove(csv_path)
    
    def test_extract_empty_file_edge_case(self):
        """Test edge case - empty CSV file."""
        # Arrange
        empty_csv_path = os.path.join(self.temp_dir, "empty.csv")
        with open(empty_csv_path, 'w') as f:
            f.write("")
        
        # Act & Assert
        with pytest.raises(CSVExtractorError, match="CSV file is empty"):
            self.extractor.extract(empty_csv_path)
        
        # Cleanup
        os.remove(empty_csv_path)
    
    def test_extract_file_not_found_failure(self):
        """Test failure case - file does not exist."""
        # Arrange
        non_existent_path = "/path/that/does/not/exist.csv"
        
        # Act & Assert
        with pytest.raises(CSVExtractorError, match="Invalid CSV source"):
            self.extractor.extract(non_existent_path)
    
    def test_extract_invalid_csv_format_failure(self):
        """Test failure case - invalid CSV format."""
        # Arrange
        invalid_csv_path = os.path.join(self.temp_dir, "invalid.csv")
        with open(invalid_csv_path, 'w') as f:
            f.write("invalid,csv,format\nwith,unmatched,quotes\"")
        
        # Act & Assert
        with pytest.raises(CSVExtractorError, match="Error parsing CSV file"):
            self.extractor.extract(invalid_csv_path)
        
        # Cleanup
        os.remove(invalid_csv_path)
    
    def test_validate_source_valid_file(self):
        """Test source validation with valid file."""
        # Act
        result = self.extractor.validate_source(self.test_csv_path)
        
        # Assert
        assert result is True
    
    def test_validate_source_non_existent_file(self):
        """Test source validation with non-existent file."""
        # Act
        result = self.extractor.validate_source("/path/that/does/not/exist.csv")
        
        # Assert
        assert result is False
    
    def test_validate_source_directory_instead_of_file(self):
        """Test source validation with directory instead of file."""
        # Act
        result = self.extractor.validate_source(self.temp_dir)
        
        # Assert
        assert result is False
    
    def test_validate_source_empty_string(self):
        """Test source validation with empty string."""
        # Act
        result = self.extractor.validate_source("")
        
        # Assert
        assert result is False
    
    def test_get_metadata(self):
        """Test metadata retrieval."""
        # Act
        metadata = self.extractor.get_metadata(self.test_csv_path)
        
        # Assert
        assert metadata['source'] == self.test_csv_path
        assert metadata['extractor_type'] == 'CSVExtractor'
        assert 'file_size_bytes' in metadata
        assert metadata['encoding'] == 'utf-8'
        assert metadata['delimiter'] == ','
    
    def test_get_metadata_non_existent_file(self):
        """Test metadata retrieval for non-existent file."""
        # Act
        metadata = self.extractor.get_metadata("/non/existent/file.csv")
        
        # Assert
        assert metadata['source'] == "/non/existent/file.csv"
        assert metadata['extractor_type'] == 'CSVExtractor'
        assert 'file_size_bytes' not in metadata
    
    def test_extract_with_max_rows_limit(self):
        """Test extraction with row limit."""
        # Arrange
        config = {'max_rows': 2}
        extractor = CSVExtractor(config)
        
        # Act
        result = extractor.extract(self.test_csv_path)
        
        # Assert
        assert len(result) == 2
        assert result.iloc[0]['name'] == 'John'
        assert result.iloc[1]['name'] == 'Jane'
    
    def test_extract_with_encoding_error(self):
        """Test extraction with encoding error."""
        # Arrange
        config = {'encoding': 'ascii'}
        extractor = CSVExtractor(config)
        
        # Create file with non-ASCII characters
        unicode_csv_path = os.path.join(self.temp_dir, "unicode.csv")
        with open(unicode_csv_path, 'w', encoding='utf-8') as f:
            f.write("name,city\nJosé,São Paulo\n")
        
        # Act & Assert
        with pytest.raises(CSVExtractorError, match="Encoding error"):
            extractor.extract(unicode_csv_path)
        
        # Cleanup
        os.remove(unicode_csv_path)
    
    @patch('pandas.read_csv')
    def test_extract_memory_error(self, mock_read_csv):
        """Test extraction with memory error."""
        # Arrange
        mock_read_csv.side_effect = MemoryError("File too large")
        
        # Act & Assert
        with pytest.raises(CSVExtractorError, match="File too large to load into memory"):
            self.extractor.extract(self.test_csv_path)
    
    def test_str_representation(self):
        """Test string representation of extractor."""
        # Act
        str_repr = str(self.extractor)
        
        # Assert
        assert "CSVExtractor" in str_repr
        assert "config" in str_repr