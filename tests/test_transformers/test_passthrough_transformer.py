"""
Unit tests for Passthrough Transformer.

Tests cover expected use cases, edge cases, and failure scenarios.
"""

import pytest
import pandas as pd
from data_pipeline.transformers.passthrough_transformer import PassthroughTransformer


class TestPassthroughTransformer:
    """Test suite for PassthroughTransformer."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = PassthroughTransformer()
        
        # Sample test data
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'age': [25, 30, 35]
        })
    
    def test_transform_expected_use(self):
        """Test expected use case - successful passthrough transformation."""
        # Act
        result = self.transformer.transform(self.test_data)
        
        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ['id', 'name', 'age']
        
        # Verify data is unchanged
        pd.testing.assert_frame_equal(result, self.test_data)
        
        # Verify it's a copy (different object)
        assert result is not self.test_data
    
    def test_transform_empty_dataframe_edge_case(self):
        """Test edge case - empty DataFrame."""
        # Arrange
        empty_df = pd.DataFrame()
        
        # Act
        result = self.transformer.transform(empty_df)
        
        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert result is not empty_df
    
    def test_transform_single_row_edge_case(self):
        """Test edge case - single row DataFrame."""
        # Arrange
        single_row_df = pd.DataFrame({'col1': [1], 'col2': ['test']})
        
        # Act
        result = self.transformer.transform(single_row_df)
        
        # Assert
        assert len(result) == 1
        assert result.iloc[0]['col1'] == 1
        assert result.iloc[0]['col2'] == 'test'
        pd.testing.assert_frame_equal(result, single_row_df)
    
    def test_transform_invalid_input_failure(self):
        """Test failure case - invalid input type."""
        # Arrange
        invalid_input = "not a dataframe"
        
        # Act & Assert
        with pytest.raises(ValueError, match="Invalid input data"):
            self.transformer.transform(invalid_input)
    
    def test_transform_none_input_failure(self):
        """Test failure case - None input."""
        # Act & Assert
        with pytest.raises(ValueError, match="Invalid input data"):
            self.transformer.transform(None)
    
    def test_transform_with_config_log_details_false(self):
        """Test transformation with log_details disabled."""
        # Arrange
        config = {'log_details': False}
        transformer = PassthroughTransformer(config)
        
        # Act
        result = transformer.transform(self.test_data)
        
        # Assert
        assert len(result) == 3
        pd.testing.assert_frame_equal(result, self.test_data)
        
        # Verify no detailed logging occurred
        assert len(transformer.get_transformation_log()) == 0
    
    def test_transform_with_config_log_details_true(self):
        """Test transformation with log_details enabled."""
        # Arrange
        config = {'log_details': True}
        transformer = PassthroughTransformer(config)
        
        # Act
        result = transformer.transform(self.test_data)
        
        # Assert
        assert len(result) == 3
        
        # Verify detailed logging occurred
        log = transformer.get_transformation_log()
        assert len(log) == 1
        assert log[0]['operation'] == 'passthrough'
        assert log[0]['input_rows'] == 3
        assert log[0]['output_rows'] == 3
    
    def test_validate_input_valid_dataframe(self):
        """Test input validation with valid DataFrame."""
        # Act
        result = self.transformer.validate_input(self.test_data)
        
        # Assert
        assert result is True
    
    def test_validate_input_empty_dataframe(self):
        """Test input validation with empty DataFrame."""
        # Arrange
        empty_df = pd.DataFrame()
        
        # Act
        result = self.transformer.validate_input(empty_df)
        
        # Assert
        assert result is True  # Empty DataFrame is valid
    
    def test_validate_input_invalid_type(self):
        """Test input validation with invalid type."""
        # Act
        result = self.transformer.validate_input("not a dataframe")
        
        # Assert
        assert result is False
    
    def test_get_transformation_stats(self):
        """Test transformation statistics."""
        # Arrange
        output_data = self.test_data.copy()
        
        # Act
        stats = self.transformer.get_transformation_stats(self.test_data, output_data)
        
        # Assert
        assert stats['input_rows'] == 3
        assert stats['output_rows'] == 3
        assert stats['input_columns'] == 3
        assert stats['output_columns'] == 3
        assert stats['rows_added'] == 0
        assert stats['columns_added'] == 0
        assert stats['transformation_type'] == 'passthrough'
        assert stats['data_modified'] is False
        assert stats['copy_created'] is True
    
    def test_transform_with_kwargs(self):
        """Test transformation with additional kwargs (should be ignored)."""
        # Act
        result = self.transformer.transform(self.test_data, unused_param="value")
        
        # Assert
        assert len(result) == 3
        pd.testing.assert_frame_equal(result, self.test_data)
    
    def test_transform_preserves_data_types(self):
        """Test that transformation preserves data types."""
        # Arrange
        typed_data = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.5, 2.5, 3.5],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True]
        })
        
        # Act
        result = self.transformer.transform(typed_data)
        
        # Assert
        assert result.dtypes.equals(typed_data.dtypes)
        pd.testing.assert_frame_equal(result, typed_data)
    
    def test_multiple_transformations_independence(self):
        """Test that multiple transformations are independent."""
        # Act
        result1 = self.transformer.transform(self.test_data)
        result2 = self.transformer.transform(self.test_data)
        
        # Assert
        pd.testing.assert_frame_equal(result1, result2)
        assert result1 is not result2  # Different objects
        
        # Modify one result and verify the other is unaffected
        result1.loc[0, 'name'] = 'Modified'
        assert result2.loc[0, 'name'] == 'John'
    
    def test_str_representation(self):
        """Test string representation of transformer."""
        # Act
        str_repr = str(self.transformer)
        
        # Assert
        assert "PassthroughTransformer" in str_repr
        assert "config" in str_repr