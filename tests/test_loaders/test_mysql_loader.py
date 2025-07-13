"""
Unit tests for MySQL Loader.

Tests cover expected use cases, edge cases, and failure scenarios.
Note: Uses mocking to avoid requiring actual MySQL database for tests.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError
from data_pipeline.loaders.mysql_loader import MySQLLoader, MySQLLoaderError


class TestMySQLLoader:
    """Test suite for MySQLLoader."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'age': [25, 30, 35]
        })
    
    def test_init_with_valid_config(self):
        """Test loader initialization with valid configuration."""
        # Act
        loader = MySQLLoader(self.config)
        
        # Assert
        assert loader.host == 'localhost'
        assert loader.port == 3306
        assert loader.user == 'test_user'
        assert loader.database == 'test_db'
        assert loader.charset == 'utf8mb4'  # default
        assert not loader.is_connected
    
    def test_init_with_missing_config_failure(self):
        """Test failure case - missing required configuration."""
        # Arrange
        incomplete_config = {'host': 'localhost', 'port': 3306}
        
        # Act & Assert
        with pytest.raises(ValueError, match="Missing required MySQL config keys"):
            MySQLLoader(incomplete_config)
    
    def test_init_with_custom_charset(self):
        """Test initialization with custom charset."""
        # Arrange
        config_with_charset = self.config.copy()
        config_with_charset['charset'] = 'latin1'
        
        # Act
        loader = MySQLLoader(config_with_charset)
        
        # Assert
        assert loader.charset == 'latin1'
    
    @patch('data_pipeline.loaders.mysql_loader.create_engine')
    def test_connect_success(self, mock_create_engine):
        """Test successful database connection."""
        # Arrange
        mock_engine = Mock()
        mock_connection = Mock()
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_context_manager
        mock_create_engine.return_value = mock_engine
        
        loader = MySQLLoader(self.config)
        
        # Act
        loader.connect()
        
        # Assert
        assert loader.is_connected
        assert loader.engine == mock_engine
        mock_create_engine.assert_called_once()
        mock_connection.execute.assert_called_once()
    
    @patch('data_pipeline.loaders.mysql_loader.create_engine')
    def test_connect_failure(self, mock_create_engine):
        """Test database connection failure."""
        # Arrange
        mock_create_engine.side_effect = SQLAlchemyError("Connection failed")
        loader = MySQLLoader(self.config)
        
        # Act & Assert
        with pytest.raises(MySQLLoaderError, match="Database connection failed"):
            loader.connect()
        
        assert not loader.is_connected
    
    def test_disconnect(self):
        """Test database disconnection."""
        # Arrange
        loader = MySQLLoader(self.config)
        mock_engine = Mock()
        mock_connection = Mock()
        loader.engine = mock_engine
        loader.connection = mock_connection
        loader._is_connected = True
        
        # Act
        loader.disconnect()
        
        # Assert
        assert not loader.is_connected
        assert loader.engine is None
        assert loader.connection is None
        mock_engine.dispose.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch('data_pipeline.loaders.mysql_loader.inspect')
    def test_table_exists_true(self, mock_inspect):
        """Test table_exists when table exists."""
        # Arrange
        loader = MySQLLoader(self.config)
        loader._is_connected = True
        loader.engine = Mock()
        
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = ['users', 'products']
        mock_inspect.return_value = mock_inspector
        
        # Act
        result = loader.table_exists('users')
        
        # Assert
        assert result is True
    
    @patch('data_pipeline.loaders.mysql_loader.inspect')
    def test_table_exists_false(self, mock_inspect):
        """Test table_exists when table doesn't exist."""
        # Arrange
        loader = MySQLLoader(self.config)
        loader._is_connected = True
        loader.engine = Mock()
        
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = ['users', 'products']
        mock_inspect.return_value = mock_inspector
        
        # Act
        result = loader.table_exists('orders')
        
        # Assert
        assert result is False
    
    def test_table_exists_not_connected_failure(self):
        """Test table_exists when not connected."""
        # Arrange
        loader = MySQLLoader(self.config)
        
        # Act
        result = loader.table_exists('users')
        
        # Assert
        assert result is False
    
    @patch.object(MySQLLoader, 'table_exists')
    def test_load_success(self, mock_table_exists):
        """Test successful data loading."""
        # Arrange
        loader = MySQLLoader(self.config)
        loader._is_connected = True
        loader.engine = Mock()
        mock_table_exists.return_value = False
        
        # Mock pandas to_sql
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            # Act
            loader.load(self.test_data, 'test_table')
            
            # Assert
            mock_to_sql.assert_called_once()
            call_args = mock_to_sql.call_args
            assert call_args[1]['name'] == 'test_table'
            assert call_args[1]['con'] == loader.engine
            assert call_args[1]['if_exists'] == 'append'
            assert call_args[1]['index'] is False
    
    def test_load_not_connected_failure(self):
        """Test load failure when not connected."""
        # Arrange
        loader = MySQLLoader(self.config)
        
        # Act & Assert
        with pytest.raises(MySQLLoaderError, match="Not connected to database"):
            loader.load(self.test_data, 'test_table')
    
    def test_load_invalid_data_failure(self):
        """Test load failure with invalid data."""
        # Arrange
        loader = MySQLLoader(self.config)
        loader._is_connected = True
        
        # Mock validate_data to return False
        with patch.object(loader, 'validate_data', return_value=False):
            # Act & Assert
            with pytest.raises(MySQLLoaderError, match="Invalid data for loading"):
                loader.load(self.test_data, 'test_table')
    
    @patch.object(MySQLLoader, 'table_exists')
    def test_load_with_custom_if_exists(self, mock_table_exists):
        """Test loading with custom if_exists parameter."""
        # Arrange
        loader = MySQLLoader(self.config)
        loader._is_connected = True
        loader.engine = Mock()
        mock_table_exists.return_value = True
        
        # Mock pandas to_sql
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            # Act
            loader.load(self.test_data, 'test_table', if_exists='replace')
            
            # Assert
            call_args = mock_to_sql.call_args
            assert call_args[1]['if_exists'] == 'replace'
    
    @patch.object(MySQLLoader, 'table_exists')
    def test_load_sqlalchemy_error(self, mock_table_exists):
        """Test load failure with SQLAlchemy error."""
        # Arrange
        loader = MySQLLoader(self.config)
        loader._is_connected = True
        loader.engine = Mock()
        mock_table_exists.return_value = False
        
        # Mock pandas to_sql to raise SQLAlchemyError
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            mock_to_sql.side_effect = SQLAlchemyError("Database error")
            
            # Act & Assert
            with pytest.raises(MySQLLoaderError, match="Database error"):
                loader.load(self.test_data, 'test_table')
    
    def test_execute_query_select(self):
        """Test executing SELECT query."""
        # Arrange
        loader = MySQLLoader(self.config)
        loader._is_connected = True
        
        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [(1, 'John'), (2, 'Jane')]
        mock_result.keys.return_value = ['id', 'name']
        
        mock_connection.execute.return_value = mock_result
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_context_manager
        loader.engine = mock_engine
        
        # Act
        result = loader.execute_query("SELECT * FROM users")
        
        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        mock_connection.execute.assert_called_once()
    
    def test_execute_query_non_select(self):
        """Test executing non-SELECT query."""
        # Arrange
        loader = MySQLLoader(self.config)
        loader._is_connected = True
        
        mock_engine = Mock()
        mock_connection = Mock()
        mock_context_manager = Mock()
        mock_context_manager.__enter__ = Mock(return_value=mock_connection)
        mock_context_manager.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_context_manager
        loader.engine = mock_engine
        
        # Act
        result = loader.execute_query("INSERT INTO users VALUES (1, 'John')")
        
        # Assert
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_execute_query_not_connected_failure(self):
        """Test execute_query when not connected."""
        # Arrange
        loader = MySQLLoader(self.config)
        
        # Act & Assert
        with pytest.raises(MySQLLoaderError, match="Not connected to database"):
            loader.execute_query("SELECT 1")
    
    def test_context_manager(self):
        """Test loader as context manager."""
        # Arrange
        loader = MySQLLoader(self.config)
        
        with patch.object(loader, 'connect') as mock_connect, \
             patch.object(loader, 'disconnect') as mock_disconnect:
            
            # Act
            with loader as context_loader:
                assert context_loader is loader
            
            # Assert
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()
    
    def test_get_metadata(self):
        """Test metadata retrieval."""
        # Arrange
        loader = MySQLLoader(self.config)
        
        # Act
        metadata = loader.get_metadata()
        
        # Assert
        assert metadata['loader_type'] == 'MySQLLoader'
        assert metadata['host'] == 'localhost'
        assert metadata['port'] == 3306
        assert metadata['database'] == 'test_db'
        assert metadata['user'] == 'test_user'
        assert metadata['is_connected'] is False
    
    def test_validate_data_valid_dataframe(self):
        """Test data validation with valid DataFrame."""
        # Arrange
        loader = MySQLLoader(self.config)
        
        # Act
        result = loader.validate_data(self.test_data)
        
        # Assert
        assert result is True
    
    def test_validate_data_empty_dataframe(self):
        """Test data validation with empty DataFrame."""
        # Arrange
        loader = MySQLLoader(self.config)
        empty_df = pd.DataFrame()
        
        # Act
        result = loader.validate_data(empty_df)
        
        # Assert
        assert result is True  # Empty DataFrame is valid
    
    def test_validate_data_invalid_type(self):
        """Test data validation with invalid type."""
        # Arrange
        loader = MySQLLoader(self.config)
        
        # Act
        result = loader.validate_data("not a dataframe")
        
        # Assert
        assert result is False
    
    def test_str_representation(self):
        """Test string representation of loader."""
        # Arrange
        loader = MySQLLoader(self.config)
        
        # Act
        str_repr = str(loader)
        
        # Assert
        assert "MySQLLoader" in str_repr
        assert "config" in str_repr