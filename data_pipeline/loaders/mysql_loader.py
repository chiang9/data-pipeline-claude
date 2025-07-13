"""
MySQL Loader

Loads data into MySQL database tables.
Provides robust connection management and flexible table operations.
"""

from typing import Any, Dict, Optional, Union
import pandas as pd
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
from .base_loader import BaseLoader


class MySQLLoaderError(Exception):
    """Custom exception for MySQL loading errors."""
    pass


class MySQLLoader(BaseLoader):
    """Loader for MySQL databases."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize MySQL loader.
        
        Args:
            config (Optional[Dict[str, Any]]): MySQL-specific configuration
                - host (str): Database host
                - port (int): Database port
                - user (str): Database username
                - password (str): Database password
                - database (str): Database name
                - charset (str): Character set (default: 'utf8mb4')
                - if_exists (str): What to do if table exists (default: 'append')
        """
        super().__init__(config)
        
        # Required configuration
        required_keys = ['host', 'port', 'user', 'password', 'database']
        missing_keys = [key for key in required_keys if key not in self.config]
        if missing_keys:
            raise ValueError(f"Missing required MySQL config keys: {missing_keys}")
        
        # MySQL configuration
        self.host = self.config['host']
        self.port = self.config['port']
        self.user = self.config['user']
        self.password = self.config['password']
        self.database = self.config['database']
        self.charset = self.config.get('charset', 'utf8mb4')
        self.if_exists = self.config.get('if_exists', 'append')
        
        # Connection objects
        self.engine = None
        self.connection = None
    
    def _build_connection_string(self) -> str:
        """
        Build SQLAlchemy connection string.
        
        Returns:
            str: MySQL connection string
        """
        return (
            f"mysql+mysqlconnector://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?charset={self.charset}"
        )
    
    def connect(self) -> None:
        """
        Establish connection to MySQL database.
        
        Raises:
            MySQLLoaderError: If connection fails
        """
        try:
            if self._is_connected:
                self.logger.info("Already connected to MySQL database")
                return
            
            self.logger.info(f"Connecting to MySQL database: {self.host}:{self.port}/{self.database}")
            
            connection_string = self._build_connection_string()
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,   # Recycle connections after 1 hour
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._is_connected = True
            self.logger.info("Successfully connected to MySQL database")
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to connect to MySQL: {e}")
            raise MySQLLoaderError(f"Database connection failed: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to MySQL: {e}")
            raise MySQLLoaderError(f"Unexpected connection error: {e}")
    
    def disconnect(self) -> None:
        """Close connection to MySQL database."""
        try:
            if self.engine:
                self.engine.dispose()
                self.engine = None
            
            if self.connection:
                self.connection.close()
                self.connection = None
            
            self._is_connected = False
            self.logger.info("Disconnected from MySQL database")
            
        except Exception as e:
            self.logger.warning(f"Error during MySQL disconnect: {e}")
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in the database.
        
        Args:
            table_name (str): Name of the table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            if not self._is_connected:
                raise MySQLLoaderError("Not connected to database")
            
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
            
        except Exception as e:
            self.logger.error(f"Error checking if table exists: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            Optional[Dict[str, Any]]: Table information or None if table doesn't exist
        """
        try:
            if not self.table_exists(table_name):
                return None
            
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)
            
            return {
                "table_name": table_name,
                "columns": [col['name'] for col in columns],
                "column_info": columns
            }
            
        except Exception as e:
            self.logger.error(f"Error getting table info: {e}")
            return None
    
    def load(self, 
             data: pd.DataFrame, 
             destination: str, 
             if_exists: Optional[str] = None,
             **kwargs) -> None:
        """
        Load data into MySQL table.
        
        Args:
            data (pd.DataFrame): Data to load
            destination (str): Target table name
            if_exists (Optional[str]): What to do if table exists 
                ('fail', 'replace', 'append')
            **kwargs: Additional pandas.to_sql parameters
            
        Raises:
            MySQLLoaderError: If loading fails
        """
        try:
            if not self.validate_data(data):
                raise MySQLLoaderError("Invalid data for loading")
            
            if not self._is_connected:
                raise MySQLLoaderError("Not connected to database. Call connect() first.")
            
            # Use instance default or provided parameter
            if_exists_mode = if_exists or self.if_exists
            
            self.logger.info(
                f"Loading {len(data)} rows into table '{destination}' "
                f"(mode: {if_exists_mode})"
            )
            
            # Check if table exists before loading
            table_existed = self.table_exists(destination)
            
            # Build pandas to_sql parameters
            load_params = {
                'name': destination,
                'con': self.engine,
                'if_exists': if_exists_mode,
                'index': False,
                'method': 'multi',  # Use multi-row INSERT for better performance
            }
            
            # Override with any kwargs
            load_params.update(kwargs)
            
            # Load data
            data.to_sql(**load_params)
            
            # Log success
            operation = "created and loaded" if not table_existed else "loaded"
            self.log_load_operation(data, destination, operation, success=True)
            
            self.logger.info(
                f"Successfully loaded {len(data)} rows into '{destination}'"
            )
            
        except SQLAlchemyError as e:
            self.log_load_operation(data, destination, "load", success=False)
            self.logger.error(f"SQLAlchemy error loading data: {e}")
            raise MySQLLoaderError(f"Database error: {e}")
        
        except Exception as e:
            self.log_load_operation(data, destination, "load", success=False)
            self.logger.error(f"Unexpected error loading data: {e}")
            raise MySQLLoaderError(f"Unexpected error: {e}")
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """
        Execute a SQL query and return results.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            Optional[pd.DataFrame]: Query results or None for non-SELECT queries
            
        Raises:
            MySQLLoaderError: If query execution fails
        """
        try:
            if not self._is_connected:
                raise MySQLLoaderError("Not connected to database")
            
            self.logger.info(f"Executing query: {query[:100]}...")
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                
                # Return DataFrame for SELECT queries
                if query.strip().upper().startswith('SELECT'):
                    return pd.DataFrame(result.fetchall(), columns=result.keys())
                else:
                    conn.commit()
                    return None
                    
        except SQLAlchemyError as e:
            self.logger.error(f"Error executing query: {e}")
            raise MySQLLoaderError(f"Query execution failed: {e}")
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the MySQL loader.
        
        Returns:
            Dict[str, Any]: Loader metadata
        """
        metadata = super().get_metadata()
        metadata.update({
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "charset": self.charset,
            "if_exists_mode": self.if_exists
        })
        return metadata