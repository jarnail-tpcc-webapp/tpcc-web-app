# """
# NeonDB Database Connector - STUDY IMPLEMENTATION SKELETON
# Participants will implement this connector to integrate with NeonDB

# This file contains TODO items that participants need to complete during the study.
# """

# import logging
# import os
# from typing import Any, Dict, List, Optional


# from .base_connector import BaseDatabaseConnector

# logger = logging.getLogger(__name__)


# class NeonConnector(BaseDatabaseConnector):
#     """
#     NeonDB database connector for TPC-C application

#     Participants will implement connection management and query execution
#     for NeonDB during the UX study.
#     """

#     def __init__(self):
#         """
#         Initialize NeonDB connection

#         TODO: Implement NeonDB connection initialization
#         - Read configuration from environment variables
#         - Set up PostgreSQL connection to NeonDB
#         - Configure connection parameters and SSL settings
#         - Handle connection pooling if needed

#         Environment variables to use:
#         - NEON_CONNECTION_STRING: PostgreSQL connection string for NeonDB
#         """
#         super().__init__()
#         self.provider_name = "NeonDB"

#         # TODO: Initialize NeonDB connection
#         self.connection = None

#         # TODO: Read configuration from environment
#         self.connection_string = os.getenv("NEON_CONNECTION_STRING")

#         # TODO: Validate required configuration
#         # TODO: Initialize PostgreSQL client and connection

#     def test_connection(self) -> bool:
#         """
#         Test connection to NeonDB database

#         TODO: Implement connection testing
#         - Test connection to NeonDB instance
#         - Execute a simple query to verify connectivity
#         - Return True if successful, False otherwise
#         - Log connection status for study data collection

#         Returns:
#             bool: True if connection successful, False otherwise
#         """
#         try:
#             # TODO: Implement connection test
#             # Example: Execute "SELECT 1" query
#             return False  # TODO: Replace with actual implementation
#         except Exception as e:
#             logger.error(f"NeonDB connection test failed: {str(e)}")
#             return False

#     def execute_query(
#         self, query: str, params: Optional[tuple] = None
#     ) -> List[Dict[str, Any]]:
#         """
#         Execute SQL query on NeonDB

#         TODO: Implement query execution
#         - Handle parameterized queries safely
#         - Convert PostgreSQL results to standard format
#         - Handle PostgreSQL-specific data types
#         - Implement proper error handling
#         - Log query performance for study metrics

#         Args:
#             query: SQL query string
#             params: Optional query parameters

#         Returns:
#             List of dictionaries representing query results
#         """
#         try:
#             # TODO: Implement query execution
#             # TODO: Handle parameterized queries
#             # TODO: Convert results to standard format
#             # TODO: Log performance metrics
#             return []  # TODO: Replace with actual implementation
#         except Exception as e:
#             logger.error(f"NeonDB query execution failed: {str(e)}")
#             raise

#     def get_provider_name(self) -> str:
#         """Return the provider name"""
#         return self.provider_name

#     def close_connection(self):
#         """
#         Close database connections

#         TODO: Implement connection cleanup
#         - Close PostgreSQL client connections
#         - Clean up any connection pools
#         - Log connection closure for study metrics
#         """
#         try:
#             # TODO: Implement connection cleanup
#             # TODO: Close client connections
#             # TODO: Log cleanup completion
#             pass
#         except Exception as e:
#             logger.error(f"Connection cleanup failed: {str(e)}")



"""
NeonDB Database Connector - IMPLEMENTATION
This file connects to NeonDB using psycopg2 and provides query execution support.
"""

import logging
import os
from typing import Any, Dict, List, Optional
import psycopg2
import psycopg2.extras

from .base_connector import BaseDatabaseConnector

logger = logging.getLogger(__name__)


class NeonConnector(BaseDatabaseConnector):
    """
    NeonDB database connector for TPC-C application
    """

    def __init__(self):
        """
        Initialize NeonDB connection

        Reads connection string from environment:
        - NEON_CONNECTION_STRING: PostgreSQL connection string for NeonDB
        """
        super().__init__()
        self.provider_name = "NeonDB"

        # Read connection string from environment
        self.connection_string = os.getenv(
            "NEON_CONNECTION_STRING",
            
        )

        if not self.connection_string:
            raise ValueError("NEON_CONNECTION_STRING environment variable is required")

        try:
            # Establish PostgreSQL connection
            self.connection = psycopg2.connect(
                self.connection_string,
                cursor_factory=psycopg2.extras.RealDictCursor,  # results as dicts
            )
            self.connection.autocommit = True
            logger.info("Connected to NeonDB successfully")
        except Exception as e:
            logger.error(f"Failed to connect to NeonDB: {str(e)}")
            raise

    def test_connection(self) -> bool:
        """
        Test connection to NeonDB database
        """
        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"NeonDB connection test failed: {str(e)}")
            return False

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL query on NeonDB
        Returns results as list of dictionaries
        """
        try:
            with self.connection.cursor() as cur:
                cur.execute(query, params or ())
                if cur.description:  # SELECT query
                    rows = cur.fetchall()
                    return rows  # Already dicts due to RealDictCursor
                return []  # For INSERT/UPDATE/DELETE
        except Exception as e:
            logger.error(f"NeonDB query execution failed: {str(e)}")
            raise

    def get_provider_name(self) -> str:
        """Return the provider name"""
        return self.provider_name

    def close_connection(self):
        """
        Close database connections
        """
        try:
            if self.connection:
                self.connection.close()
                logger.info("NeonDB connection closed")
        except Exception as e:
            logger.error(f"Connection cleanup failed: {str(e)}")
