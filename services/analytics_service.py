"""
Analytics Service - Database integration for TPC-C webapp
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class AnalyticsService:
    """
    Analytics service for TPC-C webapp

    This service provides dashboard metrics and data queries using the
    database connector that participants will implement.
    """

    def __init__(self, db_connector):
        """
        Initialize the analytics service

        Args:
            db_connector: Database connector instance
        """
        self.connector = db_connector
        if self.connector:
            logger.info(
                f"📊 Analytics Service initialized with {self.connector.get_provider_name()}"
            )
        else:
            logger.warning("⚠️ Analytics Service initialized without database connector")

    def test_connection(self) -> Dict[str, Any]:
        """
        Test database connection

        Returns:
            dict: Connection test results
        """
        if not self.connector:
            return {
                "success": False,
                "error": "No database connector available",
                "provider": "Unknown",
            }

        try:
            success = self.connector.test_connection()
            return {
                "success": success,
                "provider": self.connector.get_provider_name(),
                "message": "Connection successful" if success else "Connection failed",
            }
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "provider": self.connector.get_provider_name(),
            }

    def get_dashboard_metrics(self) -> Dict[str, Any]:
        """
        Get dashboard metrics for the webapp

        Returns:
            dict: Dashboard metrics or error information
        """
        if not self.connector:
            return {
                "error": "No database connector available",
                "metrics": self._get_default_metrics(),
            }

        try:
            # Test connection first
            if not self.connector.test_connection():
                return {
                    "error": "Database connection failed",
                    "metrics": self._get_default_metrics(),
                }

            # Try to get basic metrics using simple queries
            metrics = {}

            # Get warehouse count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM warehouse"
                )
                print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                print(result)
                metrics["total_warehouses"] = result[0]["count"] if result else 0
                print(f"Total Warehouses: {metrics['total_warehouses']}")
                print(metrics)
            except Exception as e:
                logger.warning(f"Failed to get warehouse count: {str(e)}")
                metrics["total_warehouses"] = 0

            # Get customer count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM customer"
                )
                metrics["total_customers"] = result[0]["count"] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get customer count: {str(e)}")
                metrics["total_customers"] = 0

            # Get order count
            try:
                result = self.connector.execute_query(
                    'SELECT COUNT(*) as count FROM "order"'
                )
                metrics["total_orders"] = result[0]["count"] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get order count: {str(e)}")
                metrics["total_orders"] = 0

            # Get item count
            try:
                result = self.connector.execute_query(
                    "SELECT COUNT(*) as count FROM item"
                )
                metrics["total_items"] = result[0]["count"] if result else 0
            except Exception as e:
                logger.warning(f"Failed to get item count: {str(e)}")
                metrics["total_items"] = 0

            print("Metrics collected:", metrics)
            return {
                "success": True,
                "provider": self.connector.get_provider_name(),
                "metrics": metrics,
            }

        except Exception as e:
            logger.error(f"Failed to get dashboard metrics: {str(e)}")
            return {
                "error": str(e),
                "provider": self.connector.get_provider_name()
                if self.connector
                else "Unknown",
                "metrics": self._get_default_metrics(),
            }


    def get_warehouses(self) -> Dict[str, Any]:
        """
        Get recent warehouses for the webapp

        Args:
            limit: Maximum number of warehouses to return

        Returns:
            dict: Warehouses data or error information
        """
        if not self.connector:
            return {"error": "No database connector available", "warehouses": []}

        try:
            if not self.connector.test_connection():
                return {"error": "Database connection failed", "warehouses": []}

            query = f"""
                SELECT w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip
                FROM warehouse 
                ORDER BY w_id 
                
            """

            result = self.connector.execute_query(query)

            return {
                "success": True,
                "provider": self.connector.get_provider_name(),
                "warehouses": result,
            }

        except Exception as e:
            logger.error(f"Failed to get warehouses: {str(e)}")
            return {
                "error": str(e),
                "provider": self.connector.get_provider_name()
                if self.connector
                else "Unknown",
                "warehouses": [],
            }
    def get_orders(self, limit: int = 10) -> Dict[str, Any]:
        """
        Get recent orders for the webapp

        Args:
            limit: Maximum number of orders to return

        Returns:
            dict: Orders data or error information
        """
        if not self.connector:
            return {"error": "No database connector available", "orders": []}

        try:
            if not self.connector.test_connection():
                return {"error": "Database connection failed", "orders": []}

            query = f"""
                SELECT o_id, o_w_id, o_d_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local
                FROM "order" 
                ORDER BY o_entry_d DESC 
                LIMIT {limit}
            """

            result = self.connector.execute_query(query)

            return {
                "success": True,
                "provider": self.connector.get_provider_name(),
                "orders": result,
            }

        except Exception as e:
            logger.error(f"Failed to get orders: {str(e)}")
            return {
                "error": str(e),
                "provider": self.connector.get_provider_name()
                if self.connector
                else "Unknown",
                "orders": [],
            }

    def get_inventory(self, limit: int = 10) -> Dict[str, Any]:
        """
        Get inventory data for the webapp

        Args:
            limit: Maximum number of items to return

        Returns:
            dict: Inventory data or error information
        """
        if not self.connector:
            return {"error": "No database connector available", "inventory": []}

        try:
            if not self.connector.test_connection():
                return {"error": "Database connection failed", "inventory": []}

            query = f"""
                SELECT s.s_i_id, i.i_name, s.s_w_id, s.s_quantity, i.i_price
                FROM stock s
                JOIN item i ON s.s_i_id = i.i_id
                WHERE s.s_quantity < 50
                ORDER BY s.s_quantity ASC
                LIMIT {limit}
            """

            result = self.connector.execute_query(query)

            return {
                "success": True,
                "provider": self.connector.get_provider_name(),
                "inventory": result,
            }

        except Exception as e:
            logger.error(f"Failed to get inventory: {str(e)}")
            return {
                "error": str(e),
                "provider": self.connector.get_provider_name()
                if self.connector
                else "Unknown",
                "inventory": [],
            }

    def _get_default_metrics(self) -> Dict[str, Any]:
        """Get default metrics when database is not available"""
        return {
            "total_warehouses": 0,
            "total_customers": 0,
            "total_orders": 0,
            "total_items": 0,
            "new_orders": 0,
            "low_stock_items": 0,
            "orders_last_24h": 0,
            "avg_order_value": 0.0,
        }

    def close(self):
        """Close database connections"""
        if self.connector:
            try:
                self.connector.close_connection()
                logger.info("📊 Analytics Service connections closed")
            except Exception as e:
                logger.error(f"Error closing connections: {str(e)}")
