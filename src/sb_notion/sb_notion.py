import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Type, TypeVar, Union, Any

from notion_base import NotionBase
from notion_filters import NotionFilter, NotionSort
from sb_notion_async import AsyncSBNotion

T = TypeVar('T', bound=NotionBase)

class SBNotion:
    """Synchronous wrapper for AsyncSBNotion that provides a blocking interface."""
    
    def __init__(self, api_key: str, logger: Optional[logging.Logger] = None):
        """Initialize the sync wrapper for AsyncSBNotion.
        
        Args:
            api_key: Notion API key
            logger: Optional logger instance
        """
        self._async_client = AsyncSBNotion(api_key, logger)
        self.logger = self._async_client.logger
    
    def __enter__(self) -> 'SBNotion':
        """Context manager entry"""
        asyncio.run(self._async_client.__aenter__())
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        asyncio.run(self._async_client.__aexit__(exc_type, exc_val, exc_tb))
    
    def close(self) -> None:
        """Close the client"""
        asyncio.run(self._async_client.close())
    
    def get_page(self, identifier: str) -> Optional[Dict[str, Any]]:
        """Get a page by ID or title.
        
        Args:
            identifier: Page ID or title
            
        Returns:
            Page object if found, None otherwise
        """
        return asyncio.run(self._async_client.get_page(identifier))
    
    def get_database(self, identifier: str) -> Optional[Dict[str, Any]]:
        """Get a database by ID or title.
        
        Args:
            identifier: Database ID or title
            
        Returns:
            Database object if found, None otherwise
        """
        return asyncio.run(self._async_client.get_database(identifier))
    
    @property
    def pages(self) -> Dict[str, Dict[str, Any]]:
        """Get all pages."""
        return asyncio.run(self._async_client.pages)
    
    @property
    def databases(self) -> Dict[str, Dict[str, Any]]:
        """Get all databases."""
        return asyncio.run(self._async_client.databases)

    def generate_database_class(self, database_id: str, force: bool = False) -> Optional[Path]:
        """Generate a Python dataclass for a Notion database schema.
        
        Args:
            database_id: The ID of the Notion database
            force: If True, regenerate even if schema hasn't changed
            
        Returns:
            Path to the generated file if successful, None otherwise
        """
        result = asyncio.run(self._async_client.generate_database_class(database_id, force))
        return result
    
    def create_page(self, data_obj: NotionBase) -> Optional[Dict[str, Any]]:
        """Create a new page in a database using a dataclass instance.
        
        Args:
            data_obj: Dataclass instance containing page data
            
        Returns:
            Created page object from Notion API
        """
        return asyncio.run(self._async_client.create_page(data_obj))
    
    def update_page(self, page_id: str, data_obj: NotionBase) -> Optional[Dict[str, Any]]:
        """Update an existing page using a dataclass instance.
        
        Args:
            page_id: ID of the page to update
            data_obj: Dataclass instance containing updated data
            
        Returns:
            Updated page object from Notion API
        """
        return asyncio.run(self._async_client.update_page(page_id, data_obj))
    
    def get_typed_page(self, page_id: str, class_type: Type[T]) -> Optional[T]:
        """Get a page and convert it to a dataclass instance.
        
        Args:
            page_id: ID of the page to retrieve
            class_type: The dataclass type to convert to
            
        Returns:
            Dataclass instance if successful, None otherwise
        """
        return asyncio.run(self._async_client.get_typed_page(page_id, class_type))
    
    def query_typed_database(
        self,
        class_type: Type[T],
        filter: Optional[Union[NotionFilter, dict]] = None,
        sorts: Optional[Union[List[Union[NotionSort, dict]], List[dict]]] = None,
        page_size: Optional[int] = None,
        start_cursor: Optional[str] = None
    ) -> List[T]:
        """Query a database and return results as dataclass instances.
        
        Args:
            class_type: The dataclass type to convert results to
            filter: Optional Notion filter object or raw filter dict
            sorts: Optional list of sort configurations or raw sort dicts
            page_size: Optional number of results per page
            start_cursor: Optional cursor for pagination
            
        Returns:
            List of dataclass instances. The list may be empty if no results are found.
        """
        results = asyncio.run(
            self._async_client.query_typed_database(
                class_type=class_type,
                filter=filter,
                sorts=sorts,
                page_size=page_size,
                start_cursor=start_cursor
            )
        )
        # Filter out any None values (failed conversions)
        return [r for r in results if r is not None]
