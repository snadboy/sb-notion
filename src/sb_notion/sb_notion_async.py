import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Tuple, Type, AsyncIterator, TypeVar, Generic, Any
from notion_client import AsyncClient
import asyncio
from notion_schema import SchemaGenerator, SchemaMetadata
import json
from pathlib import Path
from notion_base import NotionBase
from notion_filters import NotionFilter, NotionSort, NotionDatabaseQuery

T = TypeVar('T', bound=NotionBase)

def setup_logging(logger: Optional[logging.Logger] = None) -> logging.Logger:
    """Set up logging with proper levels for different components."""
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.WARNING)  # Set handler to WARNING level
    
    if logger is None:
        logger = logging.getLogger("sb_notion")
        logger.setLevel(logging.WARNING)  # Only show warnings and errors
        logger.propagate = False
        if not logger.handlers:
            logger.addHandler(console_handler)
    
    # Configure other loggers to be less verbose
    for logger_name in ["httpx", "notion_client"]:
        log = logging.getLogger(logger_name)
        log.setLevel(logging.WARNING)  # Only show warnings and above
        log.propagate = False
        if not log.handlers:
            log.addHandler(console_handler)
    
    return logger

class AsyncSBNotion(Generic[T]):
    """Async wrapper for the Notion API that handles pagination and caching."""
    
    def __init__(self, api_key: str, logger: Optional[logging.Logger] = None):
        """Initialize the async Notion client.
        
        Args:
            api_key: Notion API key
            logger: Optional logger instance. If not provided, a default logger will be created.
        """
        # Initialize the async Notion client
        self.client = AsyncClient(auth=api_key.strip())
        
        # Set up logging
        self.logger = setup_logging(logger)
            
        # Schema generator
        self.schema_generator = SchemaGenerator()
            
        # Cache structures
        self._pages_by_id: Dict[str, dict] = {}
        self._pages_by_name: Dict[str, dict] = {}
        self._databases_by_id: Dict[str, dict] = {}
        self._databases_by_name: Dict[str, dict] = {}
        self._database_schemas: Dict[str, str] = {}  # db_id -> schema_hash
        
        # Cache metadata
        self._last_cache_update = datetime.min
        self._cache_ttl = timedelta(minutes=5)
        
        # Rate limiting
        self._last_request_time = datetime.min
        self._min_request_interval = 0.34  # ~3 requests per second
    
    async def __aenter__(self) -> 'AsyncSBNotion[T]':
        """Async context manager entry"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit"""
        await self.client.aclose()
    
    async def close(self) -> None:
        """Close the async client"""
        await self.client.aclose()
    
    def _should_refresh_cache(self) -> bool:
        """Check if the cache should be refreshed based on TTL."""
        return datetime.now() - self._last_cache_update > self._cache_ttl
    
    async def _handle_rate_limits(self) -> None:
        """Handle rate limiting for async requests."""
        now = datetime.now()
        elapsed = (now - self._last_request_time).total_seconds()
        if elapsed < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - elapsed)
        self._last_request_time = now
    
    async def _refresh_caches(self) -> None:
        """Refresh all caches asynchronously."""
        self.logger.info("Refreshing caches...")
        await self._fetch_all_pages()
        await self._fetch_all_databases()
        self._last_cache_update = datetime.now()
    
    async def _fetch_all_pages(self) -> None:
        """Fetch all pages from Notion asynchronously."""
        self.logger.info("Fetching all pages...")
        self._pages_by_id.clear()
        self._pages_by_name.clear()
        
        async for page in self._paginated_request(self.client.search, **{
            "filter": {"property": "object", "value": "page"}
        }):
            page_id = page["id"]
            # Get the title if available
            title = ""
            if "properties" in page and "title" in page["properties"]:
                title_items = page["properties"]["title"]["title"]
                if title_items:
                    title = title_items[0]["plain_text"]
            
            self._pages_by_id[page_id] = page
            if title:  # Only store pages with titles
                self._pages_by_name[title] = page
    
    async def _fetch_all_databases(self) -> None:
        """Fetch all databases from Notion asynchronously."""
        self.logger.info("Fetching all databases...")
        self._databases_by_id.clear()
        self._databases_by_name.clear()        

        async for db in self._paginated_request(self.client.search, **{
            "filter": {"property": "object", "value": "database"}
        }):
            db_id = db["id"]
            # Get the title if available
            title = ""
            if "title" in db:
                title_items = db["title"]
                if title_items:
                    title = title_items[0]["plain_text"]
            
            # Store all databases by ID
            self._databases_by_id[db_id] = db
            
            # Only store by name if it has a title
            if title:
                self._databases_by_name[title] = db
                
            # Check for schema changes
            try:
                full_db = await self.client.databases.retrieve(db_id)
                new_hash = self.schema_generator.generate_schema_hash(full_db)
                old_hash = self._database_schemas.get(db_id)
                
                if old_hash != new_hash:
                    title = full_db.get("title", [{"plain_text": db_id}])[0].get("plain_text", db_id)
                    self.logger.info(f"Schema change detected for database: {title}")
                    self._database_schemas[db_id] = new_hash
                    # Auto-regenerate the class
                    await self.generate_database_class(db_id, force=True)
            except Exception as e:
                self.logger.error(f"Error checking schema for database {db_id}: {str(e)}")
    
    async def _paginated_request(self, method, **kwargs) -> AsyncIterator[dict]:
        """Make a paginated request to the Notion API asynchronously.
        
        Args:
            method: The Notion API method to call
            **kwargs: Additional arguments for the method
            
        Yields:
            Individual items from the paginated response
        """
        has_more = True
        next_cursor = None
        
        # Clean up kwargs - remove None values
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        
        while has_more:
            await self._handle_rate_limits()
            
            if next_cursor:
                kwargs["start_cursor"] = next_cursor
            
            try:
                response = await method(**kwargs)
                
                for item in response["results"]:
                    yield item
                
                has_more = response["has_more"]
                next_cursor = response["next_cursor"] if has_more else None
                
            except Exception as e:
                self.logger.error(f"Error in paginated request: {str(e)}")
                break
    
    async def get_page(self, identifier: str) -> Optional[dict]:
        """Get a page by ID or title asynchronously.
        
        Args:
            identifier: Page ID or title
            
        Returns:
            Page object if found, None otherwise
        """
        if self._should_refresh_cache():
            await self._refresh_caches()
        
        # Try getting by ID first
        page = self._pages_by_id.get(identifier)
        if not page:
            # Try getting by name
            page = self._pages_by_name.get(identifier)
            if not page and await self._refresh_caches():
                # Cache miss - refresh and try again
                page = self._pages_by_id.get(identifier) or self._pages_by_name.get(identifier)
        
        return page
    
    async def get_database(self, identifier: str) -> Optional[dict]:
        """Get a database by ID or title asynchronously.
        
        Args:
            identifier: Database ID or title
            
        Returns:
            Database object if found, None otherwise
        """
        if self._should_refresh_cache():
            await self._refresh_caches()
        
        # Try getting by ID first
        db = self._databases_by_id.get(identifier)
        if not db:
            # Try getting by name
            db = self._databases_by_name.get(identifier)
            if not db:
                # Cache miss - refresh and try again
                await self._refresh_caches()
                db = self._databases_by_id.get(identifier) or self._databases_by_name.get(identifier)
        
        # If found, check for schema changes
        if db:
            db_id = db["id"]
            try:
                full_db = await self.client.databases.retrieve(db_id)
                new_hash = self.schema_generator.generate_schema_hash(full_db)
                old_hash = self._database_schemas.get(db_id)
                
                if old_hash != new_hash:
                    title = full_db.get("title", [{"plain_text": db_id}])[0].get("plain_text", db_id)
                    self.logger.info(f"Schema change detected for database: {title}")
                    self._database_schemas[db_id] = new_hash
                    # Auto-regenerate the class
                    await self.generate_database_class(db_id, force=True)
                return full_db  # Return the full database object
            except Exception as e:
                self.logger.error(f"Error checking schema for database {db_id}: {str(e)}")
        
        return db
    
    @property
    async def pages(self) -> Dict[str, dict]:
        """Get all pages asynchronously."""
        if self._should_refresh_cache():
            await self._refresh_caches()
        return self._pages_by_id.copy()
    
    @property
    async def databases(self) -> Dict[str, dict]:
        """Get all databases asynchronously."""
        if self._should_refresh_cache():
            await self._refresh_caches()
        return self._databases_by_id.copy()
    
    async def generate_database_class(self, database_id: str, force: bool = False) -> Optional[Path]:
        """Generate a Python dataclass for a Notion database schema asynchronously.
        
        Args:
            database_id: The ID of the Notion database
            force: If True, regenerate even if schema hasn't changed
            
        Returns:
            Path to the generated file if successful, None otherwise
        """
        try:
            # Get database schema
            db = await self.client.databases.retrieve(database_id)
            
            # Generate source code and metadata
            source, metadata = self.schema_generator.generate_class_source(database_id, db)
            
            # Check if we need to regenerate
            output_dir = Path("generated")
            meta_file = output_dir / f"{metadata.notion_db_name.lower().replace(' ', '_')}.meta.json"
            
            if not force and meta_file.exists():
                with open(meta_file) as f:
                    existing_meta = json.load(f)
                    if existing_meta["schema_hash"] == metadata.schema_hash:
                        self.logger.info(f"Schema unchanged for {metadata.notion_db_name}, skipping generation")
                        return None
            
            # Save the generated class
            return self.schema_generator.save_schema_class(source, metadata)
            
        except Exception as e:
            self.logger.error(f"Error generating database class: {str(e)}")
            return None
    
    async def create_page(
        self,
        data: NotionBase,
    ) -> Dict[str, Any]:
        """Create a new page in the database associated with the instance's class"""
        database_id = data.__class__.get_database_id()
        if database_id is None:
            raise ValueError(f"No database_id found in class metadata for {data.__class__.__name__}")
        return await self._create_page_impl(parent_id=database_id, data=data)

    async def _create_page_impl(
        self,
        parent_id: str,
        data: NotionBase,
        parent_type: str = "database_id"
    ) -> Dict[str, Any]:
        """Internal implementation for creating a page"""
        try:
            # Convert data to Notion properties
            notion_properties = data.to_notion_properties()
            
            # Create page
            response = await self.client.pages.create(
                parent={parent_type: parent_id},
                properties=notion_properties
            )
            
            return response
        except Exception as e:
            self.logger.error(f"Error creating page: {str(e)}")
            raise

    async def query_database(
        self,
        class_type: Type[T],
        filter: Optional[Union[NotionFilter, dict]] = None,
        sorts: Optional[Union[List[Union[NotionSort, dict]], List[dict]]] = None,
        page_size: Optional[int] = None,
        start_cursor: Optional[str] = None
    ) -> List[T]:
        """Query the database associated with the given class type"""
        database_id = class_type.get_database_id()
        if database_id is None:
            raise ValueError(f"No database_id found in class metadata for {class_type.__name__}")
        return await self._query_database_impl(database_id, class_type, filter, sorts, page_size, start_cursor)

    async def _query_database_impl(
        self,
        database_id: str,
        class_type: Type[T],
        filter: Optional[Union[NotionFilter, dict]] = None,
        sorts: Optional[Union[List[Union[NotionSort, dict]], List[dict]]] = None,
        page_size: Optional[int] = None,
        start_cursor: Optional[str] = None
    ) -> List[T]:
        """Internal implementation of database querying logic"""
        try:
            # Get database schema for property name mapping
            db = await self.client.databases.retrieve(database_id)
            schema_props = db.get("properties", {})
            schema_map = {
                name.lower().replace("-", "_").replace(" ", "_"): (name, data)
                for name, data in schema_props.items()
            }
            self.logger.info(f"Schema map: {schema_map}")
            
            # Convert query parameters to dict format if needed
            query_params = {}
            
            if filter:
                if isinstance(filter, NotionFilter):
                    query_params["filter"] = filter.to_dict()
                else:
                    query_params["filter"] = filter
                    
            if sorts:
                processed_sorts = []
                for sort in sorts:
                    if isinstance(sort, NotionSort):
                        processed_sorts.append(sort.to_dict())
                    else:
                        processed_sorts.append(sort)
                query_params["sorts"] = processed_sorts
            
            if page_size is not None:
                query_params["page_size"] = page_size
                
            if start_cursor:
                query_params["start_cursor"] = start_cursor
                
            self.logger.info(f"Final query parameters: {query_params}")
            
            # Get all pages
            pages = []
            has_more = True
            current_cursor = None
            
            while has_more:
                try:
                    # Update the cursor in the query params
                    if current_cursor:
                        query_params["start_cursor"] = current_cursor
                    else:
                        query_params.pop("start_cursor", None)
                    
                    # Set page size for each request
                    query_params["page_size"] = 100
                    
                    self.logger.info(f"Query params: {query_params}")
                    
                    response = await self.client.databases.query(
                        database_id=database_id,
                        **query_params
                    )
                    
                    pages.extend(response["results"])
                    has_more = response["has_more"]
                    current_cursor = response["next_cursor"]
                except Exception as e:
                    self.logger.error(f"Error querying database: {str(e)}")
                    break
            
            # Convert pages to class instances
            results: List[T] = []
            for page in pages:
                try:
                    instance = class_type.from_notion_page(page)
                    if instance:
                        results.append(instance)
                except Exception as e:
                    self.logger.error(f"Error converting page to {class_type.__name__}: {str(e)}")
            
            return results
        except Exception as e:
            self.logger.error(f"Error querying database: {str(e)}")
        
        return []
    
    async def update_page(self, page_id: str, data: NotionBase) -> Optional[Dict[str, Any]]:
        """Update a Notion page with the given data"""
        try:
            # Get current page to ensure we have the latest data
            current_page = await self.client.pages.retrieve(page_id)
            
            # Convert data to Notion properties
            notion_properties = data.to_notion_properties()
            
            # Update page
            updated_page = await self.client.pages.update(
                page_id=page_id,
                properties=notion_properties
            )
            
            return updated_page
        except Exception as e:
            self.logger.error(f"Error updating page: {str(e)}")
            return None
    
    async def get_typed_page(
        self,
        page_id: str,
        class_type: Type[T]
    ) -> Optional[T]:
        """Get a page and convert it to a dataclass instance asynchronously.
        
        Args:
            page_id: ID of the page to retrieve
            class_type: The dataclass type to convert to
            
        Returns:
            Dataclass instance if successful, None otherwise
        """
        try:
            page = await self.client.pages.retrieve(page_id)
            # Convert page to class instance
            try:
                return class_type.from_notion_page(page)
            except Exception as e:
                self.logger.error(f"Error converting page to {class_type.__name__}: {str(e)}")
                return None
        except Exception as e:
            self.logger.error(f"Error retrieving typed page: {str(e)}")
            return None
