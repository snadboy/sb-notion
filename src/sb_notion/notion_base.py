from dataclasses import dataclass, fields, Field, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, Type, TypeVar, get_args, get_origin, List, Union, get_type_hints, ClassVar

T = TypeVar('T', bound='NotionBase')

class NotionPropertyType(str, Enum):
    TITLE = "title"
    RICH_TEXT = "rich_text"
    SELECT = "select"
    MULTI_SELECT = "multi_select"
    DATE = "date"
    NUMBER = "number"
    CHECKBOX = "checkbox"
    URL = "url"
    EMAIL = "email"
    PHONE_NUMBER = "phone_number"
    FORMULA = "formula"
    RELATION = "relation"
    ROLLUP = "rollup"
    CREATED_TIME = "created_time"
    CREATED_BY = "created_by"
    LAST_EDITED_TIME = "last_edited_time"
    LAST_EDITED_BY = "last_edited_by"
    FILES = "files"
    PEOPLE = "people"
    STATUS = "status"
    UNIQUE_ID = "unique_id"

@dataclass(slots=True)
class NotionFieldMeta:
    notion_type: NotionPropertyType
    notion_name: str
    options: Optional[list[str]] = None

def notion_field(*, 
                notion_type: NotionPropertyType, 
                notion_name: str,
                options: Optional[list[str]] = None,
                default: Any = None,
                **kwargs) -> Any:
    return field(
        default=default,
        metadata={"notion": NotionFieldMeta(
            notion_type=notion_type,
            notion_name=notion_name,
            options=options
        )},
        **kwargs
    )

@dataclass(slots=True)
class NotionBase:
    """Base class for all Notion objects"""
    
    # Class-level attribute to store the database ID
    __notion_database_id: ClassVar[str] = None
    
    id: Optional[str] = None
    created_time: Optional[datetime] = None
    last_edited_time: Optional[datetime] = None
    created_by: Optional[str] = None
    last_edited_by: Optional[str] = None
    
    def __post_init__(self):
        """Validate and convert basic Notion properties"""
        if isinstance(self.created_time, str) and self.created_time:
            # Handle Notion's Z-terminated UTC timestamps
            timestamp = self.created_time
            if timestamp.endswith('Z'):
                timestamp = timestamp[:-1] + '+00:00'
            self.created_time = datetime.fromisoformat(timestamp)
        if isinstance(self.last_edited_time, str) and self.last_edited_time:
            timestamp = self.last_edited_time
            if timestamp.endswith('Z'):
                timestamp = timestamp[:-1] + '+00:00'
            self.last_edited_time = datetime.fromisoformat(timestamp)
    
    def to_notion_properties(self) -> Dict[str, Any]:
        """Convert the object's properties to Notion format"""
        properties = {}
        for field_name, field_def in self.__class__.__dataclass_fields__.items():
            if field_name.startswith('_'):
                continue

            if 'notion' not in field_def.metadata:
                continue

            field_meta: NotionFieldMeta = field_def.metadata['notion']
            field_value = getattr(self, field_name)
            if field_value is None:
                continue

            if field_meta.notion_type == NotionPropertyType.TITLE:
                properties[field_meta.notion_name] = {
                    "title": [{"text": {"content": str(field_value)}}]
                }
            elif field_meta.notion_type == NotionPropertyType.RICH_TEXT:
                properties[field_meta.notion_name] = {
                    "rich_text": [{"text": {"content": str(field_value)}}]
                }
            elif field_meta.notion_type == NotionPropertyType.SELECT:
                if isinstance(field_value, Enum):
                    field_value = field_value.value
                properties[field_meta.notion_name] = {
                    "select": {"name": str(field_value)}
                }
            elif field_meta.notion_type == NotionPropertyType.MULTI_SELECT:
                if isinstance(field_value, (list, tuple)):
                    values = [{"name": v.value if isinstance(v, Enum) else str(v)} for v in field_value]
                else:
                    values = [{"name": field_value.value if isinstance(field_value, Enum) else str(field_value)}]
                properties[field_meta.notion_name] = {"multi_select": values}
            elif field_meta.notion_type == NotionPropertyType.DATE:
                if isinstance(field_value, datetime):
                    properties[field_meta.notion_name] = {
                        "date": {"start": field_value.isoformat()}
                    }
            elif field_meta.notion_type == NotionPropertyType.NUMBER:
                properties[field_meta.notion_name] = {"number": float(field_value)}
            elif field_meta.notion_type == NotionPropertyType.CHECKBOX:
                properties[field_meta.notion_name] = {"checkbox": bool(field_value)}
            elif field_meta.notion_type == NotionPropertyType.URL:
                properties[field_meta.notion_name] = {"url": str(field_value)}
            
        return properties

    @classmethod
    def get_database_id(cls) -> Optional[str]:
        """Get the Notion database ID associated with this class"""
        return getattr(cls, '_NotionBase__notion_database_id', None)

    @classmethod
    def from_notion_name(cls, notion_name: str) -> str:
        """Get the Python field name for a Notion property name"""
        return cls.get_field_name(notion_name)

    @classmethod
    def to_notion_name(cls, field_name: str) -> str:
        """Get the Notion property name for a field"""
        return cls.get_notion_name(field_name)

    @classmethod
    def get_notion_name(cls, field_name: str) -> str:
        """Get the original Notion property name for a field"""
        field = cls.__dataclass_fields__.get(field_name)
        if not field:
            raise ValueError(f"Field {field_name} not found in {cls.__name__}")
        return field.metadata.get("notion_name", field_name)

    @classmethod
    def get_field_name(cls, notion_name: str) -> str:
        """Get the Python field name for a Notion property name"""
        for field_name, field in cls.__dataclass_fields__.items():
            if field.metadata.get("notion_name") == notion_name:
                return field_name
        # If not found in metadata, it might be a direct match
        if notion_name in cls.__dataclass_fields__:
            return notion_name
        raise ValueError(f"No field found for Notion property {notion_name} in {cls.__name__}")

    @classmethod
    def from_notion_page(cls: Type[T], page: Dict[str, Any]) -> T:
        """Create a new instance from a Notion page object"""
        # Extract page metadata
        instance = cls()
        instance.id = page.get("id")
        instance.created_time = datetime.fromisoformat(page.get("created_time", "")[:-1] + '+00:00')
        instance.last_edited_time = datetime.fromisoformat(page.get("last_edited_time", "")[:-1] + '+00:00')
        
        properties = page.get("properties", {})
        class_fields = {f.name: f for f in fields(cls) if not f.name.startswith('_')}
        
        for field_name, field in class_fields.items():
            if field_name in ('id', 'created_time', 'last_edited_time'):
                continue
                
            if 'notion' not in field.metadata:
                continue
                
            field_meta: NotionFieldMeta = field.metadata['notion']
            notion_value = properties.get(field_meta.notion_name)
            if notion_value is None:
                continue

            value = None
            if field_meta.notion_type == NotionPropertyType.TITLE:
                title = notion_value.get("title", [])
                value = title[0].get("text", {}).get("content") if title else None
            elif field_meta.notion_type == NotionPropertyType.RICH_TEXT:
                rich_text = notion_value.get("rich_text", [])
                value = rich_text[0].get("text", {}).get("content") if rich_text else None
            elif field_meta.notion_type == NotionPropertyType.SELECT:
                select = notion_value.get("select")
                value = select.get("name") if select else None
            elif field_meta.notion_type == NotionPropertyType.MULTI_SELECT:
                multi_select = notion_value.get("multi_select", [])
                value = [item.get("name") for item in multi_select if item.get("name")]
            elif field_meta.notion_type == NotionPropertyType.DATE:
                date = notion_value.get("date")
                if date and date.get("start"):
                    value = datetime.fromisoformat(date["start"].replace('Z', '+00:00'))
            elif field_meta.notion_type == NotionPropertyType.NUMBER:
                value = notion_value.get("number")
            elif field_meta.notion_type == NotionPropertyType.CHECKBOX:
                value = notion_value.get("checkbox")
            elif field_meta.notion_type == NotionPropertyType.URL:
                value = notion_value.get("url")

            # Convert value to field type if needed
            if value is not None:
                field_type = field.type
                if get_origin(field_type) is list:
                    # Handle List types
                    item_type = get_args(field_type)[0]
                    if get_origin(item_type) is Union:  # Handle Optional
                        item_type = get_args(item_type)[0]
                    if isinstance(item_type, type) and issubclass(item_type, Enum):
                        if not isinstance(value, list):
                            value = [value]
                        value = [item_type(v) for v in value]
                else:
                    # Handle non-list types
                    if get_origin(field_type) is Union:  # Handle Optional
                        field_type = get_args(field_type)[0]
                    if isinstance(field_type, type) and issubclass(field_type, Enum):
                        value = field_type(value)
                
                setattr(instance, field_name, value)
        
        return instance
