from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple, Type, ClassVar
import hashlib
import json
import os
import sys
from pathlib import Path
from notion_base import NotionPropertyType, notion_field, NotionBase


@dataclass
class SchemaMetadata:
    """Metadata for generated schema classes"""
    schema_hash: str
    generated_at: datetime
    notion_db_id: str
    notion_db_name: str
    property_types: Dict[str, str]

class NotionTypeMapper:
    """Maps Notion property types to Python types"""
    
    @staticmethod
    def get_type_hint(prop_type: NotionPropertyType, prop_config: dict) -> str:
        """Get the Python type hint for a Notion property type"""
        type_mapping = {
            NotionPropertyType.TITLE: "Optional[str]",
            NotionPropertyType.RICH_TEXT: "Optional[str]",
            NotionPropertyType.NUMBER: "Optional[float]",
            NotionPropertyType.SELECT: "Optional[str]",  # Will be replaced with Enum
            NotionPropertyType.MULTI_SELECT: "Optional[List[str]]",  # Will be replaced with List[Enum]
            NotionPropertyType.DATE: "Optional[datetime]",
            NotionPropertyType.PEOPLE: "Optional[List[str]]",
            NotionPropertyType.FILES: "Optional[List[str]]",
            NotionPropertyType.CHECKBOX: "Optional[bool]",
            NotionPropertyType.URL: "Optional[str]",
            NotionPropertyType.EMAIL: "Optional[str]",
            NotionPropertyType.PHONE_NUMBER: "Optional[str]",
            NotionPropertyType.FORMULA: "Optional[Any]",  # Depends on formula type
            NotionPropertyType.RELATION: "Optional[List[str]]",
            NotionPropertyType.ROLLUP: "Optional[Any]",
            NotionPropertyType.CREATED_TIME: "Optional[datetime]",
            NotionPropertyType.CREATED_BY: "Optional[str]",
            NotionPropertyType.LAST_EDITED_TIME: "Optional[datetime]",
            NotionPropertyType.LAST_EDITED_BY: "Optional[str]",
            NotionPropertyType.UNIQUE_ID: "Optional[str]",
            NotionPropertyType.STATUS: "Optional[str]"  # Will be replaced with Enum like select
        }
        return type_mapping[NotionPropertyType(prop_type)]

    @staticmethod
    def generate_enum_class(enum_name: str, options: List[dict]) -> str:
        """Generate an Enum class for select/multi-select properties"""
        enum_lines = [f"class {enum_name}(str, Enum):"]
        for value in (opt["name"] for opt in options):
            # Convert value to valid Python identifier
            enum_key = value.upper().replace(" ", "_").replace("-", "_")
            # Handle numeric values and special characters
            if value[0].isdigit() or not value[0].isalpha():
                # Handle special cases
                if value.endswith("+"):
                    base = value[:-1]
                    enum_key = f"AGE_{base}_PLUS"
                elif value.startswith("-"):
                    base = value[1:]
                    enum_key = f"AGE_{base}_MINUS"
                else:
                    enum_key = f"AGE_{value}"
            # Remove any remaining special characters
            enum_key = "".join(c for c in enum_key if c.isalnum() or c == "_")
            # Add prefix if needed
            if enum_name.lower() in ["select", "status", "type"]:
                enum_key = f"{enum_name.upper()}_{enum_key}"
            
            enum_lines.append(f'    {enum_key} = "{value}"')
        
        return "\n".join(enum_lines)

class SchemaGenerator:
    """Generates Python dataclasses from Notion database schemas"""
    
    def __init__(self, output_dir: str = "generated"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_schema_hash(self, schema: dict) -> str:
        """Generate a hash of the schema to detect changes"""
        schema_str = json.dumps(schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()
    
    def generate_class_source(self, db_id: str, schema: dict) -> tuple[str, SchemaMetadata]:
        """Generate source code for a dataclass from a Notion database schema"""
        properties = schema.get("properties", {})
        db_title = schema.get("title", [{"plain_text": "Untitled"}])[0]["plain_text"]
        
        # Convert title to valid Python class name
        class_name = "".join(x for x in db_title.title() if x.isalnum())
        
        # Track property types for metadata
        property_types: Dict[str, str] = {}
        
        # Store enums to be defined as inner classes
        enums: List[Tuple[str, str]] = []
        
        # Generate field definitions
        fields: List[str] = []
        
        # Find the title property name from the schema
        title_prop_name = None
        for prop_name, prop_data in properties.items():
            if prop_data["type"] == "title":
                title_prop_name = prop_name
                break
        
        if not title_prop_name:
            raise ValueError("No title property found in database schema")
        
        # Convert title property name to valid Python identifier
        title_field_name = title_prop_name.lower()  # Convert to lowercase
        title_field_name = "".join(x if x.isalnum() or x == "_" else "_" for x in title_field_name)  # Replace non-alphanumeric with underscore
        title_field_name = "_".join(filter(None, title_field_name.split("_")))  # Remove empty segments
        if title_field_name[0].isdigit():
            title_field_name = "f" + title_field_name
        
        # Add the title field using the actual property name
        fields.append(f'    {title_field_name}: str = notion_field(')
        fields.append('        notion_type=NotionPropertyType.TITLE,')
        fields.append(f'        notion_name="{title_prop_name}",')
        fields.append('        default=""')
        fields.append('    )')
        
        for field_name, prop_data in properties.items():
            # Skip the title field since we already added it
            if prop_data["type"] == "title":
                continue
                
            # Store original Notion name
            notion_name = field_name
            
            # Convert field name to valid Python identifier using snake_case
            field_name = field_name.lower()  # Convert to lowercase
            field_name = "".join(x if x.isalnum() or x == "_" else "_" for x in field_name)  # Replace non-alphanumeric with underscore
            field_name = "_".join(filter(None, field_name.split("_")))  # Remove empty segments
            if field_name[0].isdigit():
                field_name = "f" + field_name
            
            # Get property type
            prop_type = NotionPropertyType(prop_data["type"])
            property_types[field_name] = prop_type.value
            
            # Handle special cases like select/multi-select
            if prop_type in (NotionPropertyType.SELECT, NotionPropertyType.MULTI_SELECT, NotionPropertyType.STATUS):
                options = prop_data[prop_type.value]["options"]
                enum_name = f"{field_name}Enum"  # Add Enum suffix to avoid conflict with field name
                enum_def = self._generate_enum_class(enum_name, options)
                enums.append((enum_name, enum_def))
                type_hint = f"Optional[{enum_name}]" if prop_type in (NotionPropertyType.SELECT, NotionPropertyType.STATUS) else f"Optional[List[{enum_name}]]"
                field_type = type_hint
                
                # Create field with notion_field
                fields.append(f'    {field_name}: {field_type} = notion_field(')
                fields.append(f'        notion_type=NotionPropertyType.{prop_type.name},')
                fields.append(f'        notion_name="{notion_name}",')
                fields.append(f'        options=[e.value for e in {enum_name}],')
                fields.append('        default=None')
                fields.append('    )')
            else:
                type_hint = NotionTypeMapper.get_type_hint(prop_type, prop_data)
                field_type = type_hint
                
                # Create field with notion_field
                fields.append(f'    {field_name}: {field_type} = notion_field(')
                fields.append(f'        notion_type=NotionPropertyType.{prop_type.name},')
                fields.append(f'        notion_name="{notion_name}",')
                fields.append('        default=None')
                fields.append('    )')
        
        # Generate class definition with database ID
        source = [
            "from dataclasses import dataclass, field",
            "from typing import Optional, List, ClassVar",
            "from datetime import datetime",
            "from enum import Enum",
            "from notion_base import NotionBase, notion_field, NotionPropertyType",
            "",
            "# Generated class for Notion database",
            f"# Database ID: {db_id}",
            "",
            "@dataclass(slots=True)",
            f"class {class_name}(NotionBase):",
            f'    """Generated dataclass for {db_title} Notion database"""',
            "",
            "    # Set the database ID for this class",
            f"    _NotionBase__notion_database_id: ClassVar[str] = '{db_id}'",
            "",
        ]
        
        # Add any enum definitions inside the class
        for enum_name, enum_source in enums:
            # Indent the enum definition
            indented_enum = "\n".join(f"    {line}" if line else "" for line in enum_source.split("\n"))
            source.extend(["", indented_enum])
        
        # Add fields
        source.extend(fields)
        
        # Create metadata
        metadata = SchemaMetadata(
            schema_hash=self.generate_schema_hash(schema),
            generated_at=datetime.now(),
            notion_db_id=db_id,
            notion_db_name=db_title,
            property_types=property_types
        )
        
        return "\n".join(source), metadata

    def _generate_enum_class(self, enum_name: str, options: List[dict]) -> str:
        """Generate an Enum class for select/multi-select properties"""
        enum_lines = [f"class {enum_name}(str, Enum):"]
        
        for value in (opt["name"] for opt in options):
            # Convert value to valid Python identifier
            enum_key = value.upper().replace(" ", "_").replace("-", "_")
            # Handle numeric values and special characters
            if value[0].isdigit() or not value[0].isalpha():
                # Handle special cases
                if value.endswith("+"):
                    base = value[:-1]
                    enum_key = f"AGE_{base}_PLUS"
                elif value.startswith("-"):
                    base = value[1:]
                    enum_key = f"AGE_{base}_MINUS"
                else:
                    enum_key = f"AGE_{value}"
            # Remove any remaining special characters
            enum_key = "".join(c for c in enum_key if c.isalnum() or c == "_")
            # Add prefix if needed
            if enum_name.lower() in ["select", "status", "type"]:
                enum_key = f"{enum_name.upper()}_{enum_key}"
            
            enum_lines.append(f'    {enum_key} = "{value}"')
        
        return "\n".join(enum_lines)

    def save_schema_class(self, source: str, metadata: SchemaMetadata) -> Path:
        """Save the generated schema class to a file"""
        filename = f"{metadata.notion_db_name.lower().replace(' ', '_')}.py"
        filepath = self.output_dir / filename
        
        with open(filepath, "w") as f:
            f.write(source)
        
        # Save metadata
        meta_filepath = filepath.with_suffix(".meta.json")
        with open(meta_filepath, "w") as f:
            json.dump({
                "schema_hash": metadata.schema_hash,
                "generated_at": metadata.generated_at.isoformat(),
                "notion_db_id": metadata.notion_db_id,
                "notion_db_name": metadata.notion_db_name,
                "property_types": metadata.property_types
            }, f, indent=2)
        
        return filepath
