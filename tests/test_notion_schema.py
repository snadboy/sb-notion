"""Tests for notion_schema module."""

import pytest

from sb_notion.notion_schema import generate_class_source


def test_generate_class_source(sample_database_schema):
    """Test class generation from schema."""
    source = generate_class_source(sample_database_schema)
    
    # Check class definition
    assert "class TVShows(NotionObject):" in source
    
    # Check property types
    assert "title: str" in source
    assert "status: Optional[str]" in source
    assert "rating: Optional[float]" in source
    
    # Check imports
    assert "from typing import Optional" in source
    assert "from .notion_base import NotionObject" in source


def test_generate_class_source_handles_special_chars():
    """Test class generation handles special characters in names."""
    schema = {
        "object": "database",
        "id": "test-db-id",
        "title": [{"type": "text", "text": {"content": "My-Weird Name!"}}],
        "properties": {
            "Title": {"id": "title", "name": "Title", "type": "title"},
            "Weird-Field!": {
                "id": "weird",
                "name": "Weird-Field!",
                "type": "rich_text"
            }
        }
    }
    
    source = generate_class_source(schema)
    
    # Check class name is sanitized
    assert "class MyWeirdName(NotionObject):" in source
    # Check field name is sanitized
    assert "weird_field: Optional[str]" in source


def test_generate_class_source_all_types():
    """Test class generation handles all Notion property types."""
    schema = {
        "object": "database",
        "id": "test-db-id",
        "title": [{"type": "text", "text": {"content": "AllTypes"}}],
        "properties": {
            "Title": {"id": "title", "name": "Title", "type": "title"},
            "Text": {"id": "text", "name": "Text", "type": "rich_text"},
            "Number": {"id": "number", "name": "Number", "type": "number"},
            "Select": {"id": "select", "name": "Select", "type": "select"},
            "MultiSelect": {"id": "multi", "name": "MultiSelect", "type": "multi_select"},
            "Date": {"id": "date", "name": "Date", "type": "date"},
            "Checkbox": {"id": "check", "name": "Checkbox", "type": "checkbox"},
            "URL": {"id": "url", "name": "URL", "type": "url"},
            "Email": {"id": "email", "name": "Email", "type": "email"},
            "Phone": {"id": "phone", "name": "Phone", "type": "phone_number"},
            "Relation": {"id": "rel", "name": "Relation", "type": "relation"}
        }
    }
    
    source = generate_class_source(schema)
    
    # Check all type annotations
    assert "title: str" in source
    assert "text: Optional[str]" in source
    assert "number: Optional[float]" in source
    assert "select: Optional[str]" in source
    assert "multi_select: Optional[list[str]]" in source
    assert "date: Optional[datetime]" in source
    assert "checkbox: Optional[bool]" in source
    assert "url: Optional[str]" in source
    assert "email: Optional[str]" in source
    assert "phone: Optional[str]" in source
    assert "relation: Optional[list[str]]" in source
