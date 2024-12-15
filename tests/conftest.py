"""Pytest configuration and fixtures."""

import os
from unittest.mock import MagicMock

import pytest
from notion_client import AsyncClient

@pytest.fixture
def notion_client():
    """Mock Notion client for testing."""
    client = MagicMock(spec=AsyncClient)
    return client

@pytest.fixture
def sample_database_schema():
    """Sample database schema for testing."""
    return {
        "object": "database",
        "id": "test-db-id",
        "title": [{"type": "text", "text": {"content": "TV Shows"}}],
        "properties": {
            "Title": {
                "id": "title",
                "name": "Title",
                "type": "title"
            },
            "Status": {
                "id": "status",
                "name": "Status",
                "type": "select",
                "select": {
                    "options": [
                        {"name": "Watching", "color": "blue"},
                        {"name": "Completed", "color": "green"}
                    ]
                }
            },
            "Rating": {
                "id": "rating",
                "name": "Rating",
                "type": "number",
                "number": {
                    "format": "number"
                }
            }
        }
    }

@pytest.fixture
def mock_env_vars():
    """Set up environment variables for testing."""
    original_env = dict(os.environ)
    os.environ["NOTION_API_KEY"] = "test-api-key"
    yield
    os.environ.clear()
    os.environ.update(original_env)
