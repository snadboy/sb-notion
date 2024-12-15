"""Tests for sb_notion_async module."""

import pytest
from unittest.mock import patch, MagicMock

from sb_notion.sb_notion_async import AsyncSBNotion


@pytest.mark.asyncio
async def test_async_context_manager():
    """Test AsyncSBNotion works as a context manager."""
    async with AsyncSBNotion("test-key") as notion:
        assert notion.api_key == "test-key"
        assert notion.client is not None


@pytest.mark.asyncio
async def test_get_databases(notion_client):
    """Test fetching databases."""
    notion_client.search.return_value = {
        "results": [
            {
                "object": "database",
                "id": "db1",
                "title": [{"text": {"content": "Test DB"}}]
            }
        ]
    }
    
    async with patch("notion_client.AsyncClient", return_value=notion_client):
        async with AsyncSBNotion("test-key") as notion:
            dbs = await notion.databases
            assert len(dbs) == 1
            assert "db1" in dbs
            assert dbs["db1"]["title"][0]["text"]["content"] == "Test DB"


@pytest.mark.asyncio
async def test_generate_database_class(notion_client, tmp_path):
    """Test database class generation."""
    notion_client.databases.retrieve.return_value = {
        "object": "database",
        "id": "db1",
        "title": [{"text": {"content": "TestDB"}}],
        "properties": {
            "Name": {"id": "title", "name": "Name", "type": "title"}
        }
    }
    
    async with patch("notion_client.AsyncClient", return_value=notion_client):
        async with AsyncSBNotion("test-key", output_dir=str(tmp_path)) as notion:
            file_path = await notion.generate_database_class("db1")
            assert file_path is not None
            assert (tmp_path / "test_db.py").exists()


@pytest.mark.asyncio
async def test_error_handling(notion_client):
    """Test error handling."""
    notion_client.search.side_effect = Exception("API Error")
    
    async with patch("notion_client.AsyncClient", return_value=notion_client):
        async with AsyncSBNotion("test-key") as notion:
            with pytest.raises(Exception, match="API Error"):
                await notion.databases
