"""Tests for generate CLI module."""

import os
from unittest.mock import patch

import pytest
from rich.console import Console

from sb_notion.generate.cli import main


def test_cli_requires_api_key(capsys):
    """Test CLI requires API key."""
    with patch.dict(os.environ, clear=True):
        with pytest.raises(SystemExit):
            main(["--force"])
        captured = capsys.readouterr()
        assert "API key not provided" in captured.err


def test_cli_accepts_api_key_arg(mock_env_vars):
    """Test CLI accepts API key argument."""
    with patch("sb_notion.generate.cli.AsyncSBNotion") as mock_notion:
        instance = mock_notion.return_value.__aenter__.return_value
        instance.databases = {}
        
        with pytest.raises(SystemExit) as exc_info:
            main(["--api-key", "test-key"])
        
        assert exc_info.value.code == 0
        mock_notion.assert_called_once_with(
            "test-key",
            output_dir="generated",
            logger=pytest.any
        )


def test_cli_respects_output_dir(mock_env_vars, tmp_path):
    """Test CLI respects output directory argument."""
    output_dir = str(tmp_path / "custom_dir")
    
    with patch("sb_notion.generate.cli.AsyncSBNotion") as mock_notion:
        instance = mock_notion.return_value.__aenter__.return_value
        instance.databases = {}
        
        with pytest.raises(SystemExit) as exc_info:
            main(["--output-dir", output_dir])
        
        assert exc_info.value.code == 0
        mock_notion.assert_called_once_with(
            "test-api-key",
            output_dir=output_dir,
            logger=pytest.any
        )


def test_cli_handles_filter(mock_env_vars):
    """Test CLI handles database filter argument."""
    with patch("sb_notion.generate.cli.AsyncSBNotion") as mock_notion:
        instance = mock_notion.return_value.__aenter__.return_value
        instance.databases = {
            "db1": {"title": [{"text": {"content": "TV Shows"}}]},
            "db2": {"title": [{"text": {"content": "Movies"}}]}
        }
        
        with pytest.raises(SystemExit) as exc_info:
            main(["--filter", "tv"])
        
        assert exc_info.value.code == 0
        # Should only process TV Shows database
        assert instance.generate_database_class.call_count == 1
