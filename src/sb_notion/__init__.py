"""SB Notion - A Python library for interacting with Notion databases."""

from .sb_notion import SBNotion
from .sb_notion_async import AsyncSBNotion

__version__ = "0.6.0"
__all__ = ["SBNotion", "AsyncSBNotion"]
