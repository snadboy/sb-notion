#!/usr/bin/env python3
"""CLI tool to generate Python classes from Notion databases."""

import argparse
import asyncio
import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from rich import print as rprint

from sb_notion_async import AsyncSBNotion


def setup_logger(log_file: Optional[str], log_level: str) -> logging.Logger:
    """Set up and configure logger."""
    logger = logging.getLogger("notion_generator")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Create console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Create file handler if log file specified
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10000,  # 10KB per file
            backupCount=3    # Keep 3 backup files
        )
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


async def generate_classes(
    api_key: str,
    output_dir: str,
    force: bool = False,
    database_filter: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> None:
    """Generate Python classes for Notion databases.
    
    Args:
        api_key: Notion API key
        output_dir: Directory to output generated files
        force: Force regeneration even if schema hasn't changed
        database_filter: Optional filter string to match database names
        logger: Optional logger instance
    """
    try:
        async with AsyncSBNotion(api_key, logger) as notion:
            # Get all databases
            databases = await notion.databases
            if not databases:
                logger.error("No databases found. Check your API key and permissions.")
                return

            # Filter databases if filter string provided
            if database_filter:
                filtered_dbs = {
                    db_id: db for db_id, db in databases.items()
                    if database_filter.lower() in db.get("title", [{}])[0].get("plain_text", "").lower()
                }
                if not filtered_dbs:
                    logger.warning(f"No databases found matching filter: {database_filter}")
                    return
                databases = filtered_dbs

            # Generate classes for each database
            for db in databases.values():
                title = db.get("title", [{}])[0].get("plain_text", "Untitled")
                try:
                    generated_file = await notion.generate_database_class(
                        db["id"],
                        force=force
                    )
                    if generated_file:
                        logger.info(f"Generated class for database: {title}")
                    else:
                        logger.info(f"Skipped database {title} (no changes detected)")
                except Exception as e:
                    logger.error(f"Error generating class for database {title}: {str(e)}")

    except Exception as e:
        logger.error(f"Error: {str(e)}")


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Generate Python classes from Notion databases"
    )
    parser.add_argument(
        "--api-key",
        help="Notion API key (can also be set via NOTION_API_KEY env var)"
    )
    parser.add_argument(
        "--output-dir",
        default="generated",
        help="Directory to output generated files (default: generated)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force regeneration even if schema hasn't changed"
    )
    parser.add_argument(
        "--filter",
        help="Optional filter string to match database names"
    )
    parser.add_argument(
        "--log-file",
        help="Log file path (optional)"
    )
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error", "critical"],
        default="info",
        help="Set the logging level (default: info)"
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Get API key from args or environment
    api_key = args.api_key or os.getenv("NOTION_API_KEY")
    if not api_key:
        rprint("[red]Error: Notion API key not provided. Use --api-key or set NOTION_API_KEY environment variable.[/red]")
        sys.exit(1)

    # Setup logger
    logger = setup_logger(args.log_file, args.log_level)

    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    # Run the generator
    try:
        asyncio.run(generate_classes(
            api_key=api_key,
            output_dir=str(output_dir),
            force=args.force,
            database_filter=args.filter,
            logger=logger
        ))
    except KeyboardInterrupt:
        rprint("\n[yellow]Generation cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        rprint(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
