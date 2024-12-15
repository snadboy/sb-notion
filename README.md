# SB Notion

A Python library for interacting with Notion databases.

## Features

- Sync and async clients for Notion API
- Database schema generation
- Type-safe database operations
- CLI tool for generating database classes

## Installation

```bash
pip install sb-notion
```

## Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/snadboy/sb-notion.git
   cd sb-notion
   ```

2. Create and activate virtual environment:
   ```bash
   python -m venv .venv
   # On Windows:
   .venv\Scripts\activate
   # On Unix:
   source .venv/bin/activate
   ```

3. Install development dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

4. Run tests:
   ```bash
   pytest
   ```

## Usage

### Basic Usage

```python
from sb_notion import AsyncSBNotion

async with AsyncSBNotion("your-api-key") as notion:
    databases = await notion.databases
    # Work with your databases
```

### Generate Database Classes

```bash
python -m sb_notion.generate.cli --api-key "your-key"
```

## Development

- Format code: `black src tests`
- Sort imports: `isort src tests`
- Type checking: `mypy src`
- Run tests with coverage: `pytest --cov=src`

## License

MIT
