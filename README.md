# Configurable Web Scraping Framework (CWSF)

CWSF is a Python-based, configuration-driven web scraping framework. It allows you to scrape data from multiple websites by simply adding YAML configuration files—without writing any new code.

## Key Features

- **Zero-Code Scraping**: Add new targets by dropping a YAML file into the `configs/` directory.
- **Auto-Discovery**: Automatically detects new, modified, or removed configurations.
- **Advanced Extraction**: Supports CSS selectors and XPath via `parsel`/`BeautifulSoup`.
- **JavaScript Support**: Headless browser rendering using Playwright for JS-heavy sites.
- **Flexible Pagination**: Supports URL patterns, "next" buttons, and infinite scrolling.
- **Robust Engine**: Built-in rate limiting, retries with exponential backoff, and concurrent scraping.
- **Structured Storage**: Saves results to SQLite with automatic metadata (timestamp, source URL).
- **Monitoring**: Centralized logging and error notifications via Gotify.
- **CLI Interface**: Comprehensive command-line tool for running, validating, and monitoring scrapes.

## Installation

CWSF requires Python 3.10 or higher.

1. **Clone the repository**:
   ```bash
   git clone https://github.com/peveleigh/cwsf.git
   cd cwsf
   ```

2. **Install dependencies**:
   ```bash
   pip install .
   ```

3. **Install Playwright browsers** (for JS rendering):
   ```bash
   playwright install chromium
   ```

## Quick Start

1. **Create a configuration file** in the `configs/` directory (e.g., `configs/books.yaml`):

   ```yaml
   version: "1.0"
   site_name: "books_to_scrape"
   base_url: "https://books.toscrape.com/catalogue/page-{page}.html"
   
   pagination:
     type: "url_pattern"
     param: "page"
     start: 1
     max_pages: 5

   selectors:
     container: "article.product_pod"
     fields:
       title:
         selector: "h3 > a::attr(title)"
       price:
         selector: "p.price_color::text"

   output:
     format: "sqlite"
     destination: "./output/data.db"
   ```

2. **Validate your configuration**:
   ```bash
   cwsf validate --site books_to_scrape
   ```

3. **Run the scraper**:
   ```bash
   cwsf run --site books_to_scrape
   ```

## CLI Usage

The `cwsf` command provides several subcommands:

- `cwsf run`: Process all valid configurations.
  - `--site <name>`: Run a specific site.
  - `--base-url <url>`: Override the base URL for the run.
- `cwsf validate`: Check configurations against the schema.
  - `--all`: Validate all files in the config directory.
  - `--site <name>`: Validate a specific site.
- `cwsf list`: List all discovered configurations and their status.
- `cwsf status`: Show the history and results of recent runs.
  - `--site <name>`: Show detailed history for a specific site.

## Project Structure

- `cwsf/`: Core framework source code.
  - `config/`: Configuration loading and validation.
  - `core/`: Orchestration and job queue management.
  - `engine/`: Fetching, parsing, and pagination logic.
  - `utils/`: Logging, notifications, and history tracking.
- `configs/`: Directory for user-defined YAML configurations.
- `docs/`: Additional documentation (Security, Configuration details).
