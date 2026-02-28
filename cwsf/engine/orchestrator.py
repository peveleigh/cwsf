import logging
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from cwsf.engine.fetcher import fetch, perform_login, FetchError, ScrapeRecord, ScrapeResult
from cwsf.engine.parser import parse_records
from cwsf.engine.paginator import PaginatorFactory, UrlPatternPaginator

logger = logging.getLogger(__name__)
import httpx
from cwsf.output import SqliteWriter

async def scrape_site(config: Dict[str, Any]) -> ScrapeResult:
    """
    Orchestrate fetching -> parsing -> transforms -> record emission for a single site.
    
    Args:
        config: A validated site configuration dictionary.
        
    Returns:
        ScrapeResult containing extracted records, errors, and stats.
    """
    site_name = config.get("site_name", "unknown")
    base_url = config.get("base_url")
    method = config.get("method", "GET")
    headers = config.get("headers", {})
    cookies = config.get("cookies", {})
    auth_config = config.get("auth")
    selectors = config.get("selectors", {})
    
    result = ScrapeResult(site_name=site_name)
    
    if not base_url:
        result.errors.append("Missing base_url in config")
        return result

    paginator = PaginatorFactory.get_paginator(config)
    
    # Initialize current_url. If url_pattern, the first page might be base_url with {page} replaced.
    # Story 4.1 AC 1 & 4: generate URLs by substituting {page} with values starting from pagination.start.
    if isinstance(paginator, UrlPatternPaginator):
        current_url = base_url.replace(paginator.placeholder, str(paginator.start_page))
    else:
        current_url = base_url
        
    current_page = 1 # This tracks how many pages we have fetched
    
    # Create a persistent client for the entire scrape job to maintain session (Story 4.6 AC 4)
    async with httpx.AsyncClient(headers={"User-Agent": "CWSF/1.0"}, follow_redirects=True) as client:
        # Apply initial headers and cookies
        client.headers.update(headers)
        client.cookies.update(cookies)

        # Perform login if configured (Story 4.6 AC 3)
        if auth_config:
            await perform_login(client, auth_config)

        while current_url:
            try:
                # 1. Fetch
                fetch_res = await fetch(
                    url=current_url,
                    method=method,
                    client=client,
                    renderer=config.get("renderer", "httpx"),
                    playwright_options=config.get("playwright_options"),
                    pagination_config=config.get("pagination"),
                    selectors=selectors,
                    rate_limit_config=config.get("rate_limit"),
                    retry_config=config.get("retry"),
                    site_name=site_name,
                    scrape_result=result
                )
            
                # Update stats for the last page fetched
                result.stats["status_code"] = fetch_res.status_code
                result.stats["elapsed_time"] = result.stats.get("elapsed_time", 0) + fetch_res.elapsed_time
                
                # 2. Check for non-200 status code
                if fetch_res.status_code >= 400:
                    error_msg = f"HTTP {fetch_res.status_code} error for {current_url}"
                    result.errors.append(error_msg)
                    
                    # Story 4.6 AC 5: Log a specific warning for 401/403 (possible session expiration)
                    if fetch_res.status_code in (401, 403):
                        logger.warning(
                            f"Possible session expiration or authorization failure: "
                            f"HTTP {fetch_res.status_code} for {current_url} (site: {site_name})"
                        )
                    else:
                        logger.error(f"{error_msg} (site: {site_name})")
                    # Stop pagination on error (Story 4.1 AC 3)
                    break

                # 3. Parse (includes transforms)
                raw_records = parse_records(fetch_res.body, selectors)
                
                # 4. Emit (add metadata)
                timestamp = datetime.now(timezone.utc).isoformat()
                for raw_rec in raw_records:
                    record = ScrapeRecord(
                        fields=raw_rec,
                        site_name=site_name,
                        source_url=fetch_res.url,
                        timestamp=timestamp
                    )
                    result.records.append(record)
                    
                logger.info(f"Successfully scraped {len(raw_records)} records from {current_url} for site {site_name}")
                
                # 5. Check if we should stop
                if paginator.should_stop(current_page, fetch_res, len(raw_records)):
                    break
                    
                # 6. Get next URL
                current_url = paginator.get_next_url(fetch_res, current_page)
                current_page += 1
                
            except FetchError as exc:
                result.errors.append(str(exc))
                logger.error(f"Failed to fetch {current_url} for site {site_name}: {exc}")
                break # Stop pagination on fetch error
            except Exception as exc:
                result.errors.append(f"Unexpected error during scrape: {exc}")
                logger.error(f"Unexpected error scraping {current_url} for site {site_name}: {exc}")
                break # Stop pagination on unexpected error
            
    # 7. Write results (Story 5.5)
    if result.records:
        output_config = config.get("output", {})
        format_type = output_config.get("format", "sqlite")
        if format_type == "sqlite":
            writer = SqliteWriter()
            try:
                writer.open(config)
                # Convert ScrapeRecord objects to dicts for the writer
                records_to_write = []
                for rec in result.records:
                    data = rec.fields.copy()
                    data["site_name"] = rec.site_name
                    data["source_url"] = rec.source_url
                    data["scrape_timestamp"] = rec.timestamp
                    records_to_write.append(data)
                writer.write_records(records_to_write)
            finally:
                writer.close()
            
    return result

async def run_all(
    configs: List[Dict[str, Any]],
    max_concurrency: int = 5
) -> List[ScrapeResult]:
    """
    Process multiple site configs concurrently.
    
    Args:
        configs: List of site configuration dictionaries.
        max_concurrency: Maximum number of concurrent scrape tasks.
        
    Returns:
        List of ScrapeResult objects.
    """
    semaphore = asyncio.Semaphore(max_concurrency)

    async def _scrape_task(config: Dict[str, Any]) -> ScrapeResult:
        async with semaphore:
            return await scrape_site(config)

    tasks = [_scrape_task(config) for config in configs]
    return await asyncio.gather(*tasks)
