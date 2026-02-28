from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Set
from urllib.parse import urljoin
import logging
from parsel import Selector
from cwsf.engine.fetcher import FetchResult

logger = logging.getLogger(__name__)

class BasePaginator(ABC):
    """
    Abstract base class for pagination strategies.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pagination_config = config.get("pagination", {})
        self.max_pages = self.pagination_config.get("max_pages", 1)
        self.start_page = self.pagination_config.get("start", 1)

    @abstractmethod
    def get_next_url(self, current_response: FetchResult, current_page_number: int) -> Optional[str]:
        """
        Determine the next URL to fetch.
        """
        pass

    def should_stop(self, current_page_number: int, current_response: FetchResult, num_records: int) -> bool:
        """
        Determine if pagination should stop.
        """
        # Stop if we reached max_pages
        if current_page_number >= self.max_pages:
            return True
        
        # Stop if no records were found on the current page (Story 4.1 AC 2)
        if num_records == 0:
            return True
            
        return False

class NoPaginator(BasePaginator):
    """
    Default paginator for single-page scrapes.
    """
    def get_next_url(self, current_response: FetchResult, current_page_number: int) -> Optional[str]:
        return None

    def should_stop(self, current_page_number: int, current_response: FetchResult, num_records: int) -> bool:
        return True

class UrlPatternPaginator(BasePaginator):
    """
    Pagination strategy using URL pattern substitution.
    """
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("base_url", "")
        self.param = self.pagination_config.get("param", "page")
        self.placeholder = f"{{{self.param}}}"

    def get_next_url(self, current_response: FetchResult, current_page_number: int) -> Optional[str]:
        next_page = current_page_number + 1
        # If we started at 0, current_page_number 1 means we just finished page 0.
        # But the orchestrator starts current_page at 1 and increments.
        # Let's adjust the logic: current_page_number is the count of pages fetched.
        
        # Actually, let's use the start_page from config.
        target_page = self.start_page + current_page_number
        
        if target_page >= self.start_page + self.max_pages:
            return None
            
        return self.base_url.replace(self.placeholder, str(target_page))

class NextButtonPaginator(BasePaginator):
    """
    Pagination strategy that follows a "next page" link in the HTML.
    """
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.selector = self.pagination_config.get("selector")
        self.visited_urls: Set[str] = set()

    def get_next_url(self, current_response: FetchResult, current_page_number: int) -> Optional[str]:
        if not self.selector:
            return None
            
        # Add current URL to visited to detect cycles
        self.visited_urls.add(current_response.url)
        
        sel = Selector(text=current_response.body)
        
        # Extract the href. We support both CSS and XPath via parsel.
        # If the user provided a selector like "a::attr(href)", parsel handles it.
        # If they just provided "a.next", we might need to be smarter,
        # but the story says "li.next > a::attr(href)".
        try:
            next_href = sel.css(self.selector).get()
        except Exception:
            next_href = None
            
        if not next_href:
            try:
                next_href = sel.xpath(self.selector).get()
            except Exception:
                next_href = None
        
        if not next_href:
            return None
            
        # Resolve relative URL
        next_url = urljoin(current_response.url, next_href)
        
        # Cycle detection (Story 4.2 AC 6)
        if next_url in self.visited_urls:
            logger.warning(f"Pagination cycle detected: {next_url} already visited. Stopping.")
            return None
            
        return next_url

class ScrollPaginator(BasePaginator):
    """
    Pagination strategy that scrolls to the bottom of the page to load more content.
    Note: This paginator doesn't return next URLs; it signals the fetcher to scroll.
    """
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.scroll_wait_seconds = self.pagination_config.get("scroll_wait_seconds", 2.0)
        self.container_selector = config.get("selectors", {}).get("container")

    def get_next_url(self, current_response: FetchResult, current_page_number: int) -> Optional[str]:
        # Scroll pagination happens within a single page load in our current architecture
        return None

    def should_stop(self, current_page_number: int, current_response: FetchResult, num_records: int) -> bool:
        # For scroll pagination, 'current_page_number' represents scroll iterations
        if current_page_number >= self.max_pages:
            return True
        return False

class PaginatorFactory:
    """
    Factory to create the appropriate paginator based on configuration.
    """
    @staticmethod
    def get_paginator(config: Dict[str, Any]) -> BasePaginator:
        pagination = config.get("pagination")
        if not pagination:
            return NoPaginator(config)
            
        pag_type = pagination.get("type")
        if pag_type == "url_pattern":
            return UrlPatternPaginator(config)
        elif pag_type == "next_button":
            return NextButtonPaginator(config)
        elif pag_type == "scroll":
            return ScrollPaginator(config)
        
        # Fallback or other types (to be implemented)
        return NoPaginator(config)
