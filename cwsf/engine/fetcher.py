from dataclasses import dataclass, field
from typing import Any, Optional, List, Dict
from urllib.parse import urlparse
import httpx
import time
import asyncio
import logging
from datetime import datetime, timezone
from cwsf.engine.rate_limiter import DomainRateLimiter
from cwsf.core.job import FailureContext

try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

@dataclass
class FetchResult:
    url: str
    status_code: int
    body: str
    headers: httpx.Headers
    elapsed_time: float

@dataclass
class ScrapeRecord:
    """A single extracted record with metadata."""
    fields: Dict[str, Any]
    site_name: str
    source_url: str
    timestamp: str  # ISO 8601

@dataclass
class ScrapeResult:
    """Result of a full site scrape."""
    site_name: str
    records: List[ScrapeRecord] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    failure_contexts: List[FailureContext] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)

class FetchError(Exception):
    """Raised when a fetch operation fails."""
    def __init__(self, url: str, reason: str):
        self.url = url
        self.reason = reason
        super().__init__(f"Failed to fetch {url}: {reason}")

# Global state for rate limiting
_domain_limiters: Dict[str, DomainRateLimiter] = {}
_limiters_lock = asyncio.Lock()

# For backward compatibility with tests
_last_request_times = type('MockDict', (dict,), {
    'clear': lambda self: _domain_limiters.clear(),
    'get': lambda self, k, d=None: _domain_limiters[k]._last_request_time if k in _domain_limiters else d,
    '__getitem__': lambda self, k: _domain_limiters[k]._last_request_time,
    '__contains__': lambda self, k: k in _domain_limiters
})()

_domain_semaphores = type('MockDict', (dict,), {
    'clear': lambda self: _domain_limiters.clear(),
    'get': lambda self, k, d=None: _domain_limiters[k]._semaphore if k in _domain_limiters else d,
    '__getitem__': lambda self, k: _domain_limiters[k]._semaphore,
    '__contains__': lambda self, k: k in _domain_limiters
})()

async def _get_domain_limiter(url: str, rate_limit_config: Optional[Dict[str, Any]], retry_config: Optional[Dict[str, Any]]) -> DomainRateLimiter:
    """
    Get or create a DomainRateLimiter for a domain.
    """
    domain = urlparse(url).netloc
    if not domain:
        domain = "default"
        
    async with _limiters_lock:
        if domain not in _domain_limiters:
            rl_cfg = rate_limit_config or {}
            retry_cfg = retry_config or {}
            
            _domain_limiters[domain] = DomainRateLimiter(
                delay_seconds=rl_cfg.get("delay_seconds", 1.0),
                max_concurrent=rl_cfg.get("max_concurrent", 1),
                max_retries=retry_cfg.get("max_retries", 3),
                backoff_factor=retry_cfg.get("backoff_factor", 2.0)
            )
        return _domain_limiters[domain]

async def fetch_playwright(
    url: str,
    playwright_options: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    pagination_config: Optional[Dict[str, Any]] = None,
    selectors: Optional[Dict[str, Any]] = None,
    **kwargs: Any
) -> FetchResult:
    """
    Fetch a web page using Playwright.
    Note: This is called within the limiter.execute wrapper.
    """
    if async_playwright is None:
        raise FetchError(url, "Playwright is not installed. Please install it to use 'renderer: playwright'.")

    options = playwright_options or {}
    wait_until = options.get("wait_until", "load")
    wait_for_selector = options.get("wait_for_selector")
    timeout = options.get("wait_timeout_seconds", 30.0) * 1000  # Playwright uses ms
    on_timeout = options.get("on_timeout", "proceed")
    actions = options.get("actions", [])

    start_time = time.perf_counter()
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=headers.get("User-Agent", "CWSF/1.0") if headers else "CWSF/1.0",
                extra_http_headers=headers or {}
            )
            
            if cookies:
                formatted_cookies = [
                    {"name": k, "value": v, "url": url} for k, v in cookies.items()
                ]
                await context.add_cookies(formatted_cookies)

            page = await context.new_page()
            
            try:
                response = await page.goto(url, wait_until=wait_until, timeout=timeout)
            except Exception as e:
                if on_timeout == "fail":
                    raise FetchError(url, f"Navigation timeout: {str(e)}")
                logger.warning(f"Navigation timeout for {url}, proceeding anyway: {e}")
                response = None
            
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=timeout)
                except Exception as e:
                    if on_timeout == "fail":
                        raise FetchError(url, f"Wait for selector '{wait_for_selector}' timeout: {str(e)}")
                    logger.warning(f"Wait for selector '{wait_for_selector}' timeout for {url}, proceeding anyway: {e}")

            for action_cfg in actions:
                action_type = action_cfg.get("action")
                selector = action_cfg.get("selector")
                try:
                    if action_type == "click":
                        await page.click(selector, timeout=timeout)
                    elif action_type == "wait":
                        await asyncio.sleep(action_cfg.get("seconds", 0))
                    elif action_type == "fill":
                        await page.fill(selector, action_cfg.get("value", ""), timeout=timeout)
                    elif action_type == "press":
                        await page.press(selector, action_cfg.get("key", ""), timeout=timeout)
                    elif action_type == "hover":
                        await page.hover(selector, timeout=timeout)
                except Exception as e:
                    if on_timeout == "fail":
                        raise FetchError(url, f"Action '{action_type}' failed: {str(e)}")
                    logger.warning(f"Action '{action_type}' failed for {url}, proceeding anyway: {e}")

            if pagination_config and pagination_config.get("type") == "scroll":
                max_scrolls = pagination_config.get("max_pages", 10)
                scroll_wait = pagination_config.get("scroll_wait_seconds", 2.0)
                container_selector = (selectors or {}).get("container")
                
                last_count = 0
                if container_selector:
                    last_count = await page.locator(container_selector).count()
                
                for i in range(max_scrolls):
                    logger.info(f"Scrolling iteration {i+1}/{max_scrolls} for {url}")
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(scroll_wait)
                    
                    if container_selector:
                        new_count = await page.locator(container_selector).count()
                        if new_count <= last_count:
                            logger.info(f"No new content detected after scroll {i+1}. Stopping.")
                            break
                        last_count = new_count
            
            body = await page.content()
            status = response.status if response else 200
            resp_headers = await response.all_headers() if response else {}
            
            await browser.close()
            
            elapsed_time = time.perf_counter() - start_time
            
            return FetchResult(
                url=url,
                status_code=status,
                body=body,
                headers=httpx.Headers(resp_headers),
                elapsed_time=elapsed_time
            )
    except Exception as exc:
        if isinstance(exc, FetchError):
            raise
        raise FetchError(url, f"Playwright error: {str(exc)}") from exc

async def fetch(
    url: str,
    method: str = "GET",
    headers: Optional[dict[str, str]] = None,
    cookies: Optional[dict[str, str]] = None,
    timeout: float = 30.0,
    client: Optional[httpx.AsyncClient] = None,
    renderer: str = "httpx",
    playwright_options: Optional[Dict[str, Any]] = None,
    pagination_config: Optional[Dict[str, Any]] = None,
    selectors: Optional[Dict[str, Any]] = None,
    rate_limit_config: Optional[Dict[str, Any]] = None,
    retry_config: Optional[Dict[str, Any]] = None,
    site_name: str = "unknown",
    scrape_result: Optional[ScrapeResult] = None,
    **kwargs: Any
) -> FetchResult:
    """
    Fetch a web page using the specified renderer, with rate limiting and retries.
    """
    limiter = await _get_domain_limiter(url, rate_limit_config, retry_config)

    async def _do_fetch():
        if renderer == "playwright":
            return await fetch_playwright(
                url=url,
                playwright_options=playwright_options,
                headers=headers,
                cookies=cookies,
                pagination_config=pagination_config,
                selectors=selectors,
                **kwargs
            )
        else:
            default_headers = {"User-Agent": "CWSF/1.0"}
            if headers:
                default_headers.update(headers)

            try:
                start_time = time.perf_counter()
                if client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=default_headers,
                        cookies=cookies,
                        **kwargs
                    )
                else:
                    async with httpx.AsyncClient(
                        timeout=timeout,
                        headers=default_headers,
                        cookies=cookies
                    ) as client_new:
                        response = await client_new.request(
                            method=method,
                            url=url,
                            **kwargs
                        )
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                raise FetchError(url, str(exc)) from exc
            
            elapsed_time = time.perf_counter() - start_time
            
            return FetchResult(
                url=str(response.url),
                status_code=response.status_code,
                body=response.text,
                headers=response.headers,
                elapsed_time=elapsed_time
            )

    try:
        res = await limiter.execute(url, _do_fetch, site_name=site_name)
        if res.status_code >= 400 and scrape_result is not None:
            context = FailureContext(
                site_name=site_name,
                url=url,
                http_status=res.status_code,
                error_type="HTTPError",
                error_message=f"HTTP {res.status_code} error",
                retries_attempted=limiter.max_retries,
                timestamp=datetime.now(timezone.utc).isoformat()
            )
            scrape_result.failure_contexts.append(context)
        return res
    except Exception as exc:
        if scrape_result is not None:
            status_code = None
            if isinstance(exc, FetchError) and hasattr(exc, 'status_code'):
                status_code = getattr(exc, 'status_code')
            
            context = FailureContext(
                site_name=site_name,
                url=url,
                http_status=status_code,
                error_type=type(exc).__name__,
                error_message=str(exc),
                retries_attempted=limiter.max_retries,
                timestamp=datetime.now(timezone.utc).isoformat()
            )
            scrape_result.failure_contexts.append(context)
        raise

async def perform_login(client: httpx.AsyncClient, auth_config: Dict[str, Any]) -> None:
    """
    Perform login and update the client with session credentials.
    """
    login_url = auth_config.get("login_url")
    method = auth_config.get("method", "POST")
    payload = auth_config.get("payload", {})
    token_from = auth_config.get("token_from")

    if not login_url:
        return

    try:
        res = await fetch(
            url=login_url,
            method=method,
            client=client,
            json=payload if method == "POST" else None,
            params=payload if method == "GET" else None
        )

        if res.status_code >= 400:
            logger.error(f"Login failed with status {res.status_code} for {login_url}")
            return

        if token_from:
            tf_type = token_from.get("type")
            tf_name = token_from.get("name")
            
            token_value = None
            if tf_type == "header":
                token_value = res.headers.get(tf_name)
            elif tf_type == "cookie":
                token_value = client.cookies.get(tf_name)
            elif tf_type == "body_json":
                import json
                try:
                    data = json.loads(res.body)
                    token_value = data.get(tf_name)
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON body for token extraction from {login_url}")
            elif tf_type == "body_selector":
                from cwsf.engine.parser import parse_field
                token_value = parse_field(res.body, token_from.get("selector"), token_from.get("selector_type", "css"))

            if token_value:
                if tf_type in ["body_json", "body_selector", "header"]:
                    client.headers["Authorization"] = f"Bearer {token_value}" if "token" in tf_name.lower() else token_value
                logger.info(f"Successfully extracted token from {tf_type} '{tf_name or ''}'")

    except Exception as exc:
        logger.error(f"Error during login to {login_url}: {exc}")

async def run_all(
    configs: List[Dict[str, Any]],
    max_concurrency: int = 5
) -> List[ScrapeResult]:
    """
    Fetch pages from multiple site configs concurrently.
    """
    semaphore = asyncio.Semaphore(max_concurrency)

    async def _fetch_task(config: Dict[str, Any]) -> ScrapeResult:
        site_name = config.get("site_name", "unknown")
        url = config.get("base_url")
        method = config.get("method", "GET")
        headers = config.get("headers")
        cookies = config.get("cookies")
        renderer = config.get("renderer", "httpx")
        playwright_options = config.get("playwright_options")
        rate_limit_config = config.get("rate_limit")
        retry_config = config.get("retry")
        
        result = ScrapeResult(site_name=site_name)
        
        if not url:
            result.errors.append("Missing base_url in config")
            return result

        async with semaphore:
            try:
                fetch_res = await fetch(
                    url=url,
                    method=method,
                    headers=headers,
                    cookies=cookies,
                    renderer=renderer,
                    playwright_options=playwright_options,
                    rate_limit_config=rate_limit_config,
                    retry_config=retry_config,
                    site_name=site_name,
                    scrape_result=result
                )
                result.stats["status_code"] = fetch_res.status_code
                result.stats["elapsed_time"] = fetch_res.elapsed_time
                logger.info(f"Successfully fetched {url} for site {site_name}")
            except FetchError as exc:
                result.errors.append(str(exc))
                logger.error(f"Failed to fetch {url} for site {site_name}: {exc}")
            except Exception as exc:
                result.errors.append(f"Unexpected error during fetch: {exc}")
                logger.error(f"Unexpected error fetching {url} for site {site_name}: {exc}")
        
        return result

    tasks = [_fetch_task(config) for config in configs]
    return await asyncio.gather(*tasks)
