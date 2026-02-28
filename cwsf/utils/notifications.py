"""Gotify notification utility for CWSF.

This module provides the GotifyNotifier class for sending push notifications
to a Gotify server when scraping errors occur or when a run completes.
"""

import logging
import asyncio
from typing import Dict, Any, Optional
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

@dataclass
class FailureContext:
    """Context for a scraping failure."""
    site_name: str
    url: str
    error_message: str
    http_status: Optional[int] = None
    error_type: Optional[str] = None
    retries_attempted: int = 0
    timestamp: Optional[str] = None

@dataclass
class RunSummary:
    """Summary of a scraping run."""
    total_sites: int
    sites_succeeded: int
    sites_failed: int
    total_records: int
    total_errors: int
    duration_seconds: float
    failed_sites: Dict[str, str]  # site_name -> error_description

class GotifyNotifier:
    """Sends notifications to a Gotify server."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the notifier with Gotify configuration.

        Args:
            config: Gotify configuration dictionary containing 'server_url',
                   'app_token', and optionally 'priority'.
        """
        self.config = config or {}
        self.server_url = self.config.get("server_url")
        self.app_token = self.config.get("app_token")
        self.priority = self.config.get("priority", 5)
        self.enabled = bool(self.server_url and self.app_token)

    async def _send_notification(self, title: str, message: str, priority: Optional[int] = None) -> bool:
        """Send a notification to Gotify.

        Args:
            title: Notification title.
            message: Notification message body.
            priority: Optional priority override.

        Returns:
            True if successful, False otherwise.
        """
        if not self.enabled:
            return False

        url = f"{self.server_url.rstrip('/')}/message"
        headers = {
            "X-Gotify-Key": self.app_token
        }
        payload = {
            "title": title,
            "message": message,
            "priority": priority if priority is not None else self.priority
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                return True
        except httpx.HTTPStatusError as e:
            logger.warning(f"Gotify server returned error: {e.response.status_code} {e.response.text}")
        except httpx.RequestError as e:
            logger.warning(f"Could not reach Gotify server: {str(e)}")
        except Exception as e:
            logger.warning(f"Unexpected error sending Gotify notification: {str(e)}")
        
        return False

    async def send_error(self, failure: FailureContext) -> bool:
        """Send an error notification for a failed scraping job.

        Args:
            failure: FailureContext object containing error details.

        Returns:
            True if successful, False otherwise.
        """
        title = f"CWSF Scrape Error: {failure.site_name}"
        message = (
            f"Site: {failure.site_name}\n"
            f"URL: {failure.url}\n"
            f"Error: {failure.error_message}\n"
        )
        if failure.http_status:
            message += f"Status: {failure.http_status}\n"
        if failure.retries_attempted > 0:
            message += f"Retries: {failure.retries_attempted}\n"

        return await self._send_notification(title, message)

    async def send_summary(self, summary: RunSummary) -> bool:
        """Send a summary notification for a completed scraping run.

        Args:
            summary: RunSummary object containing run results.

        Returns:
            True if successful, False otherwise.
        """
        if summary.sites_failed == 0:
            # Story 7.5 AC 7: Only send summary notification if there were failures
            return False

        title = "CWSF Run Summary (Failures Detected)"
        message = (
            f"Sites Attempted: {summary.total_sites}\n"
            f"Sites Succeeded: {summary.sites_succeeded}\n"
            f"Sites Failed: {summary.sites_failed}\n"
            f"Total Records: {summary.total_records}\n"
            f"Duration: {summary.duration_seconds:.1f}s\n\n"
            "Failed Sites:\n"
        )
        for site, error in summary.failed_sites.items():
            message += f"- {site}: {error}\n"

        return await self._send_notification(title, message)
