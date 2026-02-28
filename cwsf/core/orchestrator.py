"""Orchestrator for CWSF.

This module coordinates the configuration discovery, job queue, and scraping engine.
"""

import logging
from cwsf.utils.logging import setup_logging
import os
import asyncio
import time
from typing import Dict, Optional, List, Any

from cwsf.config.watcher import ConfigEvent, ConfigEventType, ConfigWatcher
from cwsf.config.loader import scan_config_directory
from cwsf.core.job import Job, JobStatus
from cwsf.core.queue import PriorityJobQueue
from cwsf.utils.notifications import GotifyNotifier, RunSummary
from cwsf.utils.run_history import RunHistoryStore, RunResult

logger = logging.getLogger(__name__)


class Orchestrator:
    """Coordinates the framework components.
    
    Error Lifecycle:
    1. Job Execution: Each job is wrapped in a try-except block for fault isolation (Story 7.2).
    2. Fetching: The engine handles retries and logs failure context after exhaustion (Story 7.3).
    3. Notifications: Failures trigger immediate Gotify alerts if configured (Story 7.5).
    4. Summary: Results are collected into a RunSummary, logged, and sent to Gotify (Story 7.4, 7.6).
    """

    def __init__(
        self,
        queue: PriorityJobQueue,
        config_dir: str = "./configs",
        gotify_config: Optional[Dict[str, Any]] = None,
        config_overrides: Optional[Dict[str, Any]] = None,   # NEW
    ):
        self.queue = queue
        self.config_dir = config_dir
        self._config_overrides = config_overrides or {}
        # Map file paths to site names to handle removals
        self._file_to_site: Dict[str, str] = {}
        self._watcher: Optional[ConfigWatcher] = None
        self._stop_event = asyncio.Event()
        self.notifier = GotifyNotifier(gotify_config)
        self._results: List[Any] = []
        self.last_run_summary: Optional[RunSummary] = None
        self.run_history = RunHistoryStore()

    def handle_config_event(self, event: ConfigEvent):
        """Handle configuration discovery events and update the job queue.
        
        This implements Story 2.6: Wiring Discovery Events to the Job Queue.
        """
        if event.event_type == ConfigEventType.VALIDATED:
            config = event.config
            if not config:
                logger.error(f"Validated event for {event.file_path} missing config")
                return

            site_name = config.get("site_name")
            if not site_name:
                logger.error(f"Config from {event.file_path} missing site_name")
                return

            # Track which site this file belongs to
            self._file_to_site[event.file_path] = site_name

            # Create or update job
            job = Job(site_name=site_name, config=config)
            
            # PriorityJobQueue.enqueue handles both new and existing (PENDING/RUNNING) jobs
            self.queue.enqueue(job)
            logger.info(f"Queue updated: {site_name} from {event.file_path}")

        elif event.event_type == ConfigEventType.REMOVED:
            site_name = self._file_to_site.pop(event.file_path, None)
            if site_name:
                self.queue.remove(site_name)
                logger.info(f"Queue removed: {site_name} (file {event.file_path} deleted)")
            else:
                logger.debug(f"Removed file {event.file_path} was not associated with any active site")

        elif event.event_type == ConfigEventType.REJECTED:
            # Story 2.6 Criterion 4 & 5:
            # - New file rejected: no job created (already handled by watcher not emitting VALIDATED)
            # - Previously valid file rejected: watcher logs warning and retains last-known-good.
            # Here we just log that the queue remains unchanged for this file.
            if event.file_path in self._file_to_site:
                site_name = self._file_to_site[event.file_path]
                logger.info(f"Config rejected for {event.file_path}; retaining existing job for {site_name}")
            else:
                logger.info(f"Config rejected for new file {event.file_path}; no job created")

    async def run(self, once: bool = False, log_level: Optional[str] = None, log_file: Optional[str] = None, site_name: Optional[str] = None):
        """Run the framework.
        
        Args:
            once: If True, perform a startup scan, process all jobs, and exit.
                  If False, perform a startup scan and start the file watcher.
            log_level: Optional log level to override defaults.
            log_file: Optional log file path.
            site_name: Optional site name to run a single configuration (Story 8.3).
        """
        # Initialize logging (Story 7.1)
        setup_logging(level=log_level, log_file=log_file)

        # Determine mode from environment if not explicitly set
        if not once and os.environ.get("CWSF_WATCH_MODE") == "once":
            once = True
            logger.info("Mode set to 'once' via CWSF_WATCH_MODE environment variable")

        mode_str = "one-shot" if once else "continuous polling"
        if site_name:
            mode_str = f"single-site ({site_name})"
            once = True # Single site run is always one-shot
            
        logger.info(f"Starting CWSF in {mode_str} mode")

        # 1. Startup scan
        configs = scan_config_directory(self.config_dir, overrides=self._config_overrides or None)
        
        if site_name:
            # Story 8.3: Filter for specific site name
            matched_config = next((c for c in configs if c.get("site_name") == site_name), None)
            if not matched_config:
                available_sites = [c.get("site_name") for c in configs if c.get("site_name")]
                error_msg = f"No configuration found for site: {site_name}"
                if available_sites:
                    error_msg += f". Available sites: {', '.join(available_sites)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            configs = [matched_config]

        if not configs and once and self.queue.size() == 0:
            logger.warning("No valid configs discovered in one-shot mode.")
            await self._generate_and_log_summary(0)
            return

        for config in configs:
            site_name_val = config.get("site_name")
            job = Job(site_name=site_name_val, config=config)
            self.queue.enqueue(job)

        if once:
            # 2. Process all jobs and exit
            start_time = time.perf_counter()
            await self._process_queue_until_empty()
            duration = time.perf_counter() - start_time
            
            # 3. Generate and log summary (Story 7.4)
            await self._generate_and_log_summary(duration)
            logger.info("One-shot execution complete.")
        else:
            # 2. Start watcher and run indefinitely
            self._watcher = ConfigWatcher(self.config_dir, self.handle_config_event)
            self._watcher.start()
            
            try:
                await self._run_loop()
            finally:
                self._watcher.stop()

    async def _generate_and_log_summary(self, duration: float):
        """Generate, log, and notify the run summary (Story 7.4, 7.5, 7.6)."""
        total_sites = len(self._results)
        sites_succeeded = sum(1 for r in self._results if not r.errors)
        sites_failed = total_sites - sites_succeeded
        total_records = sum(len(r.records) for r in self._results)
        total_errors = sum(len(r.errors) for r in self._results)
        
        failed_sites = {}
        for r in self._results:
            if r.errors:
                failed_sites[r.site_name] = r.errors[0] # Use first error as summary

        summary = RunSummary(
            total_sites=total_sites,
            sites_succeeded=sites_succeeded,
            sites_failed=sites_failed,
            total_records=total_records,
            total_errors=total_errors,
            duration_seconds=duration,
            failed_sites=failed_sites
        )

        # Format multi-line log message (Story 7.4 AC 1-4)
        summary_lines = [
            "========== CWSF Run Summary ==========",
            f"Duration:        {duration:.1f}s",
            f"Sites Attempted: {total_sites}",
            f"Sites Succeeded: {sites_succeeded}",
            f"Sites Failed:    {sites_failed}",
            f"Total Records:   {total_records}",
            "",
            "Per-Site Results:"
        ]
        
        for r in self._results:
            status_icon = "✗" if r.errors else "✓"
            site_name = r.site_name or "unknown"
            if r.errors:
                summary_lines.append(f"    {status_icon} {site_name:<15} — {r.errors[0]}")
            else:
                summary_lines.append(f"    {status_icon} {site_name:<15} — {len(r.records)} records")
        
        summary_lines.append("========================================")
        
        logger.info("\n".join(summary_lines))
        
        # Send summary notification if failures occurred (Story 7.5 AC 7)
        if sites_failed > 0:
            await self.notifier.send_summary(summary)
        
        self.last_run_summary = summary
        # Clear results for next batch if in continuous mode
        self._results = []

    async def _process_queue_until_empty(self):
        """Process all jobs currently in the queue and exit."""
        while self.queue.size() > 0:
            job = self.queue.dequeue()
            if job:
                await self._execute_job(job)
            await asyncio.sleep(0.1)

    async def _run_loop(self):
        """Main execution loop for continuous mode."""
        last_summary_time = time.perf_counter()
        while not self._stop_event.is_set():
            if self.queue.size() > 0:
                job = self.queue.dequeue()
                if job:
                    await self._execute_job(job)
            
            # In continuous mode, generate summary periodically if we have results
            if self._results and (time.perf_counter() - last_summary_time > 60):
                await self._generate_and_log_summary(time.perf_counter() - last_summary_time)
                last_summary_time = time.perf_counter()
                
            await asyncio.sleep(1.0)

    async def _execute_job(self, job: Job):
        """Execute a single scraping job with error isolation.
        
        This implements Story 7.2: Implement Per-Site Error Isolation (Fault Tolerance).
        """
        logger.info(f"Executing job: {job.site_name}")
        
        # Import engine orchestrator here to avoid circular imports if any
        import cwsf.engine.orchestrator
        
        try:
            # Execute the scrape
            result = await cwsf.engine.orchestrator.scrape_site(job.config)
            self._results.append(result)
            
            status = "success"
            if result.errors:
                status = "partial" if result.records else "failed"
                logger.error(f"Job {job.site_name} completed with {len(result.errors)} errors")
                # Story 7.5: Send notification for each site failure after retry exhaustion
                if result.failure_contexts:
                    for failure in result.failure_contexts:
                        await self.notifier.send_error(failure)
                elif result.errors:
                    # Fallback if failure_contexts not populated but errors exist
                    from cwsf.utils.notifications import FailureContext
                    await self.notifier.send_error(FailureContext(
                        site_name=job.site_name,
                        url=job.config.get("base_url", "unknown"),
                        error_message=result.errors[0]
                    ))
            else:
                logger.info(f"Completed job: {job.site_name} successfully")

            # Record run history (Story 8.7)
            from datetime import datetime, timezone
            self.run_history.record_run(RunResult(
                site_name=job.site_name,
                timestamp=datetime.now(timezone.utc).isoformat(),
                records_count=len(result.records),
                status=status,
                error_count=len(result.errors),
                last_error=result.errors[0] if result.errors else None
            ))
                
        except Exception as exc:
            # Story 7.2 AC 1, 2, 3: Catch unhandled exceptions in any phase
            # AC 4: Include site name, phase (if known), and exception details
            error_msg = f"Critical failure in job {job.site_name}: {type(exc).__name__}: {exc}"
            logger.error(error_msg, exc_info=True)
            
            # Record failure for summary
            from cwsf.engine.fetcher import ScrapeResult
            fail_result = ScrapeResult(site_name=job.site_name)
            fail_result.errors.append(error_msg)
            self._results.append(fail_result)
            
            # Notify of critical failure
            from cwsf.utils.notifications import FailureContext
            await self.notifier.send_error(FailureContext(
                site_name=job.site_name,
                url=job.config.get("base_url", "unknown"),
                error_message=error_msg,
                error_type=type(exc).__name__
            ))

            # Record run history for critical failure (Story 8.7)
            from datetime import datetime, timezone
            self.run_history.record_run(RunResult(
                site_name=job.site_name,
                timestamp=datetime.now(timezone.utc).isoformat(),
                records_count=0,
                status="failed",
                error_count=1,
                last_error=str(exc)
            ))

    def stop(self):
        """Stop the orchestrator."""
        self._stop_event.set()
