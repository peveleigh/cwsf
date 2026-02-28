"""File-system watcher for configuration changes.

This module implements a watcher that monitors the config directory for
new, modified, or deleted YAML configuration files.
"""

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional

from cwsf.config.loader import load_config, ConfigParseError
from cwsf.config.validator import validate_config, ValidationError
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

logger = logging.getLogger(__name__)


class ConfigEventType(Enum):
    """Types of configuration change events."""
    ADDED = auto()
    MODIFIED = auto()
    REMOVED = auto()
    VALIDATED = auto()
    REJECTED = auto()


@dataclass
class ConfigEvent:
    """Represents a configuration change event."""
    event_type: ConfigEventType
    file_path: str
    config: Optional[Dict[str, Any]] = None
    errors: List[ValidationError] = field(default_factory=list)


class ConfigWatcherHandler(FileSystemEventHandler):
    """Handles file system events for configuration files."""

    def __init__(
        self,
        callback: Callable[[ConfigEvent], None],
        debounce_seconds: float = 2.0
    ):
        self.callback = callback
        self.debounce_seconds = debounce_seconds
        self._pending_events: Dict[str, ConfigEvent] = {}
        self._timers: Dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def _is_config_file(self, path: str) -> bool:
        """Check if the file is a valid YAML configuration file."""
        filename = os.path.basename(path)
        # Ignore temporary/editor swap files
        if filename.startswith('.') or filename.endswith('~') or filename.endswith('.tmp'):
            return False
        return filename.endswith(('.yaml', '.yml'))

    def _emit_event(self, file_path: str):
        """Emit the pending event for the given file path."""
        with self._lock:
            event = self._pending_events.pop(file_path, None)
            self._timers.pop(file_path, None)
            
        if event:
            logger.info(f"Config {event.event_type.name.lower()}: {event.file_path}")
            self.callback(event)

    def _schedule_event(self, event: ConfigEvent):
        """Schedule an event with debouncing."""
        with self._lock:
            # Cancel existing timer if any
            if event.file_path in self._timers:
                self._timers[event.file_path].cancel()
            
            self._pending_events[event.file_path] = event
            timer = threading.Timer(
                self.debounce_seconds,
                self._emit_event,
                args=[event.file_path]
            )
            self._timers[event.file_path] = timer
            timer.start()

    def on_created(self, event):
        if not event.is_directory and self._is_config_file(event.src_path):
            self._schedule_event(ConfigEvent(ConfigEventType.ADDED, event.src_path))

    def on_modified(self, event):
        if not event.is_directory and self._is_config_file(event.src_path):
            self._schedule_event(ConfigEvent(ConfigEventType.MODIFIED, event.src_path))

    def on_deleted(self, event):
        if not event.is_directory and self._is_config_file(event.src_path):
            # Deletion doesn't necessarily need long debouncing, but we keep it consistent
            self._schedule_event(ConfigEvent(ConfigEventType.REMOVED, event.src_path))

    def on_moved(self, event):
        # Handle move as delete from src and add to dest
        if not event.is_directory:
            if self._is_config_file(event.src_path):
                self._schedule_event(ConfigEvent(ConfigEventType.REMOVED, event.src_path))
            if self._is_config_file(event.dest_path):
                self._schedule_event(ConfigEvent(ConfigEventType.ADDED, event.dest_path))


class ConfigWatcher:
    """Monitors a directory for configuration file changes."""

    def __init__(
        self,
        directory_path: str,
        callback: Callable[[ConfigEvent], None],
        polling_interval: float = 5.0,
        debounce_seconds: float = 2.0,
        use_polling: bool = False
    ):
        self.directory_path = os.path.abspath(directory_path)
        self.callback = callback
        self.polling_interval = polling_interval
        self.debounce_seconds = debounce_seconds
        self._last_known_good: Dict[str, Dict[str, Any]] = {}
        
        self.handler = ConfigWatcherHandler(self._handle_raw_event, debounce_seconds)
        
        if use_polling:
            self.observer = PollingObserver(timeout=polling_interval)
        else:
            self.observer = Observer(timeout=polling_interval)
            
        self.observer.schedule(self.handler, self.directory_path, recursive=False)

    def _handle_raw_event(self, event: ConfigEvent):
        """Handle raw file system events and perform auto-validation."""
        if event.event_type == ConfigEventType.REMOVED:
            self._last_known_good.pop(event.file_path, None)
            self.callback(event)
            return

        # For ADDED and MODIFIED, perform validation
        try:
            config_dict = load_config(event.file_path)
            validation_result = validate_config(config_dict)

            if validation_result.is_valid:
                self._last_known_good[event.file_path] = config_dict
                validated_event = ConfigEvent(
                    event_type=ConfigEventType.VALIDATED,
                    file_path=event.file_path,
                    config=config_dict
                )
                self.callback(validated_event)
            else:
                error_msgs = "; ".join([f"{e.field_path}: {e.message}" for e in validation_result.errors])
                
                if event.file_path in self._last_known_good:
                    logger.warning(
                        f"Config '{event.file_path}' is now invalid; retaining last-known-good job for site '{self._last_known_good[event.file_path].get('site_name')}'. "
                        f"Errors: {error_msgs}"
                    )
                else:
                    logger.warning(f"Config rejected '{event.file_path}': {error_msgs}")

                rejected_event = ConfigEvent(
                    event_type=ConfigEventType.REJECTED,
                    file_path=event.file_path,
                    errors=validation_result.errors
                )
                self.callback(rejected_event)

        except (ConfigParseError, FileNotFoundError) as e:
            if event.file_path in self._last_known_good:
                logger.warning(
                    f"Config '{event.file_path}' is now malformed; retaining last-known-good job for site '{self._last_known_good[event.file_path].get('site_name')}'. "
                    f"Error: {str(e)}"
                )
            else:
                logger.warning(f"Config rejected '{event.file_path}': {str(e)}")
            
            rejected_event = ConfigEvent(
                event_type=ConfigEventType.REJECTED,
                file_path=event.file_path,
                errors=[ValidationError(field_path="", message=str(e))]
            )
            self.callback(rejected_event)
        except PermissionError as e:
            # AC 3: Handle unreadable config files (permissions error)
            logger.warning(f"Skipping unreadable config '{event.file_path}': Permission denied")
            rejected_event = ConfigEvent(
                event_type=ConfigEventType.REJECTED,
                file_path=event.file_path,
                errors=[ValidationError(field_path="", message="Permission denied")]
            )
            self.callback(rejected_event)
        except Exception as e:
            logger.error(f"Unexpected error validating config '{event.file_path}': {str(e)}")

    def start(self):
        """Start the watcher."""
        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path, exist_ok=True)
        
        self.observer.start()
        logger.info(f"Started config watcher on {self.directory_path} (polling_interval={self.polling_interval}s)")

    def stop(self):
        """Stop the watcher."""
        self.observer.stop()
        self.observer.join()
        logger.info("Stopped config watcher")
