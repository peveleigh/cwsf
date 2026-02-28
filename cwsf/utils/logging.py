import logging
import os
import sys
from typing import Optional

def setup_logging(
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    format_str: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
) -> None:
    """
    Configures centralized logging for the framework.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). 
               Defaults to CWSF_LOG_LEVEL env var or INFO.
        log_file: Optional path to a log file.
        format_str: Format string for log messages.
    """
    if level is None:
        level = os.environ.get("CWSF_LOG_LEVEL", "INFO").upper()
    
    numeric_level = getattr(logging, level, logging.INFO)
    
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        try:
            # Ensure directory exists
            log_dir = os.path.dirname(log_file)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
            handlers.append(logging.FileHandler(log_file))
        except Exception as e:
            # Acceptance Criteria 6: Log warning to console and continue with console-only
            print(f"WARNING: Failed to setup file logging at {log_file}: {e}. Falling back to console-only logging.", file=sys.stderr)

    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format=format_str,
        handlers=handlers,
        force=True  # Override any existing configuration
    )

    logging.getLogger("cwsf").debug(f"Logging initialized at level {level}")
