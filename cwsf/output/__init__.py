from typing import Dict, Any, Type
from .base import BaseWriter
from .sqlite_writer import SqliteWriter
from .webhook_writer import WebhookWriter

class UnsupportedFormatError(Exception):
    """Raised when an unsupported output format is requested."""
    pass

# Registry of available writers
_WRITERS: Dict[str, Type[BaseWriter]] = {
    "sqlite": SqliteWriter,
    "webhook": WebhookWriter
}

def register_writer(format_name: str, writer_class: Type[BaseWriter]) -> None:
    """
    Registers a new output writer class for a specific format.
    
    Args:
        format_name: The name of the format (e.g., 'json', 'csv').
        writer_class: The BaseWriter subclass to register.
    """
    _WRITERS[format_name] = writer_class

def get_writer(config: Dict[str, Any]) -> BaseWriter:
    """
    Factory function to get the appropriate writer based on config.
    
    Args:
        config: The site configuration dictionary.
        
    Returns:
        BaseWriter: An instance of the requested writer.
        
    Raises:
        UnsupportedFormatError: If the requested format is not registered.
    """
    output_config = config.get("output", {})
    fmt = output_config.get("format", "sqlite")
    
    if fmt not in _WRITERS:
        raise UnsupportedFormatError(f"Unsupported output format: {fmt}")
        
    return _WRITERS[fmt]()

__all__ = ["BaseWriter", "SqliteWriter", "WebhookWriter", "get_writer", "register_writer", "UnsupportedFormatError"]
