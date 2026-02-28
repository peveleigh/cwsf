from typing import List, Dict, Any
from .base import BaseWriter


class WebhookWriter(BaseWriter):
    """
    Stub webhook writer for the CWSF framework.
    
    This writer is a placeholder for future implementation (V2).
    It validates the pluggable design and provides a clear extension point.
    """

    def __init__(self):
        self.url = None
        self.method = None
        self.headers = None

    def open(self, config: Dict[str, Any]) -> None:
        """
        Stores webhook configuration from the site config.
        
        Args:
            config: The site configuration dictionary.
        """
        output_config = config.get("output", {})
        self.url = output_config.get("url")
        self.method = output_config.get("method", "POST")
        self.headers = output_config.get("headers", {})

    def write_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Raises NotImplementedError as this writer is a stub.
        
        Args:
            records: A list of dictionaries containing the scraped data.
            
        Raises:
            NotImplementedError: Always raised with a message indicating V2 implementation.
        """
        raise NotImplementedError("Webhook writer is not yet implemented. Target: V2.")

    def close(self) -> None:
        """
        No-op for the stub webhook writer.
        """
        pass
