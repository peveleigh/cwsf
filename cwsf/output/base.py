from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Dict, Any


class BaseWriter(ABC):
    """
    Abstract base class for output writers.
    
    This class defines the interface for all output writers in the CWSF framework.
    It also provides a common method for enriching records with metadata.
    """

    @abstractmethod
    def open(self, config: Dict[str, Any]) -> None:
        """
        Initialize the writer based on the site config's output section.
        
        Args:
            config: The site configuration dictionary.
        """
        pass

    @abstractmethod
    def write_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Write a batch of scraped records.
        
        Args:
            records: A list of dictionaries containing the scraped data.
            
        Returns:
            int: The count of records successfully written.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Flush and release any resources (connections, file handles).
        """
        pass

    def write_metadata(self, records: List[Dict[str, Any]], site_name: str, source_url: str) -> int:
        """
        Enriches records with metadata and delegates to write_records.
        
        Args:
            records: A list of dictionaries containing the scraped data.
            site_name: The name of the site being scraped.
            source_url: The URL from which the records were extracted.
            
        Returns:
            int: The count of records successfully written.
        """
        scrape_timestamp = datetime.now(timezone.utc).isoformat()
        
        enriched_records = []
        for record in records:
            enriched_record = record.copy()
            enriched_record["site_name"] = site_name
            enriched_record["scrape_timestamp"] = scrape_timestamp
            enriched_record["source_url"] = source_url
            enriched_records.append(enriched_record)
            
        return self.write_records(enriched_records)
