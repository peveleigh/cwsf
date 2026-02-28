import logging
from typing import Any, Optional, Union, List, Dict
from parsel import Selector
from cwsf.engine.transforms import apply_transforms

logger = logging.getLogger(__name__)

class ParseError(Exception):
    """Raised when parsing fails."""
    pass

def parse_field(html_or_selector: Union[str, Selector], selector: str, selector_type: str = "css") -> Any:
    """Extract field values from HTML using CSS or XPath selectors.
    
    Args:
        html_or_selector: The HTML string or a parsel.Selector object to parse.
        selector: The CSS or XPath selector string.
        selector_type: The type of selector ("css" or "xpath"). Defaults to "css".
        
    Returns:
        The extracted value(s). Returns a single value if one match is found,
        a list of values if multiple matches are found, or None if no matches.
        
    Raises:
        ParseError: If the selector type is unknown or the selector is invalid.
    """
    if isinstance(html_or_selector, str):
        sel = Selector(text=html_or_selector)
    else:
        sel = html_or_selector

    try:
        if selector_type == "css":
            results = sel.css(selector).getall()
        elif selector_type == "xpath":
            results = sel.xpath(selector).getall()
        else:
            raise ParseError(f"Unknown selector type: {selector_type}")
    except Exception as e:
        if isinstance(e, ParseError):
            raise
        raise ParseError(f"Invalid {selector_type} selector '{selector}': {str(e)}")

    if not results:
        return None
    
    return results[0] if len(results) == 1 else results

def parse_records(html: str, selectors_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract multiple records from HTML based on a container selector.
    
    Args:
        html: The HTML string to parse.
        selectors_config: The 'selectors' section of the config, containing
                         'container' and 'fields'.
                         
    Returns:
        A list of dictionaries, where each dictionary represents one record.
        If no container is specified, extracts a single record from the full page.
    """
    sel = Selector(text=html)
    container_selector = selectors_config.get("container")
    fields_config = selectors_config.get("fields", {})

    if not container_selector:
        # Single record from full page
        record = {}
        for field_name, field_cfg in fields_config.items():
            val = parse_field(
                sel,
                field_cfg["selector"],
                field_cfg.get("type", "css")
            )
            record[field_name] = apply_transforms(val, field_cfg)
        return [record]

    # Multiple records from containers
    containers = sel.css(container_selector) if not container_selector.startswith("/") else sel.xpath(container_selector)
    
    if not containers:
        logger.warning(f"Container selector '{container_selector}' matched 0 elements.")
        return []

    records = []
    for container in containers:
        record = {}
        for field_name, field_cfg in fields_config.items():
            val = parse_field(
                container,
                field_cfg["selector"],
                field_cfg.get("type", "css")
            )
            record[field_name] = apply_transforms(val, field_cfg)
        records.append(record)
    
    return records
