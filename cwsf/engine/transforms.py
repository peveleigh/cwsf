import logging
import re
from typing import Any, Callable, Dict, Optional, Union, List

logger = logging.getLogger(__name__)

def regex_transform(value: Any, config: Optional[Dict[str, Any]] = None) -> Any:
    """Apply a regex extraction pattern to a value.
    
    Args:
        value: The value to transform.
        config: Configuration containing 'transform_pattern'.
        
    Returns:
        The first capture group if it exists, else the full match.
        Returns None if no match is found or value is None.
    """
    if value is None:
        return None
    
    if not config or "transform_pattern" not in config:
        logger.warning("Regex transform called without 'transform_pattern'")
        return value

    pattern = config["transform_pattern"]
    
    def _apply_regex(val: Any) -> Any:
        if not isinstance(val, str):
            return val
        
        match = re.search(pattern, val)
        if match:
            # Return first capture group if it exists, else the whole match
            return match.group(1) if match.groups() else match.group(0)
        
        logger.debug(f"Regex pattern '{pattern}' did not match value: {val}")
        return None

    if isinstance(value, list):
        return [_apply_regex(v) for v in value]
    
    return _apply_regex(value)

def cast_transform(value: Any, config: Optional[Dict[str, Any]] = None) -> Any:
    """Cast a value to a specified type (int, float, bool, str).
    
    Args:
        value: The value to transform.
        config: Configuration containing 'cast_type'.
        
    Returns:
        The casted value, or None if casting fails.
        Returns None if value is None.
    """
    if value is None:
        return None
    
    if not config or "cast_type" not in config:
        logger.warning("Cast transform called without 'cast_type'")
        return value

    cast_type = config["cast_type"]
    
    def _apply_cast(val: Any) -> Any:
        if val is None:
            return None
            
        try:
            if cast_type == "int":
                return int(val)
            elif cast_type == "float":
                return float(val)
            elif cast_type == "bool":
                if isinstance(val, str):
                    return val.lower() in ("true", "1", "yes", "on")
                return bool(val)
            elif cast_type == "str":
                return str(val)
            else:
                logger.warning(f"Unsupported cast type: {cast_type}")
                return val
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to cast value '{val}' to {cast_type}: {str(e)}")
            return None

    if isinstance(value, list):
        return [_apply_cast(v) for v in value]
    
    return _apply_cast(value)

def strip_transform(value: Any, config: Optional[Dict[str, Any]] = None) -> Any:
    """Strip leading/trailing whitespace from a string value.
    
    Args:
        value: The value to transform.
        config: Optional configuration for the transform (not used for strip).
        
    Returns:
        The stripped string if value is a string, otherwise the original value.
        Returns None if value is None.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return [strip_transform(v, config) for v in value]
    return value

def default_transform(value: Any, config: Optional[Dict[str, Any]] = None) -> Any:
    """Apply a default value if the current value is None or an empty string.
    
    Args:
        value: The value to transform.
        config: Configuration containing 'default'.
        
    Returns:
        The default value if value is None or "", otherwise the original value.
    """
    if not config or "default" not in config:
        return value
    
    default_val = config["default"]
    
    def _apply_default(val: Any) -> Any:
        if val is None or val == "":
            return default_val
        return val

    if isinstance(value, list):
        return [_apply_default(v) for v in value]
    
    return _apply_default(value)

# Registry of available transforms
TRANSFORMS: Dict[str, Callable[[Any, Optional[Dict[str, Any]]], Any]] = {
    "strip": strip_transform,
    "regex": regex_transform,
    "cast": cast_transform,
    "default": default_transform,
}

def apply_transforms(value: Any, field_config: Dict[str, Any]) -> Any:
    """Apply configured transforms to a value.
    
    Args:
        value: The extracted value to transform.
        field_config: The configuration for the field, which may contain a 'transform' key.
        
    Returns:
        The transformed value.
    """
    # 1. Apply primary transform if configured
    transform_key = field_config.get("transform")
    if transform_key:
        transform_fn = TRANSFORMS.get(transform_key)
        if not transform_fn:
            logger.warning(f"Unknown transform: {transform_key}")
        else:
            try:
                value = transform_fn(value, field_config)
            except Exception as e:
                logger.error(f"Error applying transform '{transform_key}': {str(e)}")

    # 2. Apply default value if configured (always runs last)
    if "default" in field_config:
        value = default_transform(value, field_config)

    return value
