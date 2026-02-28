"""Configuration loader module for CWSF.

This module provides functionality to load and parse YAML configuration files
for the Configurable Web Scraping Framework.
"""

import os
import copy
import logging
from typing import Dict, Any, List, Optional

import yaml

from cwsf.config.schema import DEFAULT_CONFIG
from cwsf.config.validator import validate_config


logger = logging.getLogger(__name__)


class ConfigParseError(Exception):
    """Exception raised when a configuration file cannot be parsed.
    
    Attributes:
        message: Human-readable error message
        file_path: Path to the configuration file that caused the error
    """
    
    def __init__(self, message: str, file_path: str = None):
        self.message = message
        self.file_path = file_path
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format the error message with file path if available."""
        if self.file_path:
            return f"{self.message} (file: {self.file_path})"
        return self.message


def load_config(file_path: str) -> Dict[str, Any]:
    """Load and parse a YAML configuration file.
    
    Args:
        file_path: Path to the YAML configuration file.
        
    Returns:
        A dictionary representing the parsed configuration contents.
        
    Raises:
        ConfigParseError: If the file cannot be read, is empty, or contains invalid YAML.
        FileNotFoundError: If the specified file does not exist.
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    # Check if file is empty
    if os.path.getsize(file_path) == 0:
        raise ConfigParseError("Configuration file is empty", file_path)
    
    # Read and parse YAML
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Check for empty content after reading (handles whitespace-only files)
        if not content.strip():
            raise ConfigParseError("Configuration file is empty", file_path)
        
        # Use safe_load to prevent arbitrary code execution (NFR-6)
        config = yaml.safe_load(content)
        
        # Handle case where YAML parses to None (e.g., file with only comments)
        if config is None:
            raise ConfigParseError("Configuration file is empty", file_path)
        
        return apply_defaults(config)
        
    except yaml.YAMLError as e:
        raise ConfigParseError(f"Invalid YAML syntax: {str(e)}", file_path)
    except PermissionError:
        raise
    except IOError as e:
        raise ConfigParseError(f"Error reading file: {str(e)}", file_path)


def apply_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    """Apply default values to a configuration dictionary.
    
    Args:
        config: The configuration dictionary to process.
        
    Returns:
        A new dictionary with default values applied for missing optional fields.
    """
    # Deep copy to avoid modifying the original
    resolved = copy.deepcopy(config)
    
    # Apply top-level defaults
    for key, value in DEFAULT_CONFIG.items():
        if key not in resolved:
            resolved[key] = copy.deepcopy(value)
        elif isinstance(value, dict) and isinstance(resolved[key], dict):
            # Apply nested defaults for dictionaries
            for sub_key, sub_value in value.items():
                if sub_key not in resolved[key]:
                    resolved[key][sub_key] = copy.deepcopy(sub_value)
                    
    return resolved


def apply_overrides(config: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Apply CLI overrides to a loaded configuration dictionary.

    Args:
        config:    A configuration dict that has already had defaults applied.
        overrides: A flat dict of top-level keys to override (e.g., {"base_url": "https://..."}).

    Returns:
        A new dict with the specified keys replaced. The original is not mutated.
    """
    result = copy.deepcopy(config)
    for key, value in overrides.items():
        if value is not None:          # only apply if the caller actually supplied a value
            result[key] = value
    return result


def scan_config_directory(
    directory_path: str = "./configs",
    overrides: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Scan a directory for YAML configuration files and load them.
    
    Args:
        directory_path: Path to the directory to scan. Defaults to "./configs".
        overrides: Optional dict of top-level config keys to override after loading
                   (e.g., {"base_url": "https://staging.example.com"}). Applied before
                   validation so the override URL is subject to the same URI format check.
        
    Returns:
        A list of validated configuration dictionaries.
        
    Note:
        - Only .yaml and .yml files are processed.
        - Subdirectories are not scanned.
        - Invalid configs are skipped and a warning is logged.
        - Non-YAML files are silently ignored.
    """
    if not os.path.exists(directory_path):
        logger.info(f"Config directory '{directory_path}' not found; created.")
        os.makedirs(directory_path, exist_ok=True)
        logger.info(f"Startup scan complete: 0 configs loaded, 0 skipped due to errors.")
        return []

    if not os.path.isdir(directory_path):
        logger.warning(f"Config path '{directory_path}' is not a directory.")
        return []

    loaded_configs = []
    skipped_count = 0
    
    # List files in the directory (non-recursive)
    try:
        all_entries = os.listdir(directory_path)
        files = []
        for f in all_entries:
            full_path = os.path.join(directory_path, f)
            # AC 6: Handle symlinked YAML files correctly (os.path.isfile follows symlinks)
            # AC 4: If a config file is replaced by a directory, os.path.isfile will be False
            if os.path.isfile(full_path):
                files.append(f)
            elif os.path.isdir(full_path) and (f.endswith(".yaml") or f.endswith(".yml")):
                logger.warning(f"Ignoring directory '{full_path}' which has a YAML extension.")
    except OSError as e:
        logger.error(f"Error accessing config directory '{directory_path}': {e}")
        return []

    # AC 2: If the config directory is empty, log message
    if not files:
        logger.info("No config files found. Waiting for configs...")
        logger.info(f"Startup scan complete: 0 configs loaded, 0 skipped due to errors.")
        return []

    for filename in files:
        if not (filename.endswith(".yaml") or filename.endswith(".yml")):
            continue

        file_path = os.path.join(directory_path, filename)
        
        try:
            # Load the config
            config_dict = load_config(file_path)

            # Apply CLI overrides before validation (design §4.1)
            if overrides:
                config_dict = apply_overrides(config_dict, overrides)

            # Validate the config
            validation_result = validate_config(config_dict)
            
            if validation_result.is_valid:
                loaded_configs.append(config_dict)
            else:
                skipped_count += 1
                error_msgs = "; ".join([f"{e.field_path}: {e.message}" for e in validation_result.errors])
                logger.warning(f"Skipping invalid config '{file_path}': {error_msgs}")
                
        except (ConfigParseError, FileNotFoundError) as e:
            skipped_count += 1
            logger.warning(f"Skipping malformed config '{file_path}': {str(e)}")
        except PermissionError as e:
            # AC 3: Handle unreadable config files (permissions error)
            skipped_count += 1
            logger.warning(f"Skipping unreadable config '{file_path}': Permission denied")
        except Exception as e:
            skipped_count += 1
            logger.error(f"Unexpected error loading config '{file_path}': {str(e)}")

    logger.info(f"Startup scan complete: {len(loaded_configs)} configs loaded, {skipped_count} skipped due to errors.")
    
    return loaded_configs
