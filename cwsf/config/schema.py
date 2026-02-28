"""JSON Schema definition for CWSF configuration validation.

This module defines the JSON Schema used to validate configuration files
for the Configurable Web Scraping Framework.

Schema Versioning:
------------------
The framework supports multiple schema versions to enable forward compatibility.
Each version has its own JSON Schema definition. When a config is loaded, the
version field is checked first, and the appropriate schema is used for validation.

Supported Versions:
- "1.0": Initial schema version
"""

# Supported schema versions
SUPPORTED_VERSIONS = ["1.0"]

# Allowed field transforms (NFR-6)
ALLOWED_TRANSFORMS = ["strip", "regex", "to_int", "to_float", "default"]

# Mapping of version to JSON Schema
SCHEMAS_BY_VERSION = {
    "1.0": None  # Will be populated with CONFIG_SCHEMA below
}

# Default values for optional configuration sections
DEFAULT_CONFIG = {
    "version": "1.0",
    "method": "GET",
    "headers": {},
    "cookies": {},
    "authentication": {},
    "pagination": {
        "type": "none",
        "start": 1,
        "max_pages": 1
    },
    "output": {
        "format": "sqlite",
        "destination": "./output/",
        "mode": "append"
    },
    "rate_limit": {
        "delay_seconds": 1.0,
        "max_concurrent": 1
    },
    "retry": {
        "max_retries": 3,
        "backoff_factor": 2.0
    },
    "priority": 10,
    "gotify": {
        "server_url": None,
        "app_token": None,
        "priority": 5
    }
}

# JSON Schema for CWSF configuration validation (Version 1.0)
CONFIG_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "CWSF Configuration Schema v1.0",
    "description": "Schema for validating CWSF YAML configuration files (version 1.0)",
    "type": "object",
    "required": ["version", "site_name", "base_url", "method", "selectors"],
    "properties": {
        "renderer": {
            "type": "string",
            "enum": ["httpx", "playwright"],
            "default": "httpx",
            "description": "Engine to use for fetching pages"
        },
        "playwright_options": {
            "type": "object",
            "properties": {
                "wait_until": {
                    "type": "string",
                    "enum": ["load", "domcontentloaded", "networkidle", "commit"],
                    "default": "load",
                    "description": "When to consider navigation finished"
                },
                "wait_for_selector": {
                    "type": "string",
                    "description": "Wait for this selector to appear before extracting HTML"
                },
                "wait_timeout_seconds": {
                    "type": "number",
                    "minimum": 0,
                    "default": 30.0,
                    "description": "Maximum time to wait for conditions"
                },
                "on_timeout": {
                    "type": "string",
                    "enum": ["proceed", "fail"],
                    "default": "proceed",
                    "description": "Action to take if a wait condition times out"
                },
                "actions": {
                    "type": "array",
                    "description": "List of actions to perform before extraction",
                    "items": {
                        "type": "object",
                        "required": ["action"],
                        "properties": {
                            "action": {
                                "type": "string",
                                "enum": ["click", "wait", "fill", "press", "hover"],
                                "description": "Action type"
                            },
                            "selector": {
                                "type": "string",
                                "description": "Selector for the element to act upon"
                            },
                            "seconds": {
                                "type": "number",
                                "minimum": 0,
                                "description": "Seconds to wait (for 'wait' action)"
                            },
                            "value": {
                                "type": "string",
                                "description": "Value to fill (for 'fill' action)"
                            },
                            "key": {
                                "type": "string",
                                "description": "Key to press (for 'press' action)"
                            }
                        }
                    },
                    "default": []
                }
            },
            "default": {
                "wait_until": "load",
                "wait_timeout_seconds": 30.0,
                "on_timeout": "proceed",
                "actions": []
            }
        },
        "site_name": {
            "type": "string",
            "description": "Unique identifier name for the scraping target"
        },
        "base_url": {
            "type": "string",
            "description": "Base URL for the scraping target",
            "format": "uri"
        },
        "method": {
            "type": "string",
            "enum": ["GET", "POST"],
            "description": "HTTP request method",
            "default": "GET"
        },
        "version": {
            "type": "string",
            "description": "Config schema version for forward compatibility",
            "default": "1.0"
        },
        "priority": {
            "type": "integer",
            "minimum": 1,
            "maximum": 100,
            "description": "Job priority (1-100, lower is higher priority)",
            "default": 10
        },
        "headers": {
            "type": "object",
            "description": "HTTP headers to include in requests",
            "additionalProperties": {
                "type": "string"
            },
            "default": {}
        },
        "cookies": {
            "type": "object",
            "description": "HTTP cookies to include in requests",
            "additionalProperties": {
                "type": "string"
            },
            "default": {}
        },
        "auth": {
            "type": "object",
            "description": "Authentication settings",
            "properties": {
                "login_url": {
                    "type": "string",
                    "format": "uri",
                    "description": "URL to perform login"
                },
                "method": {
                    "type": "string",
                    "enum": ["GET", "POST"],
                    "default": "POST",
                    "description": "HTTP method for login"
                },
                "payload": {
                    "type": "object",
                    "description": "Login payload (form fields or JSON body)"
                },
                "token_from": {
                    "type": "object",
                    "description": "Where to extract the token/cookie from",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["header", "cookie", "body_json", "body_selector"]
                        },
                        "name": {
                            "type": "string",
                            "description": "Name of the header, cookie, or JSON field"
                        },
                        "selector": {
                            "type": "string",
                            "description": "CSS/XPath selector for body_selector"
                        }
                    },
                    "required": ["type"]
                }
            },
            "required": ["login_url"]
        },
        "selectors": {
            "type": "object",
            "required": ["container", "fields"],
            "properties": {
                "container": {
                    "type": "string",
                    "description": "CSS/XPath selector for the container element"
                },
                "fields": {
                    "type": "object",
                    "minProperties": 1,
                    "description": "Field mappings for data extraction",
                    "additionalProperties": {
                        "type": "object",
                        "required": ["selector", "type"],
                        "properties": {
                            "selector": {
                                "type": "string",
                                "description": "CSS or XPath selector for the field"
                            },
                            "type": {
                                "type": "string",
                                "enum": ["css", "xpath"],
                                "description": "Selector type"
                            },
                            "transform": {
                                "type": "string",
                                "enum": ALLOWED_TRANSFORMS,
                                "description": "Transform to apply (e.g., strip, regex)"
                            }
                        }
                    }
                }
            }
        },
        "pagination": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": ["none", "url_pattern", "next_button", "scroll"],
                    "default": "none"
                },
                "param": {
                    "type": "string",
                    "default": "page"
                },
                "start": {
                    "type": "integer",
                    "minimum": 0,
                    "default": 1
                },
                "max_pages": {
                    "type": "integer",
                    "minimum": 1,
                    "default": 1
                },
                "selector": {
                    "type": "string",
                    "description": "CSS or XPath selector for the next page link (required for next_button)"
                },
                "scroll_wait_seconds": {
                    "type": "number",
                    "minimum": 0,
                    "default": 2.0,
                    "description": "Time to wait for new content after scrolling"
                }
            },
            "default": {
                "type": "none",
                "param": "page",
                "start": 1,
                "max_pages": 1
            }
        },
        "output": {
            "type": "object",
            "properties": {
                "format": {
                    "type": "string",
                    "enum": ["sqlite", "json", "csv"],
                    "default": "sqlite"
                },
                "destination": {
                    "type": "string",
                    "default": "./output/"
                },
                "mode": {
                    "type": "string",
                    "enum": ["append", "overwrite"],
                    "default": "append",
                    "description": "How to handle existing data for the same site"
                }
            },
            "default": {
                "format": "sqlite",
                "destination": "./output/",
                "mode": "append"
            }
        },
        "schedule": {
            "type": "object",
            "properties": {
                "every": {
                    "type": "string",
                    "description": "Schedule interval (e.g., '6h', '1d')"
                }
            }
        },
        "rate_limit": {
            "type": "object",
            "properties": {
                "delay_seconds": {
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "default": 1.0,
                    "description": "Minimum delay in seconds between consecutive requests to the same domain (must be > 0)"
                },
                "max_concurrent": {
                    "type": "integer",
                    "minimum": 1,
                    "default": 1,
                    "description": "Maximum number of concurrent requests to the same domain (must be >= 1)"
                }
            },
            "default": {
                "delay_seconds": 1.0,
                "max_concurrent": 1
            }
        },
        "retry": {
            "type": "object",
            "properties": {
                "max_retries": {
                    "type": "integer",
                    "minimum": 0,
                    "default": 3,
                    "description": "Maximum number of retry attempts for failed requests (must be >= 0)"
                },
                "backoff_factor": {
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "default": 2.0,
                    "description": "Exponential backoff factor for retry delays (must be > 0)"
                }
            },
            "default": {
                "max_retries": 3,
                "backoff_factor": 2.0
            }
        },
        "gotify": {
            "type": "object",
            "properties": {
                "server_url": {
                    "type": ["string", "null"],
                    "format": "uri",
                    "description": "Gotify server URL"
                },
                "app_token": {
                    "type": ["string", "null"],
                    "description": "Gotify application token"
                },
                "priority": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 10,
                    "default": 5,
                    "description": "Notification priority (0-10)"
                }
            }
        }
    }
}

# Populate the version-to-schema mapping
SCHEMAS_BY_VERSION["1.0"] = CONFIG_SCHEMA


def get_schema_for_version(version: str) -> dict:
    """Get the JSON Schema for a specific config version.
    
    Args:
        version: The schema version string (e.g., "1.0")
        
    Returns:
        The JSON Schema dictionary for the specified version.
        
    Raises:
        ValueError: If the version is not supported.
    """
    if version not in SUPPORTED_VERSIONS:
        raise ValueError(
            f"Unsupported config version '{version}'. "
            f"Supported versions: {SUPPORTED_VERSIONS}"
        )
    return SCHEMAS_BY_VERSION[version]