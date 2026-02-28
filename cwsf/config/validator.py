"""Configuration validator module for CWSF.

This module provides functionality to validate configuration dictionaries
against the JSON schema defined in schema.py.

Error Output Format:
--------------------
The ValidationResult contains a list of ValidationError objects and a list of
ValidationWarning objects. Each ValidationError includes:
- field_path: The path to the field that failed validation (e.g., "site_name", "selectors.fields.title.type")
- message: Human-readable error message describing the issue
- value: The offending value that caused the validation error

Each ValidationWarning includes:
- field_path: The path to the field that triggered the warning
- message: Human-readable warning message

Example error output:
    ValidationResult(
        is_valid=False,
        errors=[
            ValidationError(
                field_path="base_url",
                message="'base_url' is a required property",
                value=None
            ),
            ValidationError(
                field_path="method",
                message="'PATCH' is not one of ['GET', 'POST']",
                value="PATCH"
            )
        ],
        warnings=[
            ValidationWarning(
                field_path="rate_limit.unknown_key",
                message="Unrecognized key 'unknown_key' in section 'rate_limit'"
            )
        ]
    )
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from jsonschema import validate, ValidationError as JsonSchemaValidationError, Draft7Validator

from cwsf.config.schema import CONFIG_SCHEMA, SUPPORTED_VERSIONS, get_schema_for_version


@dataclass
class ValidationError:
    """Represents a single validation error.
    
    Attributes:
        field_path: The path to the field that failed validation (e.g., "site_name", "selectors.fields.title.type")
        message: Human-readable error message describing the issue
        value: The offending value that caused the validation error (may be None for missing fields)
    """
    field_path: str
    message: str
    value: Any = None


@dataclass
class ValidationWarning:
    """Represents a single validation warning (non-fatal issue).
    
    Attributes:
        field_path: The path to the field that triggered the warning
        message: Human-readable warning message describing the issue
    """
    field_path: str
    message: str


@dataclass
class ValidationResult:
    """Result of configuration validation.
    
    Attributes:
        is_valid: True if the configuration is valid, False otherwise
        errors: List of ValidationError objects describing any validation failures
        warnings: List of ValidationWarning objects describing non-fatal issues
    """
    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List["ValidationWarning"] = field(default_factory=list)
    
    def __bool__(self) -> bool:
        """Allow using ValidationResult in boolean context."""
        return self.is_valid


# Known keys for rate_limit and retry sections (Story 5.6)
_RATE_LIMIT_KNOWN_KEYS = {"delay_seconds", "max_concurrent"}
_RETRY_KNOWN_KEYS = {"max_retries", "backoff_factor"}


def validate_config(config: Dict[str, Any], config_file: Optional[str] = None) -> ValidationResult:
    """Validate a configuration dictionary against the CWSF schema.
    
    Args:
        config: A dictionary representing the parsed configuration contents.
        config_file: Optional name of the config file being validated, included
            in error messages for clearer feedback.
        
    Returns:
        A ValidationResult object containing validation status, any errors, and
        any warnings. Each error includes the field path, a human-readable message,
        and the offending value that caused the validation failure. Warnings are
        non-fatal issues such as unrecognized keys.
    """
    errors: List[ValidationError] = []
    warnings: List[ValidationWarning] = []
    file_prefix = f"[{config_file}] " if config_file else ""
    
    # 1. Check for version field (required for version-specific schema selection)
    version = config.get("version")
    if not version:
        return ValidationResult(
            is_valid=False,
            errors=[ValidationError(
                field_path="version",
                message="'version' is a required property",
                value=None
            )]
        )
    
    # 2. Check if version is supported
    if version not in SUPPORTED_VERSIONS:
        return ValidationResult(
            is_valid=False,
            errors=[ValidationError(
                field_path="version",
                message=f"Unsupported config version '{version}'. Supported versions: {SUPPORTED_VERSIONS}",
                value=version
            )]
        )
    
    # 3. Get schema for the specified version
    try:
        schema = get_schema_for_version(version)
    except ValueError as e:
        # This should theoretically be caught by the SUPPORTED_VERSIONS check above,
        # but we'll handle it just in case.
        return ValidationResult(
            is_valid=False,
            errors=[ValidationError(
                field_path="version",
                message=str(e),
                value=version
            )]
        )
    
    # 4. Create a Draft7Validator instance for the specific schema
    validator = Draft7Validator(schema)
    
    # Collect all validation errors
    for error in validator.iter_errors(config):
        field_path = ".".join(str(p) for p in error.path) if error.path else ""
        message = error.message
        offending_value = error.instance if error.instance is not None else None
        
        # Clean up the message - remove the leading dot if present
        if message.startswith("."):
            message = message[1:]
        
        # For required field errors, extract the field name from the message
        # JSON Schema error message format: "'<field_name>' is a required property"
        if error.validator == 'required':
            # The error.message contains something like "'site_name' is a required property"
            # We need to extract the field name
            import re
            match = re.search(r"'(\w+)'", message)
            if match:
                field_path = match.group(1)
            # For required fields, the value is None (field is missing)
            offending_value = None
        
        # If field_path is still empty, use a reasonable default
        if not field_path:
            field_path = "root"
            
        errors.append(ValidationError(
            field_path=field_path,
            message=message,
            value=offending_value
        ))
    
    # 5. Custom validation for rate_limit and retry unrecognized keys (Story 5.6)
    rate_limit = config.get("rate_limit", {})
    if isinstance(rate_limit, dict):
        for key in rate_limit:
            if key not in _RATE_LIMIT_KNOWN_KEYS:
                warnings.append(ValidationWarning(
                    field_path=f"rate_limit.{key}",
                    message=(
                        f"{file_prefix}Unrecognized key '{key}' in section 'rate_limit'. "
                        f"Known keys: {sorted(_RATE_LIMIT_KNOWN_KEYS)}"
                    )
                ))

    retry_section = config.get("retry", {})
    if isinstance(retry_section, dict):
        for key in retry_section:
            if key not in _RETRY_KNOWN_KEYS:
                warnings.append(ValidationWarning(
                    field_path=f"retry.{key}",
                    message=(
                        f"{file_prefix}Unrecognized key '{key}' in section 'retry'. "
                        f"Known keys: {sorted(_RETRY_KNOWN_KEYS)}"
                    )
                ))

    # 6. Custom validation for pagination (Story 4.1 AC 5)
    pagination = config.get("pagination", {})
    if pagination.get("type") == "url_pattern":
        base_url = config.get("base_url", "")
        param = pagination.get("param", "page")
        placeholder = f"{{{param}}}"
        if placeholder not in base_url:
            errors.append(ValidationError(
                field_path="base_url",
                message=f"URL pattern pagination requires placeholder '{placeholder}' in base_url",
                value=base_url
            ))
    
    if pagination.get("type") == "next_button":
        if not pagination.get("selector"):
            errors.append(ValidationError(
                field_path="pagination.selector",
                message="Next button pagination requires a 'selector'",
                value=None
            ))

    if pagination.get("type") == "scroll":
        if config.get("renderer") != "playwright":
            errors.append(ValidationError(
                field_path="pagination.type",
                message="Scroll pagination requires 'renderer: playwright'",
                value="scroll"
            ))

    # 7. Custom validation for auth (Story 4.6)
    auth = config.get("auth")
    if auth:
        token_from = auth.get("token_from")
        if token_from:
            tf_type = token_from.get("type")
            if tf_type == "body_selector" and not token_from.get("selector"):
                errors.append(ValidationError(
                    field_path="auth.token_from.selector",
                    message="token_from type 'body_selector' requires a 'selector'",
                    value=None
                ))
            if tf_type in ["header", "cookie", "body_json"] and not token_from.get("name"):
                errors.append(ValidationError(
                    field_path=f"auth.token_from.name",
                    message=f"token_from type '{tf_type}' requires a 'name'",
                    value=None
                ))
 
    # 8. Custom validation for playwright_options (Story 4.7)
    playwright_options = config.get("playwright_options", {})
    actions = playwright_options.get("actions", [])
    for i, action in enumerate(actions):
        action_type = action.get("action")
        if action_type in ["click", "fill", "press", "hover"] and not action.get("selector"):
            errors.append(ValidationError(
                field_path=f"playwright_options.actions[{i}].selector",
                message=f"Action '{action_type}' requires a 'selector'",
                value=None
            ))
        if action_type == "wait" and action.get("seconds") is None:
            errors.append(ValidationError(
                field_path=f"playwright_options.actions[{i}].seconds",
                message="Action 'wait' requires 'seconds'",
                value=None
            ))
        if action_type == "fill" and action.get("value") is None:
            errors.append(ValidationError(
                field_path=f"playwright_options.actions[{i}].value",
                message="Action 'fill' requires 'value'",
                value=None
            ))
        if action_type == "press" and action.get("key") is None:
            errors.append(ValidationError(
                field_path=f"playwright_options.actions[{i}].key",
                message="Action 'press' requires 'key'",
                value=None
            ))

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings
    )


def validate_config_with_schema(config: Dict[str, Any], schema: Dict[str, Any]) -> ValidationResult:
    """Validate a configuration against a custom schema.
    
    This is useful for testing or validating against different schema versions.
    
    Args:
        config: A dictionary representing the parsed configuration contents.
        schema: A JSON schema dictionary to validate against.
        
    Returns:
        A ValidationResult object containing validation status and any errors.
        Each error includes the field path, a human-readable message, and the
        offending value that caused the validation failure.
    """
    errors: List[ValidationError] = []
    
    validator = Draft7Validator(schema)
    
    for error in validator.iter_errors(config):
        field_path = ".".join(str(p) for p in error.path) if error.path else error.json_path
        message = error.message
        offending_value = error.instance if error.instance is not None else None
        
        if message.startswith("."):
            message = message[1:]
        
        # For required field errors, the value is None (field is missing)
        if error.validator == 'required':
            offending_value = None
            
        errors.append(ValidationError(
            field_path=field_path,
            message=message,
            value=offending_value
        ))
    
    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors
    )
