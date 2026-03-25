"""Shared ID validation utility for path traversal prevention."""

import re

_SAFE_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_\-]{1,128}$')


def validate_id(value: str, name: str = "id") -> str:
    """Validate that an ID is safe (no path traversal).

    Raises ValueError if the ID contains unsafe characters.
    """
    if not value or not _SAFE_ID_PATTERN.match(value):
        raise ValueError(f"Invalid {name}: must be alphanumeric/underscore/hyphen, 1-128 chars")
    return value
