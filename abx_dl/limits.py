"""Size parsing shared by snapshot and application-owned crawl limits."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation


FILESIZE_UNITS: dict[str, int] = {
    "": 1,
    "b": 1,
    "byte": 1,
    "bytes": 1,
    "k": 1024,
    "kb": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1024**4,
    "tib": 1024**4,
}


def parse_filesize_to_bytes(value: str | int | float | None) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        raise ValueError("Size value must be an integer or size string.")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError("Size value must resolve to a whole number of bytes.")
        return int(value)

    raw_value = str(value).strip()
    if not raw_value:
        return 0
    if raw_value.isdigit():
        return int(raw_value)

    import re

    match = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)\s*([a-z]+)", raw_value)
    if not match:
        raise ValueError(f"Invalid size value: {value}")

    amount_str, unit_str = match.groups()
    multiplier = FILESIZE_UNITS.get(unit_str.lower())
    if multiplier is None:
        raise ValueError(f"Unknown size unit: {unit_str}")

    try:
        amount = Decimal(amount_str)
    except InvalidOperation as err:
        raise ValueError(f"Invalid size value: {value}") from err

    return int(amount * multiplier)
