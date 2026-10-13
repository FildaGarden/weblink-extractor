"""Data models representing fetch operations and extracted link results."""

from dataclasses import dataclass, field
import json
from typing import Any


@dataclass(frozen=True)
class FetchResult:
    """Represents the raw response or error from attempting to fetch a URL.

    Analogous to a DTO or Go struct holding the HTTP response metadata.
    """

    url: str
    final_url: str
    status: int | None = None
    html: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class ExtractionResult:
    """Represents the final structured result for a single input URL.

    Matches the exact JSON schema required by the specification:
    {"url": "...", "final_url": "...", "status": 200, "links": [...], "error": null}
    """

    url: str
    final_url: str
    status: int | None
    links: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary matching specified JSON schema."""
        return {
            "url": self.url,
            "final_url": self.final_url,
            "status": self.status,
            "links": self.links,
            "error": self.error,
        }

    def to_json(self) -> str:
        """Serialize result to a single JSON string without extra whitespace."""
        return json.dumps(self.to_dict())
