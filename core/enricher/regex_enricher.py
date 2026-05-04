"""
regex_enricher.py
Tier 2: extracts field values from product title/description using regex patterns.
Zero cost, zero latency.
"""
from __future__ import annotations
import re
import logging
from .enricher_config import REGEX_PATTERNS

logger = logging.getLogger(__name__)

# Fields to scan for text extraction (in priority order)
_TEXT_FIELDS = ["title", "titulo", "name", "nome", "description", "descricao", "descricao_completa"]


class RegexEnricher:
    """
    Extracts product attributes from text fields (title, description)
    using configurable regex patterns.

    Usage:
        enricher = RegexEnricher()
        filled, still_empty = enricher.enrich(product, empty_fields)
    """

    def __init__(self, custom_patterns: dict[str, list[tuple]] | None = None):
        """
        Args:
            custom_patterns: Optional extra patterns merged with built-in ones.
                             Format: {field_name: [(pattern, group, transform_fn), ...]}
        """
        self._patterns = dict(REGEX_PATTERNS)
        if custom_patterns:
            for field, patterns in custom_patterns.items():
                self._patterns.setdefault(field, []).extend(patterns)

        # Pre-compile all patterns for performance
        self._compiled: dict[str, list[tuple]] = {
            field: [
                (re.compile(pattern, re.IGNORECASE), group, transform)
                for pattern, group, transform in patterns
            ]
            for field, patterns in self._patterns.items()
        }

    def enrich(
        self,
        product: dict,
        empty_fields: list[str],
    ) -> tuple[dict, list[str]]:
        """
        Fill empty fields by scanning product text fields with regex patterns.

        Args:
            product:       Current product dict (will NOT be mutated).
            empty_fields:  List of field names that need filling.

        Returns:
            (enriched_product, remaining_empty_fields)
        """
        result = dict(product)
        remaining = []

        # Collect all searchable text from the product
        search_text = self._collect_text(product)

        for field in empty_fields:
            value = self._extract(field, search_text)
            if value is not None:
                result[field] = value
                logger.debug("RegexEnricher: extracted '%s' = %r", field, value)
            else:
                remaining.append(field)

        filled_count = len(empty_fields) - len(remaining)
        logger.info(
            "RegexEnricher: extracted %d/%d fields",
            filled_count, len(empty_fields),
        )
        return result, remaining

    def _collect_text(self, product: dict) -> str:
        """Concatenates all text fields for regex scanning."""
        parts = []
        for key in _TEXT_FIELDS:
            val = product.get(key, "")
            if val and isinstance(val, str):
                parts.append(val)
        # Also scan ALL string fields as fallback
        for key, val in product.items():
            if key not in _TEXT_FIELDS and isinstance(val, str) and val:
                parts.append(val)
        return " | ".join(parts)

    def _extract(self, field: str, text: str) -> object | None:
        """
        Try all patterns for a field against the text.
        Returns the first match, or None.
        """
        compiled_list = self._compiled.get(field, [])
        for compiled_pattern, group, transform in compiled_list:
            match = compiled_pattern.search(text)
            if match:
                try:
                    raw = match.group(group)
                    return transform(raw) if transform else raw
                except (IndexError, ValueError) as exc:
                    logger.debug(
                        "RegexEnricher: pattern match failed for '%s': %s",
                        field, exc,
                    )
        return None
