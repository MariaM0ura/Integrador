"""
defaults_enricher.py
Tier 1: fills empty fields using fixed default values per destination marketplace.
Zero cost, zero latency.
"""
from __future__ import annotations
import logging
from .enricher_config import MARKETPLACE_DEFAULTS

logger = logging.getLogger(__name__)


class DefaultsEnricher:
    """
    Fills empty product fields using predefined default values
    for each destination marketplace.

    Usage:
        enricher = DefaultsEnricher()
        filled, still_empty = enricher.enrich(product, empty_fields, "shopee")
    """

    def __init__(self, custom_defaults: dict[str, dict] | None = None):
        """
        Args:
            custom_defaults: Optional override/extension of MARKETPLACE_DEFAULTS.
                             Merged on top of built-in defaults.
        """
        self._defaults = dict(MARKETPLACE_DEFAULTS)
        if custom_defaults:
            for marketplace, values in custom_defaults.items():
                self._defaults.setdefault(marketplace, {}).update(values)

    def enrich(
        self,
        product: dict,
        empty_fields: list[str],
        destination_marketplace: str,
    ) -> tuple[dict, list[str]]:
        """
        Fill empty fields using marketplace defaults.

        Args:
            product:                  Current product dict (will NOT be mutated).
            empty_fields:             List of field names that need filling.
            destination_marketplace:  Marketplace key (e.g. "shopee", "magalu").

        Returns:
            (enriched_product, remaining_empty_fields)
        """
        result = dict(product)
        remaining = []

        marketplace_key = destination_marketplace.lower().strip()
        defaults = self._defaults.get(marketplace_key, {})

        for field in empty_fields:
            if field in defaults:
                result[field] = defaults[field]
                logger.debug(
                    "DefaultsEnricher: filled '%s' = %r for marketplace '%s'",
                    field, defaults[field], marketplace_key,
                )
            else:
                remaining.append(field)

        filled_count = len(empty_fields) - len(remaining)
        logger.info(
            "DefaultsEnricher: filled %d/%d fields for '%s'",
            filled_count, len(empty_fields), marketplace_key,
        )
        return result, remaining

    def get_available_defaults(self, marketplace: str) -> dict:
        """Returns the default values available for a marketplace."""
        return dict(self._defaults.get(marketplace.lower(), {}))
