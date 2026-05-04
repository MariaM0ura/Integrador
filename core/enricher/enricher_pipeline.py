"""
enricher_pipeline.py
Orchestrates the 3-tier cascade enrichment:
    Tier 1 → Defaults (free, instant)
    Tier 2 → Regex extraction (free, instant)
    Tier 3 → LLM generation (paid, batched per product)

Integrates with SellersFlowPipeline in pipeline.py.
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Any

from .defaults_enricher import DefaultsEnricher
from .regex_enricher import RegexEnricher
from .llm_enricher import LLMEnricher

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentResult:
    """Result of enriching a single product."""
    original_product: dict
    enriched_product: dict
    filled_by_defaults: list[str] = field(default_factory=list)
    filled_by_regex: list[str] = field(default_factory=list)
    filled_by_llm: list[str] = field(default_factory=list)
    still_empty: list[str] = field(default_factory=list)

    @property
    def total_filled(self) -> int:
        return len(self.filled_by_defaults) + len(self.filled_by_regex) + len(self.filled_by_llm)

    @property
    def fill_rate(self) -> float:
        total_empty = self.total_filled + len(self.still_empty)
        return self.total_filled / total_empty if total_empty > 0 else 1.0

    def summary(self) -> str:
        return (
            f"Enriched {self.total_filled} fields "
            f"(defaults={len(self.filled_by_defaults)}, "
            f"regex={len(self.filled_by_regex)}, "
            f"llm={len(self.filled_by_llm)}, "
            f"still_empty={len(self.still_empty)}) | "
            f"fill_rate={self.fill_rate:.0%}"
        )


class EnricherPipeline:
    """
    3-tier cascade enricher pipeline.

    Typical usage in pipeline.py:

        enricher = EnricherPipeline(
            use_llm=True,
            llm_api_key=settings.ANTHROPIC_API_KEY,
        )

        for product in products:
            result = enricher.enrich_product(
                product=product,
                empty_fields=detect_empty_fields(product, destination_template_headers),
                destination_marketplace="shopee",
            )
            enriched_products.append(result.enriched_product)
    """

    def __init__(
        self,
        use_llm: bool = False,
        llm_api_key: str | None = None,
        llm_model: str = LLMEnricher.DEFAULT_MODEL,
        llm_max_tokens: int = 1500,
        custom_defaults: dict[str, dict] | None = None,
        custom_regex_patterns: dict[str, list] | None = None,
        custom_llm_examples: dict[str, list[dict]] | None = None,
    ):
        """
        Args:
            use_llm:                  Enable LLM tier (requires api_key).
            llm_api_key:              Anthropic API key.
            llm_model:                Model identifier.
            llm_max_tokens:           Max tokens per LLM call.
            custom_defaults:          Extra default values per marketplace.
            custom_regex_patterns:    Extra regex patterns per field.
            custom_llm_examples:      Extra few-shot examples per marketplace.
        """
        self.defaults_enricher = DefaultsEnricher(custom_defaults)
        self.regex_enricher = RegexEnricher(custom_regex_patterns)

        self.llm_enricher: LLMEnricher | None = None
        if use_llm:
            if not llm_api_key:
                raise ValueError(
                    "llm_api_key is required when use_llm=True. "
                    "Set ANTHROPIC_API_KEY in your environment or pass it directly."
                )
            self.llm_enricher = LLMEnricher(
                api_key=llm_api_key,
                model=llm_model,
                max_tokens=llm_max_tokens,
                custom_examples=custom_llm_examples,
            )

    def enrich_product(
        self,
        product: dict[str, Any],
        empty_fields: list[str],
        destination_marketplace: str,
    ) -> EnrichmentResult:
        """
        Enrich a single product through the 3-tier cascade.

        Args:
            product:                  Product data dict.
            empty_fields:             Fields that are empty in the destination template.
            destination_marketplace:  Target marketplace key.

        Returns:
            EnrichmentResult with enriched_product and stats per tier.
        """
        if not empty_fields:
            return EnrichmentResult(
                original_product=product,
                enriched_product=dict(product),
            )

        result = EnrichmentResult(
            original_product=dict(product),
            enriched_product=dict(product),
        )

        remaining = list(empty_fields)

        # --- Tier 1: Defaults ---
        enriched, remaining = self.defaults_enricher.enrich(
            result.enriched_product, remaining, destination_marketplace
        )
        result.filled_by_defaults = [f for f in empty_fields if f not in remaining]
        result.enriched_product = enriched

        # --- Tier 2: Regex ---
        if remaining:
            before_regex = set(remaining)
            enriched, remaining = self.regex_enricher.enrich(
                result.enriched_product, list(before_regex)
            )
            result.filled_by_regex = [f for f in before_regex if f not in remaining]
            result.enriched_product = enriched

        # --- Tier 3: LLM ---
        if remaining and self.llm_enricher:
            before_llm = set(remaining)
            enriched, remaining = self.llm_enricher.enrich(
                result.enriched_product, list(before_llm), destination_marketplace
            )
            result.filled_by_llm = [f for f in before_llm if f not in remaining]
            result.enriched_product = enriched

        result.still_empty = remaining
        logger.info("EnricherPipeline: %s", result.summary())
        return result

    def enrich_batch(
        self,
        products: list[dict],
        empty_fields_per_product: list[list[str]],
        destination_marketplace: str,
    ) -> list[EnrichmentResult]:
        """
        Enrich a list of products.

        Args:
            products:                     List of product dicts.
            empty_fields_per_product:     List of empty field lists (same length as products).
            destination_marketplace:      Target marketplace key.

        Returns:
            List of EnrichmentResult, one per product.
        """
        if len(products) != len(empty_fields_per_product):
            raise ValueError(
                "products and empty_fields_per_product must have the same length."
            )

        results = []
        for i, (product, empty_fields) in enumerate(
            zip(products, empty_fields_per_product), start=1
        ):
            logger.info(
                "EnricherPipeline: enriching product %d/%d — %s",
                i, len(products),
                product.get("title") or product.get("titulo") or f"product_{i}",
            )
            results.append(
                self.enrich_product(product, empty_fields, destination_marketplace)
            )

        total_filled = sum(r.total_filled for r in results)
        total_empty = sum(r.total_filled + len(r.still_empty) for r in results)
        overall_rate = total_filled / total_empty if total_empty > 0 else 1.0
        logger.info(
            "EnricherPipeline batch: %d products | %d/%d fields filled | overall fill rate: %.0f%%",
            len(products), total_filled, total_empty, overall_rate * 100,
        )
        return results


# ---------------------------------------------------------------------------
# Helper: detect which fields are empty in a product for a given template
# ---------------------------------------------------------------------------
def detect_empty_fields(
    product: dict,
    destination_headers: list[str],
    mapped_columns: dict[str, str] | None = None,
) -> list[str]:
    """
    Returns destination header fields that have no value in the product.

    Args:
        product:              Product dict (with destination field names as keys).
        destination_headers:  All expected fields in the destination template.
        mapped_columns:       Optional {destination_field: source_field} mapping
                              already resolved by ColumnMapper.

    Returns:
        List of destination fields that are empty/missing.
    """
    empty = []
    for dest_field in destination_headers:
        source_field = (mapped_columns or {}).get(dest_field, dest_field)
        value = product.get(dest_field) or product.get(source_field)
        if value is None or (isinstance(value, str) and not value.strip()):
            empty.append(dest_field)
    return empty
