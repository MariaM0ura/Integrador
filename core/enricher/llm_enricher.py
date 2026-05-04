"""
llm_enricher.py
Tier 3: uses an LLM (Claude via Anthropic API) to generate missing fields.
Called ONCE per product with ALL remaining empty fields — never field-by-field.
"""
from __future__ import annotations
import json
import logging
import time
from typing import Any

from .enricher_config import LLM_FEW_SHOT_EXAMPLES, LLM_SYSTEM_PROMPT_TEMPLATE

logger = logging.getLogger(__name__)


class LLMEnricher:
    """
    Generates missing product fields using an LLM.

    Batches ALL remaining empty fields into a single API call per product
    to minimize cost and latency.

    Usage:
        enricher = LLMEnricher(api_key="sk-ant-...", model="claude-sonnet-4-6")
        filled, still_empty = enricher.enrich(product, empty_fields, "shopee")
    """

    DEFAULT_MODEL = "claude-sonnet-4-6"
    MAX_RETRIES = 2
    RETRY_DELAY_SECONDS = 2

    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_MODEL,
        max_tokens: int = 1500,
        custom_examples: dict[str, list[dict]] | None = None,
    ):
        """
        Args:
            api_key:         Anthropic API key.
            model:           Model identifier.
            max_tokens:      Max tokens for LLM response.
            custom_examples: Extra few-shot examples merged with built-in ones.
        """
        try:
            import anthropic
            self._client = anthropic.Anthropic(api_key=api_key)
        except ImportError as exc:
            raise ImportError(
                "anthropic package is required for LLMEnricher. "
                "Install it with: pip install anthropic"
            ) from exc

        self._model = model
        self._max_tokens = max_tokens

        # Merge built-in + custom examples
        self._examples = dict(LLM_FEW_SHOT_EXAMPLES)
        if custom_examples:
            for marketplace, examples in custom_examples.items():
                self._examples.setdefault(marketplace, []).extend(examples)

    def enrich(
        self,
        product: dict,
        empty_fields: list[str],
        destination_marketplace: str,
    ) -> tuple[dict, list[str]]:
        """
        Generate missing fields using the LLM in a single batched call.

        Args:
            product:                  Current product dict.
            empty_fields:             List of field names to generate.
            destination_marketplace:  Target marketplace key.

        Returns:
            (enriched_product, remaining_empty_fields)
        """
        if not empty_fields:
            return product, []

        result = dict(product)
        marketplace_key = destination_marketplace.lower().strip()

        try:
            generated = self._call_llm(product, empty_fields, marketplace_key)
        except Exception as exc:
            logger.error("LLMEnricher: API call failed: %s", exc)
            return result, empty_fields  # Return unchanged if LLM fails

        remaining = []
        for field in empty_fields:
            if field in generated and generated[field] not in (None, "", []):
                result[field] = generated[field]
                logger.debug(
                    "LLMEnricher: generated '%s' = %r",
                    field, str(generated[field])[:80],
                )
            else:
                remaining.append(field)

        filled_count = len(empty_fields) - len(remaining)
        logger.info(
            "LLMEnricher: generated %d/%d fields for '%s'",
            filled_count, len(empty_fields), marketplace_key,
        )
        return result, remaining

    def _call_llm(
        self,
        product: dict,
        fields_to_generate: list[str],
        marketplace: str,
    ) -> dict[str, Any]:
        """
        Sends a single API call requesting all missing fields at once.
        Returns a dict of {field: value}.
        """
        system_prompt = self._build_system_prompt(marketplace)
        user_prompt = self._build_user_prompt(product, fields_to_generate)

        for attempt in range(self.MAX_RETRIES + 1):
            try:
                response = self._client.messages.create(
                    model=self._model,
                    max_tokens=self._max_tokens,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                )
                raw_text = response.content[0].text.strip()
                return self._parse_json_response(raw_text, fields_to_generate)

            except Exception as exc:
                if attempt < self.MAX_RETRIES:
                    logger.warning(
                        "LLMEnricher: attempt %d failed (%s), retrying in %ds...",
                        attempt + 1, exc, self.RETRY_DELAY_SECONDS,
                    )
                    time.sleep(self.RETRY_DELAY_SECONDS)
                else:
                    raise

    def _build_system_prompt(self, marketplace: str) -> str:
        examples = self._examples.get(marketplace, [])
        examples_json = json.dumps(examples, ensure_ascii=False, indent=2)
        return LLM_SYSTEM_PROMPT_TEMPLATE.format(
            marketplace=marketplace,
            examples_json=examples_json,
        )

    def _build_user_prompt(self, product: dict, fields_to_generate: list[str]) -> str:
        import math
        # Only send relevant product fields (exclude None/empty values)
        clean_product = {
            k: v for k, v in product.items()
            if v is not None and v != "" and not isinstance(v, float)
            or (isinstance(v, float) and not math.isnan(v))
        }
        return (
            f"PRODUCT DATA:\n{json.dumps(clean_product, ensure_ascii=False, indent=2)}\n\n"
            f"FIELDS TO GENERATE:\n{json.dumps(fields_to_generate, ensure_ascii=False)}\n\n"
            "Return a JSON object with ONLY these fields."
        )

    @staticmethod
    def _parse_json_response(raw: str, expected_fields: list[str]) -> dict[str, Any]:
        """Safely parses LLM JSON response, stripping markdown fences if present."""
        text = raw
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(
                line for line in lines
                if not line.strip().startswith("```")
            )

        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            # Attempt to extract JSON object from response
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                try:
                    return json.loads(text[start:end])
                except json.JSONDecodeError:
                    pass

        logger.warning(
            "LLMEnricher: could not parse JSON response. "
            "Expected fields: %s. Raw (truncated): %.200s",
            expected_fields, raw,
        )
        return {}
