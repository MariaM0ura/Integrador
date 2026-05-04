"""
enricher_config.py
Central config for all enrichment strategies.
Add new marketplaces, patterns, and examples here.
"""

# ---------------------------------------------------------------------------
# TIER 1 — Default values per marketplace
# Keys must match the destination marketplace identifier used in pipeline.py
# ---------------------------------------------------------------------------
MARKETPLACE_DEFAULTS: dict[str, dict[str, str | int | float]] = {
    "shopee": {
        "condition": "New",
        "origin": "National",
        "warranty_months": 12,
        "pre_order": False,
        "dangerous_goods": "None",
    },
    "magalu": {
        "condition": "Novo",
        "warranty_months": 12,
        "origin": "Nacional",
        "installments": 12,
    },
    "mercadolivre": {
        "condition": "new",
        "warranty_time": "12 months",
        "listing_type": "gold_special",
    },
    "amazon": {
        "condition": "New",
        "fulfillment_channel": "MERCHANT",
        "currency": "BRL",
    },
    "temu": {
        "condition": "New",
        "origin": "CN",
        "warranty_days": 90,
    },
    "walmart": {
        "condition": "New",
        "prop_65_warning": False,
    },
}


# ---------------------------------------------------------------------------
# TIER 2 — Regex extraction patterns
# Each entry: field_name → list of (pattern, group_index, transform_fn)
# transform_fn is optional; receives the raw matched string and returns the value
# ---------------------------------------------------------------------------
import re


def _upper(v: str) -> str:
    return v.upper()


def _title(v: str) -> str:
    return v.strip().title()


def _int(v: str) -> int:
    return int(v)


REGEX_PATTERNS: dict[str, list[tuple]] = {
    "voltage": [
        (r"\b(110|127|220)\s*[vV]\b", 1, _upper),
        (r"\b(bivolt|bivolt)\b", 0, lambda v: "Bivolt"),
    ],
    "color": [
        (r"\b(preto|black|branco|white|prata|silver|dourado|gold|azul|blue|"
         r"vermelho|red|verde|green|rosa|pink|cinza|gray|amarelo|yellow)\b", 0, _title),
    ],
    "size": [
        (r"\b(\d+(?:\.\d+)?)\s*(cm|mm|m|polegadas|inches|\")\b", 0, str),
        (r"\b(P|M|G|GG|XS|XM|XL|XXL|XXXL)\b", 0, _upper),
    ],
    "material": [
        (r"\b(inox|aço|madeira|couro|plástico|alumínio|vidro|tecido|borracha|nylon)\b",
         0, _title),
    ],
    "warranty_months": [
        (r"\b(\d+)\s*(meses?|months?)\s*(de\s+)?garantia\b", 1, _int),
        (r"\bgarantia\s+(\d+)\s*(meses?|months?)\b", 1, _int),
    ],
    "weight_kg": [
        (r"\b(\d+(?:[.,]\d+)?)\s*kg\b", 1, lambda v: float(v.replace(",", "."))),
    ],
    "weight_g": [
        (r"\b(\d+(?:[.,]\d+)?)\s*g\b", 1, lambda v: float(v.replace(",", "."))),
    ],
    "model": [
        (r"\bmodelo\s+([A-Z0-9\-]+)\b", 1, str),
        (r"\bmodel\s+([A-Z0-9\-]+)\b", 1, str),
    ],
    "brand": [
        (r"\b(marca|brand)[\s:]+([A-Za-z0-9\-]+)\b", 2, _title),
    ],
    "wattage": [
        (r"\b(\d+)\s*[wW]\b", 1, _int),
    ],
    "capacity_l": [
        (r"\b(\d+(?:[.,]\d+)?)\s*[lL]itros?\b", 1, lambda v: float(v.replace(",", "."))),
    ],
}


# ---------------------------------------------------------------------------
# TIER 3 — LLM few-shot examples per marketplace
# These teach the LLM the expected format/tone for each destination
# Add your own real approved examples here for best results
# ---------------------------------------------------------------------------
LLM_FEW_SHOT_EXAMPLES: dict[str, list[dict]] = {
    "shopee": [
        {
            "input": {
                "title": "Bluetooth Headphone JBL T110BT Black",
                "description": "Wireless headphone with microphone",
                "price": 159.90,
            },
            "output": {
                "bullet_1": "Stable Bluetooth connection up to 10 meters range",
                "bullet_2": "Up to 6 hours of continuous battery life",
                "bullet_3": "Built-in microphone for hands-free calls",
                "category_tree": "Electronics > Audio > Headphones",
                "short_description": "Wireless headphone with mic and 6h battery",
                "keywords": "bluetooth headphone jbl wireless microphone",
            },
        },
        {
            "input": {
                "title": "Non-stick Frying Pan 28cm Black",
                "description": "Aluminum pan with teflon coating",
                "price": 89.90,
            },
            "output": {
                "bullet_1": "Non-stick teflon coating for easy cleaning",
                "bullet_2": "Aluminum body for fast and even heat distribution",
                "bullet_3": "Compatible with gas, electric, and ceramic stoves",
                "category_tree": "Home & Kitchen > Cookware > Frying Pans",
                "short_description": "28cm non-stick aluminum frying pan",
                "keywords": "frying pan non-stick teflon aluminum kitchen",
            },
        },
    ],
    "magalu": [
        {
            "input": {
                "title": "Smartphone Samsung Galaxy A54 256GB Black",
                "description": "Android 13 smartphone with triple camera",
                "price": 1899.00,
            },
            "output": {
                "technical_description": (
                    "Samsung Galaxy A54 com tela Super AMOLED de 6,4\", "
                    "processador Exynos 1380 e 256GB de armazenamento. "
                    "Câmera tripla de 50MP com OIS para fotos nítidas."
                ),
                "category_path": "Celulares e Smartphones > Samsung",
                "highlights": "256GB | Tela 6.4\" | Câmera 50MP | Android 13",
                "search_keywords": "samsung galaxy a54 smartphone android 256gb",
            },
        },
    ],
    "mercadolivre": [
        {
            "input": {
                "title": "Tenis Nike Air Max 270 Preto 42",
                "description": "Tenis masculino corrida",
                "price": 499.90,
            },
            "output": {
                "description_ml": (
                    "Tênis Nike Air Max 270 na cor preta, tamanho 42.\n\n"
                    "✅ Amortecimento Air Max para conforto o dia todo\n"
                    "✅ Cabedal em mesh respirável\n"
                    "✅ Solado de borracha de alta durabilidade\n\n"
                    "Ideal para treinos leves, caminhadas e uso casual."
                ),
                "item_condition": "new",
                "category_id": "MLA1276",
            },
        },
    ],
    "amazon": [
        {
            "input": {
                "title": "Coffee Maker Philips Walita RI2080 12 cups",
                "description": "Drip coffee maker stainless steel",
                "price": 249.90,
            },
            "output": {
                "bullet_point_1": "Brews up to 12 cups of coffee in one cycle",
                "bullet_point_2": "Keep-warm plate maintains temperature for 40 minutes",
                "bullet_point_3": "Permanent washable filter included — no paper filter needed",
                "bullet_point_4": "Stainless steel carafe resists stains and odors",
                "bullet_point_5": "Pause & Pour feature lets you pour mid-brew",
                "product_description": (
                    "The Philips Walita RI2080 drip coffee maker delivers rich, "
                    "full-flavored coffee for the whole family. Features a 1.5L "
                    "stainless steel carafe, adjustable aroma strength, and a "
                    "keep-warm function to keep your coffee hot for up to 40 minutes."
                ),
                "search_terms": "coffee maker drip philips walita 12 cups stainless",
            },
        },
    ],
}


# ---------------------------------------------------------------------------
# LLM system prompt template (filled at runtime with marketplace + examples)
# ---------------------------------------------------------------------------
LLM_SYSTEM_PROMPT_TEMPLATE = """You are a marketplace catalog specialist for {marketplace}.
Your task is to generate missing product fields based on existing product data.

RULES:
- Return ONLY a valid JSON object with the requested fields as keys.
- Do not include fields that were not requested.
- Match the tone, language, and format of the marketplace (see examples below).
- Be accurate — do not invent specs that are not inferable from the product data.
- For text fields, match the language of the product title (Portuguese if PT, English if EN).
- Keep descriptions factual and benefit-oriented.
- Never add markdown, code fences, or extra explanation. JSON only.

MARKETPLACE: {marketplace}

EXAMPLES:
{examples_json}

Now generate the requested fields for the given product."""
