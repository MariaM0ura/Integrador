"""
tests/test_amazon_shopee.py
===========================
End-to-end tests for the Amazon → Shopee pipeline flow.

Run:
    cd MVP_Integrador && python -m pytest tests/test_amazon_shopee.py -v
"""

from __future__ import annotations

import io
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── File paths ──────────────────────────────────────────────────────────────

_DADOS = Path(__file__).parent.parent.parent / "Dados"
SHOPEE_TEMPLATE = _DADOS / "templete_SHOPEE.xlsx"
AMAZON_TEMPLATE = _DADOS / "templete_AMAZON.xlsm"
OUTPUT_DIR = Path(__file__).parent / "outputs"

# Skip all tests gracefully if data files are missing
pytestmark = pytest.mark.skipif(
    not SHOPEE_TEMPLATE.exists() or not AMAZON_TEMPLATE.exists(),
    reason="Data files not found in Dados/",
)


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def shopee_bytes() -> bytes:
    return SHOPEE_TEMPLATE.read_bytes()


@pytest.fixture(scope="module")
def amazon_bytes() -> bytes:
    return AMAZON_TEMPLATE.read_bytes()


@pytest.fixture(scope="module")
def pipeline_result(shopee_bytes, amazon_bytes):
    from pipeline import SellersFlowPipeline

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pipeline = SellersFlowPipeline(
        output_dir=str(OUTPUT_DIR),
    )
    return pipeline.run(
        amazon_file=io.BytesIO(amazon_bytes),
        template_file=io.BytesIO(shopee_bytes),
        marketplace="Shopee",
        source_marketplace="Amazon",
        use_ai=False,
        use_instructions=True,
    )


# ── Teste 1: InstructionParser extrai regras suficientes ─────────────────────

class TestInstructionParser:
    def test_extracts_minimum_columns(self, shopee_bytes):
        from core.instruction_parser import InstructionParser

        parser = InstructionParser()
        rules = parser.parse(shopee_bytes, "Shopee", "Modelo", header_row=3)

        assert len(rules) >= 20, (
            f"Expected >= 20 columns, got {len(rules)}. "
            "Check that read_only=True is not used when loading the Shopee xlsx."
        )

    def test_mandatory_fields_have_rules(self, shopee_bytes):
        from core.instruction_parser import InstructionParser
        from core.mapper import REQUIRED_FIELDS

        parser = InstructionParser()
        rules = parser.parse(shopee_bytes, "Shopee", "Modelo", header_row=3)
        required = REQUIRED_FIELDS.get("Shopee", [])

        assert required, "REQUIRED_FIELDS['Shopee'] is empty"

        covered = [col for col in required if col in rules and rules[col].regra]
        coverage = len(covered) / len(required)

        assert coverage >= 0.80, (
            f"Only {len(covered)}/{len(required)} required fields have rules: "
            f"missing={[c for c in required if c not in covered]}"
        )

    def test_correct_header_row_used(self, shopee_bytes):
        from core.instruction_parser import InstructionParser

        parser = InstructionParser()
        rules = parser.parse(shopee_bytes, "Shopee", "Modelo", header_row=3)

        # Row 3 has human-readable names — verify we got them, not internal codes
        assert "Nome do Produto" in rules, (
            "Expected 'Nome do Produto' (row 3 header). Got internal codes instead. "
            f"Keys found: {list(rules.keys())[:5]}"
        )
        assert "Preço" in rules
        assert "Peso" in rules

    def test_no_internal_codes_as_column_names(self, shopee_bytes):
        from core.instruction_parser import InstructionParser

        parser = InstructionParser()
        rules = parser.parse(shopee_bytes, "Shopee", "Modelo", header_row=3)

        # Internal codes look like "ps_product_name|1|0"
        pipe_coded = [col for col in rules if "|" in col]
        assert not pipe_coded, (
            f"Internal codes leaked as column names: {pipe_coded[:3]}"
        )


# ── Teste 2: MARKETPLACE_MAPPINGS cobre obrigatórias ─────────────────────────

class TestMarketplaceMappings:
    def test_shopee_mappings_cover_required(self):
        from core.mapper import MARKETPLACE_MAPPINGS, REQUIRED_FIELDS

        shopee_map = MARKETPLACE_MAPPINGS.get("Shopee", {})
        required = REQUIRED_FIELDS.get("Shopee", [])

        assert required, "REQUIRED_FIELDS['Shopee'] is empty"
        assert shopee_map, "MARKETPLACE_MAPPINGS['Shopee'] is empty"

        map_norm = {col.lower(): col for col in shopee_map}
        covered = [col for col in required if col.lower() in map_norm]
        coverage = len(covered) / len(required)

        assert coverage >= 0.80, (
            f"MARKETPLACE_MAPPINGS['Shopee'] covers only "
            f"{len(covered)}/{len(required)} required fields. "
            f"Missing: {[c for c in required if c.lower() not in map_norm]}"
        )

    def test_shopee_mappings_reference_valid_semantic_keys(self):
        from core.mapper import MARKETPLACE_MAPPINGS, AMAZON_SYNONYMS

        shopee_map = MARKETPLACE_MAPPINGS.get("Shopee", {})
        all_keys = set(AMAZON_SYNONYMS.keys())

        for dest_col, semantic_keys in shopee_map.items():
            for key in semantic_keys:
                assert key in all_keys, (
                    f"MARKETPLACE_MAPPINGS['Shopee']['{dest_col}'] references "
                    f"unknown semantic key '{key}'. Add it to AMAZON_SYNONYMS."
                )


# ── Teste 3: Pipeline end-to-end ─────────────────────────────────────────────

class TestPipelineEndToEnd:
    def test_pipeline_succeeds(self, pipeline_result):
        assert pipeline_result.success, (
            f"Pipeline failed. Errors: {pipeline_result.errors}"
        )

    def test_rows_processed(self, pipeline_result):
        assert pipeline_result.read_result is not None
        assert pipeline_result.read_result.df.shape[0] > 0, "No rows read from Amazon file"

    def test_mandatory_coverage_above_90pct(self, pipeline_result):
        cov = pipeline_result.phase_coverage
        assert "mandatory_coverage" in cov, (
            "pipeline_result.phase_coverage has no 'mandatory_coverage' key. "
            "Ensure use_instructions=True is passed."
        )
        mc = cov["mandatory_coverage"]
        assert mc >= 0.90, (
            f"mandatory_coverage={mc:.1%} < 90%. "
            f"Full phase coverage: {cov}"
        )

    def test_total_coverage_above_60pct(self, pipeline_result):
        cov = pipeline_result.phase_coverage.get("total", 0.0)
        assert cov >= 0.60, (
            f"Total coverage {cov:.1%} < 60%. "
            "Note: ~35% of Shopee columns (image URLs, fiscal codes) have no Amazon "
            "equivalent and require AI or manual fill."
        )

    def test_required_fields_not_empty(self, pipeline_result, shopee_bytes, amazon_bytes):
        from core.mapper import REQUIRED_FIELDS

        required = REQUIRED_FIELDS.get("Shopee", [])
        mr = pipeline_result.mapping_result
        assert mr is not None, "No mapping result"

        dec_by_name = {d.dest_col: d for d in mr.decisions}

        for col in required:
            d = dec_by_name.get(col)
            assert d is not None, f"Required column '{col}' not in mapping decisions"
            assert d.source_idx is not None, (
                f"Required column '{col}' has no source mapping. "
                f"strategy={d.strategy}, notes={d.notes}"
            )

    def test_output_file_saved(self, pipeline_result):
        output_path = pipeline_result.output_path
        assert output_path is not None, "Pipeline did not produce an output file"
        assert Path(output_path).exists(), f"Output file not found: {output_path}"

        # Copy to fixed location for visual inspection
        import shutil
        dest = OUTPUT_DIR / "shopee_output_test.xlsx"
        shutil.copy(output_path, dest)
        assert dest.exists()

    def test_phase_coverage_keys_present(self, pipeline_result):
        cov = pipeline_result.phase_coverage
        for key in ("fase1_mapping", "fase2_rule", "fase3_ai", "fase4_exemplo",
                    "total", "mandatory_coverage"):
            assert key in cov, f"Missing key '{key}' in phase_coverage: {cov}"

    def test_no_false_positive_image_mapping(self, pipeline_result):
        """Imagem de capa must not map to Amazon's 'Material de capa'."""
        mr = pipeline_result.mapping_result
        if mr is None:
            return
        for d in mr.decisions:
            if d.dest_col == "Imagem de capa":
                assert d.source_col != "Material de capa", (
                    "False positive: 'Imagem de capa' mapped to 'Material de capa' via similarity."
                )
                break

    def test_no_false_positive_variacao_marca(self, pipeline_result):
        """Nome da Variação 1 must not map to Amazon's 'Nome da marca'."""
        mr = pipeline_result.mapping_result
        if mr is None:
            return
        for d in mr.decisions:
            if d.dest_col == "Nome da Variação 1":
                assert d.source_col != "Nome da marca", (
                    "False positive: 'Nome da Variação 1' mapped to 'Nome da marca'."
                )
                break
