"""
rule_filler.py
==============
Preenchimento de colunas destino usando regras extraídas pelo InstructionParser.
Funciona 100% sem IA — zero custo para campos simples.

Estratégias (em ordem de prioridade):
  1. lookup        — normaliza valor de origem contra lista de valores aceitos
  2. concatenacao  — detecta padrão "[Campo1] [Campo2]" e monta o valor
  3. exemplo       — valor mais frequente nas linhas de exemplo
  4. padrao        — valor do campo ColumnRule.exemplo (fallback para obrigatórios)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Optional

import pandas as pd

from core.instruction_parser import ColumnRule

logger = logging.getLogger(__name__)

_SIM_THRESHOLD = 0.72


# ─── Resultado de preenchimento ───────────────────────────────────────────────

@dataclass
class RuleFillDecision:
    dest_col: str
    value: Optional[str]
    strategy: str       # lookup | concatenacao | exemplo | padrao | unmapped
    confidence: float
    notes: str = ""


# ─── Filler ───────────────────────────────────────────────────────────────────

class RuleBasedFiller:
    """
    Preenche colunas destino não cobertas pelo mapeamento de colunas.

    Uso:
        filler = RuleBasedFiller()
        decisions = filler.fill_row(
            source_row=row.to_dict(),
            dest_cols=["Condição", "Categoria"],
            col_rules=instruction_parser_result,
            example_rows=[...],          # opcional
        )
        for d in decisions:
            if d.value:
                df.at[idx, d.dest_col] = d.value
    """

    def fill_row(
        self,
        source_row: dict,
        dest_cols: list[str],
        col_rules: dict[str, ColumnRule],
        example_rows: list[dict] | None = None,
    ) -> list[RuleFillDecision]:
        """
        Retorna uma decisão por dest_col.
        source_row: {coluna_origem: valor} da linha em processamento.
        """
        decisions = []
        for dest_col in dest_cols:
            rule = col_rules.get(dest_col) or _fuzzy_rule(dest_col, col_rules)
            decisions.append(
                self._fill_one(dest_col, rule, source_row, example_rows or [])
            )
        return decisions

    def build_augmented_df(
        self,
        amazon_df: pd.DataFrame,
        unmapped_dest: dict[int, str],   # {dest_col_idx: dest_col_name}
        col_rules: dict[str, ColumnRule],
        example_rows: list[dict],
    ) -> tuple[pd.DataFrame, dict[int, int]]:
        """
        Preenche colunas não mapeadas linha a linha, retorna:
          - DataFrame aumentado (amazon_df + novas colunas prefixadas __rf__)
          - Novo index_map parcial: {dest_col_idx → new_col_idx_in_aug_df}

        As novas colunas recebem nomes "__rf__{dest_col}" para não colidir.
        """
        dest_col_names = list(unmapped_dest.values())
        new_cols: dict[str, list] = {f"__rf__{col}": [] for col in dest_col_names}

        for _, row in amazon_df.iterrows():
            src = row.to_dict()
            decisions = self.fill_row(src, dest_col_names, col_rules, example_rows)
            for d in decisions:
                new_cols[f"__rf__{d.dest_col}"].append(d.value)

        aug_df = amazon_df.copy()
        new_index_map: dict[int, int] = {}

        for dest_col_idx, dest_col_name in unmapped_dest.items():
            col_key = f"__rf__{dest_col_name}"
            vals = new_cols[col_key]
            if any(v is not None and v != "" for v in vals):
                aug_df[col_key] = vals
                new_index_map[dest_col_idx] = aug_df.columns.get_loc(col_key)

        return aug_df, new_index_map

    # ── Fill individual ───────────────────────────────────────────────────────

    def _fill_one(
        self,
        dest_col: str,
        rule: Optional[ColumnRule],
        source_row: dict,
        example_rows: list[dict],
    ) -> RuleFillDecision:

        if rule is None:
            return RuleFillDecision(dest_col, None, "unmapped", 0.0, "Sem regra.")

        # 1. Lookup de valores aceitos
        if rule.valores_aceitos:
            match = _lookup(source_row, rule.valores_aceitos)
            if match:
                return RuleFillDecision(
                    dest_col, match, "lookup", 0.9,
                    f"Normalizado para: {match}"
                )

        # 2. Concatenação estruturada
        concat = _try_concat(rule, source_row)
        if concat:
            return RuleFillDecision(
                dest_col, concat, "concatenacao", 0.85,
                f"Montado via padrão: {rule.regra[:60]}"
            )

        # 3. Herança de exemplo
        if example_rows:
            ex_val = _from_example(dest_col, example_rows)
            if ex_val:
                return RuleFillDecision(
                    dest_col, ex_val, "exemplo", 0.6,
                    "Herdado de linha de exemplo."
                )

        # 4. Padrão obrigatório
        if rule.obrigatorio and rule.exemplo:
            return RuleFillDecision(
                dest_col, rule.exemplo, "padrao", 0.5,
                "Valor padrão (campo obrigatório)."
            )

        return RuleFillDecision(dest_col, None, "unmapped", 0.0, "Nenhuma regra aplicável.")


# ─── Helpers internos ─────────────────────────────────────────────────────────

def _fuzzy_rule(dest_col: str, rules: dict[str, ColumnRule]) -> Optional[ColumnRule]:
    """Encontra a regra mais próxima pelo nome da coluna (similaridade ≥ 0.72)."""
    dest_n = dest_col.strip().lower()
    best_r, best_col = 0.0, None
    for col in rules:
        r = SequenceMatcher(None, dest_n, col.strip().lower()).ratio()
        if r > best_r:
            best_r, best_col = r, col
    if best_r >= _SIM_THRESHOLD and best_col:
        return rules[best_col]
    return None


def _lookup(source_row: dict, accepted: list[str]) -> Optional[str]:
    """
    Procura o melhor match entre algum valor da linha e a lista de valores aceitos.
    Primeiro busca match exato, depois por similaridade.
    """
    candidates = [
        str(v).strip() for v in source_row.values()
        if v and str(v).strip() not in ("", "nan", "None")
        and len(str(v).strip()) < 100
    ]
    best_r, best_acc = 0.0, None
    for val in candidates:
        val_l = val.lower()
        for acc in accepted:
            acc_l = acc.lower()
            if val_l == acc_l:
                return acc
            r = SequenceMatcher(None, val_l, acc_l).ratio()
            if r > best_r:
                best_r, best_acc = r, acc
    if best_r >= _SIM_THRESHOLD:
        return best_acc
    return None


def _try_concat(rule: ColumnRule, source_row: dict) -> Optional[str]:
    """
    Detecta padrão [Campo1] + [Campo2] ou [Campo1] [Campo2] na instrução
    e monta o valor correspondente.
    """
    if not rule.regra:
        return None
    pattern = re.findall(r"\[([^\]]+)\]", rule.regra)
    if len(pattern) < 2:
        return None

    parts = []
    for field_ref in pattern:
        ref_n = field_ref.strip().lower()
        for src_col, src_val in source_row.items():
            if not src_val or str(src_val).strip() in ("", "nan", "None"):
                continue
            src_n = str(src_col).strip().lower()
            if ref_n in src_n or src_n in ref_n:
                parts.append(str(src_val).strip())
                break

    return " ".join(parts) if len(parts) >= 2 else None


def _from_example(dest_col: str, examples: list[dict]) -> Optional[str]:
    """Retorna o valor mais frequente para dest_col nas linhas de exemplo."""
    dest_n = dest_col.strip().lower()
    freq: dict[str, int] = {}
    for ex in examples:
        for col, val in ex.items():
            if col.strip().lower() == dest_n and val and str(val).strip():
                k = str(val).strip()
                freq[k] = freq.get(k, 0) + 1
    if freq:
        return max(freq, key=lambda k: freq[k])
    return None
