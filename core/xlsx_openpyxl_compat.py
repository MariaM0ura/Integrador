"""
Compatibilidade de .xlsx com openpyxl.

Alguns templates de marketplace (ex.: Shopee) gravam ``activePane`` com valores
fora do conjunto aceito pelo openpyxl, o que quebra ``load_workbook`` ao
iterar linhas. Sanitizamos apenas o XML das worksheets, preservando o restante
do pacote Office Open XML.
"""

from __future__ import annotations

import io
import logging
import os
import re
import tempfile
import zipfile

logger = logging.getLogger(__name__)

_VALID_ACTIVE_PANE = frozenset({"topRight", "topLeft", "bottomLeft", "bottomRight"})
_ACTIVE_PANE_RE = re.compile(r'activePane="([^"]*)"')


def sanitize_xlsx_for_openpyxl(src_path: str) -> str | None:
    """
    Se ``src_path`` tiver ``activePane`` inválido em ``xl/worksheets/*.xml``,
    grava uma cópia corrigida em arquivo temporário e devolve o caminho.

    Retorna ``None`` se nada precisar ser alterado (use ``src_path``).
    O chamador deve apagar o arquivo retornado com ``os.unlink``.
    """
    try:
        with zipfile.ZipFile(src_path, "r") as zin:
            infos = zin.infolist()
            changed_members: dict[str, bytes] = {}

            for info in infos:
                fn = info.filename
                if not fn.startswith("xl/worksheets/"):
                    continue
                if not fn.endswith(".xml") or fn.endswith(".xml.rels"):
                    continue
                raw = zin.read(fn)
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    continue

                modified = False

                def _repl(m: re.Match[str]) -> str:
                    nonlocal modified
                    val = m.group(1)
                    if val in _VALID_ACTIVE_PANE:
                        return m.group(0)
                    modified = True
                    return 'activePane="topLeft"'

                new_text = _ACTIVE_PANE_RE.sub(_repl, text)
                if modified:
                    changed_members[fn] = new_text.encode("utf-8")

            if not changed_members:
                return None

            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zout:
                for info in infos:
                    fn = info.filename
                    payload = changed_members.get(fn)
                    if payload is None:
                        payload = zin.read(fn)
                    zi = zipfile.ZipInfo(filename=fn, date_time=info.date_time)
                    zi.compress_type = zipfile.ZIP_DEFLATED
                    zi.external_attr = info.external_attr
                    zout.writestr(zi, payload)

    except (zipfile.BadZipFile, OSError) as exc:
        logger.warning("sanitize_xlsx_for_openpyxl: ignorado (%s)", exc)
        return None

    out_fd, out_path = tempfile.mkstemp(suffix=".xlsx")
    with os.fdopen(out_fd, "wb") as out_f:
        out_f.write(buf.getvalue())

    logger.info(
        "xlsx sanitizado para openpyxl (%d aba(s)): %s",
        len(changed_members),
        os.path.basename(src_path),
    )
    return out_path
