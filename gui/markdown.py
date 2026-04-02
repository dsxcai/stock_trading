from __future__ import annotations

import html
import re
from typing import List


def _render_inline(text: str) -> str:
    parts = re.split(r"(`[^`]+`)", str(text or ""))
    rendered: List[str] = []
    for part in parts:
        if part.startswith("`") and part.endswith("`") and len(part) >= 2:
            rendered.append(f"<code>{html.escape(part[1:-1])}</code>")
        else:
            rendered.append(html.escape(part))
    return "".join(rendered)


def _split_table_row(line: str) -> List[str]:
    body = str(line or "").strip()
    if body.startswith("|"):
        body = body[1:]
    if body.endswith("|"):
        body = body[:-1]
    return [cell.strip() for cell in body.split("|")]


def _is_table_separator(line: str) -> bool:
    cells = _split_table_row(line)
    if not cells:
        return False
    for cell in cells:
        compact = cell.replace(" ", "")
        if not re.match(r"^:?-{3,}:?$", compact):
            return False
    return True


def _table_alignments(separator_line: str) -> List[str]:
    aligns: List[str] = []
    for cell in _split_table_row(separator_line):
        compact = cell.replace(" ", "")
        if compact.startswith(":") and compact.endswith(":"):
            aligns.append("center")
        elif compact.endswith(":"):
            aligns.append("right")
        elif compact.startswith(":"):
            aligns.append("left")
        else:
            aligns.append("left")
    return aligns


def render_markdown(markdown_text: str) -> str:
    lines = str(markdown_text or "").splitlines()
    out: List[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            i += 1
            continue
        if stripped.startswith("```"):
            block_lines: List[str] = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                block_lines.append(lines[i])
                i += 1
            if i < len(lines):
                i += 1
            out.append(f"<pre><code>{html.escape(chr(10).join(block_lines))}</code></pre>")
            continue
        if stripped.startswith("#"):
            hashes = len(stripped) - len(stripped.lstrip("#"))
            level = min(max(hashes, 1), 6)
            out.append(f"<h{level}>{_render_inline(stripped[level:].strip())}</h{level}>")
            i += 1
            continue
        if (
            "|" in line
            and i + 1 < len(lines)
            and "|" in lines[i + 1]
            and _is_table_separator(lines[i + 1])
        ):
            header_cells = _split_table_row(line)
            alignments = _table_alignments(lines[i + 1])
            body_rows: List[List[str]] = []
            i += 2
            while i < len(lines):
                row_line = lines[i]
                row_stripped = row_line.strip()
                if not row_stripped or "|" not in row_line:
                    break
                body_rows.append(_split_table_row(row_line))
                i += 1
            out.append("<table>")
            out.append("<thead><tr>")
            for index, cell in enumerate(header_cells):
                align = alignments[index] if index < len(alignments) else "left"
                out.append(f'<th class="align-{align}">{_render_inline(cell)}</th>')
            out.append("</tr></thead>")
            out.append("<tbody>")
            for row in body_rows:
                out.append("<tr>")
                for index, cell in enumerate(row):
                    align = alignments[index] if index < len(alignments) else "left"
                    out.append(f'<td class="align-{align}">{_render_inline(cell)}</td>')
                out.append("</tr>")
            out.append("</tbody></table>")
            continue
        if stripped.startswith("- "):
            items: List[str] = []
            while i < len(lines) and lines[i].strip().startswith("- "):
                items.append(lines[i].strip()[2:].strip())
                i += 1
            out.append("<ul>")
            for item in items:
                out.append(f"<li>{_render_inline(item)}</li>")
            out.append("</ul>")
            continue
        paragraph_lines = [stripped]
        i += 1
        while i < len(lines):
            peek = lines[i].strip()
            if not peek:
                break
            if peek.startswith("#") or peek.startswith("- "):
                break
            if "|" in lines[i] and i + 1 < len(lines) and "|" in lines[i + 1] and _is_table_separator(lines[i + 1]):
                break
            if peek.startswith("```"):
                break
            paragraph_lines.append(peek)
            i += 1
        out.append(f"<p>{_render_inline(' '.join(paragraph_lines))}</p>")
    return "\n".join(out)

