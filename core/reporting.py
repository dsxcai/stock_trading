from __future__ import annotations
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from core.report_meta import _effective_report_meta, _migrate_state_schema, _normalize_mode_key

def _extract_json_from_text(txt: str) -> Dict[str, Any]:
    s = txt.strip()
    if s.startswith('```'):
        lines = s.splitlines()
        lines = lines[1:]
        if lines and lines[-1].strip().startswith('```'):
            lines = lines[:-1]
        s = '\n'.join(lines).strip()
    if '```' in s and (not s.lstrip().startswith('{')):
        m = re.search('```(?:json)?\\s*([\\s\\S]*?)```', s, flags=re.IGNORECASE)
        if m:
            s = m.group(1).strip()
    return json.loads(s)

def load_schema(path: str) -> Dict[str, Any]:
    return _extract_json_from_text(Path(path).read_text(encoding='utf-8'))
_path_cache: Dict[str, List[Union[str, Tuple[str, str]]]] = {}

def _compile_path(path: str) -> List[Union[str, Tuple[str, str]]]:
    if path in _path_cache:
        return _path_cache[path]
    p = path.strip()
    is_root = p.startswith('$.')
    if is_root:
        p = p[2:]
    tokens: List[Union[str, Tuple[str, str]]] = []
    for part in p.split('.'):
        part = part.strip()
        if not part:
            continue
        if part.endswith('[*]'):
            tokens.append(('star', part[:-3]))
        else:
            tokens.append(part)
    _path_cache[path] = tokens
    return tokens

def resolve_path(obj: Any, path: str, root: Any=None) -> Any:
    if root is None:
        root = obj
    base = root if path.strip().startswith('$.') else obj
    tokens = _compile_path(path)
    cur = base
    for tok in tokens:
        if isinstance(tok, tuple) and tok[0] == 'star':
            key = tok[1]
            if not isinstance(cur, dict):
                return []
            arr = cur.get(key, [])
            return list(arr) if isinstance(arr, list) else []
        else:
            key = tok
            if isinstance(cur, dict):
                cur = cur.get(key)
            else:
                return None
    return cur

def eval_expr(expr: Any, row: Dict[str, Any], root: Dict[str, Any]) -> Any:
    if expr is None:
        return None
    if isinstance(expr, (int, float, str, bool)):
        return expr
    if isinstance(expr, dict):
        if 'const' in expr:
            return expr['const']
        if 'path' in expr:
            return resolve_path(row, expr['path'], root=root)
        et = expr.get('type')
        if et == 'gt':
            left = eval_expr(expr.get('left'), row, root)
            right = eval_expr(expr.get('right'), row, root)
            try:
                return left is not None and right is not None and (float(left) > float(right))
            except Exception:
                return False
        if et == 'div':
            num = eval_expr(expr.get('num'), row, root)
            den = eval_expr(expr.get('den'), row, root)
            try:
                if num is None or den is None:
                    return None
                den_f = float(den)
                if den_f == 0:
                    return None
                return float(num) / den_f
            except Exception:
                return None
        if et == 'if':
            cond = expr.get('cond')
            cond_val = eval_expr(cond, row, root) if isinstance(cond, dict) else bool(cond)
            if cond_val:
                return eval_expr(expr.get('then'), row, root)
            return eval_expr(expr.get('else'), row, root)
        if et == 'map':
            source_path = expr.get('source_path')
            default_path = expr.get('default_path')
            mapping = expr.get('dict') or {}
            raw = resolve_path(row, source_path, root=root) if source_path else None
            if raw in mapping:
                return mapping[raw]
            if str(raw) in mapping:
                return mapping[str(raw)]
            if default_path:
                return resolve_path(row, default_path, root=root)
            return raw
    if isinstance(expr, list):
        return [eval_expr(x, row, root) for x in expr]
    return None

def _parse_dateish(s: str) -> Optional[datetime]:
    s = str(s).strip()
    if not s:
        return None
    s2 = s.replace('/', '-')
    patterns = ['%Y-%m-%d', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']

    def _normalize_date_time(x: str) -> str:
        m = re.match('^(\\d{4})-(\\d{1,2})-(\\d{1,2})(.*)$', x)
        if not m:
            return x
        y, mo, da, tail = (m.group(1), m.group(2), m.group(3), m.group(4))
        return f'{int(y):04d}-{int(mo):02d}-{int(da):02d}{tail}'
    s2 = _normalize_date_time(s2)
    for p in patterns:
        try:
            return datetime.strptime(s2, p)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        return None

def format_value(value: Any, fmt: Optional[str], schema: Dict[str, Any], null_display: str) -> str:
    if value is None:
        return null_display
    if fmt is None:
        return str(value)
    fmt_def = (schema.get('formatters') or {}).get(fmt)
    if not fmt_def:
        return str(value)
    ftype = fmt_def.get('type')
    try:
        if ftype == 'integer':
            return f'{int(float(value)):,d}'
        if ftype == 'number':
            dec = int(fmt_def.get('decimals', 2))
            return f'{float(value):,.{dec}f}'
        if ftype == 'currency':
            dec = int(fmt_def.get('decimals', 2))
            cur = fmt_def.get('currency', '')
            prefix = '$' if cur == 'USD' else ''
            return f'{prefix}{float(value):,.{dec}f}'
        if ftype == 'percent':
            dec = int(fmt_def.get('decimals', 2))
            return f'{float(value) * 100:.{dec}f}%'
        if ftype == 'date':
            d = _parse_dateish(str(value))
            return d.strftime('%Y-%m-%d') if d else str(value)
        if ftype == 'datetime':
            d = _parse_dateish(str(value))
            return d.strftime('%Y/%m/%d %H:%M:%S') if d else str(value)
        if ftype == 'custom':
            dec = int(fmt_def.get('value_decimals', 2))
            template = fmt_def.get('template', '{value}')
            return template.format(value_p2=f'{float(value):.{dec}f}')
    except Exception:
        return str(value)
    return str(value)

def _align_marker(align: Optional[str]) -> str:
    if align == 'right':
        return '---:'
    if align == 'left':
        return ':---'
    if align == 'center':
        return ':---:'
    return '---'

def render_table_md(headers: List[str], rows: List[List[str]], aligns: List[Optional[str]]) -> str:
    sep = '| ' + ' | '.join(headers) + ' |'
    align_row = '| ' + ' | '.join((_align_marker(a) for a in aligns)) + ' |'
    body = '\n'.join(('| ' + ' | '.join(r) + ' |' for r in rows))
    return '\n'.join([sep, align_row, body]) if body else '\n'.join([sep, align_row])

def _sort_key(v: Any) -> Any:
    if v is None:
        return (1, '')
    if isinstance(v, (int, float)):
        return (0, v)
    if isinstance(v, str):
        d = _parse_dateish(v)
        if d:
            return (0, d)
        return (0, v)
    return (0, str(v))

def build_dataset(schema: Dict[str, Any], states: Dict[str, Any], dataset_id: str) -> List[Dict[str, Any]]:
    ds = (schema.get('datasets') or {}).get(dataset_id) or {}
    src = (ds.get('row_source') or {}).get('path')
    rows = resolve_path(states, src, root=states) if src else []
    rows = list(rows) if isinstance(rows, list) else []
    # Do not mutate state objects when row_computed fields are applied.
    rows = [dict(r) if isinstance(r, dict) else r for r in rows]
    if ds.get('exclude_zero_shares'):
        kept = []
        for r in rows:
            if not isinstance(r, dict):
                kept.append(r)
                continue
            try:
                shares = float(r.get('shares') or 0.0)
            except Exception:
                shares = 0.0
            if abs(shares) > 1e-12:
                kept.append(r)
        rows = kept
    sort_specs = ds.get('sort') or []
    for s in reversed(sort_specs):
        key_path = (s.get('key') or {}).get('path') if isinstance(s.get('key'), dict) else None
        order = s.get('order', 'asc')
        rows.sort(key=lambda r: _sort_key(resolve_path(r, key_path, root=states) if key_path else None), reverse=order == 'desc')
    return rows

def apply_row_computed(table_spec: Dict[str, Any], rows: List[Dict[str, Any]], states: Dict[str, Any]) -> None:
    rc = table_spec.get('row_computed') or {}
    if not rc:
        return
    for r in rows:
        for k, expr in rc.items():
            r[k] = eval_expr(expr, r, states)

def render_simple_table(table_spec: Dict[str, Any], rows: List[Dict[str, Any]], schema: Dict[str, Any], states: Dict[str, Any], null_display: str) -> str:
    columns = table_spec.get('columns') or []
    headers = [c['header'] for c in columns]
    aligns = [c.get('align') for c in columns]
    out_rows: List[List[str]] = []
    for r in rows:
        row_cells = []
        for c in columns:
            val_spec = c.get('value') or {}
            val = eval_expr(val_spec, r, states)
            cell = format_value(val, c.get('format'), schema, null_display)
            row_cells.append(cell)
        out_rows.append(row_cells)
    footer_rows = table_spec.get('footer_rows') or []
    for fr in footer_rows:
        label = fr.get('label', '')
        cells_spec = fr.get('cells') or {}
        fr_cells = [null_display for _ in columns]
        if columns:
            fr_cells[0] = label
        header_to_index = {h: i for i, h in enumerate(headers)}
        for hdr, cell_def in cells_spec.items():
            if hdr not in header_to_index:
                continue
            idx = header_to_index[hdr]
            val = eval_expr(cell_def, {}, states)
            fmt = cell_def.get('format')
            fr_cells[idx] = format_value(val, fmt, schema, null_display)
        out_rows.append(fr_cells)
    return render_table_md(headers, out_rows, aligns)

def render_grouped_trade_table(table_spec: Dict[str, Any], rows: List[Dict[str, Any]], schema: Dict[str, Any], states: Dict[str, Any], null_display: str) -> str:
    grouping = table_spec.get('grouping') or {}
    group_by_path = (grouping.get('group_by') or {}).get('path') if isinstance(grouping.get('group_by'), dict) else None
    order = grouping.get('order', 'desc')
    groups: Dict[str, List[Dict[str, Any]]] = {}
    group_order: List[str] = []
    for r in rows:
        k = resolve_path(r, group_by_path, root=states) if group_by_path else None
        k = str(k) if k is not None else ''
        if k not in groups:
            groups[k] = []
            group_order.append(k)
        groups[k].append(r)
    group_order.sort(key=lambda x: _sort_key(x), reverse=order == 'desc')
    keep = grouping.get('keep_groups') or {}
    latest_full = int(keep.get('latest_full_groups', 1))
    prev_cfg = keep.get('prev_simplified_groups') or {}
    prev_default = int(prev_cfg.get('default', 5))
    prev_path = prev_cfg.get('path')
    prev_n = prev_default
    if prev_path:
        v = resolve_path(states, prev_path, root=states)
        try:
            if v is not None:
                prev_n = int(v)
        except Exception:
            prev_n = prev_default
    kept = group_order[:latest_full + prev_n]
    col_sets = table_spec.get('column_sets') or {}
    selectors = (table_spec.get('group_rendering') or {}).get('columns_selector') or []
    md_parts: List[str] = []
    for gi, gk in enumerate(kept):
        md_parts.append(f"### Trade Date (ET): {format_value(gk, 'date', schema, null_display)}")
        col_key = None
        for sel in selectors:
            when = sel.get('when') or {}
            if 'group_index_eq' in when and gi == int(when['group_index_eq']):
                col_key = sel.get('use')
                break
            if 'group_index_between' in when:
                lo, hi = when['group_index_between']
                if gi >= int(lo) and gi <= int(hi):
                    col_key = sel.get('use')
                    break
        if col_key is None:
            col_key = 'full' if gi == 0 else 'simple'
        columns = col_sets.get(col_key) or []
        headers = [c['header'] for c in columns]
        aligns = [c.get('align') for c in columns]
        body_rows: List[List[str]] = []
        for r in groups[gk]:
            row_cells = []
            for c in columns:
                val = eval_expr(c.get('value') or {}, r, states)
                row_cells.append(format_value(val, c.get('format'), schema, null_display))
            body_rows.append(row_cells)
        md_parts.append(render_table_md(headers, body_rows, aligns))
        md_parts.append('')
    return '\n'.join(md_parts).rstrip() + '\n'

def report_title_from_meta(states: Dict[str, Any], mode: str) -> str:
    meta = _effective_report_meta(states, mode)
    cfg = states.get('config', {}) or {}
    doc = cfg.get('doc') or meta.get('doc') or 'Daily Investment Report'
    if isinstance(doc, str) and '|' in doc:
        doc = doc.split('|')[-1].strip() or 'Daily Investment Report'
    mode_label = meta.get('mode') or str(mode or '')
    return f'{doc} ({mode_label})' if mode_label else str(doc)

def report_date_default(states: Dict[str, Any], mode: str) -> str:
    meta = _effective_report_meta(states, mode)
    d = meta.get('version_anchor_et')
    if d:
        dd = _parse_dateish(str(d))
        return dd.strftime('%Y-%m-%d') if dd else str(d)
    exec_basis = meta.get('execution_basis', {}) or {}
    d = exec_basis.get('t_plus_1_et')
    if d:
        dd = _parse_dateish(str(d))
        return dd.strftime('%Y-%m-%d') if dd else str(d)
    sb = (meta.get('signal_basis') or {}).get('t_et')
    dd = _parse_dateish(str(sb)) if sb else None
    return dd.strftime('%Y-%m-%d') if dd else datetime.now().strftime('%Y-%m-%d')

def render_report(states: Dict[str, Any], schema: Dict[str, Any], mode: str) -> str:
    null_display = (schema.get('output') or {}).get('null_display') or '-'
    meta = _effective_report_meta(states, mode)
    if not any((
        meta.get('version_anchor_et'),
        meta.get('signal_basis'),
        meta.get('execution_basis'),
    )):
        raise ValueError(f'mode snapshot not found or incomplete: {mode}')
    version = meta.get('version', '')
    sb = meta.get('signal_basis', {}) or {}
    eb = meta.get('execution_basis', {}) or {}
    lines: List[str] = []
    lines.append(f'# {report_title_from_meta(states, mode)}')
    if version:
        lines.append('')
        lines.append(f'- Version: {version}')
    if sb:
        lines.append(f"- Signal Basis: t={sb.get('t_et')} ({sb.get('basis', '')})")
    if eb:
        lines.append(f"- Execution Basis: t+1={eb.get('t_plus_1_et')} ({eb.get('basis', '')})")
    for note in meta.get('price_notes') or []:
        note_text = str(note or '').strip()
        if note_text:
            lines.append(f'- {note_text}')
    lines.append('')
    for t in schema.get('tables') or []:
        title = t.get('title') or ''
        lines.append(f'## {title}')
        dataset_id = t.get('dataset')
        rows = build_dataset(schema, states, dataset_id) if dataset_id else []
        apply_row_computed(t, rows, states)
        if t.get('grouping'):
            lines.append(render_grouped_trade_table(t, rows, schema, states, null_display))
        else:
            lines.append(render_simple_table(t, rows, schema, states, null_display))
            lines.append('')
    return '\n'.join(lines).rstrip() + '\n'
