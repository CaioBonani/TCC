#!/usr/bin/env python3
import html
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from cassandra.cluster import Cluster
from fastapi import FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
DEFAULT_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "lambda_arch")
MAX_LIMIT = int(os.getenv("CASSANDRA_VIEWER_MAX_LIMIT", "500"))

app = FastAPI(title="Cassandra Viewer")


def get_session(keyspace: Optional[str] = None):
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(keyspace) if keyspace else cluster.connect()
    return cluster, session


def scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple, set)):
        return str(value)
    return value


def escape(value: Any) -> str:
    if value is None:
        return '<span class="null">NULL</span>'
    return html.escape(str(scalar(value)))


def identifier(value: str) -> str:
    if not value.replace("_", "").isalnum() or value[0].isdigit():
        raise ValueError(f"Identificador invalido: {value}")
    return value


def clamp_limit(limit: int) -> int:
    return max(1, min(limit, MAX_LIMIT))


def fetch_keyspaces() -> List[str]:
    cluster, session = get_session()
    try:
        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        ignored = {"system", "system_auth", "system_distributed", "system_schema", "system_traces"}
        return sorted(row.keyspace_name for row in rows if row.keyspace_name not in ignored)
    finally:
        cluster.shutdown()


def fetch_tables(keyspace: str) -> List[str]:
    cluster, session = get_session()
    try:
        rows = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
            [keyspace],
        )
        return sorted(row.table_name for row in rows)
    finally:
        cluster.shutdown()


def fetch_rows(keyspace: str, table: str, limit: int):
    keyspace = identifier(keyspace)
    table = identifier(table)
    cluster, session = get_session(keyspace)
    try:
        result = session.execute(f'SELECT * FROM "{table}" LIMIT %s', [clamp_limit(limit)])
        rows = list(result)
        columns = list(rows[0]._fields) if rows else []
        return columns, rows
    finally:
        cluster.shutdown()


def run_select(keyspace: str, cql: str, limit: int):
    normalized = cql.strip().rstrip(";")
    if not normalized.lower().startswith("select "):
        raise ValueError("O viewer aceita apenas SELECT.")
    if " limit " not in normalized.lower():
        normalized = f"{normalized} LIMIT {clamp_limit(limit)}"

    cluster, session = get_session(identifier(keyspace))
    try:
        result = session.execute(normalized)
        rows = list(result)
        columns = list(rows[0]._fields) if rows else []
        return columns, rows
    finally:
        cluster.shutdown()


def layout(
    *,
    keyspaces: List[str],
    tables: List[str],
    selected_keyspace: str,
    selected_table: Optional[str],
    columns: List[str],
    rows: List[Any],
    limit: int,
    query: str = "",
    error: str = "",
) -> str:
    table_links = "\n".join(
        f'<a class="nav-item {"active" if table == selected_table else ""}" href="/?{urlencode({"keyspace": selected_keyspace, "table": table, "limit": limit})}">{html.escape(table)}</a>'
        for table in tables
    )
    keyspace_options = "\n".join(
        f'<option value="{html.escape(ks)}" {"selected" if ks == selected_keyspace else ""}>{html.escape(ks)}</option>'
        for ks in keyspaces
    )

    header = "".join(f"<th>{html.escape(col)}</th>" for col in columns)
    body = "\n".join(
        "<tr>" + "".join(f"<td>{escape(getattr(row, col))}</td>" for col in columns) + "</tr>"
        for row in rows
    )
    if not rows:
        body = f'<tr><td colspan="{max(len(columns), 1)}" class="empty">Nenhum registro encontrado.</td></tr>'

    error_html = f'<div class="error">{html.escape(error)}</div>' if error else ""
    selected_label = selected_table or "consulta CQL"

    return f"""
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Cassandra Viewer</title>
  <style>
    :root {{
      --bg: #f7f8fa;
      --panel: #ffffff;
      --ink: #17202a;
      --muted: #64748b;
      --line: #dbe2ea;
      --accent: #0f766e;
      --accent-soft: #e0f2f1;
      --danger: #b42318;
      --danger-soft: #fee4e2;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
    }}
    .shell {{ display: grid; grid-template-columns: 260px 1fr; min-height: 100vh; }}
    aside {{ background: #111827; color: white; padding: 18px; }}
    h1 {{ font-size: 18px; margin: 0 0 18px; }}
    label {{ display: block; color: var(--muted); font-size: 12px; margin-bottom: 6px; }}
    aside label {{ color: #cbd5e1; }}
    select, input, textarea {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 10px;
      font: inherit;
      background: white;
      color: var(--ink);
    }}
    textarea {{ min-height: 92px; resize: vertical; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
    button, .button {{
      border: 0;
      border-radius: 6px;
      padding: 9px 12px;
      background: var(--accent);
      color: white;
      font-weight: 650;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }}
    .nav-title {{ color: #9ca3af; margin: 20px 0 8px; font-size: 12px; text-transform: uppercase; }}
    .nav-item {{
      display: block;
      color: #e5e7eb;
      text-decoration: none;
      padding: 8px 10px;
      border-radius: 6px;
      margin-bottom: 3px;
      overflow-wrap: anywhere;
    }}
    .nav-item.active, .nav-item:hover {{ background: rgba(255,255,255,.12); }}
    main {{ padding: 20px 24px; overflow: hidden; }}
    .toolbar {{
      display: grid;
      grid-template-columns: 1fr 120px auto;
      gap: 10px;
      align-items: end;
      margin-bottom: 16px;
    }}
    .query {{ display: grid; grid-template-columns: 1fr auto; gap: 10px; align-items: end; margin-bottom: 16px; }}
    .surface {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }}
    .surface-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
    }}
    .surface-head strong {{ font-size: 15px; }}
    .muted {{ color: var(--muted); }}
    .table-wrap {{ overflow: auto; max-height: calc(100vh - 245px); }}
    table {{ width: 100%; border-collapse: collapse; min-width: 760px; }}
    th, td {{ padding: 9px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: #eef3f7; z-index: 1; font-size: 12px; }}
    td {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }}
    tr:hover td {{ background: #f8fafc; }}
    .null {{ color: #94a3b8; }}
    .empty {{ color: var(--muted); text-align: center; padding: 28px; font-family: inherit; }}
    .error {{ background: var(--danger-soft); color: var(--danger); border: 1px solid #fecdca; border-radius: 6px; padding: 10px 12px; margin-bottom: 14px; }}
    @media (max-width: 820px) {{
      .shell {{ grid-template-columns: 1fr; }}
      aside {{ min-height: auto; }}
      .toolbar, .query {{ grid-template-columns: 1fr; }}
      .table-wrap {{ max-height: none; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <aside>
      <h1>Cassandra Viewer</h1>
      <form method="get" action="/">
        <label>Keyspace</label>
        <select name="keyspace" onchange="this.form.submit()">{keyspace_options}</select>
      </form>
      <div class="nav-title">Tabelas</div>
      {table_links}
    </aside>
    <main>
      {error_html}
      <form class="toolbar" method="get" action="/">
        <input type="hidden" name="keyspace" value="{html.escape(selected_keyspace)}">
        <div>
          <label>Tabela</label>
          <select name="table">{"".join(f'<option value="{html.escape(table)}" {"selected" if table == selected_table else ""}>{html.escape(table)}</option>' for table in tables)}</select>
        </div>
        <div>
          <label>Limite</label>
          <input name="limit" type="number" min="1" max="{MAX_LIMIT}" value="{limit}">
        </div>
        <button type="submit">Atualizar</button>
      </form>
      <form class="query" method="post" action="/query">
        <input type="hidden" name="keyspace" value="{html.escape(selected_keyspace)}">
        <input type="hidden" name="limit" value="{limit}">
        <div>
          <label>Consulta CQL read-only</label>
          <textarea name="cql" placeholder="SELECT * FROM speed_view LIMIT 20">{html.escape(query)}</textarea>
        </div>
        <button type="submit">Executar SELECT</button>
      </form>
      <section class="surface">
        <div class="surface-head">
          <strong>{html.escape(selected_label)}</strong>
          <span class="muted">{len(rows)} linha(s)</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr>{header}</tr></thead>
            <tbody>{body}</tbody>
          </table>
        </div>
      </section>
    </main>
  </div>
</body>
</html>
"""


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index(
    keyspace: str = Query(DEFAULT_KEYSPACE),
    table: Optional[str] = Query(None),
    limit: int = Query(50),
    error: str = Query(""),
):
    try:
        keyspaces = fetch_keyspaces()
        selected_keyspace = keyspace if keyspace in keyspaces else (keyspaces[0] if keyspaces else DEFAULT_KEYSPACE)
        tables = fetch_tables(selected_keyspace)
        selected_table = table if table in tables else (tables[0] if tables else None)
        columns: List[str] = []
        rows: List[Any] = []
        if selected_table:
            columns, rows = fetch_rows(selected_keyspace, selected_table, limit)
        return layout(
            keyspaces=keyspaces,
            tables=tables,
            selected_keyspace=selected_keyspace,
            selected_table=selected_table,
            columns=columns,
            rows=rows,
            limit=clamp_limit(limit),
            error=error,
        )
    except Exception as exc:
        return layout(
            keyspaces=[keyspace],
            tables=[],
            selected_keyspace=keyspace,
            selected_table=None,
            columns=[],
            rows=[],
            limit=clamp_limit(limit),
            error=str(exc),
        )


@app.post("/query", response_class=HTMLResponse)
def query(cql: str = Form(...), keyspace: str = Form(DEFAULT_KEYSPACE), limit: int = Form(50)):
    try:
        keyspaces = fetch_keyspaces()
        selected_keyspace = keyspace if keyspace in keyspaces else DEFAULT_KEYSPACE
        tables = fetch_tables(selected_keyspace)
        columns, rows = run_select(selected_keyspace, cql, limit)
        return layout(
            keyspaces=keyspaces,
            tables=tables,
            selected_keyspace=selected_keyspace,
            selected_table=None,
            columns=columns,
            rows=rows,
            limit=clamp_limit(limit),
            query=cql,
        )
    except Exception as exc:
        params = urlencode({"keyspace": keyspace, "limit": clamp_limit(limit), "error": str(exc)})
        return RedirectResponse(f"/?{params}", status_code=303)
