"""Databricks bridge for the JMP add-in.

databricks_connector.jsl calls dispatch(fn, args_json) and reads back a JSON
envelope, so Python errors surface in the UI instead of only in the JMP log.
"""

import configparser
import contextlib
import datetime
import json
import logging
import os
import re
import sys
import tempfile
import time
import uuid
from urllib.parse import urlparse

VERSION = "v0.1.0"

# macOS: ~/Library/Logs (shows in Console.app). Windows: %LOCALAPPDATA%, which survives temp cleanup.
LOG_PATH = os.path.join(
    os.path.expanduser("~/Library/Logs")
    if sys.platform == "darwin"
    else os.environ.get("LOCALAPPDATA", tempfile.gettempdir()),
    "DatabricksConnector.log",
)
log = logging.getLogger("dbx_jmp")


def start_log(mode="w"):
    """Called by the JSL: "w" on window open (fresh log), "a" for a refresh. SDK debug logs omit auth headers."""
    logging.basicConfig(
        filename=LOG_PATH,
        filemode=mode,
        level=logging.DEBUG,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        force=True,
    )
    os.chmod(
        LOG_PATH, 0o600
    )  # holds your email, groups and SQL: keep it private to your user


if sys.platform == "darwin":
    # Apps launched from the Dock get a minimal PATH, so databricks-cli auth can't find the CLI.
    # shortcut: covers Homebrew installs only; read the login shell's PATH if users install elsewhere.
    os.environ["PATH"] += ":/opt/homebrew/bin:/usr/local/bin"

# JMP's bundled Python has no system CA bundle. The SDK uses certifi itself, but the SQL connector
# uses the default SSL context, so without this every query fails TLS and retries with backoff.
os.environ.setdefault("SSL_CERT_FILE", __import__("certifi").where())

_ws = None  # WorkspaceClient the connector window is signed in with
_clients = {}  # host -> WorkspaceClient, so refreshing a table never switches the window's workspace

_EPOCH_OFFSET = (
    2082844800  # seconds from JMP's epoch (1904-01-01) to Unix epoch (1970-01-01)
)


def dispatch(fn, args_json):
    try:
        args = json.loads(args_json or "{}")
        log.info("call %s %s", fn, {k: v for k, v in args.items() if k != "token"})
        data = globals()[fn](**args)
        return json.dumps({"ok": True, "data": data})
    except Exception as e:  # every error goes back to the UI
        log.exception("%s failed", fn)
        return json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"})


# --- Auth --------------------------------------------------------------------


def profiles():
    """Workspace profiles from ~/.databrickscfg (account-level profiles skipped)."""
    path = os.environ.get(
        "DATABRICKS_CONFIG_FILE", os.path.expanduser("~/.databrickscfg")
    )
    cfg = configparser.ConfigParser()
    cfg.read(path)
    sections = [("DEFAULT", cfg.defaults())] + [(s, cfg[s]) for s in cfg.sections()]
    return [
        {"name": name, "host": _clean_host(sec["host"])}
        for name, sec in sections
        if sec.get("host") and "accounts." not in sec["host"]
    ]


def _clean_host(host):
    """Keep https://host only; pasted URLs like .../?o=123 break API paths."""
    return (
        "https://" + urlparse(host if "://" in host else "https://" + host).netloc
        if host
        else ""
    )


def _client(method, profile, host, token=""):
    from databricks.sdk import WorkspaceClient

    if method == "profile":
        # No host: an explicit host beats the profile's, and would get the profile's token.
        return WorkspaceClient(profile=profile)
    if method == "oauth":
        return WorkspaceClient(host=host, auth_type="external-browser")
    return WorkspaceClient(host=host, token=token, auth_type="pat")


def connect(method, profile="", host="", token=""):
    global _ws

    w = _client(method, profile, _clean_host(host), token)
    me = w.current_user.me()  # fail fast on bad credentials
    _close_conns(w.config.host)
    _clients[w.config.host] = _ws = w
    return {"user": me.user_name or me.display_name or "", "host": w.config.host}


def _workspace(host, sign_in):
    """Client for a workspace a saved table points at; reconnects with the saved sign-in if needed."""
    host = _clean_host(host)
    w = _clients.get(host)
    if w is None:
        method, _, profile = sign_in.partition(":")
        if method not in ("profile", "oauth"):  # PATs are never saved with a table
            raise PermissionError(
                f"Not signed in to {host}. Sign in with the Databricks Connector."
            )
        w = _client(method, profile, host)
        # Check before me() sends the profile's token.
        if _clean_host(w.config.host) != host:
            raise PermissionError(
                f"Profile '{profile}' is for {w.config.host}, not {host}. Sign in to {host} with the Databricks Connector."
            )
        w.current_user.me()  # fail before touching the table if the sign-in is no longer valid
        _clients[host] = w
    return w


# --- Browse ------------------------------------------------------------------


LAST_WAREHOUSE_PATH = os.path.expanduser(
    "~/.databricks_jmp_last_warehouse.json"
)  # {host: warehouse_id}


def _last_warehouses():
    try:
        with open(LAST_WAREHOUSE_PATH) as f:
            return json.load(f)
    except (OSError, ValueError):  # first run, or a damaged file: just don't remember
        return {}


def _remember_warehouse(host, warehouse_id):
    last = _last_warehouses()
    if last.get(host) != warehouse_id:
        with open(LAST_WAREHOUSE_PATH, "w") as f:
            json.dump(last | {host: warehouse_id}, f)


def warehouses():
    """The last warehouse used in this workspace first, then running ones, then by name."""
    last = _last_warehouses().get(_ws.config.host)
    whs = sorted(
        _ws.warehouses.list(),
        key=lambda w: (w.id != last, w.state.value != "RUNNING", w.name.lower()),
    )
    return [{"id": w.id, "name": w.name, "state": w.state.value.title()} for w in whs]


def catalogs():
    return sorted(c.name for c in _ws.catalogs.list())


def schemas(catalog):
    return sorted(s.name for s in _ws.schemas.list(catalog_name=catalog))


def tables(catalog, schema):
    ts = _ws.tables.list(
        catalog_name=catalog,
        schema_name=schema,
        omit_columns=True,
        omit_properties=True,
    )
    return sorted(t.name for t in ts)


def table(full_name):
    t = _ws.tables.get(full_name)
    return {
        "type": t.table_type.value.replace("_", " ").title() if t.table_type else "",
        "owner": t.owner or "",
        "comment": t.comment or "",
        "columns": [
            {
                "name": c.name,
                "type": c.type_text or "",
                "comment": c.comment or "",
                "kind": _metric_kind(c),
            }
            for c in t.columns or []
        ],
    }


def _metric_kind(col):
    """'dimension' or 'measure' for a metric view column (from its type_json metadata), else ''."""
    meta = json.loads(col.type_json or "{}").get("metadata", {})
    return meta.get("metric_view.type", "")


# --- Import / query ----------------------------------------------------------

_CASTS = {"Number": "DOUBLE", "Text": "STRING", "Date": "DATE", "Datetime": "TIMESTAMP"}
_OPS = {
    "=": "{c} = {v}",
    "≠": "{c} <> {v}",
    ">": "{c} > {v}",
    "≥": "{c} >= {v}",
    "<": "{c} < {v}",
    "≤": "{c} <= {v}",
    "contains": "contains(CAST({c} AS STRING), {v})",
    "starts with": "startswith(CAST({c} AS STRING), {v})",
    "in list": "{c} IN ({vs})",
    "is empty": "({c} IS NULL OR CAST({c} AS STRING) = '')",
    "is not empty": "({c} IS NOT NULL AND CAST({c} AS STRING) <> '')",
}


def _quote(name):
    return "`" + name.replace("`", "``") + "`"


def _lit(value):
    # Always a string literal; the warehouse casts it to the column's type (and errors if it can't).
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def build_sql(full_name, columns, filters, limit, measures=()):
    """columns: [{name, alias, as}] in output order; filters: [{column, op, value}] ANDed.
    measures: a metric view's measure columns. They are aggregated with MEASURE(), rows are
    grouped by the included dimensions, and filters on measures go in HAVING."""
    measures = set(measures)
    agg = lambda name: f"MEASURE({_quote(name)})" if name in measures else _quote(name)
    exprs, seen = [], set()
    for c in columns:
        e = agg(c["name"])
        if c.get("as") in _CASTS:
            e = f"CAST({e} AS {_CASTS[c['as']]})"
        alias = (c.get("alias") or "").strip() or c["name"]
        if alias.lower() in seen:  # JMP column names are case-insensitive
            raise ValueError(
                f"Two columns would both be named '{alias}'. Rename one of them."
            )
        seen.add(alias.lower())
        if alias != c["name"] or e != _quote(
            c["name"]
        ):  # MEASURE() always: its own name is "measure(x)"
            e += f" AS {_quote(alias)}"
        exprs.append(e)
    sql = f"SELECT {', '.join(exprs) or '*'} FROM {'.'.join(_quote(p) for p in full_name.split('.'))}"
    where, having = [], []
    for f in filters:
        value = str(f.get("value", "")).strip()
        items = [v.strip() for v in value.split(",") if v.strip()]
        op = _OPS[f["op"]]
        if ("{v}" in op and not value) or ("{vs}" in op and not items):
            continue  # a row with no value is ignored rather than sent as ''
        (having if f["column"] in measures else where).append(
            op.format(
                c=agg(f["column"]),
                v=_lit(value),
                vs=", ".join(_lit(v) for v in items),
            )
        )
    if where:
        sql += " WHERE " + " AND ".join(where)
    if measures:  # without it a metric view returns raw source rows, not one row per dimension group
        sql += " GROUP BY ALL"
    if having:
        sql += " HAVING " + " AND ".join(having)
    if int(limit) > 0:
        sql += f" LIMIT {int(limit)}"
    return sql


_QUERY = re.compile(
    r"\s*(?:--[^\r\n]*[\r\n]+\s*)*(select|with|from|values|table)\b", re.IGNORECASE
)


def limit_sql(sql, limit):
    """Cap user-written SQL without editing it. SHOW, DESCRIBE etc. can't be a subquery, so they run as written."""
    sql = sql.strip().rstrip(";")
    if int(limit) <= 0 or not _QUERY.match(sql):
        return sql
    # Newline before ")" so a trailing "-- comment" can't swallow it.
    return f"SELECT * FROM ({sql}\n) LIMIT {int(limit)}"


_conns = {}  # (host, warehouse_id) -> open connection; opening a session costs ~0.3 s


def _close_conns(host=None):
    for key in [k for k in _conns if host in (None, k[0])]:
        with contextlib.suppress(Exception):  # already dead; nothing to clean up
            _conns.pop(key).close()


def disconnect():
    """Switch workspace: forget the window's client and close its warehouse sessions."""
    global _ws
    if _ws:
        _close_conns(_ws.config.host)
        _clients.pop(_ws.config.host, None)
    _ws = None
    return {}


def _query(w, warehouse_id, sql, max_rows, params=None):
    from databricks import sql as dbsql

    key = (w.config.host, warehouse_id)
    if key not in _conns:
        cfg = w.config
        _conns[key] = dbsql.connect(
            server_hostname=urlparse(cfg.host).netloc,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            credentials_provider=lambda: cfg.authenticate,  # reuse the SDK sign-in
            user_agent_entry=f"Databricks Connector - JMP {VERSION}",  # backend usage tracking
        )
    with _conns[key].cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchmany_arrow(max_rows) if max_rows else cur.fetchall_arrow()


def run_sql(warehouse_id, sql, max_rows=0, w=None):
    """Run on the warehouse and return a pyarrow.Table (the connector downloads result chunks in parallel)."""
    w = w or _ws
    key = (w.config.host, warehouse_id)
    t = time.perf_counter()
    reused = key in _conns
    try:
        tbl = _query(w, warehouse_id, sql, max_rows)
    except Exception:
        _conns.pop(key, None)
        if not reused:
            raise
        log.info(
            "cached connection failed; retrying on a new one"
        )  # e.g. the session expired
        tbl = _query(w, warehouse_id, sql, max_rows)
    log.info(
        "fetched %d rows x %d cols in %.2fs",
        tbl.num_rows,
        tbl.num_columns,
        time.perf_counter() - t,
    )
    _remember_warehouse(
        w.config.host, warehouse_id
    )  # the picker defaults to it next time
    return tbl


preview_values = []  # read by JSL with Python Get: JSON round-trips turned some text cells into non-strings


def preview(warehouse_id, sql, rows=100):
    global preview_values
    tbl = run_sql(warehouse_id, limit_sql(sql, rows), max_rows=rows)
    preview_values = [
        ["" if v is None else str(v) for v in col.to_pylist()] for col in tbl.columns
    ]
    return {"columns": tbl.column_names, "rows": tbl.num_rows}


# --- Genie -------------------------------------------------------------------


def genie_spaces():
    """Genie spaces you can use, by title: [{id, title}]."""
    spaces, token = [], None
    while True:
        r = _ws.genie.list_spaces(page_token=token)
        spaces += r.spaces or []
        token = r.next_page_token
        if not token:
            break
    return sorted(
        ({"id": s.space_id, "title": s.title or s.space_id} for s in spaces),
        key=lambda s: s["title"].lower(),
    )


def _genie_sql(query):
    """Genie's SQL with its :parameters filled in, so it runs anywhere (preview, import, refresh)."""
    sql = query.query or ""
    for p in query.parameters or []:
        # shortcut: values go in as string literals the warehouse casts (Genie's sql_type is not applied),
        # and a ":name" inside a quoted string would be replaced too; parse the SQL if that shows up.
        lit = _lit(p.value or "")
        sql = re.sub(
            rf"(?<![:\w]):{re.escape(p.keyword)}\b", lambda _, lit=lit: lit, sql
        )
    return sql


_GENIE_TIMEOUT = datetime.timedelta(
    minutes=3
)  # JMP waits on this call; the SDK default is 20 min


def genie_ask(space_id, question, conversation_id=""):
    """Ask Genie (a follow-up when conversation_id is set). Genie's own query result is not used:
    the JSL runs the SQL on the warehouse picked in the window."""
    if conversation_id:
        m = _ws.genie.create_message_and_wait(
            space_id, conversation_id, question, timeout=_GENIE_TIMEOUT
        )
    else:
        m = _ws.genie.start_conversation_and_wait(
            space_id, question, timeout=_GENIE_TIMEOUT
        )
    text, sql, title, suggestions = [], "", "", []
    for a in m.attachments or []:
        if a.text and a.text.content:
            text.append(a.text.content)
        if a.query and a.query.query:
            sql, title = _genie_sql(a.query), a.query.title or ""
            if a.query.description:
                text.append(a.query.description)
        if a.suggested_questions:
            suggestions += a.suggested_questions.questions or []
    if m.error and m.error.error:
        text.append(m.error.error)
    return {
        "conversation_id": m.conversation_id,
        "text": text or ["Genie didn't return an answer."],
        "sql": sql,
        "title": title,
        "suggestions": suggestions,
    }


def convert(col):
    """pyarrow column -> (JMP-ready list, kind). kind in num | date | datetime | char."""
    import pyarrow as pa

    t = col.type
    if pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t):
        # shortcut: BIGINT/DECIMAL become doubles, so >15 significant digits lose precision.
        return _floats(col.cast(pa.float64(), safe=False)), "num"
    if pa.types.is_date32(t):
        return _floats(col.cast(pa.int32()), 86400, _EPOCH_OFFSET), "date"
    if pa.types.is_timestamp(t):  # stored as UTC epoch, so tz-aware values land in UTC
        scale = 1 / {"s": 1, "ms": 1e3, "us": 1e6, "ns": 1e9}[t.unit]
        return _floats(col.cast(pa.int64()), scale, _EPOCH_OFFSET), "datetime"
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_boolean(t):
        return [
            "" if v is None else v for v in col.cast(pa.string()).to_pylist()
        ], "char"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return ["" if v is None else str(v) for v in col.to_pylist()], "char"
    return [
        "" if v is None else json.dumps(v, default=str) for v in col.to_pylist()
    ], "char"  # arrays, maps, structs


def _floats(col, scale=1, offset=0):
    import pyarrow as pa

    arr = col.cast(pa.float64()).to_numpy()  # nulls -> NaN, which JMP reads as missing
    return (arr * scale + offset).tolist()


def import_query(warehouse_id, sql, name):
    """Run the query and build a new JMP data table from the result."""
    import jmp

    tbl = run_sql(warehouse_id, sql)
    return _fill(jmp.DataTable(name, tbl.num_rows), tbl)


def refresh_table(table, host, warehouse_id, sql, sign_in):
    """Re-run a table's saved query into it. Rows are replaced only after the query succeeds."""
    import jmp

    if not (host and warehouse_id and sql):
        raise ValueError(
            "This table has no saved Databricks query. Import it again with the Databricks Connector."
        )
    w = _workspace(host, sign_in)
    tbl = run_sql(warehouse_id, sql, w=w)
    dt = jmp.table(table)
    converted = [convert(c) for c in tbl.columns]
    existing = {dt[i].name: dt[i].dtype for i in range(dt.ncols)}
    for name, (_, kind) in zip(tbl.column_names, converted, strict=True):
        if name in existing and existing[name] != _dtype(jmp, kind):
            raise ValueError(
                f"Column '{name}' changed type in Databricks. Import the table again."
            )
    # Replace rows only; columns stay, so your formula columns, scripts and graphs keep working.
    jsl_name = table.replace("\\", "\\!\\").replace('"', '\\!"')  # JSL string escapes
    jmp.run_jsl(
        f'dt = Data Table("{jsl_name}"); If(N Rows(dt), dt << Delete Rows(1 :: N Rows(dt)));'
    )
    if tbl.num_rows:
        dt.add_rows(tbl.num_rows)
    return _fill(dt, tbl, converted)


def _dtype(jmp, kind):
    return jmp.DataType.Character if kind == "char" else jmp.DataType.Numeric


def _fill(dt, tbl, converted=None):
    """Write each result column into dt (by name, adding missing columns). dt must have tbl.num_rows rows."""
    import jmp

    t = time.perf_counter()
    converted = converted or [convert(c) for c in tbl.columns]
    index = {dt[i].name: i for i in range(dt.ncols)}
    kinds = {}
    for name, (values, kind) in zip(tbl.column_names, converted, strict=True):
        if name not in index:
            dt.new_column(name, _dtype(jmp, kind))
            index[name] = dt.ncols - 1
        dt[index[name]] = values
        kinds[name] = kind
    log.info("built JMP table in %.2fs", time.perf_counter() - t)
    return {
        "table": dt.name,
        "rows": tbl.num_rows,
        "date_cols": [c for c, k in kinds.items() if k == "date"],
        "datetime_cols": [c for c, k in kinds.items() if k == "datetime"],
    }


# --- Publish to Unity Catalog ------------------------------------------------

PUBLISH_MODES = ("create", "append", "overwrite", "upsert")
PUBLISH_TYPES = ("DOUBLE", "BIGINT", "DATE", "TIMESTAMP", "STRING")
FAST_WRITE_SUGGEST_ROWS = 10_000
_PARAM_LIMIT = 10_000  # the warehouse rejects statements with more parameters than this


def publish_columns(table, columns):
    """Default target name, type and include flag per JMP column: the grid the user edits."""
    # Only Fixed Dec 0 numbers need their values (the BIGINT whole-number check); don't read the rest.
    names = [
        c["name"]
        for c in columns
        if c["data_type"] == "Numeric" and _fixed0(c.get("format", ""))
    ]
    arrow = _jmp_arrow(table, names)
    return [_column_default(c, arrow) for c in columns]


def target_info(full_name, warehouse_id=""):
    full_name = _resolve_target(_ws, warehouse_id, full_name)
    return {**_target_info(_ws, full_name, warehouse_id), "full_name": full_name}


def _resolve_target(w, warehouse_id, name):
    """table or schema.table -> catalog.schema.table, using the warehouse's defaults (the workspace default catalog)."""
    parts = name.split(".")
    if len(parts) < 3 and all(p.strip() for p in parts):
        if not warehouse_id:
            raise ValueError(
                "Choose a SQL warehouse, or enter the full name: catalog.schema.table."
            )
        d = run_sql(
            warehouse_id, "SELECT current_catalog() AS c, current_schema() AS s", w=w
        ).to_pylist()[0]
        name = ".".join([d["c"], d["s"]][: 3 - len(parts)] + parts)
    _check_target(name)
    return name


def _check_target(name):
    if len(parts := name.split(".")) != 3 or not all(p.strip() for p in parts):
        raise ValueError(
            f"Target '{name}' must be a full name: catalog.schema.table, e.g. main.default.{parts[-1] or 'my_table'}."
        )
    if parts[0].lower() == "hive_metastore":
        raise ValueError(
            f"`{name}` is in the Hive metastore. Publish writes to Unity Catalog only: enter a Unity Catalog catalog."
        )


def publish(
    table,
    target,
    mode="create",
    columns=None,
    keys=(),
    fast_write=False,
    volume="",
    comment="",
    include_excluded=False,
    excluded_rows=(),
    dry_run=False,
    warehouse_id="",
    host="",
    sign_in="",
):
    """Write a JMP table to a Unity Catalog table. Reads the JMP table; publish_arrow does the rest."""
    w = _workspace(host, sign_in) if host else _ws
    cols = columns or publish_columns(table, _jmp_columns(table))
    names = [c["name"] for c in cols if c["include"]]
    arrow = _jmp_arrow(table, names, () if include_excluded else excluded_rows)
    return publish_arrow(
        arrow,
        w,
        warehouse_id,
        target,
        mode,
        cols,
        keys,
        fast_write,
        volume,
        comment,
        dry_run,
    )


def publish_arrow(
    arrow,
    w,
    warehouse_id,
    target,
    mode,
    cols,
    keys=(),
    fast_write=False,
    volume="",
    comment="",
    dry_run=False,
):
    """Core of publish. arrow holds the JMP columns by JMP name; cols says how each one lands."""
    import pyarrow as pa

    cols = [c for c in cols if c["include"]]
    keys = list(keys)
    targets = [c["target"] for c in cols]
    if mode not in PUBLISH_MODES:
        raise ValueError(f"Mode must be one of {', '.join(PUBLISH_MODES)}.")
    if not warehouse_id:
        raise ValueError("Choose a SQL warehouse.")
    target = _resolve_target(w, warehouse_id, target)
    if not cols:
        raise ValueError("No columns are included.")
    bad_types = sorted({c["type"] for c in cols if c["type"] not in PUBLISH_TYPES})
    if bad_types:  # types are pasted into SQL, and a recipe can be edited inside a shared .jmp file
        raise ValueError(f"Unsupported column type(s): {', '.join(bad_types)}.")
    dupes = sorted({t for t in targets if targets.count(t) > 1})
    if dupes:
        raise ValueError(
            f"Target name(s) used by more than one column: {', '.join(dupes)}. Rename them."
        )
    if fast_write and not volume:
        raise ValueError(
            "Fast write needs a volume, e.g. /Volumes/catalog/schema/volume/."
        )
    if mode == "upsert":
        if not keys:
            raise ValueError("Upsert needs at least one key column.")
        unknown = [k for k in keys if k not in targets]
        if unknown:
            raise ValueError(
                f"Key column(s) {', '.join(unknown)} are not included columns."
            )
    info = _target_info(w, target, warehouse_id)
    if mode != "create" and not info["exists"]:
        raise ValueError(f"`{target}` does not exist. Use create to make it.")
    existing = {c["name"] for c in info["columns"]}
    missing = [t for t in targets if info["exists"] and t not in existing]
    if missing:
        raise ValueError(
            f"`{target}` has no column(s) {', '.join(missing)}. Rename the columns to match, or publish to a new table."
        )
    tbl = pa.table({c["target"]: _typed(arrow[c["name"]], c) for c in cols})
    if mode == "upsert":
        unique = tbl.group_by(keys).aggregate([]).num_rows
        if unique < tbl.num_rows:
            raise ValueError(
                f"Upsert keys are not unique: {tbl.num_rows - unique:,} rows repeat a key ({', '.join(keys)})."
            )

    n = tbl.num_rows
    pid = uuid.uuid4().hex[:12]
    pre = f"/* jmp-publish v1 {pid} */ "
    qt = _qname(target)
    warns = []
    if mode == "create" and info["exists"]:
        warns.append(f"`{target}` already exists, so the rows will be appended to it.")
    if n > FAST_WRITE_SUGGEST_ROWS and not fast_write:
        warns.append(f"{n:,} rows: Fast write is much faster for a table this large.")
    setup = [(pre + _create_sql(qt, cols, comment), None)] if mode == "create" else []
    if fast_write:
        path = f"{volume.rstrip('/')}/jmp_tmp_{pid}.parquet"
        src = f"SELECT {_col_list(cols)} FROM read_files({_lit(path)}, format => 'parquet')"
        load = [
            (
                pre
                + _load_sql(mode, qt, cols, keys, src, overwrite=mode == "overwrite"),
                None,
            )
        ]
    else:
        load = _default_stmts(tbl, cols, mode, qt, keys, pre)
    version_before = _version(w, warehouse_id, target, pre) if info["exists"] else -1
    via = f"a Parquet upload to {volume}" if fast_write else "the SQL connector"
    now = (
        f" (currently {info['rows']:,} rows)"
        if info["exists"] and info["rows"] >= 0
        else ""
    )
    result = {
        "publish_id": pid,
        "target": target,
        "summary": f"{mode.capitalize()} {n:,} rows and {len(cols)} columns to `{target}`{now} using {via}.",
        "warnings": warns,
        "sql": list(dict.fromkeys(s for s, _ in setup + load)),
        "rows_written": 0,
        "version_before": version_before,
        "table_version": -1,
        "url": f"{w.config.host}/explore/data/{target.replace('.', '/')}",
        "dry_run": dry_run,
    }
    if dry_run:
        return result

    if fast_write:
        written = _fast_write(w, warehouse_id, tbl, path, setup + load, warns)
    else:
        for sql, _ in setup:
            _exec(w, warehouse_id, sql)
        written = 0
        try:
            for sql, params in load:
                written += _exec(w, warehouse_id, sql, params)
        except Exception as e:
            undo = (
                f"RESTORE TABLE {qt} TO VERSION AS OF {version_before}"
                if version_before >= 0
                else f"DROP TABLE {qt}"  # this publish created it
            )
            raise RuntimeError(
                f"Publish stopped after {written:,} of {n:,} rows: {e}. To undo the rows written, run: {undo}"
            ) from e
    result["rows_written"] = written
    result["table_version"] = _version(w, warehouse_id, target, pre)
    if written != n:
        warns.append(
            f"Databricks reported {written:,} rows written, but the JMP table has {n:,}. Check `{target}` at version {result['table_version']}."
        )
    return result


def _jmp_arrow(table, names, excluded=()):
    """Named JMP columns as a pyarrow Table. Missing numbers and empty text become NULL; excluded are 1-based row numbers."""
    import jmp
    import numpy as np
    import pyarrow as pa

    dt = jmp.table(table)
    keep = np.ones(dt.nrows, bool)
    keep[np.array(excluded, int) - 1] = False
    cols = {}
    for i in range(dt.ncols):
        col = dt[i]
        if col.name not in names:
            continue
        if col.dtype == jmp.DataType.Numeric:
            v = np.array(col, float)[keep]
            cols[col.name] = pa.array(v, mask=np.isnan(v))
        else:
            cols[col.name] = pa.array(
                [v or None for v, k in zip(col, keep) if k], pa.string()
            )
    return pa.table(cols)


def _jmp_columns(table):
    """JMP column names and kinds. Python can't read a column's format, so date detection needs publish_columns input."""
    import jmp

    dt = jmp.table(table)
    cols = [dt[i] for i in range(dt.ncols)]
    return [
        {
            "name": c.name,
            "data_type": "Numeric" if c.dtype == jmp.DataType.Numeric else "Character",
            "format": "",
            "notes": "",
        }
        for c in cols
    ]


def _column_default(c, arrow):
    out = {
        "name": c["name"],
        "target": re.sub(r"\W+", "_", c["name"]).strip("_").lower(),
        "comment": c.get("notes", ""),
    }
    if c["data_type"] == "Character":
        return out | {
            "type": "STRING",
            "choices": ["STRING"],
            "include": True,
            "dropped": "",
        }
    if c["data_type"] != "Numeric":
        return out | {
            "type": "",
            "choices": [],
            "include": False,
            "dropped": f"{c['data_type']} columns are not written",
        }
    fmt = c.get("format", "")
    kind = _date_type(fmt)
    if kind is None:
        whole = _fixed0(fmt) and not _bad_bigint(arrow[c["name"]].to_numpy()).any()
        kind = "BIGINT" if whole else "DOUBLE"
    return out | {
        "type": kind,
        "choices": list(PUBLISH_TYPES),
        "include": True,
        "dropped": "",
    }


def _fixed0(fmt):
    return bool(re.search(r"fixed dec.*,\s*\d+\s*,\s*0\s*\)", fmt.lower()))


def _date_type(fmt):
    """Delta type for a JMP date or date-time format string, or None for a plain number."""
    f = fmt.lower()
    if re.search(r"time|hh|:", f):
        return "TIMESTAMP"
    if re.search(r"date|yy|mmm|month|[ymd][/-]|[/-][ymd]", f):
        return "DATE"
    return None


def _bad_bigint(v):
    """Mask of values that can't be BIGINT. Missing (NaN) values are not bad."""
    import numpy as np

    return ~np.isnan(v) & ((v != np.floor(v)) | (np.abs(v) >= 2.0**63))


def _typed(col, c):
    """A JMP column cast to its chosen Delta type. Raises ValueError when values don't fit."""
    import numpy as np
    import pyarrow as pa

    kind = c["type"]
    if not pa.types.is_floating(col.type):
        if kind != "STRING":
            raise ValueError(f"'{c['name']}' is text, so it can only be STRING.")
        return col
    if kind == "DOUBLE":
        return col
    if kind == "STRING":
        return col.cast(pa.string())
    v = col.to_numpy()  # NaN = missing
    miss = np.isnan(v)
    if kind == "BIGINT":
        bad = _bad_bigint(v).sum()
        if bad:
            raise ValueError(
                f"'{c['name']}' has {bad:,} values that are not whole numbers in BIGINT range. Choose DOUBLE for it."
            )
        return pa.array(np.where(miss, 0, v).astype(np.int64), mask=miss)
    if kind == "DATE":
        days = np.floor((v - _EPOCH_OFFSET) / 86400)
        return pa.array(np.where(miss, 0, days).astype(np.int32), mask=miss).cast(
            pa.date32()
        )
    micros = np.round((v - _EPOCH_OFFSET) * 1e6)  # TIMESTAMP is UTC, like JMP's epoch
    return pa.array(np.where(miss, 0, micros).astype(np.int64), mask=miss).cast(
        pa.timestamp("us", tz="UTC")
    )


def _target_info(w, full_name, warehouse_id=""):
    from databricks.sdk.errors import NotFound

    try:
        t = w.tables.get(full_name)
    except NotFound:
        return {"exists": False, "owner": "", "comment": "", "rows": -1, "columns": []}
    rows = -1
    if warehouse_id:
        rows = (
            run_sql(warehouse_id, f"SELECT count(*) AS n FROM {_qname(full_name)}", w=w)
            .column("n")[0]
            .as_py()
        )
    return {
        "exists": True,
        "owner": t.owner or "",
        "comment": t.comment or "",
        "rows": rows,
        "columns": [
            {"name": c.name, "type": c.type_text or ""} for c in t.columns or []
        ],
    }


def _version(w, warehouse_id, target, pre):
    return (
        run_sql(warehouse_id, f"{pre}DESCRIBE HISTORY {_qname(target)} LIMIT 1", w=w)
        .column("version")[0]
        .as_py()
    )


def _exec(w, warehouse_id, sql, params=None):
    """Run one write statement and return the rows it affected (0 for DDL). Never retried: a retry could write twice."""
    key = (w.config.host, warehouse_id)
    try:
        tbl = _query(w, warehouse_id, sql, 0, params)
    except Exception:
        _conns.pop(key, None)  # a dead session would fail every later publish
        raise
    if "num_affected_rows" not in tbl.column_names:
        return 0
    return tbl.column("num_affected_rows")[0].as_py() or 0


def _fast_write(w, warehouse_id, tbl, path, stmts, warns):
    import io

    import pyarrow.parquet as pq
    from databricks.sdk.errors import NotFound

    buf = io.BytesIO()
    pq.write_table(tbl, buf)
    buf.seek(0)
    try:
        w.files.upload(path, buf, overwrite=True)
        return sum(_exec(w, warehouse_id, sql, params) for sql, params in stmts)
    finally:
        try:
            w.files.delete(path)
        except NotFound:
            pass  # the upload never finished
        except Exception:  # noqa: BLE001 - a failed cleanup must not hide the write's result
            log.warning(
                "could not delete temporary file %s", path
            )  # kept even if the write failed too
            warns.append(
                f"Could not delete the temporary file {path}. Delete it by hand."
            )


def _qname(full_name):
    return ".".join(_quote(p) for p in full_name.split("."))


def _col_list(cols):
    return ", ".join(_quote(c["target"]) for c in cols)


def _create_sql(qt, cols, comment):
    defs = [
        f"{_quote(c['target'])} {c['type']}"
        + (f" COMMENT {_lit(c['comment'])}" if c["comment"] else "")
        for c in cols
    ]
    tail = f" COMMENT {_lit(comment)}" if comment else ""
    return f"CREATE TABLE IF NOT EXISTS {qt} ({', '.join(defs)}){tail}"


def _load_sql(mode, qt, cols, keys, src, overwrite):
    """src is a VALUES list or a SELECT. Overwrite uses INSERT OVERWRITE only for the first batch."""
    if mode == "upsert":
        return _merge_sql(qt, cols, keys, src)
    verb = "INSERT OVERWRITE" if overwrite else "INSERT INTO"
    return f"{verb} {qt} ({_col_list(cols)}) {src}"


def _merge_sql(qt, cols, keys, src):
    names = [c["target"] for c in cols]
    on = " AND ".join(f"t.{_quote(k)} <=> s.{_quote(k)}" for k in keys)
    sets = [f"t.{_quote(n)} = s.{_quote(n)}" for n in names if n not in keys]
    matched = f" WHEN MATCHED THEN UPDATE SET {', '.join(sets)}" if sets else ""
    vals = ", ".join(f"s.{_quote(n)}" for n in names)
    return (
        f"MERGE INTO {qt} AS t USING ({src}) AS s ON {on}{matched}"
        f" WHEN NOT MATCHED THEN INSERT ({_col_list(cols)}) VALUES ({vals})"
    )


def _default_stmts(tbl, cols, mode, qt, keys, pre):
    """(sql, params) per batch on the default path. Each batch holds max(1, 10_000 // columns) rows."""
    per = max(1, _PARAM_LIMIT // len(cols))
    rows = list(zip(*(col.to_pylist() for col in tbl.columns)))
    if mode == "overwrite" and not rows:
        return [(pre + f"TRUNCATE TABLE {qt}", None)]
    out = []
    for i in range(0, len(rows), per):
        batch = rows[i : i + per]
        marks = ", ".join(["(" + ", ".join(["?"] * len(cols)) + ")"] * len(batch))
        if mode == "upsert":
            typed = ", ".join(
                f"CAST({_quote(c['target'])} AS {c['type']}) AS {_quote(c['target'])}"
                for c in cols
            )
            src = f"SELECT {typed} FROM VALUES {marks} AS s({_col_list(cols)})"
        else:
            src = f"VALUES {marks}"
        sql = _load_sql(
            mode, qt, cols, keys, src, overwrite=mode == "overwrite" and i == 0
        )
        out.append((pre + sql, [v for row in batch for v in row]))
    return out
