"""Databricks bridge for the JMP add-in.

databricks_connector.jsl calls dispatch(fn, args_json) and reads back a JSON
envelope, so Python errors surface in the UI instead of only in the JMP log.
"""

import configparser
import contextlib
import json
import logging
import os
import re
import sys
import tempfile
import time
from urllib.parse import urlparse

LOG_PATH = os.path.join(
    os.path.expanduser("~/Library/Logs")
    if sys.platform == "darwin"
    else tempfile.gettempdir(),
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
        return WorkspaceClient(profile=profile, host=host)
    if method == "oauth":
        return WorkspaceClient(host=host, auth_type="external-browser")
    return WorkspaceClient(host=host, token=token, auth_type="pat")


def connect(method, profile="", host="", token=""):
    global _ws

    if method == "profile":
        host = next(p["host"] for p in profiles() if p["name"] == profile)
    w = _client(method, profile, _clean_host(host), token)
    me = w.current_user.me()  # fail fast on bad credentials
    _close_conns(w.config.host)
    _clients[w.config.host] = _ws = w
    return {"user": me.user_name or me.display_name or "", "host": w.config.host}


# --- Browse ------------------------------------------------------------------


def warehouses():
    whs = sorted(
        _ws.warehouses.list(),
        key=lambda w: (w.state.value != "RUNNING", w.name.lower()),
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
            {"name": c.name, "type": c.type_text or "", "comment": c.comment or ""}
            for c in t.columns or []
        ],
    }


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


def build_sql(full_name, columns, filters, limit):
    """columns: [{name, alias, as}] in output order; filters: [{column, op, value}] ANDed."""
    exprs, seen = [], set()
    for c in columns:
        e = _quote(c["name"])
        if c.get("as") in _CASTS:
            e = f"CAST({e} AS {_CASTS[c['as']]})"
        alias = (c.get("alias") or "").strip() or c["name"]
        if alias.lower() in seen:  # JMP column names are case-insensitive
            raise ValueError(
                f"Two columns would both be named '{alias}'. Rename one of them."
            )
        seen.add(alias.lower())
        if alias != c["name"] or e != _quote(c["name"]):
            e += f" AS {_quote(alias)}"
        exprs.append(e)
    sql = f"SELECT {', '.join(exprs) or '*'} FROM {'.'.join(_quote(p) for p in full_name.split('.'))}"
    where = []
    for f in filters:
        value = str(f.get("value", "")).strip()
        items = [v.strip() for v in value.split(",") if v.strip()]
        op = _OPS[f["op"]]
        if ("{v}" in op and not value) or ("{vs}" in op and not items):
            continue  # a row with no value is ignored rather than sent as ''
        where.append(
            op.format(
                c=_quote(f["column"]),
                v=_lit(value),
                vs=", ".join(_lit(v) for v in items),
            )
        )
    if where:
        sql += " WHERE " + " AND ".join(where)
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


def _query(w, warehouse_id, sql, max_rows):
    from databricks import sql as dbsql

    key = (w.config.host, warehouse_id)
    if key not in _conns:
        cfg = w.config
        _conns[key] = dbsql.connect(
            server_hostname=urlparse(cfg.host).netloc,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            credentials_provider=lambda: cfg.authenticate,  # reuse the SDK sign-in
        )
    with _conns[key].cursor() as cur:
        cur.execute(sql)
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
    return tbl


preview_values = []  # read by JSL with Python Get: JSON round-trips turned some text cells into non-strings


def preview(warehouse_id, sql, rows=100):
    global preview_values
    tbl = run_sql(warehouse_id, limit_sql(sql, rows), max_rows=rows)
    preview_values = [
        ["" if v is None else str(v) for v in col.to_pylist()] for col in tbl.columns
    ]
    return {"columns": tbl.column_names, "rows": tbl.num_rows}


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
    host = _clean_host(host)
    w = _clients.get(host)
    if w is None:
        method, _, profile = sign_in.partition(":")
        if method not in ("profile", "oauth"):  # PATs are never saved with a table
            raise PermissionError(
                f"Not signed in to {host}. Sign in with the Databricks Connector."
            )
        w = _client(method, profile, host)
        w.current_user.me()  # fail before touching the table if the sign-in is no longer valid
        _clients[host] = w
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
