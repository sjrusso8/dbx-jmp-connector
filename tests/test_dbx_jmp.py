import json
import math
import re
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "addin"))
import dbx_jmp
import pytest


@pytest.fixture(autouse=True)
def _no_home_writes(monkeypatch, tmp_path):
    """run_sql remembers the warehouse in a file under ~; keep tests out of the real one."""
    monkeypatch.setattr(
        dbx_jmp, "LAST_WAREHOUSE_PATH", str(tmp_path / "last_warehouse.json")
    )


def test_build_sql_columns_and_filters():
    sql = dbx_jmp.build_sql(
        "main.sales.or`ders",
        [
            {"name": "id", "alias": "", "as": "Auto"},
            {"name": "total", "alias": "Total $", "as": "Number"},
            {"name": "region", "alias": "region", "as": "Auto"},
        ],
        [
            {"column": "region", "op": "in list", "value": "EU, O'Neil"},
            {"column": "note", "op": "contains", "value": "a\\b"},
            {"column": "id", "op": "is not empty", "value": ""},
        ],
        10,
    )
    assert sql == (
        "SELECT `id`, CAST(`total` AS DOUBLE) AS `Total $`, `region` FROM `main`.`sales`.`or``ders`"
        " WHERE `region` IN ('EU', 'O\\'Neil') AND contains(CAST(`note` AS STRING), 'a\\\\b')"
        " AND (`id` IS NOT NULL AND CAST(`id` AS STRING) <> '') LIMIT 10"
    )
    assert dbx_jmp.build_sql("a.b.c", [], [], 0) == "SELECT * FROM `a`.`b`.`c`"


def test_limit_sql_wraps_user_sql():
    assert (
        dbx_jmp.limit_sql("SELECT 1 -- note;\n", 5)
        == "SELECT * FROM (SELECT 1 -- note\n) LIMIT 5"
    )
    assert dbx_jmp.limit_sql("SHOW TABLES", 0) == "SHOW TABLES"
    assert dbx_jmp.limit_sql("SHOW TABLES IN x", 5) == "SHOW TABLES IN x"
    assert dbx_jmp.limit_sql(
        "-- top\r with t as (select 1) select * from t", 5
    ).startswith("SELECT * FROM (-- top")


def test_convert_types():
    import datetime as dt
    from decimal import Decimal

    import pyarrow as pa

    def conv(values, typ):
        return dbx_jmp.convert(pa.chunked_array([pa.array(values, typ)]))

    vals, kind = conv([Decimal("1.50"), None], pa.decimal128(5, 2))
    assert kind == "num" and vals[0] == 1.5 and math.isnan(vals[1])
    assert conv([dt.date(1904, 1, 2)], pa.date32()) == ([86400.0], "date")
    vals, kind = conv(
        [dt.datetime(1904, 1, 1, 0, 0, 1, tzinfo=dt.UTC), None],
        pa.timestamp("us", tz="UTC"),
    )
    assert kind == "datetime" and vals[0] == 1.0 and math.isnan(vals[1])
    assert conv(["x", None], pa.string()) == (["x", ""], "char")
    assert conv([True], pa.bool_()) == (["true"], "char")
    assert conv([[1, 2]], pa.list_(pa.int64())) == (["[1, 2]"], "char")
    assert conv([b"ab"], pa.binary()) == (["b'ab'"], "char")


def test_profiles_skips_account_profiles(tmp_path, monkeypatch):
    cfg = tmp_path / "cfg"
    cfg.write_text(
        "[ws]\nhost = https://ws.cloud.databricks.com\n"
        "[ws2]\nhost = https://ws2.cloud.databricks.com\naccount_id = 1\n"
        "[acct]\nhost = https://accounts.cloud.databricks.com\naccount_id = 1\n"
    )
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfg))
    assert [p["name"] for p in dbx_jmp.profiles()] == ["ws", "ws2"]


def test_dispatch_wraps_errors():
    assert json.loads(dbx_jmp.dispatch("build_sql", "{}"))["ok"] is False


def test_connect_strips_url_query(monkeypatch):
    import databricks.sdk

    seen = {}

    class FakeClient:
        def __init__(self, **kw):
            seen.update(kw)
            self.config = type("C", (), {"host": kw["host"]})
            self.current_user = type(
                "U", (), {"me": lambda: type("M", (), {"user_name": "me"})}
            )

    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", FakeClient)
    dbx_jmp.connect("pat", host="https://x.cloud.databricks.com/?o=123", token="t")
    assert seen["host"] == "https://x.cloud.databricks.com"


def test_saved_profile_never_signs_in_to_another_host(monkeypatch):
    import databricks.sdk

    calls = []

    class FakeClient:
        def __init__(self, **kw):
            assert "host" not in kw  # an explicit host would override the profile's
            self.config = types.SimpleNamespace(
                host="https://mine.cloud.databricks.com"
            )
            self.current_user = types.SimpleNamespace(me=lambda: calls.append("me"))

    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", FakeClient)
    monkeypatch.setattr(dbx_jmp, "_clients", {})
    with pytest.raises(PermissionError, match="not https://evil.example.com"):
        dbx_jmp._workspace("https://evil.example.com/", "profile:DEFAULT")
    assert calls == []  # the token was never sent

    dbx_jmp._workspace("https://mine.cloud.databricks.com", "profile:DEFAULT")
    assert calls == ["me"]


def test_build_sql_skips_blank_filters_and_rejects_duplicate_names():
    import pytest

    cols = [{"name": "a", "alias": "", "as": "Auto"}]
    blank = [
        {"column": "a", "op": "=", "value": "  "},
        {"column": "a", "op": "in list", "value": " , "},
        {"column": "a", "op": "is empty", "value": ""},
    ]
    assert dbx_jmp.build_sql("x.y.z", cols, blank, 0) == (
        "SELECT `a` FROM `x`.`y`.`z` WHERE (`a` IS NULL OR CAST(`a` AS STRING) = '')"
    )
    with pytest.raises(ValueError, match="both be named"):
        dbx_jmp.build_sql(
            "x.y.z", cols + [{"name": "b", "alias": "A", "as": "Auto"}], [], 0
        )


def test_jsl_brackets_balance():
    """No JMP in CI, so at least catch unbalanced (), {}, [] in the add-in's JSL."""
    src = "\n".join(
        f.read_text() for f in (Path(__file__).parents[1] / "addin").glob("*.jsl")
    )
    pairs, stack, i, line = {")": "(", "}": "{", "]": "["}, [], 0, 1
    while i < len(src):
        ch = src[i]
        if src.startswith('"\\[', i):  # raw string "\[ ... ]\"
            end = src.index(']\\"', i)
            line += src.count("\n", i, end)
            i = end + 3
            continue
        if ch == '"':  # string; \! escapes the next character
            i += 1
            while src[i] != '"':
                i += 2 if src.startswith("\\!", i) else 1
        elif src.startswith("//", i):
            i = src.index("\n", i) - 1
        elif src.startswith("/*", i):
            end = src.index("*/", i)
            line += src.count("\n", i, end)
            i = end + 1
        elif ch == "\n":
            line += 1
        elif ch in "({[":
            stack.append((ch, line))
        elif ch in pairs:
            assert stack and stack[-1][0] == pairs[ch], (
                f"unmatched {ch!r} at line {line}"
            )
            stack.pop()
        i += 1
    assert not stack, f"unclosed {stack[-1][0]!r} opened at line {stack[-1][1]}"


def test_run_sql_reuses_connection_and_retries_stale_one(monkeypatch):
    import databricks.sql
    import pyarrow as pa

    opened = []

    class FakeConn:
        def __init__(self):
            self.dead = False
            opened.append(self)

        def cursor(self):
            conn = self

            class Cur:
                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    pass

                def execute(self, sql, params=None):
                    if conn.dead:
                        raise RuntimeError("session expired")

                def fetchall_arrow(self):
                    return pa.table({"a": [1, 2, 3]})

                def fetchmany_arrow(self, n):
                    return pa.table({"a": [1, 2, 3][:n]})

            return Cur()

        def close(self):
            pass

    def connect(**kw):
        assert kw["user_agent_entry"] == "Databricks Connector - JMP v0.1.0"
        return FakeConn()

    monkeypatch.setattr(databricks.sql, "connect", connect)
    monkeypatch.setattr(
        dbx_jmp,
        "_ws",
        type(
            "W",
            (),
            {"config": type("C", (), {"host": "https://h", "authenticate": None})},
        ),
    )
    dbx_jmp._conns.clear()
    assert dbx_jmp.run_sql("wh", "q").num_rows == 3
    assert dbx_jmp.run_sql("wh", "q", max_rows=2).num_rows == 2
    assert len(opened) == 1  # reused
    opened[0].dead = True
    assert (
        dbx_jmp.run_sql("wh", "q").num_rows == 3
    )  # stale one dropped, retried on a new one
    assert len(opened) == 2


def test_refresh_never_touches_the_table_unless_the_query_succeeds(monkeypatch):
    import types

    import pyarrow as pa
    import pytest

    jsl = []

    class Col:
        def __init__(self, name, dtype):
            self.name, self.dtype = name, dtype

    class FakeTable:
        name = 'My "sales"'

        def __init__(self):
            self.cols = [
                Col("x", "n"),
                Col("my_formula", "n"),
            ]  # imported + one the user added
            self.values = {}

        @property
        def ncols(self):
            return len(self.cols)

        def __getitem__(self, i):
            return self.cols[i]

        def add_rows(self, n):
            self.rows = n

        def new_column(self, name, dtype):
            self.cols.append(Col(name, dtype))

        def __setitem__(self, i, values):
            self.values[self.cols[i].name] = values

    table = FakeTable()
    fake_jmp = types.SimpleNamespace(
        run_jsl=jsl.append,
        table=lambda name: table,
        DataType=types.SimpleNamespace(Character="c", Numeric="n"),
    )
    monkeypatch.setitem(sys.modules, "jmp", fake_jmp)
    monkeypatch.setattr(dbx_jmp, "_clients", {})
    args = {
        "table": 'My "sales"',
        "host": "https://h",
        "warehouse_id": "wh",
        "sql": "SELECT 1",
    }

    with pytest.raises(PermissionError):  # PAT tables need a live sign-in
        dbx_jmp.refresh_table(**args, sign_in="pat")

    dbx_jmp._clients["https://h"] = object()

    def failing(*a, **k):
        raise RuntimeError("401 invalid token")

    monkeypatch.setattr(dbx_jmp, "run_sql", failing)
    with pytest.raises(RuntimeError):
        dbx_jmp.refresh_table(**args, sign_in="pat")
    assert jsl == []  # nothing cleared on failure

    monkeypatch.setattr(
        dbx_jmp, "run_sql", lambda *a, **k: pa.table({"x": ["now text"]})
    )
    with pytest.raises(ValueError, match="changed type"):
        dbx_jmp.refresh_table(**args, sign_in="pat")
    assert jsl == []  # type change caught before any rows were deleted

    monkeypatch.setattr(
        dbx_jmp, "run_sql", lambda *a, **k: pa.table({"x": [1, 2], "y": ["a", "b"]})
    )
    r = dbx_jmp.refresh_table(**args, sign_in="pat")
    assert r["rows"] == 2
    assert 'Data Table("My \\!"sales\\!"")' in jsl[0]  # quotes escaped for JSL
    assert "Delete Columns" not in jsl[0]
    assert [c.name for c in table.cols] == [
        "x",
        "my_formula",
        "y",
    ]  # user column kept, new one added
    assert table.values == {"x": [1.0, 2.0], "y": ["a", "b"]}


# --- Publish to Unity Catalog -------------------------------------------------


class FakeCol:
    def __init__(self, name, dtype, values):
        self.name, self.dtype, self.values = name, dtype, values

    def __array__(self, dtype=None, copy=None):
        import numpy as np

        return np.asarray(self.values, dtype=dtype)

    def __iter__(self):
        return iter(self.values)


def fake_jmp(monkeypatch, cols):
    class Table:
        name = "t"
        nrows = len(cols[0].values)
        ncols = len(cols)

        def __getitem__(self, i):
            return cols[i]

    monkeypatch.setitem(
        sys.modules,
        "jmp",
        types.SimpleNamespace(
            table=lambda name: Table(),
            DataType=types.SimpleNamespace(Numeric="n", Character="c"),
        ),
    )


class FakeWS:
    """Stands in for WorkspaceClient and the SQL connector: records every statement."""

    def __init__(self, existing=None, fail=None, rows=0, fast_rows=5):
        self.existing = existing or {}  # "c.s.t" -> its column names
        self.fail = fail or (lambda sql, params: False)
        self.rows, self.fast_rows = rows, fast_rows
        self.config = types.SimpleNamespace(host="https://h")
        self.tables = types.SimpleNamespace(get=self.get_table)
        self.uploads, self.deletes, self.sql = [], [], []
        self.files = types.SimpleNamespace(
            upload=lambda path, buf, overwrite: self.uploads.append(path),
            delete=self.deletes.append,
        )

    def get_table(self, name):
        from databricks.sdk.errors import NotFound

        if name not in self.existing:
            raise NotFound(name)
        cols = [
            types.SimpleNamespace(name=c, type_text="string", comment="")
            for c in self.existing[name]
        ]
        return types.SimpleNamespace(owner="me", comment="", columns=cols)

    def query(self, sql, params):
        import pyarrow as pa

        self.sql.append((sql, params))
        if self.fail(sql, params):
            raise RuntimeError("boom")
        if "count(*)" in sql:
            return pa.table({"n": [self.rows]})
        if "DESCRIBE HISTORY" in sql:
            return pa.table({"version": [3]})
        if "read_files" in sql:
            return pa.table({"num_affected_rows": [self.fast_rows]})
        if params is None:  # DDL returns no row counts
            return pa.table({"ok": [1]})
        return pa.table({"num_affected_rows": [sql.count("(?")]})


def patch_query(monkeypatch):
    monkeypatch.setattr(
        dbx_jmp, "_query", lambda w, wh, sql, n, params=None: w.query(sql, params)
    )


def writes(w):
    """Write statements without their comment prefix, with their parameters."""
    return [
        (s.split("*/ ", 1)[1], p)
        for s, p in w.sql
        if re.search(r"\*/ (CREATE|INSERT|MERGE|TRUNCATE)", s)
    ]


PUBLISH_COLS = [
    {
        "name": "Lot ID",
        "target": "lot_id",
        "type": "BIGINT",
        "include": True,
        "comment": "lot",
    },
    {
        "name": "Name",
        "target": "name",
        "type": "STRING",
        "include": True,
        "comment": "",
    },
]


def arrow(lots):
    import pyarrow as pa

    return pa.table({"Lot ID": pa.array(lots, pa.float64()), "Name": ["x"] * len(lots)})


def test_publish_columns_maps_types_and_names(monkeypatch):
    nan = float("nan")
    fake_jmp(
        monkeypatch,
        [
            FakeCol("Thickness (um)", "n", [1.5, 2.0, nan]),
            FakeCol("Lot ID", "n", [1.0, 2.0, nan]),
            FakeCol("Count #", "n", [1.5, 2.0, nan]),
            FakeCol("Sampled on", "n", [0.0, 86400.0, nan]),
            FakeCol("Measured at", "n", [0.0, 1.0, nan]),
            FakeCol("Note", "c", ["a", "", "b"]),
        ],
    )
    cols = dbx_jmp.publish_columns(
        "t",
        [
            {
                "name": "Thickness (um)",
                "data_type": "Numeric",
                "format": 'Format("Fixed Dec", 10, 2)',
                "notes": "um",
            },
            {
                "name": "Lot ID",
                "data_type": "Numeric",
                "format": 'Format("Fixed Dec", 10, 0)',
                "notes": "",
            },
            {
                "name": "Count #",
                "data_type": "Numeric",
                "format": 'Format("Fixed Dec", 10, 0)',
                "notes": "",
            },
            {
                "name": "Sampled on",
                "data_type": "Numeric",
                "format": 'Format("yyyy-mm-dd", 10)',
                "notes": "",
            },
            {
                "name": "Measured at",
                "data_type": "Numeric",
                "format": 'Format("yyyy-mm-dd hh:mm:ss", 19)',
                "notes": "",
            },
            {"name": "Note", "data_type": "Character", "format": "", "notes": ""},
            {"name": "Formula", "data_type": "Expression", "format": "", "notes": ""},
        ],
    )
    assert {c["name"]: (c["target"], c["type"], c["include"]) for c in cols} == {
        "Thickness (um)": ("thickness_um", "DOUBLE", True),
        "Lot ID": ("lot_id", "BIGINT", True),
        "Count #": ("count", "DOUBLE", True),  # 1.5 is not whole, so not BIGINT
        "Sampled on": ("sampled_on", "DATE", True),
        "Measured at": ("measured_at", "TIMESTAMP", True),
        "Note": ("note", "STRING", True),
        "Formula": ("formula", "", False),
    }
    assert cols[0]["comment"] == "um" and cols[0]["choices"] == list(
        dbx_jmp.PUBLISH_TYPES
    )
    assert cols[-1]["dropped"] == "Expression columns are not written"


def test_publish_sql_per_mode_and_batches(monkeypatch):
    patch_query(monkeypatch)
    tbl = arrow([float(i) for i in range(5001)])  # 2 columns -> 5,000 rows per batch

    w = FakeWS()
    r = dbx_jmp.publish_arrow(
        tbl, w, "wh", "c.s.t", "create", PUBLISH_COLS, comment="x"
    )
    (create, _), (first, p1), (_last, p2) = writes(w)
    assert create == (
        "CREATE TABLE IF NOT EXISTS `c`.`s`.`t` (`lot_id` BIGINT COMMENT 'lot', `name` STRING) COMMENT 'x'"
    )
    assert first.startswith(
        "INSERT INTO `c`.`s`.`t` (`lot_id`, `name`) VALUES (?, ?), "
    )
    assert len(p1) == 10_000 and len(p2) == 2  # warehouse limit: 10,000 parameters
    assert (
        r["rows_written"] == 5001
        and r["table_version"] == 3
        and r["version_before"] == -1
    )
    assert r["summary"].startswith("Create 5,001 rows and 2 columns to `c.s.t`")
    assert re.match(r"/\* jmp-publish v1 \w{12} \*/ ", w.sql[0][0])

    w = FakeWS(existing={"c.s.t": ["lot_id", "name"]})
    dbx_jmp.publish_arrow(tbl, w, "wh", "c.s.t", "overwrite", PUBLISH_COLS)
    (first, _), (second, _) = writes(w)
    assert first.startswith("INSERT OVERWRITE `c`.`s`.`t`") and second.startswith(
        "INSERT INTO"
    )

    w = FakeWS(existing={"c.s.t": ["lot_id", "name"]})
    dbx_jmp.publish_arrow(arrow([]), w, "wh", "c.s.t", "overwrite", PUBLISH_COLS)
    assert writes(w) == [("TRUNCATE TABLE `c`.`s`.`t`", None)]

    w = FakeWS(existing={"c.s.t": ["lot_id", "name"]})
    dbx_jmp.publish_arrow(
        tbl, w, "wh", "c.s.t", "upsert", PUBLISH_COLS, keys=["lot_id"]
    )
    merge, _ = writes(w)[0]
    assert merge.startswith(
        "MERGE INTO `c`.`s`.`t` AS t USING (SELECT CAST(`lot_id` AS BIGINT) AS `lot_id`, "
        "CAST(`name` AS STRING) AS `name` FROM VALUES (?, ?), "
    )
    assert "AS s(`lot_id`, `name`)) AS s ON t.`lot_id` <=> s.`lot_id`" in merge
    assert merge.endswith(
        "WHEN MATCHED THEN UPDATE SET t.`name` = s.`name` "
        "WHEN NOT MATCHED THEN INSERT (`lot_id`, `name`) VALUES (s.`lot_id`, s.`name`)"
    )

    w = FakeWS(existing={"c.s.t": ["lot_id", "name"]})
    dbx_jmp.publish_arrow(
        tbl,
        w,
        "wh",
        "c.s.t",
        "append",
        PUBLISH_COLS,
        fast_write=True,
        volume="/Volumes/c/s/v",
    )
    (load,) = writes(w)
    assert load[0].startswith(
        "INSERT INTO `c`.`s`.`t` (`lot_id`, `name`) SELECT `lot_id`, `name` FROM read_files('/Volumes/c/s/v/jmp_tmp_"
    )
    assert load[0].endswith("format => 'parquet')")
    assert len(w.uploads) == 1 and w.deletes == w.uploads


def test_publish_validation_errors_stop_before_any_write(monkeypatch):
    import pytest

    patch_query(monkeypatch)
    existing = {"c.s.t": ["lot_id", "name"]}
    cases = [
        (
            FakeWS(existing),
            arrow([1.0]),
            {"mode": "upsert", "keys": []},
            "Upsert needs",
        ),
        (
            FakeWS(existing),
            arrow([1.0, 1.0]),
            {"mode": "upsert", "keys": ["lot_id"]},
            "not unique",
        ),
        (FakeWS(), arrow([1.0]), {"mode": "append", "keys": []}, "does not exist"),
        (
            FakeWS({"c.s.t": ["lot_id"]}),
            arrow([1.0]),
            {"mode": "append", "keys": []},
            "no column",
        ),
        (FakeWS(existing), arrow([1.5]), {"mode": "append", "keys": []}, "BIGINT"),
    ]
    for w, tbl, kw, message in cases:
        with pytest.raises(ValueError, match=message):
            dbx_jmp.publish_arrow(tbl, w, "wh", "c.s.t", cols=PUBLISH_COLS, **kw)
        assert writes(w) == [] and w.uploads == []
    # A recipe is JSON inside a shared .jmp file; a tampered type must never reach the SQL.
    w = FakeWS()
    evil = [dict(PUBLISH_COLS[0], type="STRING) ; DROP TABLE x --")] + PUBLISH_COLS[1:]
    with pytest.raises(ValueError, match="Unsupported column type"):
        dbx_jmp.publish_arrow(arrow([1.0]), w, "wh", "c.s.t", "create", evil)
    assert writes(w) == []


def test_publish_fast_write_deletes_temp_file_on_failure(monkeypatch):
    import pytest

    patch_query(monkeypatch)
    w = FakeWS(
        {"c.s.t": ["lot_id", "name"]}, fail=lambda sql, params: "read_files" in sql
    )
    with pytest.raises(RuntimeError, match="boom"):
        dbx_jmp.publish_arrow(
            arrow([1.0, 2.0]),
            w,
            "wh",
            "c.s.t",
            "append",
            PUBLISH_COLS,
            fast_write=True,
            volume="/Volumes/c/s/v",
        )
    assert len(w.uploads) == 1 and w.uploads[0].startswith("/Volumes/c/s/v/jmp_tmp_")
    assert w.deletes == w.uploads


def test_publish_partial_write_reports_rows_and_restore(monkeypatch):
    import pytest

    patch_query(monkeypatch)
    w = FakeWS(
        {"c.s.t": ["lot_id", "name"]},
        fail=lambda sql, params: params is not None and len(params) == 2,  # 2nd batch
    )
    with pytest.raises(RuntimeError) as e:
        dbx_jmp.publish_arrow(
            arrow([float(i) for i in range(5001)]),
            w,
            "wh",
            "c.s.t",
            "append",
            PUBLISH_COLS,
        )
    assert "stopped after 5,000 of 5,001 rows" in str(e.value)
    assert "RESTORE TABLE `c`.`s`.`t` TO VERSION AS OF 3" in str(e.value)


def test_jsl_no_missing_separators():
    """A line ending in ")" followed by one starting with a name is a missing "," or ";" in JSL."""
    import itertools
    import re

    for f in (Path(__file__).parents[1] / "addin").glob("*.jsl"):
        src = re.sub(
            r'"\\\[.*?\]\\"', '""', f.read_text(), flags=re.DOTALL
        )  # raw strings hold Python
        lines = [re.sub(r"//.*$", "", line).rstrip() for line in src.split("\n")]
        for i, (a, b) in enumerate(itertools.pairwise(lines)):
            assert not (a.endswith(")") and re.match(r"\s*[A-Za-z]", b)), (
                f"{f.name}:{i + 1}"
            )


def test_last_used_warehouse_is_listed_first(monkeypatch):
    import types

    whs = [
        types.SimpleNamespace(id=i, name=n, state=types.SimpleNamespace(value=st))
        for i, n, st in [("rt", "a realtime", "RUNNING"), ("pro", "b pro", "STOPPED")]
    ]
    host = "https://h"
    ws = types.SimpleNamespace(
        config=types.SimpleNamespace(host=host),
        warehouses=types.SimpleNamespace(list=lambda: whs),
    )
    monkeypatch.setattr(dbx_jmp, "_ws", ws)
    assert [w["id"] for w in dbx_jmp.warehouses()] == [
        "rt",
        "pro",
    ]  # running first by default
    dbx_jmp._remember_warehouse(host, "pro")
    assert [w["id"] for w in dbx_jmp.warehouses()] == [
        "pro",
        "rt",
    ]  # remembered one first


def test_genie_ask_fills_parameters_and_follows_up(monkeypatch):
    ns = types.SimpleNamespace
    query = ns(
        query="SELECT * FROM t WHERE zip = :zip AND d::date > :since",
        title="Trips by zip",
        description="Filters on zip.",
        parameters=[
            ns(keyword="zip", value="10'01"),
            ns(keyword="since", value="2024-01-01"),
        ],
    )
    msg = ns(
        conversation_id="c1",
        error=None,
        attachments=[
            ns(text=ns(content="Here you go."), query=None, suggested_questions=None),
            ns(text=None, query=query, suggested_questions=None),
            ns(text=None, query=None, suggested_questions=ns(questions=["By month?"])),
        ],
    )
    calls = []
    genie = ns(
        start_conversation_and_wait=lambda *a, **k: calls.append(("start", a)) or msg,
        create_message_and_wait=lambda *a, **k: calls.append(("follow", a)) or msg,
    )
    monkeypatch.setattr(dbx_jmp, "_ws", ns(genie=genie))
    r = dbx_jmp.genie_ask("s1", "trips by zip")
    assert r["sql"] == (
        "SELECT * FROM t WHERE zip = '10\\'01' AND d::date > '2024-01-01'"
    )
    assert r["conversation_id"] == "c1" and r["suggestions"] == ["By month?"]
    assert r["text"] == ["Here you go.", "Filters on zip."]
    dbx_jmp.genie_ask("s1", "by month", "c1")
    assert [c[0] for c in calls] == ["start", "follow"] and calls[1][1][1] == "c1"


def test_publish_target_defaults_to_the_workspace_catalog_and_schema(monkeypatch):
    import pyarrow as pa

    sqls = []
    monkeypatch.setattr(
        dbx_jmp,
        "run_sql",
        lambda wh, sql, **k: (
            sqls.append(sql) or pa.table({"c": ["ws_cat"], "s": ["default"]})
        ),
    )
    resolve = lambda name, wh="wh": dbx_jmp._resolve_target(None, wh, name)
    assert resolve("genie") == "ws_cat.default.genie"
    assert resolve("sales.genie") == "ws_cat.sales.genie"
    assert len(sqls) == 2
    assert (
        resolve("main.sales.genie") == "main.sales.genie" and len(sqls) == 2
    )  # full name: no lookup
    for bad in ["main..genie", "a.b.c.d", "hive_metastore.default.genie"]:
        with pytest.raises(ValueError, match="catalog.schema.table|Unity Catalog"):
            resolve(bad)
    with pytest.raises(ValueError, match="SQL warehouse"):
        resolve("genie", wh="")


def test_build_sql_metric_view_aggregates_measures():
    sql = dbx_jmp.build_sql(
        "c.s.mv",
        [
            {"name": "product", "alias": "", "as": "Auto"},
            {"name": "Total Sales", "alias": "", "as": "Number"},
        ],
        [
            {"column": "product", "op": "≠", "value": "x"},
            {"column": "Total Sales", "op": ">", "value": "100"},
            {
                "column": "Orders",
                "op": ">",
                "value": "5",
            },  # a measure that isn't included
        ],
        50,
        measures=["Total Sales", "Orders"],
    )
    assert sql == (
        "SELECT `product`, CAST(MEASURE(`Total Sales`) AS DOUBLE) AS `Total Sales` "
        "FROM `c`.`s`.`mv` WHERE `product` <> 'x' GROUP BY ALL "
        "HAVING MEASURE(`Total Sales`) > '100' AND MEASURE(`Orders`) > '5' LIMIT 50"
    )
    # A plain table is unchanged: no MEASURE, no GROUP BY.
    assert (
        dbx_jmp.build_sql("c.s.t", [{"name": "a"}], [], 0)
        == "SELECT `a` FROM `c`.`s`.`t`"
    )
    # A measure always gets an alias, or the column comes back named "measure(m)".
    assert dbx_jmp.build_sql("c.s.mv", [{"name": "m"}], [], 0, ["m"]) == (
        "SELECT MEASURE(`m`) AS `m` FROM `c`.`s`.`mv` GROUP BY ALL"
    )


def test_metric_kind_from_column_metadata():
    ns = types.SimpleNamespace
    meta = '{"metadata": {"metric_view.type": "measure", "metric_view.expr": "SUM(x)"}}'
    assert dbx_jmp._metric_kind(ns(type_json=meta)) == "measure"
    assert dbx_jmp._metric_kind(ns(type_json='{"name": "a", "metadata": {}}')) == ""
    assert dbx_jmp._metric_kind(ns(type_json=None)) == ""
