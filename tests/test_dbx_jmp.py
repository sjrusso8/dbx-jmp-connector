import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "addin"))
import dbx_jmp


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

                def execute(self, sql):
                    if conn.dead:
                        raise RuntimeError("session expired")

                def fetchall_arrow(self):
                    return pa.table({"a": [1, 2, 3]})

                def fetchmany_arrow(self, n):
                    return pa.table({"a": [1, 2, 3][:n]})

            return Cur()

        def close(self):
            pass

    monkeypatch.setattr(databricks.sql, "connect", lambda **kw: FakeConn())
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
