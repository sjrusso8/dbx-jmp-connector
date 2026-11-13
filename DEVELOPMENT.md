# Developing the Databricks Connector for JMP

For using the connector, see [README.md](README.md).

## How it works

- **UI:** a native JMP window, written in JSL (`addin/databricks_connector.jsl`).
- **Databricks calls:** the Databricks SDK for Python, running in JMP's built-in Python (`addin/dbx_jmp.py`). The JSL calls `dbx_jmp.dispatch(fn, args_json)` and reads back a JSON envelope, so Python errors show in the window.
- **Data transfer:** the Databricks SQL Connector on a SQL warehouse. It downloads compressed Arrow results in parallel.

## Layout

```
addin/
  addin.def                  add-in id, version, minimum JMP version
  addin.jmpcust              adds "Add-Ins > Databricks Connector…"
  databricks_connector.jsl   the window
  dbx_common.jsl             shared JSL: Python loading, dbx:call, status, colors
  dbx_refresh.jsl            "Refresh from Databricks" table script
  dbx_publish.jsl            "Publish to Unity Catalog" table script
  dbx_jmp.py                 Python bridge: auth, browse, query, Genie, publish
build.sh                     zips addin/ into dist/DatabricksConnector.jmpaddin
tests/                       pytest for the Python parts, plus JSL syntax checks
spikes/                      probes and notes (Delta Sharing, metric views)
reference/                   specs and reference material
```

The version is `VERSION` in `addin/dbx_jmp.py` (sent in the SQL user agent) and `addinVersion` in `addin/addin.def`. Update both for a release.

## Run from source

Pick **one** option.

### Option A: register the source folder (best while iterating)

JMP loads the add-in straight from `addin/`. After you edit a file, reopen the window. No rebuild needed.

1. In JMP: **File > New > New Script**.
2. Paste this, with your path, and run it (**Edit > Run Script**: ⌘R on macOS, Ctrl+R on Windows):

   ```jsl
   Register Addin(
       "com.databricks.jmp.connector",
       "<path to repo>/addin/",
       Display Name( "Databricks Connector (dev)" )
   );
   ```

3. Open **Add-Ins > Databricks Connector…**.

To remove it: **View > Add-Ins**, select *Databricks Connector*, click **Unregister**.

### Option B: build and install the package

```bash
./build.sh    # creates dist/DatabricksConnector.jmpaddin
```

Open the file in JMP (double-click it, or `open dist/DatabricksConnector.jmpaddin` on macOS) and click **Install**.

### Option C: run the script directly

**File > Open** → `addin/databricks_connector.jsl` → **Run Script**.

## Tests

```bash
uv venv -p 3.13 && uv pip install databricks-sdk 'databricks-sql-connector[pyarrow]' numpy pytest
.venv/bin/pytest -q
```

The tests fake the `jmp` module and the Databricks clients, so they need no JMP and no workspace. They also check the JSL files for balanced brackets and missing separators.

## Logs

Each time the window opens, Python starts a fresh log with SDK HTTP debug output (tokens are not logged):

- macOS: `~/Library/Logs/DatabricksConnector.log`
- Windows: `%LOCALAPPDATA%\DatabricksConnector.log`

A table refresh or publish appends to it. Also check JMP's **View > Log**.

## Scripting without the window

From JMP's Python, after the connector has been opened once (it puts `addin/` on `sys.path`):

```python
import dbx_jmp
dbx_jmp.publish(table="Lots", target="quality.spc.lots", mode="append",
                warehouse_id="<id>", host="<workspace URL>", sign_in="profile:DEFAULT")
```

In JSL, run a table's saved publish with `Include( "$ADDIN_HOME(com.databricks.jmp.connector)/dbx_publish.jsl" );` while the table is current.
