# Databricks Connector for JMP (POC)

A JMP add-in that signs in to a Databricks workspace, lets you browse Unity Catalog (catalog → schema → table), and imports the table you pick into a JMP data table.

- **UI:** a native JMP window, written in JSL (`addin/databricks_connector.jsl`)
- **Databricks calls:** the Databricks SDK for Python, running in JMP's built-in Python (`addin/dbx_jmp.py`)
- **Data transfer:** the Databricks SQL Connector on a SQL warehouse. It downloads compressed Arrow results in parallel, and the status bar shows rows downloaded so far

Requires JMP 18+ (tested target: JMP 19 on macOS) and access to a SQL warehouse.

## Layout

```
addin/
  addin.def                  add-in id + metadata
  addin.jmpcust              adds "Add-Ins > Databricks Connector…" menu item
  databricks_connector.jsl   the window
  dbx_jmp.py                 Databricks SDK bridge (auth, browse, query, build data table)
build.sh                     zips addin/ into dist/DatabricksConnector.jmpaddin
tests/                       pytest for the pure-Python parts
```

## Install locally for testing

Pick **one** option.

### Option A: register the source folder (best while iterating)

JMP loads the add-in straight from `addin/`, so after you edit a file you only need to reopen the window. You don't have to rebuild or reinstall.

1. In JMP, open a new script window: **File > New > New Script**.
2. Paste the following and run it (**Edit > Run Script**, or ⌘R):

   ```jsl
   Register Addin(
       "com.databricks.jmp.connector",
       "/Users/steven.russo/code/poc/jmp_plugin/addin/",
       Display Name( "Databricks Connector (dev)" )
   );
   ```

3. Open the connector from **Add-Ins > Databricks Connector…**.

To remove it: **View > Add-Ins**, select *Databricks Connector*, click **Unregister**.

### Option B: install the packaged add-in (how end users get it)

```bash
./build.sh                      # creates dist/DatabricksConnector.jmpaddin
open dist/DatabricksConnector.jmpaddin
```

JMP asks whether to install. Click **Install**. The connector then appears under **Add-Ins > Databricks Connector…**. To remove it, use **View > Add-Ins > Unregister**.

### Option C: run the script directly (no install)

**File > Open** → `addin/databricks_connector.jsl` → **Run Script**.

### If the menu item doesn't appear

Run this in a script window to open the connector anyway:

```jsl
Include( "$ADDIN_HOME(com.databricks.jmp.connector)/databricks_connector.jsl" );
```

Please tell me if this happens. It means the menu definition in `addin.jmpcust` needs adjusting.

## First run

On first launch the connector checks JMP's Python for `databricks-sdk`, `databricks-sql-connector` and `pyarrow`. If any are missing, it offers to install them with `jmputils.jpip`, which takes about a minute. To install it yourself instead, run:

```jsl
Python Submit( "import jmputils; jmputils.jpip('install', 'databricks-sdk databricks-sql-connector[pyarrow]')" );
```

## Using it

1. **Sign in**: the first screen asks how to connect. Click **Sign in** to continue.
   - **Saved CLI profile**: any workspace profile in `~/.databrickscfg`. This covers profiles created with `databricks auth login` (OAuth) and PAT profiles. Account-level profiles are hidden.
   - **Browser sign-in (OAuth)**: enter the workspace URL. A browser window opens for SSO.
   - **Personal access token**: enter the workspace URL and a token.
2. **Get data**: once you're signed in, the data screen opens. Pick a **SQL warehouse** at the top. Running warehouses are listed first, and a stopped warehouse starts automatically when you run a query. **Switch workspace** returns to the sign-in screen.
3. Choose a mode with the tabs:
   - **Select data**: click a catalog, then a schema, then a table. Then edit how it's imported:
     - **Columns**: untick a column to leave it out. Use **Import as** to change its type (Number, Text, Date or Datetime; the warehouse does the `CAST`). Use **Rename to** to give it a new name, and ▲/▼ to reorder.
     - **Filters**: **+ Add filter** adds a column / operator / value row. Rows are combined with AND. **in list** takes comma-separated values.
   - **Write SQL**: type any query. **Query name** becomes the data table's name.
4. Set the **Row limit** (0 = all rows), then click **Preview** to see the first 100 rows and the SQL that will run.
5. Click **Import into JMP**. A new data table opens. The SQL that produced it is saved in the table variable **Databricks Source**.

### Refreshing a table

Each imported table gets a **Refresh from Databricks** script (red triangle menu, or the table panel). It re-runs the saved SQL on the same warehouse:

- The query runs first. Rows are replaced only if it succeeds, so a failed refresh never changes your data.
- Columns are kept and updated by name, so formula columns, saved scripts and graphs keep working. New result columns are added. If a column changed type in Databricks, the refresh stops before changing anything and asks you to import again.
- Signing in again: if the connector is signed in to that workspace, the refresh uses that session. Otherwise:
  - **Profile** and **OAuth** tables reconnect by themselves. OAuth may open your browser if its saved sign-in expired.
  - **PAT** tables ask you to sign in again, because tokens are never saved with a table.
- When a refresh fails, a dialog shows the error, says your data was not changed, and offers **Sign in…**, which opens the connector. Sign in, then run the refresh again.

### Security

- **Tokens:**
  - A personal access token is held only in memory by the Databricks SDK client.
  - The token box is cleared after sign-in.
  - The token is removed from JMP's Python variables after each call.
  - It's never written to the log or saved with a table. Tables save only the host, warehouse ID, SQL, and how you signed in (profile name, `oauth` or `pat`).
- **OAuth:** tokens are cached by the Databricks SDK in `~/.config/databricks-sdk-py/oauth`, readable only by you. CLI profiles use the Databricks CLI's own token cache.
- **Switch workspace** signs out: the window's client and its warehouse sessions are closed.
- **Encryption:** all connections use HTTPS with certificate checks. Pasted URLs are cut down to `https://<host>`.
- **The log** (`~/Library/Logs/DatabricksConnector.log`) is readable only by you. It holds your user name, groups and SQL text, but no tokens.

### Type mapping

| Databricks | JMP |
|---|---|
| tinyint/smallint/int/bigint/float/double/decimal | Numeric, Continuous |
| date | Numeric, `yyyy-mm-dd` format |
| timestamp / timestamp_ntz | Numeric, `yyyy-mm-ddThh:mm:ss` format (timestamps are converted to UTC) |
| everything else (string, boolean, arrays, structs…) | Character |

Large `bigint` and `decimal` values are stored as doubles. Values with more than about 15 significant digits lose precision.

## Development

```bash
uv venv -p 3.13 && uv pip install databricks-sdk 'databricks-sql-connector[pyarrow]' pytest
.venv/bin/pytest -q
```

Each time the window opens, the Python side starts a fresh log at `~/Library/Logs/DatabricksConnector.log`, with SDK HTTP debug output (tokens are not logged). On Windows the log goes to the temp folder. When something fails in JMP, check that file and **View > Log**. Python errors from Databricks also appear in red in the connector's status bar.

## Known limits / not built (POC)

- The JMP window doesn't respond (spinning wheel) while a query runs, because Python calls block the JSL thread. There's no progress display.
- Download speed is limited by your network. On a 7 MB/s link, 5M rows of a wide table (about 1.2 GB) take about 3 minutes. Use the row limit, column selection or a `WHERE` filter for large tables.
- Some workspaces block direct cloud-storage downloads with a firewall or private link. If an import stalls at the download step, that's the likely cause.
- Not built yet from the Excel add-in reference: saved workspace queries, query parameters, saved imports, write back, pivot, and value pickers for filters. There are also no saved preferences such as the last profile or warehouse used.
