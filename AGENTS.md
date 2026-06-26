# DAL — Data Access Layer

> **Project type:** .NET Framework 4.8.1 Class Library (SDK-style project)
> **Solution:** `DAL.sln` (Visual Studio 2019/2022 compatible, also builds with `dotnet` CLI)
> **Assembly:** `DAL.dll` — "Capa de conexion a Datos"
> **Version:** 1.0.4.4
> **Author:** Giancarlo Molina / aurorasi SA

---

## 1. Project Overview

This is a **single-project solution** that provides a database-agnostic Data Access Layer (DAL) for .NET Framework applications. It wraps multiple ADO.NET providers behind a unified API so that consuming code does not need to bind to a specific database client (SQL Server, MySQL, Oracle, PostgreSQL, SQLite, ODBC, OLE DB, etc.).

The library is distributed as a class library (`OutputType = Library`) and is meant to be referenced by other .NET Framework projects (web or desktop).

### Key source files

| File | Purpose |
|------|---------|
| `DataAccess.cs` | Main DAL implementation (~1,050 lines). Contains `DataAccess` class, `ProviderFactory`, `ParamStruct`, and provider enums. |
| `ColumnDefinition.cs` | Simple DTO in the `DATAMAP` namespace for SQL script generation (column metadata). |
| `Properties/AssemblyInfo.cs` | Assembly metadata. |
| `App.config` | Runtime configuration, Entity Framework providers, Oracle ODP.NET factory, and assembly binding redirects. |

---

## 2. Technology Stack

- **Runtime:** .NET Framework 4.8.1 (`net481`)
- **Build system:** SDK-style `.csproj` (compatible with `dotnet` CLI and Visual Studio 2019/2022)
- **Package manager:** NuGet via `PackageReference`
- **IDE target:** Visual Studio 2019/2022

### Major NuGet dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `EntityFramework` | 6.5.2 | EF6 provider infrastructure. |
| `MySql.Data` | 9.7.0 | MySQL/MariaDB provider. |
| `Npgsql` | 8.0.6 | PostgreSQL provider. |
| `Oracle.ManagedDataAccess` | 23.26.200 | Oracle ODP.NET managed driver. |
| `System.Data.SQLite.Core` | 1.0.119.0 | SQLite provider. |
| `Newtonsoft.Json` | 13.0.4 | JSON serialization for `ExecDataReaderJson`. |
| `Microsoft.Bcl.AsyncInterfaces` / `System.Text.Json` / `System.Memory` | Various | Transitive dependencies for modern data drivers on .NET Framework. |

> **Note:** The project was migrated from legacy `packages.config` to SDK-style `PackageReference`. All absolute `HintPath` references were removed. Use `dotnet restore` or Visual Studio to restore packages.

---

## 3. Architecture & Code Organization

### Namespace layout

- **`DAL`** — Main namespace. Contains:
  - `EnumProviders` — ADO.NET provider selector (`SQLClient`, `ODBC`, `OLEDB`, `SQLLITE`, `MySqlClient`, `OracleClient`, `Npgsql`, `SAPHANNA`).
  - `EnumProvidersDB` — Logical database type selector (`DB_SQL`, `DB_ORACLE`, `DB_MYSQL`, `DB_POSTGRESQL`, `DB_SQLITE`, etc.).
  - `ParamStruct` — Serializable parameter bag used instead of provider-specific parameter types.
  - `ProviderFactory` — Internal static factory that creates `IDbConnection`, `IDbCommand`, `IDbDataAdapter`, `IDbDataParameter`, and `IDbTransaction` instances based on the selected `EnumProviders`. **Refactored** to use a `Dictionary<EnumProviders, ProviderMeta>` instead of large `switch` blocks.
  - `DataAccess` — Public API class exposing `ExecDataSet`, `ExecDataReader`, `ExecNonQuery`, `ExecScalar`, `SaveDataSet`, transaction helpers, JSON serialization utilities, and **async variants** (`ExecDataSetAsync`, `ExecNonQueryAsync`, `ExecScalarAsync`).
- **`DATAMAP`** — Secondary namespace (only in `ColumnDefinition.cs`). Holds `ColumnDefinition`, a lightweight model for table/column metadata.

### Design patterns

1. **Abstract Factory** — `ProviderFactory` switches on `EnumProviders` to instantiate the correct ADO.NET types.
2. **Facade** — `DataAccess` hides all provider-specific mechanics behind a single class.
3. **Disconnected & Connected modes** — `DataSet` methods create/destroy connections automatically; `DataReader` methods and transactions reuse an open connection.

### Configuration sources

The library reads settings in this priority:

1. `App.config` `appSettings` (`ConnectionString`, `Provider`, `ProviderDB`).
2. If absent, falls back to **`ConnectionString.xml`** in the working directory (must contain `<CS>`, `<Provider>`, `<ProviderDB>` elements).

**Performance improvement:** configuration values are **cached** on first read and invalidated via `ProviderFactory.RefreshConfigCache()` (called automatically by `WriteXMLConnection`).

### Transaction model

- `BeginTrans` / `CommitTrans` / `AbortTrans` manage an internal `IDbTransaction`.
- When a transaction is active, `PrepareAll` reuses `_conn` and `_trans` instead of opening a new connection.
- Default isolation level: `IsolationLevel.ReadCommitted`.
- Default `CommandBehavior` for readers: `CommandBehavior.CloseConnection`.
- `DataAccess` now implements **`IDisposable`** to ensure connections and transactions are released if the consumer forgets to call `CommitTrans`/`AbortTrans`.

---

## 4. Public API Surface

Here are the most commonly used members of `DataAccess`:

```csharp
// Execution
DataSet   ExecDataSet(string sql, CommandType type, ParamStruct[] params)
void      ExecDataSet(DataSet ds, string sql, CommandType type, ParamStruct[] params)
IDataReader ExecDataReader(string sql, CommandType type, ParamStruct[] params)
int       ExecNonQuery(string sql, CommandType type, ParamStruct[] params)
object    ExecScalar(string sql, CommandType type, ParamStruct[] params)

// JSON helpers
string    ExecDataReaderJson(string sql, CommandType type)
IEnumerable<Dictionary<string, object>> SerializeEnumerable(IDataReader reader)
List<Dictionary<string, object>> Serialize(IDataReader reader)

// Static convenience wrappers
static int    ExecNonQueryS(string sql, CommandType type)
static object ExecScalarS(string sql, CommandType type)
static int    ExecScalarInteger(string sql, CommandType type, int default)
static double ExecScalarDouble(string sql, CommandType type, double default)
static string ExecDataReaderJsonS(string sql, CommandType type)

// Async variants (.NET Framework 4.8.1 compatible)
Task<DataSet>   ExecDataSetAsync(string sql, CommandType type, ParamStruct[] params)
Task<int>       ExecNonQueryAsync(string sql, CommandType type, ParamStruct[] params)
Task<object>    ExecScalarAsync(string sql, CommandType type, ParamStruct[] params)

// Transactions
void BeginTrans(IsolationLevel level)
void BeginTrans(string connString, IsolationLevel level)
void CommitTrans(bool closeConnection = true)
void AbortTrans()
bool IsInTransaction()

// Connection / pooling
void          ClearAllPools()
IDbConnection GetConnection()
static bool   TestConnection(ref string msg)

// Batch updates via DataAdapter
void SaveDataSet(DataSet ds, string insertSQL, string deleteSQL, string updateSQL,
                 ParamStruct[] insertParams, ParamStruct[] deleteParams, ParamStruct[] updateParams)

// Schema (previously stubs, now implemented)
DataTable GetShema(string collectionName)
DataTable GetShemaTable()
```

### Parameter construction

```csharp
var p = new ParamStruct {
    ParamName = "@Id",
    DataType = DbType.Int32,
    value = 42,
    direction = ParameterDirection.Input,
    sourceColumn = "",
    size = 0
};
```

---

## 5. Build and Test Commands

### Build (Visual Studio)

```powershell
# Open DAL.sln in Visual Studio and build
# or use the .NET CLI:
dotnet build DAL.sln
```

### Build (dotnet CLI — recommended)

Because the project is now **SDK-style**, `dotnet build` works out of the box:

```powershell
dotnet restore DAL.sln
dotnet build DAL.sln -c Release
```

### Package restore

```powershell
dotnet restore DAL.sln
# or via Visual Studio: right-click solution → Restore NuGet Packages
```

### Testing

- **There is no test project in this solution.**
- There are no unit tests, integration tests, or automated test scripts committed to the repository.
- Validation is typically done manually by referencing the built `DAL.dll` from a host application and calling `DataAccess.TestConnection`.

---

## 6. Code Style Guidelines

- **Language:** Code comments are mixed English and Spanish; public XML documentation tends to be in English, while inline developer notes are often in Spanish.
- **Naming:**
  - PascalCase for types and public members (`ExecDataSet`, `ParamStruct`).
  - camelCase for private fields (`_trans`, `_conn`, `_cmdTimeout`).
  - Some Hungarian-style prefixes remain in local variables (`strSQL`, `cmdtype`, `da`).
- **Braces:** Opening braces are typically on a new line (Allman style).
- **Visibility:** `ProviderFactory` is `internal`; `DataAccess` and `ParamStruct` are `public`.
- **Exception handling:** The DAL catches provider-specific exceptions (`SqlException`, `OleDbException`, `MySqlException`, `NpgsqlException`, `SQLiteException`, `OracleException`) and re-throws them wrapped in a generic `Exception` with formatted messages. There is **no custom exception hierarchy**.

---

## 7. Development Conventions

1. **Adding a new database provider:**
   - Add a new value to `EnumProviders`.
   - Register the provider in `ProviderFactory._providers` dictionary (connection, command, adapter, parameter, command builder, clear pools).
   - Add a provider-specific exception handler in `DataAccess.GenericExceptionHandler`.
   - Add the corresponding NuGet package to `.csproj` `<PackageReference>`.

2. **Connection strings:**
   - Prefer `App.config` `appSettings` for production.
   - `ConnectionString.xml` is a runtime fallback; it can be written programmatically via `DataAccess.WriteXMLConnection`.

3. **SQLite special note (from code comments):**
   - The only required NuGet for SQLite runtime is `System.Data.SQLite.Core`.
   - The `System.Data.SQLite` package (designer support) is **not** required for runtime.
   - The project must explicitly reference the SQLite package so that `sqliteinterop.dll` (x64/x86) is copied to the output folder.

4. **Command timeout:**
   - Default is `100` seconds (`COMMAND_TIMEOUT` constant).
   - Override via the `CmdTimeout` property.

5. **Async usage:**
   - Async methods (`ExecDataSetAsync`, `ExecNonQueryAsync`, `ExecScalarAsync`) are available for I/O-bound operations.
   - `ExecDataSetAsync` uses `Task.Run(() => da.Fill(ds))` because `DbDataAdapter.Fill` does not offer an async API in .NET Framework.

---

## 8. Deployment Considerations

- The output is `bin\Debug\net481\DAL.dll` or `bin\Release\net481\DAL.dll` when building with SDK-style / `dotnet`.
- When building with Visual Studio legacy output paths, it may still map to `bin\Debug\DAL.dll` depending on MSBuild version.
- Because the project is a class library, it is deployed by copying the DLL plus all referenced assemblies (including provider-specific ones like `Oracle.ManagedDataAccess.dll`, `MySql.Data.dll`, `Npgsql.dll`, `System.Data.SQLite.dll`, etc.) to the host application\'s `bin` folder.
- Ensure the host application\'s config file contains the same `DbProviderFactories` and `entityFramework` sections as `App.config`, or merge `App.config` contents into the host config.

---

## 9. Security Considerations

- **SQL Injection:** The DAL supports parameterized queries via `ParamStruct[]`, but it also exposes methods that accept raw SQL strings. Always use parameters; never concatenate user input into `strSQL`.
- **Connection strings in XML:** `ConnectionString.xml` is written as plain text by `WriteXMLConnection`. If used, ensure the file is protected by filesystem ACLs and excluded from source control.
- **No encryption:** There is no built-in encryption for connection strings or sensitive data.
- **Assembly binding redirects:** `App.config` contains many `<bindingRedirect>` entries required by modern NuGet packages on .NET Framework. Removing them can cause `FileLoadException` at runtime.

---

## 10. Known Caveats

- SAP HANA (`EnumProviders.SAPHANNA` / `EnumProvidersDB.DB_SAPHANNA`) is declared but **not implemented** in `ProviderFactory`; using it will fall through to the `SQLClient` defaults.
- `System.ValueTuple` binding conflicts may appear in build logs if an older version exists in the local `packages/` folder (legacy residue). The conflict is non-fatal.
