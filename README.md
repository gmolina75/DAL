# DAL

> **Capa de acceso a datos (Data Access Layer) agnóstica a la base de datos para .NET Framework 4.8.1**

`DAL` es una biblioteca de clases (class library) que envuelve los principales proveedores ADO.NET detrás de una única API unificada. Su propósito es que **el código de tu aplicación no dependa de un motor de base de datos concreto**: el mismo código puede trabajar con SQL Server, Oracle, MySQL, PostgreSQL, SQLite, ODBC u OLE DB simplemente cambiando un valor de configuración.

| | |
|---|---|
| **Plataforma** | .NET Framework 4.8.1 (`net481`) |
| **Tipo de proyecto** | Biblioteca de clases (SDK-style) |
| **Ensamblado** | `DAL.dll` |
| **Versión** | 1.0.4.4 |
| **Solución** | `DAL.sln` (Visual Studio 2019/2022 o `dotnet` CLI) |
| **Autor** | Giancarlo Molina / aurorasi SA |

---

## Tabla de contenidos

1. [Características](#características)
2. [Bases de datos soportadas](#bases-de-datos-soportadas)
3. [Requisitos](#requisitos)
4. [Instalación](#instalación)
5. [Configuración](#configuración)
6. [Inicio rápido](#inicio-rápido)
7. [Referencia de API](#referencia-de-api)
8. [Transacciones](#transacciones)
9. [Parámetros](#parámetros)
10. [Uso asíncrono](#uso-asíncrono)
11. [Manejo de errores](#manejo-de-errores)
12. [Estructura del proyecto](#estructura-del-proyecto)
13. [Compilación](#compilación)
14. [Despliegue](#despliegue)
15. [Seguridad](#seguridad)
16. [Limitaciones conocidas](#limitaciones-conocidas)

---

## Características

- **API unificada y agnóstica**: una sola clase `DataAccess` para consultar, insertar, actualizar y eliminar datos sin importar el proveedor.
- **Multi-proveedor ADO.NET**: SQL Server, Oracle, MySQL/MariaDB, PostgreSQL, SQLite, ODBC y OLE DB.
- **Modos conectado y desconectado**:
  - `DataSet` / `DataTable`: conexión se abre y cierra automáticamente.
  - `DataReader` / transacciones: reutilizan una conexión abierta.
- **Parámetros seguros**: objeto `ParamStruct` que evita la concatenación de SQL (protección contra inyección SQL).
- **Transacciones**: `BeginTrans`, `CommitTrans` y `AbortTrans` con nivel de aislamiento configurable.
- **Métodos asíncronos**: variantes `async` para operaciones de I/O intensivas.
- **Serialización a JSON**: convierte un `DataReader` directamente a JSON (Newtonsoft.Json).
- **Ayudantes estáticos**: métodos de una sola línea para operaciones frecuentes (escalares, non-query, JSON).
- **Esquema de base de datos**: obtención de metadatos (`GetSchema`) sin conocer el proveedor.
- **`IDisposable`**: libera automáticamente conexiones y transacciones pendientes si olvidas confirmarlas/abortarlas.
- **Configuración flexible**: lee de `App.config` o de un archivo `ConnectionString.xml`, con caché de valores para mejor rendimiento.

---

## Bases de datos soportadas

| Base de datos | Proveedor ADO.NET | Paquete NuGet |
|---|---|---|
| SQL Server | `System.Data.SqlClient` | incluido en .NET Framework |
| MySQL / MariaDB | `MySql.Data` | `MySql.Data` 26.7.0 |
| Oracle | ODP.NET Managed Driver | `Oracle.ManagedDataAccess` 23.26.300 |
| PostgreSQL | `Npgsql` | `Npgsql` 8.0.6 |
| SQLite | `System.Data.SQLite` | `System.Data.SQLite.Core` 1.0.119.0 |
| ODBC genérico | `System.Data.Odbc` | incluido en .NET Framework |
| OLE DB genérico | `System.Data.OleDb` | incluido en .NET Framework |

> **Nota:** SAP HANA (`SAPHANNA`) está declarado en los enumerados pero **no implementado**; usarlo caerá en el comportamiento por defecto de SQL Client.

---

## Requisitos

- .NET Framework 4.8.1 (o superior) — Windows.
- Visual Studio 2019/2022 o .NET SDK con soporte para `net481`.
- NuGet para la restauración de paquetes.

---

## Instalación

1. **Compila el proyecto** (o descarga `DAL.dll`):

   ```powershell
   dotnet restore DAL.sln
   dotnet build DAL.sln -c Release
   ```

2. **Referencia la DLL** en tu proyecto:

   - **Visual Studio:** clic derecho en *References* → *Add Reference* → *Browse* → selecciona `bin\Release\net481\DAL.dll`.
   - **SDK-style csproj** (agrega una referencia de proyecto o de ensamblado):

     ```xml
     <ProjectReference Include="..\DAL\DAL.csproj" />
     ```

   - **CLI:**

     ```powershell
     dotnet add reference X:\ruta\a\DAL.csproj
     ```

3. **Copia el contenido de `App.config`** a la configuración de tu aplicación host (ver [Configuración](#configuración)).

---

## Configuración

La biblioteca busca la configuración en este orden de prioridad:

1. **`App.config`** (o `Web.config` de la aplicación host), sección `appSettings`.
2. Si no existe, un archivo **`ConnectionString.xml`** en el directorio de trabajo.

### Opción A — App.config

```xml
<configuration>
  <appSettings>
    <!-- Cadena de conexión completa según el proveedor elegido -->
    <add key="ConnectionString" value="Server=localhost;Database=MiBD;Integrated Security=True" />
    <!-- Indice numérico del proveedor (EnumProviders) -->
    <add key="Provider" value="1" />
    <!-- Indice numérico del tipo de base de datos (EnumProvidersDB) -->
    <add key="ProviderDB" value="1" />
  </appSettings>
</configuration>
```

Valores de `Provider` (enum `EnumProviders`):

| Valor | Proveedor |
|---|---|
| 0 | ODBC |
| 1 | SQLClient (SQL Server) |
| 2 | OLEDB |
| 3 | SQLLITE |
| 4 | MySqlClient |
| 5 | OracleClient |
| 6 | SAPHANNA (no implementado) |
| 7 | Npgsql (PostgreSQL) |

Valores de `ProviderDB` (enum `EnumProvidersDB`): `1 = DB_SQL`, `5 = DB_ORACLE`, `14 = DB_MYSQL`, `16 = DB_POSTGRESQL`, `17 = DB_SQLITE`, etc.

> `App.config` también contiene las secciones `entityFramework`, `DbProviderFactories` (para ODP.NET) y los `bindingRedirect` necesarios para los paquetes NuGet modernos. **No los elimines** o la aplicación fallará en tiempo de ejecución con `FileLoadException`.

### Opción B — ConnectionString.xml

Se puede crear manualmente o de forma programática con `WriteXMLConnection`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<ConnectionString>
  <CS>Server=localhost;Database=MiBD;Integrated Security=True</CS>
  <Provider>1</Provider>
  <ProviderDB>1</ProviderDB>
</ConnectionString>
```

### Opción C — Configuración en código

```csharp
var dal = new DataAccess();
dal.Provider = EnumProviders.SQLClient;
dal.ProviderDB = EnumProvidersDB.DB_SQL;
dal.setConnectionString("Server=localhost;Database=MiBD;Integrated Security=True");
```

---

## Inicio rápido

### Consultar y devolver un `DataSet`

```csharp
using DAL;

var dal = new DataAccess();
DataSet ds = dal.ExecDataSet("SELECT * FROM Clientes");

foreach (DataRow row in ds.Tables[0].Rows)
{
    Console.WriteLine(row["Nombre"]);
}
```

### Leer con un `DataReader`

```csharp
var dal = new DataAccess();
using (IDataReader reader = dal.ExecDataReader("SELECT Id, Nombre FROM Clientes"))
{
    while (reader.Read())
    {
        Console.WriteLine($"{reader["Id"]} - {reader["Nombre"]}");
    }
}
```

### Insertar / Actualizar / Eliminar

```csharp
var dal = new DataAccess();
int filas = dal.ExecNonQuery(
    "INSERT INTO Clientes (Nombre, Edad) VALUES (@Nombre, @Edad)",
    CommandType.Text,
    new ParamStruct[] {
        new ParamStruct("@Nombre", DbType.String, "Juan Pérez"),
        new ParamStruct("@Edad", DbType.Int32, 30)
    });
```

### Obtener un valor único

```csharp
int total = ExecScalarInteger("SELECT COUNT(*) FROM Clientes");
string nombre = ExecScalarString("SELECT TOP 1 Nombre FROM Clientes");
```

### JSON listo para APIs

```csharp
string json = ExecDataReaderJsonS("SELECT Id, Nombre FROM Clientes");
// o mediante instancia:
// string json = new DataAccess().ExecDataReaderJson("SELECT * FROM Clientes");
```

### Ayudantes estáticos de una línea

```csharp
int filas = ExecNonQueryS("UPDATE Clientes SET Activo = 1");
object valor = ExecScalarS("SELECT MAX(Id) FROM Clientes");
```

---

## Referencia de API

### Ejecución

| Método | Descripción |
|---|---|
| `DataSet ExecDataSet(string sql, CommandType type, ParamStruct[] params)` | Devuelve un `DataSet`. |
| `void ExecDataSet(DataSet ds, string sql, CommandType type, ParamStruct[] params)` | Rellena un `DataSet` existente. |
| `IDataReader ExecDataReader(string sql, CommandType type, ParamStruct[] params)` | Devuelve un lector de solo avance (modo conectado). |
| `int ExecNonQuery(string sql, CommandType type, ParamStruct[] params)` | Ejecuta INSERT/UPDATE/DELETE y devuelve filas afectadas. |
| `object ExecScalar(string sql, CommandType type, ParamStruct[] params)` | Devuelve un único valor. |
| `ArrayList ExecPreparedSQL(...)` | Ejecuta un comando y recolecta los valores de parámetros de salida. |
| `void SaveDataSet(DataSet ds, ...insertSQL/updateSQL/deleteSQL...)` | Guarda cambios de un `DataSet` mediante un `DataAdapter`. |

### JSON

| Método | Descripción |
|---|---|
| `string ExecDataReaderJson(string sql, CommandType type)` | Ejecuta y serializa a JSON con Newtonsoft.Json. |
| `IEnumerable<Dictionary<string, object>> SerializeEnumerable(IDataReader)` | Convierte filas a diccionarios (streaming). |
| `List<Dictionary<string, object>> Serialize(IDataReader)` | Convierte filas a una lista de diccionarios. |

### Ayudantes estáticos

| Método | Descripción |
|---|---|
| `static int ExecNonQueryS(string sql, CommandType type)` | `ExecNonQuery` sin instanciar `DataAccess`. |
| `static object ExecScalarS(string sql, CommandType type)` | `ExecScalar` sin instanciar `DataAccess`. |
| `static int ExecScalarInteger(string sql, CommandType type, int def)` | Escalar con tipo seguro y valor por defecto. |
| `static double ExecScalarDouble(string sql, CommandType type, double def)` | Ídem para `double`. |
| `static string ExecScalarString(string sql, CommandType type, string def)` | Ídem para `string`. |
| `static string ExecDataReaderJsonS(string sql, CommandType type)` | JSON sin instanciar `DataAccess`. |

### Transacciones

| Método | Descripción |
|---|---|
| `void BeginTrans(IsolationLevel level)` | Inicia transacción con la conexión por defecto. |
| `void BeginTrans(string connString, IsolationLevel level)` | Inicia transacción con una cadena de conexión explícita. |
| `void CommitTrans(bool closeConnection = true)` | Confirma y cierra (o conserva) la conexión. |
| `void AbortTrans()` | Revierte la transacción. |
| `bool IsInTransaction()` | ¿Hay una transacción activa? |

### Conexión / Pooling

| Método | Descripción |
|---|---|
| `void ClearAllPools()` | Limpia los pools de conexión del proveedor activo. |
| `IDbConnection GetConnection()` | Devuelve una conexión nueva ya configurada. |
| `static bool TestConnection(ref string msg)` | Prueba la conexión; en `msg` devuelve el error si falla. |
| `void WriteXMLConnection(...)` | Escribe `ConnectionString.xml` y refresca la caché. |

### Esquema

| Método | Descripción |
|---|---|
| `DataTable GetShema(string collectionName)` | Metadatos (tablas, columnas, etc.) vía `GetSchema`. |
| `DataTable GetShemaTable()` | Atajo para `GetShema("Tables")`. |

### Propiedades

| Propiedad | Descripción |
|---|---|
| `Provider` / `ProviderDB` | Proveedor y tipo de base de datos activos. |
| `GetConnectionString` / `setConnectionString()` | Cadena de conexión en uso. |
| `CmdTimeout` | Timeout de comandos (por defecto **100 s**). |
| `TransIsolationLevel` | Nivel de aislamiento (por defecto `ReadCommitted`). |
| `ReaderCommandBehavior` | Comportamiento del lector (por defecto `CloseConnection`). |

---

## Transacciones

```csharp
var dal = new DataAccess();
try
{
    dal.BeginTrans(IsolationLevel.ReadCommitted);
    dal.ExecNonQuery("UPDATE Cuentas SET Saldo = Saldo - 100 WHERE Id = 1");
    dal.ExecNonQuery("UPDATE Cuentas SET Saldo = Saldo + 100 WHERE Id = 2");
    dal.CommitTrans(); // confirma y cierra la conexión
}
catch
{
    dal.AbortTrans(); // revierte todo
    throw;
}
```

Cuando hay una transacción activa, todos los métodos de `DataAccess` reutilizan la conexión y la transacción internas en lugar de abrir una nueva. Como `DataAccess` implementa `IDisposable`, conviene usarlo con `using` como protección extra:

```csharp
using (var dal = new DataAccess())
{
    dal.BeginTrans(IsolationLevel.Serializable);
    // ... operaciones ...
    dal.CommitTrans();
}
```

---

## Parámetros

Usa `ParamStruct` para pasar parámetros con seguridad (evita la inyección SQL). El tipo de dato se expresa con `System.Data.DbType`, independiente del proveedor.

```csharp
var p = new ParamStruct
{
    ParamName = "@Id",
    DataType = DbType.Int32,
    value = 42,
    direction = ParameterDirection.Input,
    sourceColumn = "",
    size = 0
};
```

También hay un constructor compacto:

```csharp
var p = new ParamStruct("@Nombre", DbType.String, "Juan");
```

Convenciones por proveedor:

| Proveedor | Prefijo de parámetro |
|---|---|
| SQL Server / SQLite | `@Nombre` |
| MySQL / PostgreSQL / Oracle | `@Nombre` (acepta también `:Nombre` en Oracle) |
| ODBC / OLE DB | `?` (parámetros posicionales) |

**Nota para ODBC/OLE DB:** los parámetros son posicionales; el orden del array debe coincidir con el orden de los `?` en el SQL.

---

## Uso asíncrono

Disponible para operaciones de I/O. Compatible con .NET Framework 4.8.1:

```csharp
var dal = new DataAccess();

Task<DataSet> t = dal.ExecDataSetAsync("SELECT * FROM Pedidos");
DataSet ds = await t;

int filas = await dal.ExecNonQueryAsync("UPDATE Pedidos SET Estado = @Estado",
    CommandType.Text, new ParamStruct[] { new ParamStruct("@Estado", DbType.String, "Enviado") });

object valor = await dal.ExecScalarAsync("SELECT COUNT(*) FROM Pedidos");
```

| Método asíncrono | Notas |
|---|---|
| `Task<DataSet> ExecDataSetAsync(...)` | Usa `Task.Run` para el `Fill`, porque `DbDataAdapter.Fill` no expone API async en .NET Framework. |
| `Task<int> ExecNonQueryAsync(...)` | Usa `ExecuteNonQueryAsync`. |
| `Task<object> ExecScalarAsync(...)` | Usa `ExecuteScalarAsync`. |

---

## Manejo de errores

El DAL captura las excepciones específicas de cada proveedor y las re-lanza envueltas en un `Exception` con un mensaje formateado (mensaje, servidor y origen):

- `SqlException`, `OleDbException`, `OdbcException`, `SQLiteException`, `MySqlException`, `NpgsqlException`, `OracleException`.

Las excepciones que no corresponden a ningún proveedor se re-lanzan con su tipo original (`ExceptionDispatchInfo`).

```csharp
try
{
    var ds = new DataAccess().ExecDataSet("SELECT * FROM NoExiste");
}
catch (Exception ex)
{
    Console.WriteLine(ex.Message); // Error formateado con detalle del proveedor
}
```

> No existe una jerarquía de excepciones personalizada; todo llega como `System.Exception` con el mensaje enriquecido.

---

## Estructura del proyecto

```
DAL/
├── DAL.sln                  # Solución de Visual Studio
├── DAL.csproj               # Proyecto SDK-style (.NET Framework 4.8.1)
├── App.config               # Configuración en tiempo de ejecución (EF6, Oracle, binding redirects)
├── DataAccess.cs            # Implementación principal (~1050 líneas)
│                             #   - DataAccess (API pública)
│                             #   - ProviderFactory (fábrica abstracta de proveedores)
│                             #   - ParamStruct (bag de parámetros)
│                             #   - EnumProviders / EnumProvidersDB
├── ColumnDefinition.cs      # DTO DATAMAP.ColumnDefinition (metadatos de columna)
├── Properties/
│   └── AssemblyInfo.cs      # Metadatos del ensamblado (v1.0.4.4)
└── libman.json              # Bibliotecas de cliente (frontend)
```

### Namespaces

| Namespace | Contenido |
|---|---|
| `DAL` | Clases principales: `DataAccess`, `ProviderFactory`, `ParamStruct`, `EnumProviders`, `EnumProvidersDB`. |
| `DATAMAP` | `ColumnDefinition`, un modelo ligero para metadatos de columnas (generación de scripts SQL). |

### Patrones de diseño

1. **Abstract Factory** — `ProviderFactory` crea `IDbConnection`, `IDbCommand`, `IDbDataAdapter`, `IDbDataParameter` y `DbCommandBuilder` según el proveedor seleccionado (implementado con un diccionario en lugar de `switch`).
2. **Facade** — `DataAccess` oculta toda la mecánica específica del proveedor.
3. **Modos conectado y desconectado** — los métodos `DataSet` crean/destruyen conexiones; los `DataReader` y las transacciones reutilizan conexión abierta.

---

## Compilación

```powershell
# Restaurar paquetes NuGet
dotnet restore DAL.sln

# Compilar (Debug o Release)
dotnet build DAL.sln -c Debug
dotnet build DAL.sln -c Release
```

También puedes abrir `DAL.sln` en Visual Studio 2019/2022 y compilar desde el IDE.

**Salida:** `bin\Debug\net481\DAL.dll` o `bin\Release\net481\DAL.dll`.

---

## Despliegue

1. Copia `DAL.dll` y **todos los ensamblados referenciados** a la carpeta `bin` de la aplicación host, especialmente los específicos de cada proveedor:
   - `Oracle.ManagedDataAccess.dll`
   - `MySql.Data.dll`
   - `Npgsql.dll`
   - `System.Data.SQLite.dll` + `sqliteinterop.dll` (x64/x86)
2. El `App.config` de la aplicación host debe contener las mismas secciones que el de este proyecto:
   - `entityFramework` (si usas EF6)
   - `system.data` / `DbProviderFactories` (para ODP.NET)
   - `runtime` / `bindingRedirect` (obligatorios para los paquetes modernos en .NET Framework)

---

## Seguridad

- **Inyección SQL:** usa siempre `ParamStruct[]` con sentencias parametrizadas. Los métodos que aceptan SQL crudo deben usarse solo con texto confiable; **nunca** concatenes entrada de usuario.
- **ConnectionString.xml:** se escribe como texto plano. Si lo usas, protéjelo con ACL del sistema de archivos y exclúyelo del control de versiones (`.gitignore`).
- **Sin cifrado:** no hay cifrado integrado para cadenas de conexión o datos sensibles.
- **Binding redirects:** no los elimines; su ausencia provoca `FileLoadException` en tiempo de ejecución.

---

## Limitaciones conocidas

- **SAP HANA** (`EnumProviders.SAPHANNA` / `DB_SAPHANNA`) está declarado pero **no implementado** en `ProviderFactory`; usar ese proveedor cae por defecto a SQL Client.
- **`System.ValueTuple`** puede mostrar conflictos de binding en los logs de compilación si existe una versión antigua en la carpeta `packages/` local (residuo de migraciones previas). El conflicto no es fatal.
- **No hay proyecto de pruebas** en la solución. La validación típica es llamar a `DataAccess.TestConnection(ref msg)` desde una aplicación host.
- Los métodos asíncronos capturan excepciones y devuelven valores por defecto (`null`, `-1`, `DataSet` vacío) en lugar de propagarlas; valida los resultados.
