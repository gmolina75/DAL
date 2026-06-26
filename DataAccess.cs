using MySql.Data.MySqlClient;
using Npgsql;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace DAL
{
    public enum EnumProviders
    {
        ODBC,
        SQLClient,
        OLEDB,
        SQLLITE,
        MySqlClient,
        OracleClient,
        SAPHANNA,
        Npgsql,
        none = -1
    }

    public enum EnumProvidersDB
    {
        DB_UNKNOWN_PROV,
        DB_SQL,
        DB_FOX,
        DB_ACCESS,
        DB_TEXT,
        DB_ORACLE,
        DB_EXCEL,
        DB_ODBC,
        DB_XML,
        DB_MYSQL,
        DB_DB2,
        DB_CLIPPER,
        DB_SYBASE_ASA,
        DB_SYBASE_ASE,
        DB_INFORMIX,
        DB_SQL2005,
        DB_POSTGRESQL,
        DB_PERVASIVE,
        DB_SQLITE,
        DB_SAPHANNA,
        DB_TOP,
        DB_NONE = -1
    }

    [Serializable]
    public struct ParamStruct
    {
        public string ParamName;
        public DbType DataType;
        public object value;
        public ParameterDirection direction;
        public string sourceColumn;
        public int size;

        public ParamStruct(string paramName, DbType dataType, object value = null,
            ParameterDirection direction = ParameterDirection.Input,
            string sourceColumn = "", int size = 0)
        {
            ParamName = paramName;
            DataType = dataType;
            this.value = value;
            this.direction = direction;
            this.sourceColumn = sourceColumn;
            this.size = size;
        }
    }

    internal static class ProviderFactory
    {
        private sealed class ProviderMeta
        {
            public Func<IDbConnection> ConnectionFactory { get; set; }
            public Func<IDbCommand> CommandFactory { get; set; }
            public Func<IDbDataAdapter> AdapterFactory { get; set; }
            public Func<IDbDataParameter> ParameterFactory { get; set; }
            public Func<DbCommandBuilder> CommandBuilderFactory { get; set; }
            public Action ClearPools { get; set; }
        }

        private static readonly Dictionary<EnumProviders, ProviderMeta> _providers = new Dictionary<EnumProviders, ProviderMeta>
        {
            [EnumProviders.SQLClient] = new ProviderMeta
            {
                ConnectionFactory = () => new SqlConnection(),
                CommandFactory = () => new SqlCommand(),
                AdapterFactory = () => new SqlDataAdapter(),
                ParameterFactory = () => new SqlParameter(),
                CommandBuilderFactory = () => new SqlCommandBuilder(),
                ClearPools = SqlConnection.ClearAllPools
            },
            [EnumProviders.ODBC] = new ProviderMeta
            {
                ConnectionFactory = () => new OdbcConnection(),
                CommandFactory = () => new OdbcCommand(),
                AdapterFactory = () => new OdbcDataAdapter(),
                ParameterFactory = () => new OdbcParameter(),
                CommandBuilderFactory = () => new OdbcCommandBuilder(),
                ClearPools = () => { }
            },
            [EnumProviders.OLEDB] = new ProviderMeta
            {
                ConnectionFactory = () => new OleDbConnection(),
                CommandFactory = () => new OleDbCommand(),
                AdapterFactory = () => new OleDbDataAdapter(),
                ParameterFactory = () => new OleDbParameter(),
                CommandBuilderFactory = () => new OleDbCommandBuilder(),
                ClearPools = () => { }
            },
            [EnumProviders.SQLLITE] = new ProviderMeta
            {
                ConnectionFactory = () => new SQLiteConnection(),
                CommandFactory = () => new SQLiteCommand(),
                AdapterFactory = () => new SQLiteDataAdapter(),
                ParameterFactory = () => new SQLiteParameter(),
                CommandBuilderFactory = () => new SQLiteCommandBuilder(),
                ClearPools = () => { }
            },
            [EnumProviders.MySqlClient] = new ProviderMeta
            {
                ConnectionFactory = () => new MySqlConnection(),
                CommandFactory = () => new MySqlCommand(),
                AdapterFactory = () => new MySqlDataAdapter(),
                ParameterFactory = () => new MySqlParameter(),
                CommandBuilderFactory = () => new MySqlCommandBuilder(),
                ClearPools = MySqlConnection.ClearAllPools
            },
            [EnumProviders.OracleClient] = new ProviderMeta
            {
                ConnectionFactory = () => new OracleConnection(),
                CommandFactory = () => new OracleCommand(),
                AdapterFactory = () => new OracleDataAdapter(),
                ParameterFactory = () => new OracleParameter(),
                CommandBuilderFactory = () => new OracleCommandBuilder(),
                ClearPools = OracleConnection.ClearAllPools
            },
            [EnumProviders.Npgsql] = new ProviderMeta
            {
                ConnectionFactory = () => new NpgsqlConnection(),
                CommandFactory = () => new NpgsqlCommand(),
                AdapterFactory = () => new NpgsqlDataAdapter(),
                ParameterFactory = () => new NpgsqlParameter(),
                CommandBuilderFactory = () => new NpgsqlCommandBuilder(),
                ClearPools = NpgsqlConnection.ClearAllPools
            }
        };

        private static ProviderMeta GetMeta(EnumProviders provider)
        {
            if (_providers.TryGetValue(provider, out var meta))
                return meta;
            Trace.WriteLine($"Provider {provider} no esta implementado. Usando SQLClient por defecto.");
            return _providers[EnumProviders.SQLClient];
        }

        public static void ClearAllPools(EnumProviders provider)
        {
            try
            {
                GetMeta(provider).ClearPools?.Invoke();
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error al limpiar el pool para {provider}: {ex.Message}");
            }
        }

        public static IDbConnection GetConnection(EnumProviders provider) => GetMeta(provider).ConnectionFactory();
        public static IDbCommand GetCommand(EnumProviders provider) => GetMeta(provider).CommandFactory();
        public static IDbDataAdapter GetAdapter(EnumProviders provider) => GetMeta(provider).AdapterFactory();
        public static IDbDataParameter GetParameter(EnumProviders provider) => GetMeta(provider).ParameterFactory();
        public static DbCommandBuilder GetCommandBuilder(EnumProviders provider) => GetMeta(provider).CommandBuilderFactory();

        public static IDbCommand GetCommand(string strCmdText, CommandType cmdType, int cmdTimeout, ParamStruct[] parameterArray, EnumProviders provider)
        {
            var cmd = GetCommand(provider);
            if (parameterArray != null)
            {
                foreach (var ps in parameterArray)
                {
                    var pm = GetParameter(ps.ParamName, ps.direction, ps.value, ps.DataType, ps.sourceColumn, ps.size, provider);
                    cmd.Parameters.Add(pm);
                }
            }
            cmd.CommandTimeout = cmdTimeout;
            cmd.CommandType = cmdType;
            cmd.CommandText = strCmdText;
            return cmd;
        }

        public static IDbConnection GetConnection(string strConnString, EnumProviders provider)
        {
            if (string.IsNullOrEmpty(strConnString))
                strConnString = GetConnectionStringFromConfig;
            var conn = GetConnection(provider);
            conn.ConnectionString = strConnString;
            return conn;
        }

        public static IDbDataParameter GetParameter(string paramName, ParameterDirection paramDirection, object paramValue, DbType paramtype, string sourceColumn, int size, EnumProviders provider)
        {
            IDbDataParameter param = GetParameter(provider);
            param.ParameterName = paramName;
            param.DbType = paramtype;
            if (size > 0)
                param.Size = size;
            if (paramValue != null)
                param.Value = paramValue;
            param.Direction = paramDirection;
            if (!string.IsNullOrEmpty(sourceColumn))
                param.SourceColumn = sourceColumn;
            return param;
        }

        public static IDbTransaction GetTransaction(IDbConnection conn, IsolationLevel transisolationLevel) => conn.BeginTransaction(transisolationLevel);

        private static string _cachedConnectionString;
        private static EnumProviders _cachedProvider = EnumProviders.none;
        private static EnumProvidersDB _cachedProviderDB = EnumProvidersDB.DB_NONE;
        private static readonly object _configSync = new object();

        public static void RefreshConfigCache()
        {
            lock (_configSync)
            {
                _cachedConnectionString = null;
                _cachedProvider = EnumProviders.none;
                _cachedProviderDB = EnumProvidersDB.DB_NONE;
            }
        }

        private static string ReadAppSetting(string key)
        {
            try { return ConfigurationManager.AppSettings.Get(key); }
            catch { return null; }
        }

        private static T ReadXmlValue<T>(string elementName, T defaultValue) where T : IConvertible
        {
            const string fileName = "ConnectionString.xml";
            if (!File.Exists(fileName)) return defaultValue;
            try
            {
                using (var reader = XmlReader.Create(fileName))
                {
                    while (reader.Read())
                    {
                        if (reader.NodeType == XmlNodeType.Element && reader.Name == elementName)
                        {
                            string value = reader.ReadString();
                            return (T)Convert.ChangeType(value, typeof(T));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error leyendo {elementName} de {fileName}: {ex.Message}");
            }
            return defaultValue;
        }

        public static string GetConnectionString
        {
            get
            {
                if (_cachedConnectionString != null) return _cachedConnectionString;
                lock (_configSync)
                {
                    if (_cachedConnectionString != null) return _cachedConnectionString;
                    var val = ReadAppSetting("ConnectionString");
                    _cachedConnectionString = val ?? ReadXmlValue("CS", "");
                    return _cachedConnectionString;
                }
            }
        }

        public static string GetConnectionStringFromConfig => GetConnectionString;

        public static EnumProviders GetProvider
        {
            get
            {
                if (_cachedProvider != EnumProviders.none) return _cachedProvider;
                lock (_configSync)
                {
                    if (_cachedProvider != EnumProviders.none) return _cachedProvider;
                    var val = ReadAppSetting("Provider");
                    if (val != null && short.TryParse(val, out short p))
                        _cachedProvider = (EnumProviders)p;
                    else
                        _cachedProvider = ReadXmlValue("Provider", EnumProviders.SQLClient);
                    return _cachedProvider;
                }
            }
        }

        public static EnumProvidersDB GetProviderDB
        {
            get
            {
                if (_cachedProviderDB != EnumProvidersDB.DB_NONE) return _cachedProviderDB;
                lock (_configSync)
                {
                    if (_cachedProviderDB != EnumProvidersDB.DB_NONE) return _cachedProviderDB;
                    var val = ReadAppSetting("ProviderDB");
                    if (val != null && short.TryParse(val, out short p))
                        _cachedProviderDB = (EnumProvidersDB)p;
                    else
                        _cachedProviderDB = ReadXmlValue("ProviderDB", EnumProvidersDB.DB_SQL);
                    return _cachedProviderDB;
                }
            }
        }
    }

    public class DataAccess : IDisposable
    {
        private bool _disposed;
        private IDbTransaction _trans;
        private IsolationLevel _isolationLevel;
        private IDbConnection _conn;
        private int _cmdTimeout;
        private string _connString;
        private EnumProviders _provider;
        private EnumProvidersDB _providerDB;
        private const int COMMAND_TIMEOUT = 100;
        private CommandBehavior _commandBehavior;

        public DataAccess()
        {
            _isolationLevel = IsolationLevel.ReadCommitted;
            _commandBehavior = CommandBehavior.CloseConnection;
            _provider = ProviderFactory.GetProvider;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                if (_trans != null)
                {
                    try { _trans.Rollback(); } catch { /* ignore */ }
                    _trans.Dispose();
                    _trans = null;
                }
                if (_conn != null)
                {
                    if (_conn.State != ConnectionState.Closed)
                        try { _conn.Close(); } catch { /* ignore */ }
                    _conn.Dispose();
                    _conn = null;
                }
            }
            _disposed = true;
        }

        public string getConnectionString() => _connString;
        public void setConnectionString(string connString) => _connString = connString;

        public void WriteXMLConnection(string ConnectionString, EnumProviders provider, EnumProvidersDB providerdb)
        {
            try
            {
                if (File.Exists("ConnectionString.xml"))
                    File.Delete("ConnectionString.xml");
            }
            catch (Exception e)
            {
                Trace.WriteLine($"Exception: {e}");
            }

            try
            {
                var settings = new XmlWriterSettings { Indent = true, IndentChars = "    " };
                using (var writer = XmlWriter.Create("ConnectionString.xml", settings))
                {
                    writer.WriteStartDocument();
                    writer.WriteStartElement("ConnectionString");
                    writer.WriteElementString("CS", ConnectionString);
                    writer.WriteElementString("Provider", Convert.ToInt32(provider).ToString());
                    writer.WriteElementString("ProviderDB", Convert.ToInt32(providerdb).ToString());
                    writer.WriteEndElement();
                    writer.WriteEndDocument();
                    writer.Flush();
                }
                ProviderFactory.RefreshConfigCache();
            }
            catch (Exception e)
            {
                Trace.WriteLine($"Exception: {e}");
            }
        }

        private void EnsureNotDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(DataAccess));
        }

        private void PrepareAll(IDbCommand cmd, ref IDbConnection conn, string strSQL, CommandType cmdType, ParamStruct[] parameterArray)
        {
            if (!IsInTransaction())
            {
                if (conn == null)
                    conn = ProviderFactory.GetConnection(GetConnectionString, Provider);
                cmd.Connection = conn;
                if (conn.State != ConnectionState.Open)
                    conn.Open();
            }
            else
            {
                cmd.Transaction = _trans;
                cmd.Connection = _conn;
            }
        }

        public void ClearAllPools() => ProviderFactory.ClearAllPools(Provider);

        public IDbConnection GetConnection()
        {
            EnsureNotDisposed();
            return ProviderFactory.GetConnection(GetConnectionString, Provider);
        }

        public EnumProviders Provider
        {
            get => _provider;
            set => _provider = value;
        }

        public EnumProvidersDB ProviderDB
        {
            get => _providerDB;
            set => _providerDB = value;
        }

        public string GetConnectionString
        {
            get => _connString ?? ProviderFactory.GetConnectionString;
            set => _connString = value;
        }

        public IsolationLevel TransIsolationLevel
        {
            get => _isolationLevel;
            set => _isolationLevel = value;
        }

        public int CmdTimeout
        {
            get => _cmdTimeout == 0 ? COMMAND_TIMEOUT : _cmdTimeout;
            set => _cmdTimeout = value;
        }

        public CommandBehavior ReaderCommandBehavior
        {
            get => _commandBehavior;
            set => _commandBehavior = value;
        }

        public static bool TestConnection(ref string msg)
        {
            using (var ds = new DataAccess())
            {
                using (IDbConnection conn = ds.GetConnection())
                {
                    try
                    {
                        conn.Open();
                        return true;
                    }
                    catch (Exception ex)
                    {
                        msg = ex.Message;
                        return false;
                    }
                }
            }
        }

        public void BeginTrans(string connString, IsolationLevel transisolationLevel)
        {
            EnsureNotDisposed();
            if (IsInTransaction())
                throw new InvalidOperationException("Ya existe una transaccion activa.");
            _conn = ProviderFactory.GetConnection(connString, Provider);
            _conn.Open();
            _trans = ProviderFactory.GetTransaction(_conn, transisolationLevel);
        }

        public void BeginTrans(IsolationLevel transisolationLevel)
        {
            EnsureNotDisposed();
            if (IsInTransaction())
                throw new InvalidOperationException("Ya existe una transaccion activa.");
            _conn = ProviderFactory.GetConnection(_connString, Provider);
            _conn.Open();
            _trans = ProviderFactory.GetTransaction(_conn, transisolationLevel);
        }

        public void CommitTrans() => CommitTrans(true);

        public void CommitTrans(bool closeConnection)
        {
            EnsureNotDisposed();
            if (_trans == null) return;
            try
            {
                _trans.Commit();
            }
            finally
            {
                DisposeTrans(closeConnection);
            }
        }

        public void AbortTrans()
        {
            EnsureNotDisposed();
            if (!IsInTransaction()) return;
            try
            {
                _trans.Rollback();
            }
            catch (Exception ex)
            {
                Trace.WriteLine($"Error en Rollback: {ex.Message}");
            }
            finally
            {
                DisposeTrans(true);
            }
        }

        private void DisposeTrans(bool closeConnection)
        {
            if (_trans != null)
            {
                _trans.Dispose();
                _trans = null;
            }
            if (closeConnection && _conn != null)
            {
                if (_conn.State != ConnectionState.Closed)
                {
                    try { _conn.Close(); } catch { /* ignore */ }
                }
                _conn.Dispose();
                _conn = null;
            }
        }

        public bool IsInTransaction() => _trans != null;

        public void ExecDataSet(DataSet ds, string strSQL, CommandType cmdtype = CommandType.Text)
        {
            ExecDataSet(ds, strSQL, cmdtype, null);
        }

        public DataSet ExecDataSet(string strSQL, CommandType cmdtype = CommandType.Text)
        {
            return ExecDataSet(strSQL, cmdtype, null);
        }

        public DataSet ExecDataSet(string strSQL, CommandType cmdtype, ParamStruct[] parameterArray)
        {
            using (var ds = new DataSet("DataSet"))
            {
                ExecDataSet(ds, strSQL, cmdtype, parameterArray);
                return ds;
            }
        }

        public void ExecDataSet(DataSet ds, string strSQL, CommandType cmdtype, ParamStruct[] parameterArray)
        {
            EnsureNotDisposed();
            try
            {
                using (IDbConnection conn = ProviderFactory.GetConnection(GetConnectionString, Provider))
                using (IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                {
                    cmd.Connection = conn;
                    conn.Open();
                    IDbDataAdapter da = ProviderFactory.GetAdapter(Provider);
                    da.SelectCommand = cmd;
                    da.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
            }
        }

        public IDataReader ExecDataReader(string strSQL, CommandType cmdtype, ParamStruct[] parameterArray, IDbConnection conn = null)
        {
            EnsureNotDisposed();
            try
            {
                IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider);
                PrepareAll(cmd, ref conn, strSQL, cmdtype, parameterArray);
                return cmd.ExecuteReader(ReaderCommandBehavior);
            }
            catch (Exception ex)
            {
                if (!IsInTransaction() && conn != null)
                {
                    try
                    {
                        if (conn.State != ConnectionState.Closed)
                            conn.Close();
                    }
                    catch { /* ignore */ }
                    conn.Dispose();
                }
                GenericExceptionHandler(ex);
                return null;
            }
        }

        public IDataReader ExecDataReader(string strSQL, CommandType cmdtype = CommandType.Text)
        {
            return ExecDataReader(strSQL, cmdtype, null);
        }

        public IEnumerable<Dictionary<string, object>> SerializeEnumerable(IDataReader reader)
        {
            while (reader.Read())
            {
                var row = new Dictionary<string, object>(reader.FieldCount);
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                }
                yield return row;
            }
        }

        public List<Dictionary<string, object>> Serialize(IDataReader reader)
        {
            using (reader)
            {
                return new List<Dictionary<string, object>>(SerializeEnumerable(reader));
            }
        }

        public string ExecDataReaderJson(string strSQL, CommandType cmdtype = CommandType.Text)
        {
            using (IDataReader reader = ExecDataReader(strSQL, cmdtype, null))
            {
                if (reader == null) return "[]";
                return JsonConvert.SerializeObject(SerializeEnumerable(reader), Newtonsoft.Json.Formatting.Indented);
            }
        }

        public static string ExecDataReaderJsonS(string strSQL, CommandType cmdType = CommandType.Text)
        {
            using (var dal = new DataAccess())
            {
                return dal.ExecDataReaderJson(strSQL, cmdType);
            }
        }

        public static int ExecNonQueryS(string strSQL, CommandType cmdType = CommandType.Text)
        {
            using (var dal = new DataAccess())
            {
                return dal.ExecNonQuery(strSQL, cmdType, null);
            }
        }

        public int ExecNonQuery(string strSQL, CommandType cmdType = CommandType.Text)
        {
            return ExecNonQuery(strSQL, cmdType, null);
        }

        public int ExecNonQuery(string strSQL, CommandType cmdtype, ParamStruct[] parameterArray)
        {
            EnsureNotDisposed();
            try
            {
                using (IDbConnection conn = ProviderFactory.GetConnection(GetConnectionString, Provider))
                using (IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                {
                    cmd.Connection = conn;
                    conn.Open();
                    return cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
                return -1;
            }
        }

        public void SaveDataSet(DataSet ds, string insertSQL, string deleteSQL, string updateSQL,
            ParamStruct[] InsertparameterArray, ParamStruct[] DeleteparameterArray, ParamStruct[] UpdateparameterArray)
        {
            EnsureNotDisposed();
            IDbDataAdapter da = ProviderFactory.GetAdapter(Provider);
            try
            {
                using (IDbConnection cn = ProviderFactory.GetConnection(GetConnectionString, Provider))
                {
                    if (!IsInTransaction())
                    {
                        if (!string.IsNullOrEmpty(insertSQL))
                        {
                            da.InsertCommand = ProviderFactory.GetCommand(insertSQL, CommandType.StoredProcedure, CmdTimeout, InsertparameterArray, Provider);
                            da.InsertCommand.Connection = cn;
                        }
                        if (!string.IsNullOrEmpty(updateSQL))
                        {
                            da.UpdateCommand = ProviderFactory.GetCommand(updateSQL, CommandType.StoredProcedure, CmdTimeout, UpdateparameterArray, Provider);
                            da.UpdateCommand.Connection = cn;
                        }
                        if (!string.IsNullOrEmpty(deleteSQL))
                        {
                            da.DeleteCommand = ProviderFactory.GetCommand(deleteSQL, CommandType.StoredProcedure, CmdTimeout, DeleteparameterArray, Provider);
                            da.DeleteCommand.Connection = cn;
                        }
                        cn.Open();
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(insertSQL))
                        {
                            da.InsertCommand = ProviderFactory.GetCommand(insertSQL, CommandType.StoredProcedure, CmdTimeout, InsertparameterArray, Provider);
                            da.InsertCommand.Connection = _conn;
                            da.InsertCommand.Transaction = _trans;
                        }
                        if (!string.IsNullOrEmpty(updateSQL))
                        {
                            da.UpdateCommand = ProviderFactory.GetCommand(updateSQL, CommandType.StoredProcedure, CmdTimeout, UpdateparameterArray, Provider);
                            da.UpdateCommand.Connection = _conn;
                            da.UpdateCommand.Transaction = _trans;
                        }
                        if (!string.IsNullOrEmpty(deleteSQL))
                        {
                            da.DeleteCommand = ProviderFactory.GetCommand(deleteSQL, CommandType.StoredProcedure, CmdTimeout, DeleteparameterArray, Provider);
                            da.DeleteCommand.Connection = _conn;
                            da.DeleteCommand.Transaction = _trans;
                        }
                    }
                    da.Update(ds);
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
            }
            finally
            {
                SafeDisposeCommand(da.InsertCommand);
                SafeDisposeCommand(da.UpdateCommand);
                SafeDisposeCommand(da.DeleteCommand);
                (da as IDisposable)?.Dispose();
            }
        }

        private void SafeDisposeCommand(IDbCommand cmd)
        {
            if (cmd == null) return;
            try { cmd.Parameters.Clear(); } catch { /* ignore */ }
            cmd.Dispose();
        }

        public static object ExecScalarS(string strSQL, CommandType cmdType = CommandType.Text)
        {
            using (var dal = new DataAccess())
            {
                return dal.ExecScalar(strSQL, cmdType, null);
            }
        }

        public static int ExecScalarInteger(string strSQL, CommandType cmdType = CommandType.Text, int intDefault = 0)
        {
            using (var dal = new DataAccess())
            {
                object obj = dal.ExecScalar(strSQL, cmdType, null);
                return (obj != null && obj != DBNull.Value) ? Convert.ToInt32(obj) : intDefault;
            }
        }

        public static double ExecScalarDouble(string strSQL, CommandType cmdType = CommandType.Text, double dblDefault = 0)
        {
            using (var dal = new DataAccess())
            {
                object obj = dal.ExecScalar(strSQL, cmdType, null);
                return (obj != null && obj != DBNull.Value) ? Convert.ToDouble(obj) : dblDefault;
            }
        }
        public static string ExecScalarString(string strSQL, CommandType cmdType = CommandType.Text, string strDefault = "")
        {
            using (var dal = new DataAccess())
            {
                object obj = dal.ExecScalar(strSQL, cmdType, null);
                return (obj != null && obj != DBNull.Value) ? Convert.ToString(obj) : strDefault;
            }
        }
        public object ExecScalar(string strSQL, CommandType cmdtype = CommandType.Text, ParamStruct[] parameterArray = null)
        {
            EnsureNotDisposed();
            try
            {
                using (IDbConnection conn = ProviderFactory.GetConnection(GetConnectionString, Provider))
                using (IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                {
                    cmd.Connection = conn;
                    conn.Open();
                    return cmd.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
                return null;
            }
        }

        public DataTable GetShema(string collectionName)
        {
            EnsureNotDisposed();
            try
            {
                using (DbConnection dbConn = (DbConnection)GetConnection())
                {
                    dbConn.Open();
                    return dbConn.GetSchema(collectionName);
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
                return null;
            }
        }

        public DataTable GetShemaTable()
        {
            return GetShema("Tables");
        }

        public ArrayList ExecPreparedSQL(string strSQL, CommandType cmdtype, ParamStruct[] parameterArray)
        {
            EnsureNotDisposed();
            IDbConnection conn = null;
            var alParams = new ArrayList();
            try
            {
                using (IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                {
                    PrepareAll(cmd, ref conn, strSQL, cmdtype, parameterArray);
                    cmd.ExecuteNonQuery();
                    foreach (IDbDataParameter iParam in cmd.Parameters)
                    {
                        if (iParam.Direction == ParameterDirection.Output || iParam.Direction == ParameterDirection.InputOutput)
                            alParams.Add(iParam.Value);
                    }
                    return alParams;
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
                return null;
            }
            finally
            {
                if (!IsInTransaction() && conn != null)
                {
                    try
                    {
                        if (conn.State != ConnectionState.Closed)
                            conn.Close();
                    }
                    catch { /* ignore */ }
                    conn.Dispose();
                }
            }
        }

        private void GenericExceptionHandler(Exception ex)
        {
            if (ex is SqlException) SQLExceptionHandler((SqlException)ex);
            else if (ex is OleDbException) OLEDBExceptionHandler((OleDbException)ex);
            else if (ex is OdbcException) ODBCExceptionHandler((OdbcException)ex);
            else if (ex is SQLiteException) SQLiteExceptionHandler((SQLiteException)ex);
            else if (ex is MySqlException) MySqlExceptionHandler((MySqlException)ex);
            else if (ex is NpgsqlException) NpgsqlExceptionHandler((NpgsqlException)ex);
            else if (ex is OracleException) OracleExceptionHandler((OracleException)ex);
            else ExceptionDispatchInfo.Capture(ex).Throw();
        }

        private void BuildErrorMessage(StringBuilder sb, string message, string source, string server = null)
        {
            sb.AppendFormat("Error: {0}{1}", message, Environment.NewLine);
            if (!string.IsNullOrEmpty(server))
                sb.AppendFormat("Server: {0}{1}", server, Environment.NewLine);
            sb.AppendFormat("Source: {0}{1}", source, Environment.NewLine);
            sb.AppendLine("-----------------------------------------------");
        }

        private void SQLExceptionHandler(SqlException ex)
        {
            var sb = new StringBuilder();
            foreach (SqlError sqlerr in ex.Errors)
            {
                BuildErrorMessage(sb, sqlerr.Message, sqlerr.Source, sqlerr.Server);
            }
            throw new Exception(sb.ToString(), ex);
        }

        private void OLEDBExceptionHandler(OleDbException ex)
        {
            var sb = new StringBuilder();
            foreach (OleDbError oledberr in ex.Errors)
            {
                BuildErrorMessage(sb, oledberr.Message, oledberr.Source);
            }
            throw new Exception(sb.ToString(), ex);
        }

        private void ODBCExceptionHandler(OdbcException ex)
        {
            var sb = new StringBuilder();
            foreach (OdbcError odbcerr in ex.Errors)
            {
                BuildErrorMessage(sb, odbcerr.Message, odbcerr.Source);
            }
            throw new Exception(sb.ToString(), ex);
        }

        private void SQLiteExceptionHandler(SQLiteException ex)
        {
            var sb = new StringBuilder();
            BuildErrorMessage(sb, ex.Message, ex.Source);
            throw new Exception(sb.ToString(), ex);
        }

        private void MySqlExceptionHandler(MySqlException ex)
        {
            var sb = new StringBuilder();
            BuildErrorMessage(sb, ex.Message, ex.Source);
            throw new Exception(sb.ToString(), ex);
        }

        private void NpgsqlExceptionHandler(NpgsqlException ex)
        {
            var sb = new StringBuilder();
            BuildErrorMessage(sb, ex.Message, ex.Source);
            throw new Exception(sb.ToString(), ex);
        }

        private void OracleExceptionHandler(OracleException ex)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < ex.Errors.Count; i++)
            {
                var err = ex.Errors[i];
                BuildErrorMessage(sb, err.Message, err.Source);
            }
            throw new Exception(sb.ToString(), ex);
        }

        public async Task<DataSet> ExecDataSetAsync(string strSQL, CommandType cmdtype = CommandType.Text, ParamStruct[] parameterArray = null)
        {
            EnsureNotDisposed();
            var ds = new DataSet("DataSet");
            try
            {
                using (var conn = (DbConnection)ProviderFactory.GetConnection(GetConnectionString, Provider))
                using (var cmd = (DbCommand)ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                {
                    cmd.Connection = conn;
                    await conn.OpenAsync();
                    var da = (DbDataAdapter)ProviderFactory.GetAdapter(Provider);
                    da.SelectCommand = cmd;
                    await Task.Run(() => da.Fill(ds));
                    return ds;
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
                return ds;
            }
        }

        public async Task<int> ExecNonQueryAsync(string strSQL, CommandType cmdtype = CommandType.Text, ParamStruct[] parameterArray = null)
        {
            EnsureNotDisposed();
            try
            {
                using (var conn = (DbConnection)ProviderFactory.GetConnection(GetConnectionString, Provider))
                using (var cmd = (DbCommand)ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                {
                    cmd.Connection = conn;
                    await conn.OpenAsync();
                    return await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
                return -1;
            }
        }

        public async Task<object> ExecScalarAsync(string strSQL, CommandType cmdtype = CommandType.Text, ParamStruct[] parameterArray = null)
        {
            EnsureNotDisposed();
            try
            {
                using (var conn = (DbConnection)ProviderFactory.GetConnection(GetConnectionString, Provider))
                using (var cmd = (DbCommand)ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                {
                    cmd.Connection = conn;
                    await conn.OpenAsync();
                    return await cmd.ExecuteScalarAsync();
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
                return null;
            }
        }
    }
}
