using MySql.Data.MySqlClient;
using Npgsql;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Text;
using System.Xml;

namespace DAL
{
    // Each provider supported will be a part of this enum
    public enum EnumProviders
    {
        ODBC,
        SQLClient,
        OLEDB,
        SQLLITE,
        MySqlClient,
        OracleClient,
        SAPHANNA,
        Npgsql,     // postgre sql,
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

    // The sctrucure is to hold parameter info. An Array of this structure
    // is sent to the DAL bcos we should not bind to a 
    // specific type of parameter like SQLParamter
    [Serializable()]
    public struct ParamStruct
    {
        public string ParamName;
        public DbType DataType;
        public object value;
        public ParameterDirection direction;
        public string sourceColumn;
        public int size;
    }


    internal class ProviderFactory
    {

        // Should not be instantiated. So that is always shared
        private ProviderFactory()
        {
        }
        public static void ClearAllPools(EnumProviders provider)
        {
            try
            {


                switch (provider)
                {
                    case EnumProviders.SQLClient:
                        System.Data.SqlClient.SqlConnection.ClearAllPools();
                        break;
                    case EnumProviders.ODBC:
                    case EnumProviders.OLEDB:
                    case EnumProviders.SQLLITE:
                        break;
                    case EnumProviders.MySqlClient:
                        MySql.Data.MySqlClient.MySqlConnection.ClearAllPools();
                        break;
                    case EnumProviders.OracleClient:
                        {
                            Oracle.ManagedDataAccess.Client.OracleConnection.ClearAllPools();
                        }
                        break;
                    case EnumProviders.Npgsql:
                        {

                            Npgsql.NpgsqlConnection.ClearAllPools();
                        }
                        break;
                    default:
                        Console.WriteLine($"ClearAllPools no soportado para: {provider}");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al limpiar el pool para {provider}: {ex.Message}");
            }

        }

        public static IDbCommand GetCommand(EnumProviders provider)
        {
            switch (provider)
            {
                case EnumProviders.SQLClient:
                    {
                        return new SqlCommand();
                    }
                case EnumProviders.ODBC:
                    {
                        return new OdbcCommand();
                    }
                case EnumProviders.OLEDB:
                    {
                        return new OleDbCommand();
                    }
                case EnumProviders.SQLLITE:
                    {
                        return new SQLiteCommand();
                    }
                case EnumProviders.MySqlClient:
                    {
                        return new MySqlCommand();
                    }
                case EnumProviders.OracleClient:
                    {
                        return new OracleCommand();
                    }
                case EnumProviders.Npgsql:
                    {
                        return new NpgsqlCommand();
                    }
            }
            return new SqlCommand();
        }


        public static IDbCommand GetCommand(string strCmdText, CommandType cmdType, int cmdTimeout, ParamStruct[] ParameterArray, EnumProviders provider)
        {
            IDbCommand cmd;
            if (provider == EnumProviders.SQLClient)
                cmd = new SqlCommand();
            else
                cmd = GetCommand(provider);
            Int16 i;
            if (ParameterArray != null)
            {
                for (i = 0; i <= ParameterArray.Length - 1; i++)
                {
                    ParamStruct ps = ParameterArray[i];
                    IDbDataParameter pm = GetParameter(ps.ParamName, ps.direction, ps.value, ps.DataType, ps.sourceColumn, ps.size, provider);
                    cmd.Parameters.Add(pm);
                }
            }
            cmd.CommandTimeout = cmdTimeout;
            cmd.CommandType = cmdType;
            cmd.CommandText = strCmdText;
            return cmd;
        }


        public static IDbConnection GetConnection(EnumProviders provider)
        {
            try
            {
                switch (provider)
                {
                    case EnumProviders.SQLClient:
                        {
                            return new SqlConnection();
                        }
                    case EnumProviders.ODBC:
                        {
                            return new OdbcConnection();
                        }
                    case EnumProviders.OLEDB:
                        {
                            return new OleDbConnection();
                        }

                    case EnumProviders.SQLLITE:
                        {
                            return new SQLiteConnection();
                        }
                    case EnumProviders.MySqlClient:
                        {
                            return new MySqlConnection();
                        }
                    case EnumProviders.OracleClient:
                        {
                            return new OracleConnection();
                        }
                    case EnumProviders.Npgsql:
                        {
                            return new NpgsqlConnection();
                        }
                    default:
                        return new SqlConnection();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return new SqlConnection();

            }

        }

        public static IDbConnection GetConnection(string strConnString, EnumProviders provider)
        {
            if (string.IsNullOrEmpty(strConnString))
                strConnString = GetConnectionStringFromConfig;
            IDbConnection con = null;
            try
            {
                if (provider == EnumProviders.SQLClient)
                    con = new SqlConnection();
                else
                    con = GetConnection(provider);
            }
            catch (Exception)
            {

                if (provider == EnumProviders.SQLClient)
                {
                    con = new SqlConnection();
                }
            }
            //gm parametro dudoso no se de donde salio 2022-09-04
            if (provider == EnumProviders.SQLClient)
            {
                strConnString = strConnString + ";App=" + provider.ToString() + " Provider";
            }
            if (con != null)
                con.ConnectionString = strConnString;
            return con;
        }

        public static IDbDataAdapter GetAdapter(EnumProviders provider)
        {
            switch (provider)
            {
                case EnumProviders.ODBC:
                    {
                        return new OdbcDataAdapter();
                    }

                case EnumProviders.SQLClient:
                    {
                        return new SqlDataAdapter();
                    }

                case EnumProviders.OLEDB:
                    {
                        return new OleDbDataAdapter();
                    }

                case EnumProviders.SQLLITE:
                    {
                        return new SQLiteDataAdapter();
                    }
                case EnumProviders.MySqlClient:
                    {
                        return new MySqlDataAdapter();
                    }
                case EnumProviders.OracleClient:
                    {
                        return new OracleDataAdapter();
                    }
                case EnumProviders.Npgsql:
                    {
                        return new NpgsqlDataAdapter();
                    }
            }
            return new SqlDataAdapter();
        }

        public static IDbDataParameter GetParameter(EnumProviders provider)
        {
            switch (provider)
            {
                case EnumProviders.ODBC:
                    {
                        return new OdbcParameter();
                    }

                case EnumProviders.SQLClient:
                    {
                        return new SqlParameter();
                    }

                case EnumProviders.OLEDB:
                    {
                        return new OleDbParameter();
                    }
                case EnumProviders.SQLLITE:
                    {
                        return new SQLiteParameter();
                    }
                case EnumProviders.MySqlClient:
                    {
                        return new MySqlParameter();
                    }
                case EnumProviders.OracleClient:
                    {
                        return new OracleParameter();
                    }
                case EnumProviders.Npgsql:
                    {
                        return new NpgsqlParameter();
                    }
            }
            return new SqlParameter();
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
            if (sourceColumn != "")
                param.SourceColumn = sourceColumn;
            return param;
        }



        public static IDbTransaction GetTransaction(IDbConnection conn, IsolationLevel transisolationLevel)
        {
            return conn.BeginTransaction(transisolationLevel);
        }



        public static object GetCommandBuilder(EnumProviders provider)
        {
            switch (provider)
            {
                case EnumProviders.ODBC:
                    {
                        return new OdbcCommandBuilder();
                    }

                case EnumProviders.SQLClient:
                    {
                        return new SqlCommandBuilder();
                    }

                case EnumProviders.OLEDB:
                    {
                        return new OleDbCommandBuilder();
                    }
                case EnumProviders.SQLLITE:
                    {
                        return new SQLiteCommandBuilder();
                    }
                case EnumProviders.MySqlClient:
                    {
                        return new MySqlCommandBuilder();
                    }
                case EnumProviders.OracleClient:
                    {
                        return new OracleCommandBuilder();
                    }
                case EnumProviders.Npgsql:
                    {
                        return new NpgsqlCommandBuilder();
                    }
                default:
                    {
                        return new SqlCommandBuilder();
                    }
            }
        }



        // Get the configuration settings
        public static string GetConnectionString
        {
            get
            {
                if (ConfigurationManager.AppSettings.Get("ConnectionString") != null)
                    return ConfigurationManager.AppSettings.Get("ConnectionString");
                else
                    return GetConnectionStringFromXML;
            }
        }
        public static string GetConnectionStringFromConfig
        {
            get
            {
                if (ConfigurationManager.AppSettings.Get("ConnectionString") != null)
                    return ConfigurationManager.AppSettings.Get("ConnectionString");
                else
                    return GetConnectionStringFromXML;
            }
        }
        public static string GetConnectionStringFromXML
        {
            get
            {
                string ConnectionString = "";
                if (File.Exists("ConnectionString.xml"))
                {
                    using (XmlTextReader reader = new XmlTextReader("ConnectionString.xml"))
                    {
                        while ((reader.Read()))
                        {
                            if ((reader.NodeType == XmlNodeType.Element & reader.Name == "CS"))
                                ConnectionString = reader.ReadString();
                        }
                    }
                }
                return ConnectionString;
            }
        }
        public static EnumProviders GetProvider
        {
            get
            {
                try
                {
                    if (ConfigurationManager.AppSettings.Get("Provider") != null)
                        return (EnumProviders)Convert.ToInt16(ConfigurationManager.AppSettings.Get("Provider"));
                    else
                        return GetProviderFromXML;
                }
                catch (Exception)
                {

                    return EnumProviders.SQLClient;
                }

            }
        }
        public static EnumProvidersDB GetProviderDB
        {
            get
            {
                try
                {
                    if (ConfigurationManager.AppSettings.Get("ProviderDB") != null)
                        return (EnumProvidersDB)Convert.ToInt16(ConfigurationManager.AppSettings.Get("ProviderDB"));
                    else
                        return GetProviderDBFromXML;
                }
                catch (Exception)
                {

                    return EnumProvidersDB.DB_SQL;
                }

            }
        }
        public static EnumProviders GetProviderFromXML
        {
            get
            {
                try
                {
                    EnumProviders Provider = EnumProviders.OLEDB;
                    if (File.Exists("ConnectionString.xml"))
                    {
                        using (XmlTextReader reader = new XmlTextReader("ConnectionString.xml"))
                        {
                            while ((reader.Read()))
                            {
                                if ((reader.NodeType == XmlNodeType.Element & reader.Name == "Provider"))
                                    Provider = (EnumProviders)Convert.ToInt16(reader.ReadString());
                            }
                        }
                    }
                    return Provider;
                }
                catch (Exception)
                {
                }
                return EnumProviders.OLEDB;
            }
        }
        public static EnumProvidersDB GetProviderDBFromXML
        {
            get
            {
                try
                {
                    EnumProvidersDB Provider = EnumProvidersDB.DB_UNKNOWN_PROV;
                    if (File.Exists("ConnectionString.xml"))
                    {
                        using (XmlTextReader reader = new XmlTextReader("ConnectionString.xml"))
                        {
                            while ((reader.Read()))
                            {
                                if ((reader.NodeType == XmlNodeType.Element & reader.Name == "ProviderDB"))
                                    Provider = (EnumProvidersDB)Convert.ToInt16(reader.ReadString());
                            }
                        }
                    }
                    return Provider;
                }
                catch (Exception)
                {
                }
                return EnumProvidersDB.DB_UNKNOWN_PROV;
            }
        }
    }


    public class DataAccess
    {
        private IDbTransaction _trans;
        private IsolationLevel _isolationLevel;
        private IDbConnection _conn;
        private int _cmdTimeout;
        private string _connString;
        private EnumProviders _provider;
        private EnumProvidersDB _providerDB;
        private const int COMMAND_TIMEOUT = 100;
        private CommandBehavior _commandBehavior;


        // Getting the config settings and set the default isolation level and
        // DataReader command behavior
        public DataAccess()
        {
            _isolationLevel = IsolationLevel.ReadCommitted;
            _commandBehavior = CommandBehavior.CloseConnection;
            _provider = ProviderFactory.GetProvider;
        }
        public string getConnectionString()
        {
            return _connString;
        }
        public void setConnectionString(string connString)
        {

            _connString = connString;
        }
        // Getting the config settings and set the default isolation level and
        // DataReader command behavior
        public void WriteXMLConnection(string ConnectionString,
            EnumProviders provider,
            EnumProvidersDB providerdb)
        {
            try
            {
                if (File.Exists("ConnectionString.xml"))
                    File.Delete("ConnectionString.xml");
                //My.Computer.FileSystem.DeleteFile("ConnectionString.xml", Microsoft.VisualBasic.FileIO.UIOption.OnlyErrorDialogs, Microsoft.VisualBasic.FileIO.RecycleOption.DeletePermanently, Microsoft.VisualBasic.FileIO.UICancelOption.ThrowException);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: {0}", e.ToString());
            }
            try
            {
                XmlWriterSettings settings = new XmlWriterSettings
                {
                    Indent = true,
                    IndentChars = "    "
                };

                using (XmlWriter writer = XmlWriter.Create("ConnectionString.xml", settings))
                {
                    // Write XML data.
                    writer.WriteStartDocument();

                    writer.WriteStartElement("ConnectionString");
                    writer.WriteElementString("CS", ConnectionString);
                    writer.WriteValue(ConnectionString);
                    writer.WriteElementString("Provider", Convert.ToInt32(provider).ToString());
                    writer.WriteElementString("ProviderDB", Convert.ToInt32(ProviderDB).ToString());

                    writer.WriteEndElement();
                    writer.WriteEndDocument();

                    writer.Flush();
                }
            }

            catch (Exception e)
            {
                Console.WriteLine("Exception: {0}", e.ToString());
            }
        }

        // This method is used by ExecDataSet, ExecScalar, ExecReader and ExecNonQuery.
        // This is a common piece of 
        // code called in these methods
        private void PrepareAll(IDbCommand cmd, ref IDbConnection conn, string strSQL, CommandType cmdType, ParamStruct[] parameterArray)
        {
            // If transaction has already been started
            if (!IsInTransaction())
            {
                string strconn = GetConnectionString;
                //if(string.IsNullOrEmpty(strconn)
                //  strconn= ProviderFactory.GetConnectionStringFromConfig();
                if (conn == null)
                    conn = ProviderFactory.GetConnection(strconn, Provider);
                if (cmd == null)
                    cmd = ProviderFactory.GetCommand(strSQL, cmdType, CmdTimeout, parameterArray, Provider);
                cmd.Connection = conn;
                conn.Open();
            }
            else
            {
                cmd = ProviderFactory.GetCommand(strSQL, cmdType, CmdTimeout, parameterArray, Provider);
                cmd.Transaction = _trans;
                cmd.Connection = _conn;
            }
        }
        public void ClearAllPools()
        {
            ProviderFactory.ClearAllPools(Provider);
        }
        public IDbConnection GetConnection()
        {
            // If transaction has already been started            
            string strconn = GetConnectionString;
            return ProviderFactory.GetConnection(strconn, Provider);
        }


        public EnumProviders Provider
        {
            get
            {
                return _provider;
            }
            set
            {
                _provider = value;
            }
        }
        public EnumProvidersDB ProviderDB
        {
            get
            {
                return _providerDB;
            }
            set
            {
                _providerDB = value;
            }
        }

        public string GetConnectionString
        {
            get
            {
                return _connString;
            }
            set
            {
                _connString = value;
            }
        }

        public IsolationLevel TransIsolationLevel
        {
            get
            {
                return _isolationLevel;
            }
            set
            {
                _isolationLevel = value;
            }
        }

        public int CmdTimeout
        {
            get
            {
                if (_cmdTimeout == 0)
                    return COMMAND_TIMEOUT;
                return _cmdTimeout;
            }
            set
            {
                _cmdTimeout = value;
            }
        }

        // To be used exclusively by the Datareader
        public CommandBehavior ReaderCommandBehavior
        {
            get
            {
                return _commandBehavior;
            }
            set
            {
                _commandBehavior = value;
            }
        }

        public static bool TestConnection(ref string msg)
        {

            DataAccess ds = new DataAccess();
            using (IDbConnection conn = ds.GetConnection())
                try
                {
                    conn.Open();
                    conn.Close();
                    conn.Dispose();
                    return true;
                }
                catch (Exception ex)
                {
                    if (msg != null)
                    {
                        msg = ex.Message;
                    }
                }


            return false;
        }


        public void BeginTrans(string connString, IsolationLevel transisolationLevel)
        {
            _conn = ProviderFactory.GetConnection(connString, Provider);
            _conn.Open();
            _trans = ProviderFactory.GetTransaction(_conn, transisolationLevel);
        }

        public void BeginTrans(IsolationLevel transisolationLevel)
        {
            _conn = ProviderFactory.GetConnection(_connString, Provider);
            _conn.Open();
            _trans = ProviderFactory.GetTransaction(_conn, transisolationLevel);
        }

        public void CommitTrans()
        {
            CommitTrans(true);
        }

        // This is for DataReader usage only. The caller has to pass false here so that
        // the connection is not closed before the DR is closed
        public void CommitTrans(bool CloseConnection)
        {
            _trans.Commit();
            DisposeTrans(CloseConnection);
        }

        public void AbortTrans()
        {
            if (IsInTransaction())
            {
                _trans.Rollback();
                DisposeTrans(true);
            }
        }

        private void DisposeTrans(bool CloseConnection)
        {
            if (CloseConnection)
            {
                if (_conn != null)
                {
                    _conn.Close();
                    _conn.Dispose();
                }
            }
            _trans.Dispose();
        }

        public bool IsInTransaction()
        {
            return (_trans != null);
        }


        // To return a DataSet after running a SQL Statement

        public void ExecDataSet(DataSet ds, string strSQL, CommandType cmdtype = CommandType.Text)
        {
            ExecDataSet(ds, strSQL, cmdtype, null/* TODO Change to default(_) if this is not a reference type */);
        }

        public DataSet ExecDataSet(string strSQL, CommandType cmdtype = CommandType.Text)
        {
            return ExecDataSet(strSQL, cmdtype, null/* TODO Change to default(_) if this is not a reference type */);
        }

        public DataSet ExecDataSet(string strSQL, CommandType cmdtype, ParamStruct[] parameterArray)
        {
            using (DataSet ds = new DataSet("DataSet"))
            {
                ExecDataSet(ds, strSQL, cmdtype, parameterArray);
                return ds;
            }
        }

        public void ExecDataSet(DataSet ds, string strSQL, CommandType cmdtype, ParamStruct[] parameterArray)
        {


            try
            {
                string strconn = GetConnectionString;
                //if(string.IsNullOrEmpty(strconn)
                //  strconn= ProviderFactory.GetConnectionStringFromConfig();
                using (IDbConnection conn = ProviderFactory.GetConnection(strconn, Provider))
                {

                    ///PrepareAll(ref cmd, ref conn, strSQL, cmdtype, parameterArray);
                    using (IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                    {
                        cmd.Connection = conn;
                        conn.Open();
                        IDbDataAdapter da = ProviderFactory.GetAdapter(Provider);
                        da.SelectCommand = cmd;
                        da.Fill(ds);
                    }

                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
            }

        }



        // To run SQL Statements to return DataReader.
        public IDataReader ExecDataReader(string strSQL, CommandType cmdtype, ParamStruct[] parameterArray, IDbConnection conn = null)
        {
            /* TODO Change to default(_) if this is not a reference type */
            ;
            try
            {
                using (IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                {

                    PrepareAll(cmd, ref conn, strSQL, cmdtype, parameterArray);
                    //if ((CmdTimeout >= 0))
                    //    cmd.CommandTimeout = CmdTimeout;

                    return cmd.ExecuteReader(ReaderCommandBehavior);
                }
            }
            catch (Exception ex)
            {
                if (!IsInTransaction() & conn != null)
                {
                    if (conn.State != ConnectionState.Closed)
                        conn.Close();
                    conn.Dispose();
                }
                GenericExceptionHandler(ex);
            }

            return null/* TODO Change to default(_) if this is not a reference type */;
        }

        public IDataReader ExecDataReader(string strSQL, CommandType cmdtype = CommandType.Text)
        {
            return ExecDataReader(strSQL, cmdtype, null);
        }
        public IEnumerable<Dictionary<string, object>> Serialize(IDataReader reader)
        {
            /*var results = new List<Dictionary<string, object>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
                results.Add(SerializeRow(cols, reader));
            reader.Close();
            reader.Dispose();
            return results;*/
            var results = new List<Dictionary<string, object>>();

            using (reader) // Asegura el cierre del reader automáticamente
            {
                while (reader.Read())
                {
                    var row = new Dictionary<string, object>();

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    }

                    results.Add(row);
                }
            }
            return results;
        }
        public string ExecDataReaderJson(string strSQL, CommandType cmdtype = CommandType.Text)
        {
            return JsonConvert.SerializeObject(Serialize(ExecDataReader(strSQL, cmdtype, null/* TODO Change to default(_) if this is not a reference type */)), Newtonsoft.Json.Formatting.Indented);
        }
        public static string ExecDataReaderJsonS(string strSQL, CommandType cmdType = CommandType.Text)
        {
            return new DataAccess().ExecDataReaderJson(strSQL, cmdType);
        }
        private Dictionary<string, object> SerializeRow(IEnumerable<string> cols,
                                                        IDataReader reader)
        {
            var result = new Dictionary<string, object>();
            foreach (var col in cols)
                result.Add(col, reader[col]);
            return result;
        }

        // TO run simple SQL statements W/O returning anything(records) back
        public static int ExecNonQueryS(string strSQL, CommandType cmdType = System.Data.CommandType.Text)
        {
            return new DataAccess().ExecNonQuery(strSQL, cmdType, null);
        }

        public int ExecNonQuery(string strSQL, CommandType cmdType = CommandType.Text)
        {
            return ExecNonQuery(strSQL, cmdType, null/* TODO Change to default(_) if this is not a reference type */);
        }
        public int ExecNonQuery(string strSQL, CommandType cmdtype, ParamStruct[] parameterArray)
        {
            int result = -1;

            try
            {
                string strconn = GetConnectionString;
                using (IDbConnection conn = ProviderFactory.GetConnection(strconn, Provider))
                {
                    using (IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                    {
                        cmd.Connection = conn;
                        conn.Open();
                        result = cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
            }
            finally
            {


            }
            return result;
        }




        // This method saves data in a dataset with a single table and mandates the table name to be "Table".
        // Operations on a single table are batched.
        public void SaveDataSet(DataSet ds, string insertSQL, string deleteSQL, string updateSQL, ParamStruct[] InsertparameterArray, ParamStruct[] DeleteparameterArray, ParamStruct[] UpdateparameterArray
        )
        {
            IDbDataAdapter da = ProviderFactory.GetAdapter(Provider);
            try
            {
                string strconn = GetConnectionString;
                using (IDbConnection cn = ProviderFactory.GetConnection(strconn, Provider))
                {

                    if (!IsInTransaction())
                    {

                        if (insertSQL != "")
                        {
                            da.InsertCommand = ProviderFactory.GetCommand(insertSQL, CommandType.StoredProcedure, CmdTimeout, InsertparameterArray, Provider);
                            da.InsertCommand.Connection = cn;
                        }
                        if (updateSQL != "")
                        {
                            da.UpdateCommand = ProviderFactory.GetCommand(updateSQL, CommandType.StoredProcedure, CmdTimeout, UpdateparameterArray, Provider);
                            da.UpdateCommand.Connection = cn;
                        }
                        if (deleteSQL != "")
                        {
                            da.DeleteCommand = ProviderFactory.GetCommand(deleteSQL, CommandType.StoredProcedure, CmdTimeout, DeleteparameterArray, Provider);
                            da.DeleteCommand.Connection = cn;
                        }
                        cn.Open();

                    }
                    else
                    {
                        if (insertSQL != "")
                        {
                            da.InsertCommand = ProviderFactory.GetCommand(insertSQL, CommandType.StoredProcedure, CmdTimeout, InsertparameterArray, Provider);
                            da.InsertCommand.Connection = _conn;
                            da.InsertCommand.Transaction = _trans;
                        }
                        if (updateSQL != "")
                        {
                            da.UpdateCommand = ProviderFactory.GetCommand(updateSQL, CommandType.StoredProcedure, CmdTimeout, UpdateparameterArray, Provider);
                            da.UpdateCommand.Connection = _conn;
                            da.UpdateCommand.Transaction = _trans;
                        }
                        if (deleteSQL != "")
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
                //if (!IsInTransaction())
                //{
                //    cn.Close();
                //    cn.Dispose();
                //}
                if (insertSQL != "")
                {
                    da.InsertCommand.Parameters.Clear();
                    da.InsertCommand.Dispose();
                }
                if (updateSQL != "")
                {
                    da.UpdateCommand.Parameters.Clear();
                    da.UpdateCommand.Dispose();
                }
                if (deleteSQL != "")
                {
                    da.DeleteCommand.Parameters.Clear();
                    da.DeleteCommand.Dispose();
                }
                ((IDisposable)da).Dispose();
            }
        }


        // To be used for getting single values. like Average, Sum etc from the DB
        public static object ExecScalarS(string strSQL, CommandType cmdType = CommandType.Text)
        {
            DataAccess dal = new DataAccess();
            return dal.ExecScalar(strSQL, cmdType, null/* TODO Change to default(_) if this is not a reference type */);
        }
        public static int ExecScalarInteger(string strSQL, CommandType cmdType = CommandType.Text, int intDefault = 0)
        {
            DataAccess dal = new DataAccess();
            object obj = dal.ExecScalar(strSQL, cmdType, null);
            return (obj != null && obj != DBNull.Value) ? Convert.ToInt32(obj) : intDefault;

        }
        public static double ExecScalarDouble(string strSQL, CommandType cmdType = CommandType.Text, double dblDefault = 0)
        {
            DataAccess dal = new DataAccess();
            object obj = dal.ExecScalar(strSQL, cmdType, null);
            return (obj != null && obj != DBNull.Value) ? Convert.ToDouble(obj) : dblDefault;
        }
        //public static double ExecScalarDateTime(string strSQL, CommandType cmdType = CommandType.Text)
        //{
        //    DataAccess dal = new DataAccess();
        //    object obj = dal.ExecScalar(strSQL, cmdType, null);
        //    return (obj != null && obj != DBNull.Value) ? .ToDouble(obj) : dblDefault;
        //}
        public object ExecScalar(string strSQL, CommandType cmdtype = CommandType.Text, ParamStruct[] parameterArray = null)
        {
            try
            {
                string strconn = GetConnectionString;
                using (IDbConnection conn = ProviderFactory.GetConnection(strconn, Provider))
                {
                    using (IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                    {
                        cmd.Connection = conn;
                        conn.Open();
                        return cmd.ExecuteScalar();
                    }
                }

            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
            }
            finally
            {


            }
            return null;
        }

        //public object ExecScalar(string strSQL, CommandType cmdtype = CommandType.Text)
        //{
        //    return ExecScalar(strSQL, cmdtype, null/* TODO Change to default(_) if this is not a reference type */);
        //}


        // To be used for getting single values. like Average, Sum etc from the DB

        public DataTable GetShema(string strSQL)
        {
            try
            {
                //using (IDbConnection objConnection = GetConnection()) { 
                //    objConnection.Open();
                //    DataTable tab = objConnection.GetSchema(strSQL);
                //    objConnection.Close();
                //    return tab;
                //}
                return null;
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
                return null/* TODO Change to default(_) if this is not a reference type */;
            }
        }
        public DataTable GetShemaTable()
        {
            return GetShema("tables");
        }
        // This can be used to execute an SP and get an array of output params from it
        public ArrayList ExecPreparedSQL(string strSQL, CommandType cmdtype, ParamStruct[] parameterArray)
        {

            IDbConnection conn = null/* TODO Change to default(_) if this is not a reference type */;
            ArrayList alParams = new ArrayList();
            try
            {
                using (IDbCommand cmd = ProviderFactory.GetCommand(strSQL, cmdtype, CmdTimeout, parameterArray, Provider))
                {
                    PrepareAll(cmd, ref conn, strSQL, cmdtype, parameterArray);
                    cmd.ExecuteNonQuery();
                    //IDbDataParameter iParam;
                    foreach (IDbDataParameter iParam in cmd.Parameters)
                    {
                        if (iParam.Direction == ParameterDirection.Output | iParam.Direction == ParameterDirection.InputOutput)
                            alParams.Add(iParam.Value);
                    }
                    return alParams;
                }
            }
            catch (Exception ex)
            {
                GenericExceptionHandler(ex);
            }
            finally
            {
                if (!IsInTransaction())
                {
                    if (conn.State != ConnectionState.Closed)
                        conn.Close();
                    conn.Dispose();
                }

            }
            return null/* TODO Change to default(_) if this is not a reference type */;
        }


        // There should be one hanlder for each supported provider.
        // This is a template and more error handling code should come into place
        private void GenericExceptionHandler(Exception ex)
        {
            try
            {
                if (ex is SqlException)
                    SQLExceptionHandler((SqlException)ex);
                else if (ex is OleDbException)
                    OLEDBExceptionHandler((OleDbException)ex);
                else if (ex is OdbcException)
                    ODBCExceptionHandler((OdbcException)ex);
                else if (ex is SQLiteException)
                    SQLiteExceptionHandler((SQLiteException)ex);
                else if (ex is MySqlException)
                    MySqlExceptionHandler((MySqlException)ex);
                else if (ex is NpgsqlException)
                    NpgsqlExceptionHandler((NpgsqlException)ex);
                else // parece que falta el de oracle ??
                    throw ex;
            }
            catch (Exception)
            {
                throw ex;
            }

        }

        private void SQLExceptionHandler(SqlException ex)
        {

            StringBuilder sb = new StringBuilder();
            if (ex.Errors.Count > 1)
            {
                foreach (SqlException sqlerr in ex.Errors)
                {
                    sb.AppendFormat("Error: {0}{1}", sqlerr.Message, Environment.NewLine);
                    sb.AppendFormat("Server: {0}{1}", sqlerr.Server, Environment.NewLine);
                    sb.AppendFormat("Source: {0}{1}", sqlerr.Source, Environment.NewLine);
                    sb.Append("-----------------------------------------------");
                }
            }
            else
            {
                sb.AppendFormat("Error: {0}{1}", ex.Message, Environment.NewLine);
                sb.AppendFormat("Server: {0}{1}", ex.Server, Environment.NewLine);
                sb.AppendFormat("Source: {0}{1}", ex.Source, Environment.NewLine);
                sb.Append("----------------------------------------------");
            }
            // TODO For each custom sql server error have an entry
            throw new Exception(sb.ToString(), ex);
        }

        private void OLEDBExceptionHandler(OleDbException ex)
        {

            StringBuilder sb = new StringBuilder();
            foreach (OleDbError oledberr in ex.Errors)
            {
                sb.AppendFormat("Error: {0}{1}", oledberr.Message, Environment.NewLine);
                sb.AppendFormat("Source: {0}{1}", oledberr.Source, Environment.NewLine);
                sb.Append("-----------------------------------------------");
            }
            // TODO For each custom sql server error have an entry
            throw new Exception(sb.ToString(), ex);
        }

        private void ODBCExceptionHandler(OdbcException ex)
        {

            StringBuilder sb = new StringBuilder();
            foreach (OdbcError odbcerr in ex.Errors)
            {
                sb.AppendFormat("Error: {0}{1}", odbcerr.Message, Environment.NewLine);
                sb.AppendFormat("Source: {0}{1}", odbcerr.Source, Environment.NewLine);
                sb.Append("-----------------------------------------------");
            }
            // TODO For each custom sql server error have an entry
            throw new Exception(sb.ToString(), ex);
        }
        private void SQLiteExceptionHandler(SQLiteException ex)
        {
            // Dim odbcerr As SQLiteErrorCode

            StringBuilder sb = new StringBuilder();
            // For Each odbcerr In ex.
            sb.AppendFormat("SQLite Error: {0}{1}", ex.Message, Environment.NewLine);
            sb.AppendFormat("Source: {0}{1}", ex.Source, Environment.NewLine);
            sb.Append("-----------------------------------------------");
            // Next
            // TODO For each custom sql server error have an entry
            throw new Exception(sb.ToString(), ex);
        }
        private void MySqlExceptionHandler(MySqlException ex)
        {
            // Dim odbcerr As SQLiteErrorCode

            StringBuilder sb = new StringBuilder();
            // For Each odbcerr In ex.
            sb.AppendFormat("MySql Error: {0}{1}", ex.Message, Environment.NewLine);
            sb.AppendFormat("Source: {0}{1}", ex.Source, Environment.NewLine);
            sb.Append("-----------------------------------------------");
            // Next
            // TODO For each custom sql server error have an entry
            throw new Exception(sb.ToString(), ex);
        }
        private void NpgsqlExceptionHandler(NpgsqlException ex)
        {
            // Dim odbcerr As SQLiteErrorCode

            StringBuilder sb = new StringBuilder();
            // For Each odbcerr In ex.
            sb.AppendFormat("Pgsql Error: {0}{1}", ex.Message, Environment.NewLine);
            sb.AppendFormat("Source: {0}{1}", ex.Source, Environment.NewLine);
            sb.Append("-----------------------------------------------");
            // Next
            // TODO For each custom sql server error have an entry
            throw new Exception(sb.ToString(), ex);
        }
    }
}