using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using XMIoT.Framework.Helpers;
using XMIoT.Framework.Settings;

namespace XMPro.SQLAgents
{
    internal class SQLHelpers
    {
        internal static string GetConnectionString(string SQLServer, string SQLUser, string SQLPassword, bool SQLUseSQLAuth, string SQLDatabase)
        {
            string conString;
            if (SQLUseSQLAuth)
                conString = String.Format(@"Data Source={0};User ID={1};Password={2};", SQLServer, SQLUser, SQLPassword);
            else
                conString = String.Format(@"Data Source={0};User ID={1};Integrated Security=true;", SQLServer, SQLUser);

            if (String.IsNullOrWhiteSpace(SQLDatabase) == false)
                conString = String.Format("{0}Initial Catalog={1};", conString, SQLDatabase);

            return conString;
        }

        internal static string AddTableQuotes(string Table)
        {
            return String.Join(".", Table.Split(".".ToCharArray()).Select(s => "[" + s + "]"));
        }

        internal static string AddColumnQuotes(string Col)
        {
            return "[" + Col + "]";
        }

        internal static string GetParameterName(string Col)
        {
            return "@" + Regex.Replace(Col, "[^0-9a-zA-Z]+", (Match match) =>
            {
                return string.Join("", match.Value.Select(c => ((uint)c).ToString()));
            });
        }

        internal static IList<string> GetDatabases(TextBox SQLServer, TextBox SQLUser, CheckBox SQLUseSQLAuth, string SQLPassword, out string errorMessage)
        {
            errorMessage = "";
            try
            {
                if (StringExtentions.IsNullOrWhiteSpace(SQLServer.Value, SQLUser.Value) == false
                    && (SQLUseSQLAuth.Value == false || String.IsNullOrWhiteSpace(SQLPassword) == false))
                {
                    using (SqlConnection connection = new SqlConnection(GetConnectionString(SQLServer.Value, SQLUser.Value, SQLPassword, SQLUseSQLAuth.Value, null)))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand("select name from sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb') ORDER BY name", connection))
                        {
                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                var databases = new List<string>();
                                while (reader.Read())
                                    databases.Add(reader.GetString(0));
                                return databases;
                            }
                        }
                    }
                }
                SQLServer.HelpText = String.Empty;
            }
            catch (Exception ex)
            {
                errorMessage = "Unable to fetch list of Databases. Check the login details or try entering the Database name manually.";
            }
            return new List<string>();
        }

        internal static IList<string> GetTables(TextBox SQLServer, TextBox SQLUser, CheckBox SQLUseSQLAuth, string SQLPassword, DropDown SQLDatabase, out string errorMessage)
        {
            errorMessage = "";
            try
            {
                if (StringExtentions.IsNullOrWhiteSpace(SQLServer.Value, SQLUser.Value, SQLDatabase.Value) == false
                    && (SQLUseSQLAuth.Value == false || String.IsNullOrWhiteSpace(SQLPassword) == false))
                {
                    using (SqlConnection connection = new SqlConnection(GetConnectionString(SQLServer.Value, SQLUser.Value, SQLPassword, SQLUseSQLAuth.Value, SQLDatabase.Value)))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand("SELECT [TABLE_SCHEMA] + '.' + [TABLE_NAME] FROM INFORMATION_SCHEMA.TABLES ORDER BY [TABLE_SCHEMA], [TABLE_NAME]", connection))
                        {
                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                var tables = new List<string>();
                                while (reader.Read())
                                    tables.Add(reader.GetString(0));
                                return tables;
                            }
                        }
                    }
                }
                SQLServer.HelpText = String.Empty;
            }
            catch (Exception ex)
            {
                errorMessage = "Unable to fetch list of Tables";
            }
            return new List<string>();
        }

        internal static IList<string> GetUserDefinedTableTypes(TextBox SQLServer, TextBox SQLUser, CheckBox SQLUseSQLAuth, string SQLPassword, DropDown SQLDatabase, out string errorMessage)
        {
            errorMessage = "";
            try
            {
                if (StringExtentions.IsNullOrWhiteSpace(SQLServer.Value, SQLUser.Value, SQLDatabase.Value) == false
                    && (SQLUseSQLAuth.Value == false || String.IsNullOrWhiteSpace(SQLPassword) == false))
                {
                    using (SqlConnection connection = new SqlConnection(GetConnectionString(SQLServer.Value, SQLUser.Value, SQLPassword, SQLUseSQLAuth.Value, SQLDatabase.Value)))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand("select name from sys.types where is_user_defined = 1 and is_table_type = 1", connection))
                        {
                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                var tables = new List<string>();
                                while (reader.Read())
                                    tables.Add(reader.GetString(0));
                                return tables;
                            }
                        }
                    }
                }
                SQLServer.HelpText = String.Empty;
            }
            catch (Exception ex)
            {
                errorMessage = "Unable to fetch list of user defined Table Types";
            }
            return new List<string>();
        }

        internal static List<string> GetTableTypeCoumns(TextBox SQLServer, TextBox SQLUser, CheckBox SQLUseSQLAuth, string SQLPassword, DropDown SQLDatabase, string SQLTableTypeName , out string errorMessage)
        {
            errorMessage = "";
            try
            {
                if (StringExtentions.IsNullOrWhiteSpace(SQLServer.Value, SQLUser.Value, SQLDatabase.Value) == false
                    && (SQLUseSQLAuth.Value == false || String.IsNullOrWhiteSpace(SQLPassword) == false))
                {
                    using (SqlConnection connection = new SqlConnection(GetConnectionString(SQLServer.Value, SQLUser.Value, SQLPassword, SQLUseSQLAuth.Value, SQLDatabase.Value)))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand("select c.name as column_name from sys.table_types t inner join sys.columns c on c.object_id = t.type_table_object_id inner join sys.types y on y.user_type_id = c.user_type_id where t.is_user_defined = 1   and t.is_table_type = 1 and t.name = '" + SQLTableTypeName + "'" , connection))
                        {
                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                var tables = new List<string>();
                                while (reader.Read())
                                    tables.Add(reader.GetString(0));
                                return tables;
                            }
                        }
                    }
                }
                SQLServer.HelpText = String.Empty;
            }
            catch (Exception ex)
            {
                errorMessage = "Unable to fetch list of user defined Table Type Columns";
            }
            return new List<string>();
        }



        internal static IList<DataColumn> GetColumns(string SQLServer, string SQLUser, bool SQLUseSQLAuth, string SQLPassword, string SQLDatabase, string SQLTable)
        {
            if (StringExtentions.IsNullOrWhiteSpace(SQLServer, SQLUser, SQLDatabase, SQLTable) == false
                && (SQLUseSQLAuth == false || String.IsNullOrWhiteSpace(SQLPassword) == false))
            {
                DataTable dt = new DataTable();
                using (SqlConnection connection = new SqlConnection(GetConnectionString(SQLServer, SQLUser, SQLPassword, SQLUseSQLAuth, SQLDatabase)))
                {
                    using (SqlDataAdapter a = new SqlDataAdapter(string.Format("SELECT * FROM {0}", SQLHelpers.AddTableQuotes(SQLTable)), connection))
                    {
                        a.FillSchema(dt, SchemaType.Source);
                        return dt.Columns.Cast<DataColumn>().ToList();
                    }
                }
            }
            else
            {
                return new List<DataColumn>();
            }
        }

        internal static IList<string> GetStoredProcs(TextBox SQLServer, TextBox SQLUser, CheckBox SQLUseSQLAuth, string SQLPassword, DropDown SQLDatabase, out string errorMessage)
        {
            errorMessage = "";
            try
            {
                if (StringExtentions.IsNullOrWhiteSpace(SQLServer.Value, SQLUser.Value, SQLDatabase.Value) == false
                    && (SQLUseSQLAuth.Value == false || String.IsNullOrWhiteSpace(SQLPassword) == false))
                {
                    using (SqlConnection connection = new SqlConnection(GetConnectionString(SQLServer.Value, SQLUser.Value, SQLPassword, SQLUseSQLAuth.Value, SQLDatabase.Value)))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand("select s.name+'.'+p.name as oname from sys.procedures p inner join sys.schemas s on p.schema_id = s.schema_id order by s.name, p.name", connection))
                        {
                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                var procs = new List<string>();
                                while (reader.Read())
                                    procs.Add(reader.GetString(0));
                                return procs;
                            }
                        }
                    }
                }
                SQLServer.HelpText = String.Empty;
            }
            catch (Exception ex)
            {
                errorMessage = "Unable to fetch list of Stored Procedures";
            }
            return new List<string>();
        }

        internal static List<SqlParameter> GetStoredProcParams(string SQLServer, string SQLUser, bool SQLUseSQLAuth, string SQLPassword, string SQLDatabase, string storedProcName)
        {
            if (StringExtentions.IsNullOrWhiteSpace(SQLServer, SQLUser, SQLDatabase) == false
                && (SQLUseSQLAuth == false || String.IsNullOrWhiteSpace(SQLPassword) == false))
            {
                using (SqlConnection connection = new SqlConnection(GetConnectionString(SQLServer, SQLUser, SQLPassword, SQLUseSQLAuth, SQLDatabase)))
                {
                    SqlCommand cmd = new SqlCommand(storedProcName, connection) { CommandType = CommandType.StoredProcedure };
                    cmd.Connection.Open();
                    SqlCommandBuilder.DeriveParameters(cmd);
                    return cmd.Parameters.Cast<SqlParameter>().ToList();
                }
            }

            return new List<SqlParameter>();
        }

        internal static List<SqlParameter> GetStoredProcInParams(string SQLServer, string SQLUser, bool SQLUseSQLAuth, string SQLPassword, string SQLDatabase, string storedProcName)
        {
            return GetStoredProcParams(SQLServer, SQLUser, SQLUseSQLAuth, SQLPassword, SQLDatabase, storedProcName)
                .Where(p => p.Direction == ParameterDirection.Input || p.Direction == ParameterDirection.InputOutput).ToList();
        }

        internal static List<SqlParameter> GetStoredProcOutParams(string SQLServer, string SQLUser, bool SQLUseSQLAuth, string SQLPassword, string SQLDatabase, string storedProcName)
        {
            return GetStoredProcParams(SQLServer, SQLUser, SQLUseSQLAuth, SQLPassword, SQLDatabase, storedProcName)
                .Where(p => p.Direction == ParameterDirection.Output || p.Direction == ParameterDirection.InputOutput).ToList();
        }

        internal static Type GetSystemType(DbType type)
        {
            switch (type)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return typeof(string);
                    break;
                case DbType.VarNumeric:
                    return typeof(long);
                    break;
                case DbType.Time:
                case DbType.Date:
                case DbType.DateTime2:
                    return typeof(DateTime);
                    break;
                case DbType.Binary:
                    return typeof(System.Byte[]);
                    break;
                default:
                    return Type.GetType("System." + type.ToString());

            }
        }
    }
}