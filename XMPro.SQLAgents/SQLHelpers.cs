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
    }
}