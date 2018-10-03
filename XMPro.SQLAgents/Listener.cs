using System;
using System.Data;
using System.Linq;
using System.Data.SqlClient;
using XMIoT.Framework;
using System.Collections;
using System.Collections.Generic;
using XMIoT.Framework.Settings;
using XMIoT.Framework.Helpers;

namespace XMPro.SQLAgents
{
    public class Listener : IAgent, IPollingAgent
    {
        private Configuration config;
        private SqlConnection connection;
        private object LastTimestamp;

        private string SQLServer => this.config["SQLServer"];
        private string SQLUser => this.config["SQLUser"];
        private string SQLPassword => this.decrypt(this.config["SQLPassword"]);

        private bool SQLUseSQLAuth
        {
            get
            {
                var temp = false;
                bool.TryParse(this.config["SQLUseSQLAuth"], out temp);
                return temp;
            }
        }

        private string SQLDatabase => this.config["SQLDatabase"];
        private string SQLTable => this.config["SQLTable"];
        private string SQLTimestampColumn => this.config["SQLTimestampColumn"];

        private string decrypt(string value)
        {
            var request = new OnDecryptRequestArgs(value);
            this.OnDecryptRequest?.Invoke(this, request);
            return request.DecryptedValue;
        }

        public long UniqueId { get; set; }

        public event EventHandler<OnPublishArgs> OnPublish;

        public event EventHandler<OnDecryptRequestArgs> OnDecryptRequest;

        public string GetConfigurationTemplate(string template, IDictionary<string, string> parameters)
        {
            var settings = Settings.Parse(template);
            new Populator(parameters).Populate(settings);
            TextBox SQLServer = settings.Find("SQLServer") as TextBox;
            SQLServer.HelpText = string.Empty;
            TextBox SQLUser = settings.Find("SQLUser") as TextBox;
            CheckBox SQLUseSQLAuth = settings.Find("SQLUseSQLAuth") as CheckBox;
            TextBox SQLPassword = settings.Find("SQLPassword") as TextBox;
            SQLPassword.Visible = SQLUseSQLAuth.Value;
            string errorMessage = "";

            IList<string> databases = SQLHelpers.GetDatabases(SQLServer, SQLUser, SQLUseSQLAuth, this.decrypt(SQLPassword.Value), out errorMessage);
            DropDown SQLDatabase = settings.Find("SQLDatabase") as DropDown;
            SQLDatabase.Options = databases.Select(i => new Option() { DisplayMemeber = i, ValueMemeber = i }).ToList();

            if (!String.IsNullOrWhiteSpace(SQLDatabase.Value))
            {
                IList<string> tables = SQLHelpers.GetTables(SQLServer, SQLUser, SQLUseSQLAuth, this.decrypt(SQLPassword.Value), SQLDatabase, out errorMessage);
                DropDown SQLTable = settings.Find("SQLTable") as DropDown;
                SQLTable.Options = tables.Select(i => new Option() { DisplayMemeber = i, ValueMemeber = i }).ToList();
                if (tables.Contains(SQLTable.Value) == false)
                    SQLTable.Value = "";

                if (!String.IsNullOrWhiteSpace(SQLTable.Value))
                {
                    IList<DataColumn> columns = SQLHelpers.GetColumns(SQLServer.Value, SQLUser.Value, SQLUseSQLAuth.Value, this.decrypt(SQLPassword.Value), SQLDatabase.Value, SQLTable.Value);
                    DropDown SQLTimestampColumn = settings.Find("SQLTimestampColumn") as DropDown;
                    SQLTimestampColumn.Options = columns.Select(i => new Option() { DisplayMemeber = i.ColumnName, ValueMemeber = i.ColumnName }).ToList();
                    if (columns.Any(c => c.ColumnName == SQLTimestampColumn.Value) == false)
                        SQLTimestampColumn.Value = "";
                }
            }            

            if (!String.IsNullOrWhiteSpace(errorMessage))
                SQLServer.HelpText = errorMessage;

            return settings.ToString();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };
            return SQLHelpers.GetColumns(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.SQLTable)
                .Select(col => new XMIoT.Framework.Attribute(col.ColumnName, col.DataType.GetIoTType()));
        }

        public void Create(Configuration configuration)
        {
            this.config = configuration;
        }

        public void Start()
        {
            this.connection = new SqlConnection(SQLHelpers.GetConnectionString(SQLServer, SQLUser, SQLPassword, SQLUseSQLAuth, SQLDatabase));

            using (SqlDataAdapter a = new SqlDataAdapter(string.Format("SELECT TOP 1 * FROM {0} ORDER BY {1} DESC", SQLHelpers.AddTableQuotes(SQLTable), SQLHelpers.AddColumnQuotes(SQLTimestampColumn)), connection))
            {
                DataTable t = new DataTable();
                a.Fill(t);
                if (t.Rows.Count > 0)
                {
                    this.LastTimestamp = t.Rows[0][SQLTimestampColumn];
                }
            }
        }

        public void Poll()
        {
            using (SqlDataAdapter a = new SqlDataAdapter(string.Format("SELECT * FROM {0} WHERE @checkpoint IS NULL OR {1} > @checkpoint ORDER BY {1}", SQLHelpers.AddTableQuotes(SQLTable), SQLHelpers.AddColumnQuotes(SQLTimestampColumn)), connection))
            {
                a.SelectCommand.Parameters.AddWithValue("@checkpoint", this.LastTimestamp ?? DBNull.Value);
                DataTable t = new DataTable();
                a.Fill(t);
                if (t.Rows.Count > 0)
                {
                    this.LastTimestamp = t.Rows[t.Rows.Count - 1][SQLTimestampColumn];
                }
                IList<IDictionary<string, object>> rtr = new List<IDictionary<string, object>>();
                foreach (DataRow row in t.Rows)
                {
                    IDictionary<string, object> r = new Dictionary<string, object>();
                    foreach (DataColumn col in t.Columns)
                        r.Add(col.ColumnName, row[col]);
                    rtr.Add(r);
                }
                if (rtr.Count > 0)
                    this.OnPublish?.Invoke(this, new OnPublishArgs(rtr.ToArray()));
            }
        }

        public void Destroy()
        {
            try
            {
                connection.Close();
            }
            catch
            {
                connection?.Dispose();
            }
        }

        public string[] Validate(IDictionary<string, string> parameters)
        {
            int i = 1;
            var errors = new List<string>();
            this.config = new Configuration() { Parameters = parameters };

            if (String.IsNullOrWhiteSpace(this.SQLServer))
                errors.Add($"Error {i++}: SQL Server is not specified.");

            if (String.IsNullOrWhiteSpace(this.SQLUser))
                errors.Add($"Error {i++}: Username is not specified.");

            if (this.SQLUseSQLAuth && String.IsNullOrWhiteSpace(this.SQLPassword))
                errors.Add($"Error {i++}: Password is not specified.");

            if (String.IsNullOrWhiteSpace(this.SQLDatabase))
                errors.Add($"Error {i++}: Database is not specified.");

            if (String.IsNullOrWhiteSpace(this.SQLTable))
                errors.Add($"Error {i++}: Table is not specified.");

            if (String.IsNullOrWhiteSpace(this.SQLTimestampColumn))
                errors.Add($"Error {i++}: Timestamp Column is not specified.");

            if (errors.Any() == false)
            {
                var errorMessage = "";
                var server = new TextBox() { Value = this.SQLServer };
                //IList<string> databases = SQLHelpers.GetDatabases(server, new TextBox() { Value = this.SQLUser }, new CheckBox() { Value = this.SQLUseSQLAuth }, this.SQLPassword, out errorMessage);

                //if (string.IsNullOrWhiteSpace(errorMessage) == false)
                //{
                //    errors.Add($"Error {i++}: {errorMessage}");
                //    return errors.ToArray();
                //}

                //if (databases.Any(d => d == this.SQLDatabase) == false)
                //    errors.Add($"Error {i++}: Databse '{this.SQLDatabase}' cannot be found at the server.");

                IList<string> tables = SQLHelpers.GetTables(server, new TextBox() { Value = this.SQLUser }, new CheckBox() { Value = this.SQLUseSQLAuth }, this.SQLPassword, new DropDown() { Value = this.SQLDatabase }, out errorMessage);

                if (string.IsNullOrWhiteSpace(errorMessage) == false)
                {
                    errors.Add($"Error {i++}: {errorMessage}");
                    return errors.ToArray();
                }

                if (tables.Any(d => d == this.SQLTable) == false)
                    errors.Add($"Error {i++}: Table '{this.SQLTable}' cannot be found in {this.SQLDatabase}.");

                try
                {
                    var cols = SQLHelpers.GetColumns(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.SQLTable);
                    
                    if (cols.Any(d => d.ColumnName == this.SQLTimestampColumn) == false)
                        errors.Add($"Error {i++}: Timestamp Column '{this.SQLTimestampColumn}' cannot be found in {this.SQLTable}.");
                }
                catch (Exception ex)
                {
                    errors.Add($"Error {i++}: Could not retrieve the list of colmuns for '{this.SQLTable}' - {ex.Message}");
                }
            }

            return errors.ToArray();
        }
    }
}