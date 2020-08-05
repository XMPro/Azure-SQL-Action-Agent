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
    public class Listener : IAgent, IPollingAgent, IPublishesError, IUsesVariable
    {
        private Configuration config;
        private SqlConnection connection;
        private object LastTimestamp;

        private bool UseConnectionVariables => bool.TryParse(this.config["UseConnectionVariables"], out bool result) && result;
        private string SQLServer => UseConnectionVariables ? GetVariableValue(this.config["vSQLServer"]) : this.config["SQLServer"];
        private string SQLUser => UseConnectionVariables ? GetVariableValue(this.config["vSQLUser"]) : this.config["SQLUser"];
        private string SQLPassword => UseConnectionVariables ? GetVariableValue(this.config["vSQLPassword"], true) : this.decrypt(this.config["SQLPassword"]);

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
		private string SQLColumns => this.config["SQLColumns"];

        private string decrypt(string value)
        {
            var request = new OnDecryptRequestArgs(value);
            this.OnDecryptRequest?.Invoke(this, request);
            return request.DecryptedValue;
        }

        public long UniqueId { get; set; }

        public event EventHandler<OnPublishArgs> OnPublish;

        public event EventHandler<OnDecryptRequestArgs> OnDecryptRequest;
        public event EventHandler<OnErrorArgs> OnPublishError; public event EventHandler<OnVariableRequestArgs> OnVariableRequest;

        public string GetVariableValue(string variableName, bool isEncrypted = false)
        {
            var x = new OnVariableRequestArgs(variableName);
            this.OnVariableRequest?.Invoke(this, x);
            return isEncrypted ? this.decrypt(x.Value) : x.Value;
        }

        public string GetConfigurationTemplate(string template, IDictionary<string, string> parameters)
        {
            var settings = Settings.Parse(template);
            new Populator(parameters).Populate(settings);
            CheckBox UseConnectionVariables = (CheckBox)settings.Find("UseConnectionVariables");
            TextBox SQLServer = settings.Find("SQLServer") as TextBox;
            SQLServer.HelpText = string.Empty;
            TextBox SQLUser = settings.Find("SQLUser") as TextBox;
            SQLServer.Visible = SQLUser.Visible = (UseConnectionVariables.Value == false);
            CheckBox SQLUseSQLAuth = settings.Find("SQLUseSQLAuth") as CheckBox;
            TextBox SQLPassword = settings.Find("SQLPassword") as TextBox;
            SQLPassword.Visible = !UseConnectionVariables.Value && SQLUseSQLAuth.Value;
            VariableBox vSQLServer = settings.Find("vSQLServer") as VariableBox;
            vSQLServer.HelpText = string.Empty;
            VariableBox vSQLUser = settings.Find("vSQLUser") as VariableBox;
            vSQLServer.Visible = vSQLUser.Visible = (UseConnectionVariables.Value == true);
            VariableBox vSQLPassword = settings.Find("vSQLPassword") as VariableBox;
            vSQLPassword.Visible = UseConnectionVariables.Value && SQLUseSQLAuth.Value;
            string errorMessage = "";

            TextBox sqlServer = SQLServer;
            TextBox sqlUser = SQLUser;
            TextBox sqlPassword = SQLPassword;

            if (UseConnectionVariables.Value)
            {
                sqlServer = new TextBox() { Value = GetVariableValue(vSQLServer.Value) };
                sqlUser = new TextBox() { Value = GetVariableValue(vSQLUser.Value) };
                if (SQLUseSQLAuth.Value)
                    sqlPassword = new TextBox() { Value = GetVariableValue(vSQLPassword.Value) };
            }

            IList<string> databases = SQLHelpers.GetDatabases(sqlServer, sqlUser, SQLUseSQLAuth, this.decrypt(sqlPassword.Value), out errorMessage);
            DropDown SQLDatabase = settings.Find("SQLDatabase") as DropDown;
            SQLDatabase.Options = databases.Select(i => new Option() { DisplayMemeber = i, ValueMemeber = i }).ToList();

            if (!String.IsNullOrWhiteSpace(SQLDatabase.Value))
            {
                IList<string> tables = SQLHelpers.GetTables(sqlServer, sqlUser, SQLUseSQLAuth, this.decrypt(sqlPassword.Value), SQLDatabase, out errorMessage);
                DropDown SQLTable = settings.Find("SQLTable") as DropDown;
                SQLTable.Options = tables.Select(i => new Option() { DisplayMemeber = i, ValueMemeber = i }).ToList();
                if (tables.Contains(SQLTable.Value) == false)
                    SQLTable.Value = "";

                if (!String.IsNullOrWhiteSpace(SQLTable.Value))
                {
                    IList<DataColumn> columns = SQLHelpers.GetColumns(sqlServer.Value, sqlUser.Value, SQLUseSQLAuth.Value, this.decrypt(sqlPassword.Value), SQLDatabase.Value, SQLTable.Value);
                    DropDown SQLTimestampColumn = settings.Find("SQLTimestampColumn") as DropDown;
                    SQLTimestampColumn.Options = columns.Select(i => new Option() { DisplayMemeber = i.ColumnName, ValueMemeber = i.ColumnName }).ToList();
                    if (columns.Any(c => c.ColumnName == SQLTimestampColumn.Value) == false)
                        SQLTimestampColumn.Value = "";

					TokenBox SQLColumns = settings.Find("SQLColumns") as TokenBox;
					SQLColumns.Options = columns.Select(c => new Option() { DisplayMemeber = c.ColumnName, ValueMemeber = c.ColumnName }).ToList();
					if (columns.Count > 0 && !String.IsNullOrEmpty(SQLColumns.Value))
					{
						SQLColumns.Value = String.Join(",", SQLColumns.Value.Split(',').Where(c => columns.Any(col => c == col.ColumnName)));
					}
				}
            }            

            if (!String.IsNullOrWhiteSpace(errorMessage))
                SQLServer.HelpText = vSQLServer.HelpText = errorMessage;

            return settings.ToString();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };
			IList<DataColumn> columns = SQLHelpers.GetColumns(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.SQLTable);

			if (!String.IsNullOrWhiteSpace(this.SQLColumns))
				return columns.Where(c => this.SQLColumns.Split(',').Contains(c.ColumnName)).Select(c => new XMIoT.Framework.Attribute(c.ColumnName, c.DataType.GetIoTType()));
			else
				return columns.Select(c => new XMIoT.Framework.Attribute(c.ColumnName, c.DataType.GetIoTType()));
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
            try
            {
                var columns = String.IsNullOrWhiteSpace(SQLColumns) ? "*" : String.Join(",", SQLColumns.Split(',').Select(c => SQLHelpers.AddColumnQuotes(c)));

                using (SqlDataAdapter a = new SqlDataAdapter(string.Format("SELECT {2} FROM {0} WHERE @checkpoint IS NULL OR {1} > @checkpoint ORDER BY {1}",
                    SQLHelpers.AddTableQuotes(SQLTable), SQLHelpers.AddColumnQuotes(SQLTimestampColumn), columns), connection))
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
                        this.OnPublish?.Invoke(this, new OnPublishArgs(rtr.ToArray(), "Output"));
                }
            }
            catch (Exception ex)
            {
                this.OnPublishError?.Invoke(this, new OnErrorArgs(this.UniqueId, DateTime.UtcNow, "XMPro.SQLAgents.Listener.Poll", ex.Message, ex.InnerException?.ToString() ?? ""));
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