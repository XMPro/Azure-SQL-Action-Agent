using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using XMIoT.Framework;
using XMIoT.Framework.Helpers;
using XMIoT.Framework.Settings;
using XMPro.Filter;
using XMPro.Filter.Parser;

namespace XMPro.SQLAgents
{
    public class ContextProvider : IAgent, IPollingAgent, IPublishesError, IUsesVariable
    {
        private Configuration config;
        private SqlConnection connection;
		private string SortClause;
		private string WhereClause;
		private string TopClause;
		private FilterExpressionBase filterExpression;

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
		private string SQLColumns => this.config["SQLColumns"];
		private int MaxRows
		{
			get
			{
				Int32.TryParse(config["MaxRows"] ?? "0", out int rows);
				return rows;
			}
		}
		private string Filters => config["Filters"];

		private string decrypt(string value)
        {
            var request = new OnDecryptRequestArgs(value);
            this.OnDecryptRequest?.Invoke(this, request);
            return request.DecryptedValue;
        }

        public long UniqueId { get; set; }

        public event EventHandler<OnPublishArgs> OnPublish;

        public event EventHandler<OnDecryptRequestArgs> OnDecryptRequest;
        public event EventHandler<OnErrorArgs> OnPublishError;
        public event EventHandler<OnVariableRequestArgs> OnVariableRequest;

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
					TokenBox SQLColumns = settings.Find("SQLColumns") as TokenBox;
					SQLColumns.Options = columns.Select(c => new Option() { DisplayMemeber = c.ColumnName, ValueMemeber = c.ColumnName }).ToList();
					if (columns.Count > 0 && !String.IsNullOrEmpty(SQLColumns.Value))
					{
						SQLColumns.Value = String.Join(",", SQLColumns.Value.Split(',').Where(c => columns.Any(col => c == col.ColumnName)));
					}

					XMIoT.Framework.Settings.Filter Filters = settings.Find("Filters") as XMIoT.Framework.Settings.Filter;
					Filters.Fields = columns.Select(i => new TypedOption() { Type = i.DataType.GetIoTType(), DisplayMemeber = i.ColumnName, ValueMemeber = i.ColumnName }).ToList();

					Grid SortGrid = settings.Find("SortGrid") as Grid;
					DropDown SortColumn = SortGrid.Columns.First(s => s.Key == "SortColumn") as DropDown;
					SortColumn.Options = columns.Select(i => new Option() { DisplayMemeber = i.ColumnName, ValueMemeber = i.ColumnName }).ToList();

					var newRows = new JArray();
					var rows = SortGrid.Rows?.ToList() ?? new List<IDictionary<string, object>>();
					foreach (var row in rows)
					{
						if (columns.Select(c => c.ColumnName).Contains(row["SortColumn"].ToString()) == true)
							newRows.Add(JObject.FromObject(row));
					}
					SortGrid.Value = newRows.ToString();
				}
            }
            
            if (!String.IsNullOrWhiteSpace(errorMessage))
                SQLServer.HelpText = vSQLServer.HelpText = errorMessage;

            return settings.ToString();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetInputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            yield break;
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };
			string connectionStr = SQLHelpers.GetConnectionString(this.SQLServer, this.SQLUser, this.SQLPassword, this.SQLUseSQLAuth, this.SQLDatabase);			

			if (string.IsNullOrWhiteSpace(connectionStr))
			{
				return new XMIoT.Framework.Attribute[0];
			}
			else
			{
				IList<DataColumn> columns = SQLHelpers.GetColumns(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.SQLTable);
				if (!String.IsNullOrWhiteSpace(this.SQLColumns))
					return columns.Where(c => this.SQLColumns.Split(',').Contains(c.ColumnName)).Select(c => new XMIoT.Framework.Attribute(c.ColumnName, c.DataType.GetIoTType()));
				else
					return columns.Select(c => new XMIoT.Framework.Attribute(c.ColumnName, c.DataType.GetIoTType()));
			}
		}

        public void Create(Configuration configuration)
        {
            this.config = configuration;			
		}

        public void Start()
        {
			this.connection = new SqlConnection(SQLHelpers.GetConnectionString(SQLServer, SQLUser, SQLPassword, SQLUseSQLAuth, SQLDatabase));

			Grid sortGrid = new Grid
			{
				Value = config["SortGrid"]
			};
			SortClause = "";
			foreach (var row in sortGrid.Rows)
			{
				SortClause += " " + row["SortColumn"].ToString() + " " + row["SortOrder"] + ",";
			}
			if (!String.IsNullOrWhiteSpace(SortClause))
				SortClause = " ORDER BY " + SortClause.TrimEnd(',');

			WhereClause = "";
			if (Filters != "null" && !string.IsNullOrWhiteSpace(Filters))
			{
				filterExpression = ExpressionReader.Evaluate(Filters);
				WhereClause = filterExpression.ConvertToSQL();
				if (!string.IsNullOrWhiteSpace(WhereClause))
					WhereClause = "WHERE " + WhereClause;
			}

			TopClause = MaxRows > 0 ? "TOP " + MaxRows : "";
		}

        public void Poll()
        {
            try
            {
                var columns = String.IsNullOrWhiteSpace(SQLColumns) ? "*" : String.Join(",", SQLColumns.Split(',').Select(c => SQLHelpers.AddColumnQuotes(c)));
                var sql = string.Format("SELECT {0} {1} FROM {2} {3} {4}", TopClause, columns, SQLHelpers.AddTableQuotes(SQLTable), WhereClause, SortClause);
                using (SqlDataAdapter a = new SqlDataAdapter(sql, connection))
                {
                    DataTable dt = new DataTable();
                    a.Fill(dt);
                    IList<IDictionary<string, object>> rtr = new List<IDictionary<string, object>>();
                    foreach (DataRow row in dt.Rows)
                    {
                        IDictionary<string, object> r = new Dictionary<string, object>();
                        foreach (DataColumn col in dt.Columns)
                            r.Add(col.ColumnName, row[col]);
                        rtr.Add(r);
                    }
                    if (rtr.Count > 0)
                        this.OnPublish?.Invoke(this, new OnPublishArgs(rtr.ToArray(), "Output"));
                }
            }
            catch (Exception ex)
            {
                this.OnPublishError?.Invoke(this, new OnErrorArgs(this.UniqueId, DateTime.UtcNow, "XMPro.SQLAgents.ContextProvider.Poll", ex.Message, ex.InnerException?.ToString() ?? ""));
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

			if (errors.Any() == false)
			{
				var server = new TextBox() { Value = this.SQLServer };
				var errorMessage = "";
				
                IList<string> tables = SQLHelpers.GetTables(server, new TextBox() { Value = this.SQLUser }, new CheckBox() { Value = this.SQLUseSQLAuth }, this.SQLPassword, new DropDown() { Value = this.SQLDatabase }, out errorMessage);

				if (string.IsNullOrWhiteSpace(errorMessage) == false)
				{
					errors.Add($"Error {i++}: {errorMessage}");
					return errors.ToArray();
				}

				if (tables.Any(d => d == this.SQLTable) == false)
					errors.Add($"Error {i++}: Table '{this.SQLTable}' cannot be found in {this.SQLDatabase}.");
			}

            return errors.ToArray();
        }
    }
}