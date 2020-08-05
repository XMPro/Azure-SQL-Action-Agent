using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using XMIoT.Framework;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using XMIoT.Framework.Settings;
using XMIoT.Framework.Helpers;
using XMIoT.Framework.Settings.Enums;

namespace XMPro.SQLAgents
{
    public class ActionAgent : IAgent, IReceivingAgent, IPublishesError, IUsesVariable
    {
        private Configuration config;
        private DataTable dt;
        private List<XMIoT.Framework.Attribute> parentOutputs;
        private List<SqlParameter> storedProcParams;

        private bool UseConnectionVariables => bool.TryParse(this.config["UseConnectionVariables"], out bool result) && result;
        private string SQLServer => UseConnectionVariables ? GetVariableValue(this.config["vSQLServer"]) : this.config["SQLServer"];
        private string SQLUser => UseConnectionVariables ? GetVariableValue(this.config["vSQLUser"]) : this.config["SQLUser"];
        private string SQLPassword => UseConnectionVariables ? GetVariableValue(this.config["vSQLPassword"], true) : this.decrypt(this.config["SQLPassword"]);

        private bool AllowTriggers
        {
            get
            {
                bool allowTriggers = false;
                Boolean.TryParse(this.config["AllowTriggers"], out allowTriggers);
                return allowTriggers;
            }
        }

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

        private string SQLTable
        {
            get
            {
                if (CreateTable == true)
                    return this.config["SQLTableNew"];
                else
                    return this.config["SQLTable"];
            }
        }

        private const string CreateTableSQL = @"IF NOT EXISTS(SELECT * FROM sysobjects WHERE name = '{0}' AND xtype = 'U')
                                            CREATE TABLE {1}(
                                                [{0}_ID] [bigint] IDENTITY(1,1) NOT NULL,
                                                {2},
                                                CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ([{0}_ID] ASC)
                                            )";

        private bool CreateTable => bool.TryParse(this.config["CreateTable"], out bool createTable) && createTable;

        private bool UsingStoredProc => bool.TryParse(this.config["UsingStoredProc"], out bool usingStoredProd) && usingStoredProd;

        private string StoredProc => this.config["StoredProc"];

        private List<XMIoT.Framework.Attribute> ParentOutputs
        {
            get
            {
                if (parentOutputs == null)
                {
                    var args = new OnRequestParentOutputAttributesArgs(this.UniqueId, "Input");
                    this.OnRequestParentOutputAttributes.Invoke(this, args);
                    parentOutputs = args.ParentOutputs.ToList();
                }
                return parentOutputs;
            }
        }

        private string decrypt(string value)
        {
            var request = new OnDecryptRequestArgs(value);
            this.OnDecryptRequest?.Invoke(this, request);
            return request.DecryptedValue;
        }

        public long UniqueId { get; set; }

        public event EventHandler<OnPublishArgs> OnPublish;

        public event EventHandler<OnRequestParentOutputAttributesArgs> OnRequestParentOutputAttributes;

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

            CheckBox UsingStoredProc = settings.Find("UsingStoredProc") as CheckBox;
            DropDown StoredProc = settings.Find("StoredProc") as DropDown;
            CheckBox CreateTable = settings.Find("CreateTable") as CheckBox;
            TextBox SQLTableNew = settings.Find("SQLTableNew") as TextBox;
            DropDown SQLTable = settings.Find("SQLTable") as DropDown;
            CheckBox AllowTriggers = settings.Find("AllowTriggers") as CheckBox;

            if (UsingStoredProc.Value == true)
            {
                StoredProc.Visible = true;
                CreateTable.Visible = SQLTableNew.Visible = SQLTable.Visible = AllowTriggers.Visible = false;
            }
            else
            {
                StoredProc.Visible = false;
                CreateTable.Visible = AllowTriggers.Visible = true;
                SQLTableNew.Visible = (CreateTable.Value == true);
                SQLTable.Visible = (CreateTable.Value == false);
            }

            if (!String.IsNullOrWhiteSpace(SQLDatabase.Value))
            {
                IList<string> tables = SQLHelpers.GetTables(sqlServer, sqlUser, SQLUseSQLAuth, this.decrypt(sqlPassword.Value), SQLDatabase, out errorMessage);                
                SQLTable.Options = tables.Select(i => new Option() { DisplayMemeber = i, ValueMemeber = i }).ToList();
                if (tables.Contains(SQLTable.Value) == false)
                    SQLTable.Value = "";

                var storedprocs = SQLHelpers.GetStoredProcs(sqlServer, sqlUser, SQLUseSQLAuth, this.decrypt(sqlPassword.Value), SQLDatabase, out errorMessage);
                StoredProc.Options = storedprocs.Select(i => new Option() { DisplayMemeber = i, ValueMemeber = i }).ToList();
                if (storedprocs.Contains(StoredProc.Value) == false)
                    StoredProc.Value = "";
            }

            if (!String.IsNullOrWhiteSpace(errorMessage))
                SQLServer.HelpText = vSQLServer.HelpText = errorMessage;

            return settings.ToString();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetInputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };
            if (UsingStoredProc)
                return SQLHelpers.GetStoredProcInParams(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.StoredProc)
                        .Select(p => new XMIoT.Framework.Attribute(p.ParameterName.TrimStart(new char[] { '@' }), SQLHelpers.GetSystemType(p.DbType).GetIoTType()));
            else if (CreateTable == false)
                return SQLHelpers.GetColumns(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.SQLTable)
                    .Where(c => !c.AutoIncrement && !c.ReadOnly)
                    .Select(col => new XMIoT.Framework.Attribute(col.ColumnName, col.DataType.GetIoTType()));
            else
                return new XMIoT.Framework.Attribute[0];
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };
            if (UsingStoredProc)
                return SQLHelpers.GetStoredProcParams(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.StoredProc)
                        .Select(p => new XMIoT.Framework.Attribute(p.ParameterName.TrimStart(new char[] { '@' }), SQLHelpers.GetSystemType(p.DbType).GetIoTType()));
            else if (CreateTable == false)
                return SQLHelpers.GetColumns(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.SQLTable)
                    .Select(col => new XMIoT.Framework.Attribute(col.ColumnName, col.DataType.GetIoTType()));
            else
            {
                return ParentOutputs;
            }
        }

        public void Create(Configuration configuration)
        {
            this.config = configuration;

            if (UsingStoredProc == true)
                storedProcParams = SQLHelpers.GetStoredProcParams(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.StoredProc).ToList();
            else
            {
                if (CreateTable == true)
                {
                    var newColumns = GetColumns(ParentOutputs);
                    using (SqlConnection connection = new SqlConnection(SQLHelpers.GetConnectionString(SQLServer, SQLUser, SQLPassword, SQLUseSQLAuth, SQLDatabase)))
                    {
                        SqlCommand command = new SqlCommand(String.Format(CreateTableSQL, RemoveSchemaName(SQLTable), SQLTable, newColumns), connection);
                        command.CommandType = CommandType.Text;
                        command.Connection.Open();
                        command.ExecuteNonQuery();
                    }
                }

                var adp = new SqlDataAdapter(String.Format("Select Top 0 * FROM {0}", SQLHelpers.AddTableQuotes(SQLTable)), SQLHelpers.GetConnectionString(SQLServer, SQLUser, SQLPassword, SQLUseSQLAuth, SQLDatabase));
                this.dt = new DataTable();
                adp.Fill(this.dt);
            }
        }

        public void Start()
        {
        }

        public void Receive(String endpointName, JArray events)
        {
            try
            {
                if (UsingStoredProc)
                {
                    using (SqlConnection connection = new SqlConnection(SQLHelpers.GetConnectionString(SQLServer, SQLUser, SQLPassword, SQLUseSQLAuth, SQLDatabase)))
                    {
                        connection.Open();
                        SqlCommand cmd = new SqlCommand(StoredProc) { CommandType = CommandType.StoredProcedure };
                        cmd.CommandTimeout = 60;
                        cmd.Connection = connection;

                        foreach (JObject _event in events)
                        {
                            cmd.Parameters.Clear();

                            foreach (var param in storedProcParams)
                            {
                                var paramName = param.ParameterName.TrimStart(new char[] { '@' });
                                var paramValue = ((JValue)_event[paramName])?.Value ?? DBNull.Value;
                                var sqlParam = new SqlParameter(param.ParameterName, paramValue);
                                sqlParam.Direction = param.Direction;
                                cmd.Parameters.Add(sqlParam);
                            }
                            cmd.ExecuteNonQuery();

                            foreach (var outParam in storedProcParams.Where(p => p.Direction != ParameterDirection.Input))
                            {
                                var paramName = outParam.ParameterName.TrimStart(new char[] { '@' });
                                var paramValue = JToken.FromObject(cmd.Parameters[outParam.ParameterName].Value);
                                if (_event.Properties().Any(p => p.Name == paramName))
                                    _event[paramName] = paramValue;
                                else
                                    _event.Add(paramName, paramValue);
                            }
                        }
                    }
                }
                else if (dt != null)
                {
                    this.dt.Clear();
                    foreach (JObject _event in events)
                    {
                        var newRow = this.dt.NewRow();
                        foreach (var attribute in _event.Properties())
                        {
                            if (newRow.Table.Columns.Contains(attribute.Name))
                            {
                                if (attribute.Value != null)
                                    newRow[attribute.Name] = ((JValue)attribute.Value).Value;
                            }
                        }
                        this.dt.Rows.Add(newRow);
                    }

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(SQLHelpers.GetConnectionString(SQLServer, SQLUser, SQLPassword, SQLUseSQLAuth, SQLDatabase), AllowTriggers ? SqlBulkCopyOptions.FireTriggers : SqlBulkCopyOptions.Default))
                    {
                        bulkCopy.DestinationTableName = this.SQLTable;
                        bulkCopy.WriteToServer(this.dt);
                    }
                }

                this.OnPublish?.Invoke(this, new OnPublishArgs(events, "Output"));//publish the new rows
            }
            catch (Exception ex)
            {
                this.OnPublishError?.Invoke(this, new OnErrorArgs(this.UniqueId, DateTime.UtcNow, "XMPro.SQLAgents.ActionAgent.Receive", ex.Message, ex.InnerException?.ToString() ?? "", events));
            }
        }

        public void Destroy()
        {
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

            if (this.UsingStoredProc && String.IsNullOrWhiteSpace(this.StoredProc))
                errors.Add($"Error {i++}: Stored Procedure is not specified.");

            if (!this.UsingStoredProc && String.IsNullOrWhiteSpace(this.SQLTable))
                errors.Add($"Error {i++}: Table is not specified.");

            if (errors.Any() == false)
            {
                var errorMessage = "";
                var server = new TextBox() { Value = this.SQLServer };
                
                if (!this.UsingStoredProc && this.CreateTable == false)
                {
                    IList<string> tables = SQLHelpers.GetTables(server, new TextBox() { Value = this.SQLUser }, new CheckBox() { Value = this.SQLUseSQLAuth }, this.SQLPassword, new DropDown() { Value = this.SQLDatabase }, out errorMessage);

                    if (string.IsNullOrWhiteSpace(errorMessage) == false)
                    {
                        errors.Add($"Error {i++}: {errorMessage}");
                        return errors.ToArray();
                    }

                    if (tables.Any(d => d == this.SQLTable) == false)
                        errors.Add($"Error {i++}: Table '{this.SQLTable}' cannot be found in {this.SQLDatabase}.");
                }
            }

            return errors.ToArray();
        }

        #region Helper Methods

        private string GetColumns(List<XMIoT.Framework.Attribute> parentOutputs)
        {
            List<string> colList = new List<string>();

            foreach (var column in parentOutputs)
            {
                colList.Add(SQLHelpers.AddColumnQuotes(column.Name) + " " + GetSQLType(column.Type) + " NULL ");
            }

            return string.Join(",", colList);
        }

        private string RemoveSchemaName(string tableName)
        {
            if (!String.IsNullOrWhiteSpace(tableName))
            {
                var arr = tableName.Replace("[", "").Replace("]", "").Split('.');
                return arr[arr.Length - 1];
            }
            else
                return tableName;
        }

        private string GetSQLType(Types type)
        {
            switch (type)
            {
                case Types.Boolean:
                    return "bit";

                case Types.DateTime:
                    return "datetime";

                case Types.Double:
                    return "float";

                case Types.Int:
                    return "int";

                case Types.Long:
                    return "bigint";

                default:
                    return "nvarchar(max)";
            }
        }

        #endregion Helper Methods
    }
}