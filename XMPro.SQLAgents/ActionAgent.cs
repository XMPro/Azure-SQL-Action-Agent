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
    public class ActionAgent : IAgent, IReceivingAgent
    {
        private Configuration config;
        private DataTable dt;
        private List<XMIoT.Framework.Attribute> parentOutputs;

        private string SQLServer => this.config["SQLServer"];
        private string SQLUser => this.config["SQLUser"];
        private string SQLPassword => this.decrypt(this.config["SQLPassword"]);

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

        private bool CreateTable
        {
            get
            {
                var createTable = false;
                bool.TryParse(this.config["CreateTable"], out createTable);
                return createTable;
            }
        }

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
            
            CheckBox CreateTable = settings.Find("CreateTable") as CheckBox;
            TextBox SQLTableNew = settings.Find("SQLTableNew") as TextBox;
            SQLTableNew.Visible = (CreateTable.Value == true);

            DropDown SQLTable = settings.Find("SQLTable") as DropDown;
            SQLTable.Visible = (CreateTable.Value == false);

            if (!String.IsNullOrWhiteSpace(SQLDatabase.Value))
            {
                IList<string> tables = SQLHelpers.GetTables(SQLServer, SQLUser, SQLUseSQLAuth, this.decrypt(SQLPassword.Value), SQLDatabase, out errorMessage);                
                SQLTable.Options = tables.Select(i => new Option() { DisplayMemeber = i, ValueMemeber = i }).ToList();
                if (tables.Contains(SQLTable.Value) == false)
                    SQLTable.Value = "";
            }

            if (!String.IsNullOrWhiteSpace(errorMessage))
                SQLServer.HelpText = errorMessage;

            return settings.ToString();
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetInputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };
            if (CreateTable == false)
                return SQLHelpers.GetColumns(this.SQLServer, this.SQLUser, this.SQLUseSQLAuth, this.SQLPassword, this.SQLDatabase, this.SQLTable)
                    .Where(c => !c.AutoIncrement && !c.ReadOnly)
                    .Select(col => new XMIoT.Framework.Attribute(col.ColumnName, col.DataType.GetIoTType()));
            else
                return new XMIoT.Framework.Attribute[0];
        }

        public IEnumerable<XMIoT.Framework.Attribute> GetOutputAttributes(string endpoint, IDictionary<string, string> parameters)
        {
            this.config = new Configuration() { Parameters = parameters };
            if (CreateTable == false)
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

        public void Start()
        {
        }

        public void Receive(String endpointName, JArray events)
        {
            if (dt != null)
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

#warning publish new rows below
            this.OnPublish?.Invoke(this, new OnPublishArgs(events));//publish the new rows
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

            if (String.IsNullOrWhiteSpace(this.SQLTable))
                errors.Add($"Error {i++}: Table is not specified.");

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

                if (this.CreateTable == false)
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