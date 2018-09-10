# Azure-SQL-Action-Agent
## Prerequisites
- Visual Studio (any version that supports .Net Core 2.1)
- [XMPro IoT Framework NuGet package](https://www.nuget.org/packages/XMPro.IOT.Framework/3.0.2-beta)
- Please see the [Building an Agent for XMPro IoT](https://docs.xmpro.com/lessons/writing-an-agent-for-xmpro-iot/) guide for a better understanding of how the XMPro IoT Framework works.

## Description
The *Azure SQL Action Agent* allows a specified database and table to be updated with stream data at any point in the use case flow. In other words, this agent receives data and writes it to a database table. This agent is a virtual agent; thus, not relying on a specific environment to function and does not require you to have SQL Server installed.

## How the code works
All settings referred to in the code need to correspond with the settings defined in the template that has been created for the agent using the Stream Integration Manager. Refer to the [Stream Integration Manager](https://docs.xmpro.com/topic/getting-to-know-the-framework/#1534129009509-379bd7d3-9f40) guide for instructions on how to define the settings in the template and package the agent after building the code. 

After packaging the agent, you can upload it to XMPro IoT and start using it.

### Settings
When a user needs to use the *Azure SQL* action agent, they need to provide the name of the SQL Server instance in which the database resides to which they want to write the incoming data to, along with a username and password that can be used. Retrieve these values from the configuration using the following code: 
```csharp
private string SQLServer => this.config["SQLServer"];
private string SQLUser => this.config["SQLUser"];
private string SQLPassword => this.decrypt(this.config["SQLPassword"]);
```

When configuring the agent, the user has the option of choosing to fire database triggers when a record is inserted. This setting need to be defined as follows:
```csharp
private bool AllowTriggers
{
    get
    {
        bool allowTriggers = false;
        Boolean.TryParse(this.config["AllowTriggers"], out allowTriggers);
        return allowTriggers;
    }
}
```
To get the value of the check box that indicates if SQL Server Authentication should be used when connecting to the database or not, use the following code:
```csharp
private bool SQLUseSQLAuth
{
    get
    {
        var temp = false;
        bool.TryParse(this.config["SQLUseSQLAuth"], out temp);
        return temp;
    }
}
```

Get the database name:
```csharp
private string SQLDatabase => this.config["SQLDatabase"];
```

The user can either create a new table or connect to an existing one. You need to retrieve the name of either the new table or an existing table. If a user checks the *Create New Table* check box, he/ she will be prompted to provide the name of the new table.
```csharp
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

private bool CreateTable
{
    get
    {
        var createTable = false;
        bool.TryParse(this.config["CreateTable"], out createTable);
        return createTable;
    }
}
```
### Configurations
In the *GetConfigurationTemplate* method, parse the JSON representation of the settings into the Settings object.
```csharp
var settings = Settings.Parse(template);
new Populator(parameters).Populate(settings);
```

Create controls for the SQL Server instance name, username and password, and the *SQL Server Authentication* checkbox and set their values.
```csharp
TextBox SQLServer = settings.Find("SQLServer") as TextBox;
SQLServer.HelpText = string.Empty;
TextBox SQLUser = settings.Find("SQLUser") as TextBox;
CheckBox SQLUseSQLAuth = settings.Find("SQLUseSQLAuth") as CheckBox;
TextBox SQLPassword = settings.Find("SQLPassword") as TextBox;
SQLPassword.Visible = SQLUseSQLAuth.Value;
```
Get all the databases available on the specified server instance. Populate the *Database* drop down with the values.
```csharp
string errorMessage = "";
IList<string> databases = SQLHelpers.GetDatabases(SQLServer, SQLUser, SQLUseSQLAuth, this.decrypt(SQLPassword.Value), out errorMessage);
DropDown SQLDatabase = settings.Find("SQLDatabase") as DropDown;
SQLDatabase.Options = databases.Select(i => new Option() { DisplayMemeber = i, ValueMemeber = i }).ToList();
```

Create controls for the remaining settings and set their values.
```csharp
CheckBox CreateTable = settings.Find("CreateTable") as CheckBox;
TextBox SQLTableNew = settings.Find("SQLTableNew") as TextBox;
SQLTableNew.Visible = (CreateTable.Value == true);

DropDown SQLTable = settings.Find("SQLTable") as DropDown;
SQLTable.Visible = (CreateTable.Value == false);

if (!String.IsNullOrWhiteSpace(SQLDatabase.Value))
{
    IList<string> tables = SQLHelpers.GetTables(SQLServer, SQLUser, SQLUseSQLAuth, this.decrypt(SQLPassword.Value), SQLDatabase, out              errorMessage);                
    SQLTable.Options = tables.Select(i => new Option() { DisplayMemeber = i, ValueMemeber = i }).ToList();
    if (tables.Contains(SQLTable.Value) == false)
        SQLTable.Value = "";
}

if (!String.IsNullOrWhiteSpace(errorMessage))
  SQLServer.HelpText = errorMessage;
```

### Validate
The settings listed below should not be left empty. If they're left empty, an error needs to be added when the stream is validated.
* SQL Server instance name
* SQL username
* SQL password
* Database name
* Table name

```csharp
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
```

If all of the above values have been specified, verify that the table exists.
```csharp
if (errors.Any() == false)
{
    var errorMessage = "";
    var server = new TextBox() { Value = this.SQLServer };
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
```
### Create
Set the config variable to the configuration received in the *Create* method.
```csharp
this.config = configuration;
```
If the user chooses to create a new table, the agent needs to get the column structure from its parent agent. Use the *CreateTableSQL* script to create the new table.
```csharp
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
```
*CreateTableSQL* script:
```csharp
private const string CreateTableSQL = @"IF NOT EXISTS(SELECT * FROM sysobjects WHERE name = '{0}' AND xtype = 'U')
                                            CREATE TABLE {1}(
                                                [{0}_ID] [bigint] IDENTITY(1,1) NOT NULL,
                                                {2},
                                                CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ([{0}_ID] ASC)
                                            )";
```

### Start
There is no need to do anything in the *Start* method.

### Destroy
There is no need to do anything in the *Destroy* method.

### Publishing Events
This agent requires you to implement the *IReceivingAgent* interface; thus, the *Receive* method needs to be added to the code. 

Each of the incoming items needs to be written to the database table the user specified in the settings. Create a data table, and copy the incoming data to the SQL Server table by using the following code:
```csharp
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

    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(SQLHelpers.GetConnectionString(SQLServer, SQLUser, SQLPassword, SQLUseSQLAuth, SQLDatabase), 
        AllowTriggers ? SqlBulkCopyOptions.FireTriggers : SqlBulkCopyOptions.Default))
    {
        bulkCopy.DestinationTableName = this.SQLTable;
        bulkCopy.WriteToServer(this.dt);
    }
}
```

To publish events, invoke the *OnPublish* event.
```csharp
this.OnPublish?.Invoke(this, new OnPublishArgs(events));
```

### Decrypting Values
Since this agent needs secure settings (*SQL Password*), the values will automatically be encrypted. Use the following code to decrypt the values.
```csharp
private string decrypt(string value)
{
    var request = new OnDecryptRequestArgs(value);
    this.OnDecryptRequest?.Invoke(this, request);
    return request.DecryptedValue;
}
```
