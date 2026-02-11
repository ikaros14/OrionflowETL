using OrionflowETL.Adapters.Csv;
using OrionflowETL.Adapters.SqlServer;
using OrionflowETL.Adapters.SqlServer.Sinks;
using OrionflowETL.Adapters.Postgres.Sinks;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Execution;
using OrionflowETL.Core.Transforms;
using OrionflowETL.Adapters.Postgres.Sources;
using OrionflowETL.Adapters.MySql.Sinks;

namespace OrionflowETL.Demo;

/// <summary>
/// This program demonstrates the intended usage of OrionflowETL.
/// It defines a simple pipeline that reads from a CSV, transforms data, and writes to SQL Server.
/// </summary>
public static class Program
{
    public static void Main()
    {
        //Console.WriteLine("Starting OrionflowETL Demo Pipeline...");

        //// 1. Configure the Data Source (Extract)
        //// Reading from a hypothetical CSV file with user data.
        //var source = new CsvSource(new CsvOptions 
        //{ 
        //    Path = "users_import.csv",
        //    HasHeader = true,
        //    Delimiter = ','
        //});

        //// 2. Configure the Pipeline Steps (Transform)
        //// Define the sequence of operations to apply to each row.
        //var steps = new IPipelineStep[]
        //{
        //    // Normalize headers: Trims spaces from column names (e.g., " Email " -> "Email")
        //    new NormalizeHeadersStep(trim: true),

        //    // Clean values: Trims whitespace from all string values
        //    new TrimStringsStep(),

        //    // Rename columns: Maps CSV headers to internal schema names
        //    // Example: "Full Name" -> "Name", "Email Address" -> "Email"
        //    new RenameColumnsStep(new Dictionary<string, string>
        //    {
        //        { "Full Name", "Name" },
        //        { "Email Address", "Email" },
        //        { "Role Type", "Role" }
        //    }),

        //    // Filter rows: Discard rows that have empty required fields
        //    new FilterRowsStep(row => !string.IsNullOrWhiteSpace(row.Get<string>("Email"))),

        //    // Validate requirements: Ensure critical columns are present before sinking
        //    new ValidateRequiredStep(new[] { "Name", "Email", "Role" })
        //};

        //// 3. Configure the Data Sink (Load)
        //// Writing the transformed data to a SQL Server table.
        //var sink = new SqlServerSink(new SqlServerSinkOptions
        //{
        //    ConnectionString = "Server=localhost;Database=DemoDb;User Id=sa;Password=SecurePassword123!;Encrypt=True;",
        //    TableName = "[Sales].[Users]",
        //    ColumnMapping = new Dictionary<string, string>
        //    {
        //        // Map internal schema names to database column names
        //        { "Name", "UserName" },
        //        { "Email", "UserEmail" },
        //        { "Role", "UserRole" }
        //    }
        //});

        //// 4. Execute the Pipeline
        //// The executor orchestrates the flow: Source -> Steps -> Sink
        //var executor = new PipelineExecutor();
        //var result = executor.Execute(source, steps, sink);

        //// 5. Inspect Results
        //Console.WriteLine($"Pipeline Finished with Status: {result.Status}");
        //Console.WriteLine($" - Rows Read:      {result.TotalRowsRead}");
        //Console.WriteLine($" - Rows Processed: {result.TotalRowsProcessed}");
        //Console.WriteLine($" - Rows Succeeded: {result.TotalRowsSucceeded}");
        //Console.WriteLine($" - Rows Failed:    {result.TotalRowsFailed}");

        //if (result.Errors.Count > 0)
        //{
        //    Console.WriteLine("Errors encountered during execution:");
        //    foreach (var error in result.Errors)
        //    {
        //        Console.WriteLine($" - {error.Message}");
        //    }
        //}

        // example: sql server to postgresql
        // connect to: sqldb-suiteoperativa-tle-dev.database.windows.net using dba and select  [Name], [Email] from [SecurityManagement].[Users]
        // insert to: localhost:5432 using postgres user and password lambda123 db: orionflowetl and insert into:

        // CREATE TABLE test.users (
        //     id          BIGSERIAL PRIMARY KEY,
        //     name     varchar(200),
        //     email       VARCHAR(200) NOT NULL ,
        //     created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        // );
        //Console.WriteLine("Test sql server to postgresql");

        //var source = new SqlServerSource(new SqlServerOptions
        //{
        //    ConnectionString = "Server=sqldb-suiteoperativa-tle-dev.database.windows.net;Database=Global_ATC;User Id=dba;Password=hg78s243FDFgkafj_gsdfgj-afsd3zxcmmd;Encrypt=True;",
        //    Query = "select [Name], [Email] from [SecurityManagement].[Users]"
        //});

        //var sink = new PostgresSink(new PostgresSinkOptions
        //{
        //    ConnectionString = "Server=localhost;Database=orionflowetl;User Id=postgres;Password=lambda123;",
        //    TableName = "test.users",
        //    ColumnMapping = new Dictionary<string, string>
        //    {
        //        { "Name", "name" },
        //        { "Email", "email" }
        //    }
        //});

        //var executor = new PipelineExecutor();
        //var result = executor.Execute(source, Enumerable.Empty<IPipelineStep>(), sink);

        //Console.WriteLine($"Pipeline Finished with Status: {result.Status}");
        //Console.WriteLine($" - Rows Read:      {result.TotalRowsRead}");
        //Console.WriteLine($" - Rows Processed: {result.TotalRowsProcessed}");
        //Console.WriteLine($" - Rows Succeeded: {result.TotalRowsSucceeded}");
        //Console.WriteLine($" - Rows Failed:    {result.TotalRowsFailed}");

        //if (result.Errors.Count > 0)
        //{
        //    Console.WriteLine("Errors encountered during execution:");
        //    foreach (var error in result.Errors)
        //    {
        //        Console.WriteLine($" - {error.Message}");
        //    }
        //}

        //next example: pgsql to mysql
        // mysql: localhost:3306, user: root, password: lambda123, db: orionflowetl, table: test.users

        Console.WriteLine("Test pgsql to mysql");

        var source = new PostgresSource(new PostgresSourceOptions
        {
            ConnectionString = "Server=localhost;Database=orionflowetl;User Id=postgres;Password=lambda123;",
            Query = "select name, email from test.users"
        });

        var sink = new MySqlSink(new MySqlSinkOptions
        {
            ConnectionString = "Server=localhost;Database=test;User Id=root;Password=lambda123;",
            TableName = "users",
            ColumnMapping = new Dictionary<string, string>
            {
                { "name", "name" },
                { "email", "email" }
            }
        });

        var executor = new PipelineExecutor();
        var result = executor.Execute(source, Enumerable.Empty<IPipelineStep>(), sink);

        Console.WriteLine($"Pipeline Finished with Status: {result.Status}");
        Console.WriteLine($" - Rows Read:      {result.TotalRowsRead}");
        Console.WriteLine($" - Rows Processed: {result.TotalRowsProcessed}");
        Console.WriteLine($" - Rows Succeeded: {result.TotalRowsSucceeded}");
        Console.WriteLine($" - Rows Failed:    {result.TotalRowsFailed}");

        if (result.Errors.Count > 0)
        {
            Console.WriteLine("Errors encountered during execution:");
            foreach (var error in result.Errors)
            {
                Console.WriteLine($" - {error.Message}");
            }
        }


    }
}