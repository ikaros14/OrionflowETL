# OrionflowETL Usage Guide

This document provides a practical guide for developers integrating **OrionflowETL** into their .NET applications. It covers core concepts, standard workflows, and examples for common ETL scenarios using CSV, SQL Server, PostgreSQL, and MySQL.

**Prerequisites**:
*   Basic knowledge of C# and .NET.
*   An existing .NET project (Console, API, service, etc.).
*   A database instance (SQL Server, PostgreSQL, or MySQL) if using database adapters.

---

## Core Concepts

OrionflowETL operates on a strictly linear pipeline model:

**Source** &rarr; **Transform Steps** &rarr; **Sink**

1.  **Source (`ISource`)**: Reads data from an external origin (e.g., a CSV file or SQL query) and produces a stream of rows.
2.  **Transform (`IPipelineStep`)**: A sequence of operations that modify, validate, or filter rows one by one. Steps are executed in the order defined.
3.  **Sink (`IDataSink`)**: Writes the processed rows to a destination (e.g., a CSV file or SQL table).
4.  **Executor (`PipelineExecutor`)**: The engine that orchestrates the flow, passing data from source to steps and finally to the sink.

---

## 3. Example: CSV to CSV Transformation

This basic scenario reads a CSV file, cleans the text values, and writes the result to a new CSV file.

```csharp
using OrionflowETL.Adapters.Csv;
using OrionflowETL.Adapters.Csv.Sinks;
using OrionflowETL.Core.Execution;
using OrionflowETL.Core.Transforms;

public void RunCsvToCsv()
{
    // 1. Configure Source
    var source = new CsvSource(new CsvOptions 
    { 
        Path = "input.csv",
        HasHeader = true,
        Delimiter = ',' 
    });

    // 2. Configure Transformation Steps
    var steps = new IPipelineStep[]
    {
        // Remove leading/trailing whitespace from all values
        new TrimStringsStep(),

        // Normalize header names (e.g. " First Name " -> "First Name")
        new NormalizeHeadersStep(trim: true)
    };

    // 3. Configure Sink
    var sink = new CsvSink(new CsvSinkOptions
    {
        Path = "cleaned_output.csv",
        Columns = new[] { "Id", "First Name", "Last Name", "Email" }
    });

    // 4. Execute
    var executor = new PipelineExecutor();
    var result = executor.Execute(source, steps, sink);

    Console.WriteLine($"Status: {result.Status}");
}
```

---

## 4. Example: CSV to SQL Server

This common use case maps loosely typed CSV columns to specific database columns.

```csharp
using OrionflowETL.Adapters.Csv;
using OrionflowETL.Adapters.SqlServer.Sinks;
using OrionflowETL.Core.Execution;
using OrionflowETL.Core.Transforms;

public void RunCsvToSql()
{
    // 1. Source
    var source = new CsvSource(new CsvOptions { Path = "users.csv", HasHeader = true });

    // 2. Steps
    var steps = new IPipelineStep[]
    {
        // Standardize headers first
        new NormalizeHeadersStep(trim: true),
        
        // Map CSV headers to internal Schema names
        new RenameColumnsStep(new Dictionary<string, string>
        {
            { "user_email", "Email" },
            { "full_name", "Name" }
        }),

        // Filter out invalid rows
        new FilterRowsStep(row => !string.IsNullOrWhiteSpace(row.Get<string>("Email"))),

        // Validate required data
        new ValidateRequiredStep(new[] { "Name", "Email" })
    };

    // 3. Sink
    var sink = new SqlServerSink(new SqlServerSinkOptions
    {
        ConnectionString = "Server=localhost;Database=AppDb;Integrated Security=True;TrustServerCertificate=True;",
        TableName = "[dbo].[Users]",
        
        // Map Internal Schema names -> Database Column names
        ColumnMapping = new Dictionary<string, string>
        {
            { "Name", "UserName" },
            { "Email", "UserEmail" }
        }
    });

    // 4. Execute
    var executor = new PipelineExecutor();
    executor.Execute(source, steps, sink);
}
```

---

## 5. Example: SQL Server to CSV

Extracting data from SQL Server and exporting to a file.

```csharp
using OrionflowETL.Adapters.Csv.Sinks;
using OrionflowETL.Adapters.SqlServer; // Source is in root namespace
using OrionflowETL.Core.Execution;

public void RunSqlToCsv()
{
    // 1. Source (SQL Query)
    var source = new SqlServerSource(new SqlServerOptions
    {
        ConnectionString = "Server=localhost;Database=AppDb;Integrated Security=True;TrustServerCertificate=True;",
        Query = "SELECT ClientId, ClientName, TotalOrders FROM [Sales].[Clients] WHERE IsActive = 1"
    });

    // 2. Sink (No transform steps in this example)
    var sink = new CsvSink(new CsvSinkOptions
    {
        Path = "active_clients_export.csv",
        Columns = new[] { "ClientId", "ClientName", "TotalOrders" }
    });

    // 3. Execute with empty steps
    var executor = new PipelineExecutor();
    var result = executor.Execute(source, Enumerable.Empty<IPipelineStep>(), sink);
}
```

---

## 6. Example: PostgreSQL to MySQL

Migrating data from PostgreSQL to MySQL.

```csharp
using OrionflowETL.Adapters.Postgres.Sources;
using OrionflowETL.Adapters.MySql.Sinks;
using OrionflowETL.Core.Execution;

public void RunPostgresToMySql()
{
    // 1. Source (PostgreSQL)
    var source = new PostgresSource(new PostgresSourceOptions
    {
        ConnectionString = "Server=localhost;Database=source_db;User Id=postgres;Password=password;",
        Query = "SELECT id, product_name, price FROM products WHERE stock > 0"
    });

    // 2. Sink (MySQL)
    var sink = new MySqlSink(new MySqlSinkOptions
    {
        ConnectionString = "Server=localhost;Database=target_db;Uid=root;Pwd=password;",
        TableName = "products_catalog",
        ColumnMapping = new Dictionary<string, string>
        {
            // Internal (Postgres col) -> Target (MySQL col)
            { "id", "product_id" },
            { "product_name", "name" },
            { "price", "unit_price" }
        }
    });

    // 3. Execute
    var executor = new PipelineExecutor();
    var result = executor.Execute(source, Enumerable.Empty<IPipelineStep>(), sink);
}
```

---

## 7. Example: MySQL to PostgreSQL

Moving data from MySQL to PostgreSQL.

```csharp
using OrionflowETL.Adapters.MySql.Sources;
using OrionflowETL.Adapters.Postgres.Sinks;
using OrionflowETL.Core.Execution;

public void RunMySqlToPostgres()
{
    // 1. Source (MySQL)
    var source = new MySqlSource(new MySqlSourceOptions
    {
        ConnectionString = "Server=localhost;Database=legacy_app;Uid=root;Pwd=password;",
        Query = "SELECT order_id, customer_email, total_amount FROM orders"
    });

    // 2. Sink (PostgreSQL)
    var sink = new PostgresSink(new PostgresSinkOptions
    {
        ConnectionString = "Server=localhost;Database=analytics_db;User Id=postgres;Password=password;",
        TableName = "public.sales_orders",
        ColumnMapping = new Dictionary<string, string>
        {
            // Internal (MySQL col) -> Target (Postgres col)
            { "order_id", "id" },
            { "customer_email", "email" },
            { "total_amount", "amount" }
        }
    });

    // 3. Execute
    var executor = new PipelineExecutor();
    var result = executor.Execute(source, Enumerable.Empty<IPipelineStep>(), sink);
}
```

---

## 8. Handling Results

The `PipelineExecutor.Execute` method returns an `ExecutionResult` object containing metrics and status information.

```csharp
var result = executor.Execute(source, steps, sink);

// 1. Check Status
if (result.Status == ExecutionStatus.Success) 
{
    Console.WriteLine("Pipeline completed successfully.");
}
else if (result.Status == ExecutionStatus.PartialSuccess)
{
    Console.WriteLine("Pipeline completed with some failed rows.");
}
else if (result.Status == ExecutionStatus.Failed)
{
    Console.Error.WriteLine("Critical failure occurred (e.g., connection lost).");
}

// 2. Inspect Metrics
Console.WriteLine($"Rows Read:      {result.TotalRowsRead}");
Console.WriteLine($"Rows Processed: {result.TotalRowsProcessed}");
Console.WriteLine($"Rows Succeeded: {result.TotalRowsSucceeded}");
Console.WriteLine($"Rows Failed:    {result.TotalRowsFailed}");

// 3. Inspect Errors
foreach (var error in result.Errors)
{
    Console.WriteLine($"Error processing row: {error.Message}");
    // Access error.Exception for the full stack trace
}
```

---

## 9. Advanced Transformations: Row Hashing

The `HashRowStep` computes a unique fingerprint (SHA256 or MD5) of a row based on a selection of columns. This is essential for **Change Data Capture (CDC)** or **Delta Loads** where you need to detect if a row has changed since the last execution.

```csharp
using OrionflowETL.Core.Transforms;

var steps = new IPipelineStep[]
{
    new HashRowStep(
        targetColumn: "__row_hash",
        columns: new[] { "FirstName", "LastName", "Address", "Salary" },
        algorithm: HashAlgorithmType.SHA256
    )
};
```

*Note: Null values are handled using a stable separator to avoid collisions.*

---

## 10. Pattern: SQL Server to SQL Server Upsert

Since high-performance database sinks use `SqlBulkCopy` (which is insert-only), the recommended pattern for performing an **Upsert** (Update if exists, Insert if not) is the **Staging Table + MERGE** pattern.

### Step-by-Step Upsert Workflow:

1.  **Define a Staging Table**: Create a temporary or staging table in the target database with the same schema as the target.
2.  **Bulk Insert to Staging**: Use `OrionflowETL` to move data from the source to the staging table.
3.  **Execute MERGE**: Run a native SQL `MERGE` statement to synchronize the staging table with the production table.

```csharp
// 1. Execute the Pipeline (Source -> Staging Table)
var source = new SqlServerSource(new SqlServerOptions { Query = "SELECT * FROM SourceUsers" });
var stagingSink = new SqlServerSink(new SqlServerSinkOptions 
{ 
    TableName = "stg.Users", 
    ConnectionString = targetConnStr,
    ColumnMapping = myMappings 
});

var executor = new PipelineExecutor();
executor.Execute(source, Enumerable.Empty<IPipelineStep>(), stagingSink);

// 2. Execute the MERGE command manually
using (var conn = new SqlConnection(targetConnStr))
{
    conn.Open();
    var mergeSql = @"
        MERGE dbo.Users AS target
        USING stg.Users AS source
        ON (target.UserId = source.UserId)
        WHEN MATCHED THEN
            UPDATE SET target.Email = source.Email, target.ModifiedAt = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (UserId, Email, CreatedAt)
            VALUES (source.UserId, source.Email, GETDATE());
            
        -- Cleanup staging
        TRUNCATE TABLE stg.Users;";
        
    using var cmd = new SqlCommand(mergeSql, conn);
    cmd.ExecuteNonQuery();
}
```

### Option B: Clean Sync (In-Memory Comparison)

If you cannot modify the target schema to add a `_row_hash` column, you can perform the comparison entirely in memory. This is ideal for medium-sized catalogs (e.g., < 100k rows).

1.  **Pre-load Existing Hashes**: Fetch keys and a "state fingerprint" from the target into a `Dictionary`.
2.  **Filter in Pipeline**: Use `FilterRowsStep` to compare the incoming row's hash against the dictionary.

```csharp
// 1. Pre-load existing state (e.g., ID + Hash of searchable fields)
var existingHashes = new Dictionary<string, string>();
using (var conn = new SqlConnection(targetConnStr))
{
    conn.Open();
    var query = "SELECT ProductId, HASHBYTES('SHA2_256', CONCAT(Name, Price, Category)) as StateHash FROM Products";
    using var cmd = new SqlCommand(query, conn);
    using var reader = cmd.ExecuteReader();
    while (reader.Read()) 
    {
        existingHashes[reader["ProductId"].ToString()] = Convert.ToHexString((byte[])reader["StateHash"]).ToLower();
    }
}

// 2. Define the Pipeline
var source = new SqlServerSource(new SqlServerOptions { Query = "SELECT * FROM SourceProducts" });
var steps = new IPipelineStep[]
{
    // Compute current hash for the incoming row
    new HashRowStep("__current_hash", new[] { "Name", "Price", "Category" }),

    // CROSS-REFERENCE: Keep only if new or changed
    new FilterRowsStep(row => 
    {
        var id = row.Get<string>("ProductId");
        var currentHash = row.Get<string>("__current_hash");

        if (!existingHashes.TryGetValue(id, out var oldHash))
            return true; // New record

        return currentHash != oldHash; // Changed record
    })
};

// 3. Sink (Using the Staging pattern as in Option A to handle Updates + Inserts)
executor.Execute(source, steps, stagingSink);
```

---

## 11. Advanced Scenario: Multi-Source Consolidation

When consolidating data from multiple independent sources (e.g., three different Business Units) into a single centralized database, you must address **ID collisions** and **data attribution**.

### Architectural Pattern:
1.  **Orchestrator**: A loop in your host application iterates through the source connection strings.
2.  **Attribution**: Use `SetConstantStep` to tag each row with its source origin.
3.  **Composite Keys**: Use `(SourceId, EntityId)` as the unique key in the target.

```csharp
var sources = new[] 
{
    new { BuId = "BU_NORTH", Conn = "..." },
    new { BuId = "BU_SOUTH", Conn = "..." },
    new { BuId = "BU_WEST",  Conn = "..." }
};

foreach (var sourceInfo in sources)
{
    Log.Information($"Starting sync for {sourceInfo.BuId}");

    var source = new SqlServerSource(new SqlServerOptions { 
        ConnectionString = sourceInfo.Conn, 
        Query = "SELECT * FROM Transactions WHERE IsProcessed = 0" 
    });

    var steps = new IPipelineStep[]
    {
        // 1. Tag the data source
        new SetConstantStep("BusinessUnit", sourceInfo.BuId),
        
        // 2. Standardize
        new TrimStringsStep(),
        
        // 3. Add load metadata
        new AddTimestampStep("ProcessedAt")
    };

    var sink = new SqlServerSink(new SqlServerSinkOptions {
        TableName = "stg.ConsolidatedTransactions", // Bulk insert into staging
        ConnectionString = targetConnStr,
        ColumnMapping = GetMappings()
    });

    // Execute for this BU
    executor.Execute(source, steps, sink);
    
    // Post-execution: Run MERGE for this specific BU
    RunMergeForBU(sourceInfo.BuId);
}
```

---

## 12. Error Handling Strategies

The `PipelineExecutor` supports different strategies for dealing with row-level exceptions via the `ErrorStrategy` enum:

| Strategy | Description |
| :--- | :--- |
| **FailFast** | (Default) Aborts the entire pipeline execution as soon as the first error occurs. |
| **ContinueOnError** | Collects the error in the `ExecutionResult` and proceeds with the next row. |

### Usage:

```csharp
var result = executor.Execute(source, steps, sink, ErrorStrategy.ContinueOnError);

if (result.TotalRowsFailed > 0)
{
    Console.WriteLine($"Warning: {result.TotalRowsFailed} rows failed processing.");
}
```

---

## 12. Batch Processing and High Performance (IBatchAware)

OrionflowETL is designed to be highly memory efficient by processing rows one by one. Starting with version 0.2.0, the core execution engine supports **Batch Processing** without sacrificing the low-memory footprint.

When a Sink implements the `IBatchAware` interface, the `PipelineExecutor` automatically buffers the processed rows and commits them in blocks.

### Database Bulk Inserts
All offical Database Sinks (`SqlServerSink`, `PostgresSink`, `MySqlSink`) implement `IBatchAware`. Instead of executing an `INSERT` statement per row, they map the data into memory buffers and perform high-speed Bulk Inserts within a database transaction during the `OnBatchCommit` phase:

*   **SQL Server**: Uses `SqlBulkCopy`.
*   **PostgreSQL**: Uses Npgsql's extremely fast `BeginBinaryImport` (COPY FROM STDIN FORMAT BINARY).
*   **MySQL**: Uses `MySqlBulkCopy`.

This results in a **massive performance increase** for network I/O and database load when processing large amounts of data. It also guarantees transaction safety: if a row fails midway through a batch, the entire batch is rolled back (`OnBatchRollback`).

*You do not need to configure anything extra; batching is handled automatically by the Executor when using these Sinks.*

---

## 10. Best Practices

1.  **Normalization First**: Place `NormalizeHeadersStep` and `TrimStringsStep` at the beginning of your pipeline to ensure accurate column mapping later.
2.  **Filter Early**: Use `FilterRowsStep` as early as possible to reduce unnecessary processing.
3.  **Explicit Mapping**: Always use `RenameColumnsStep` or clean `ColumnMapping` settings in Sinks to decouple your internal logic from external database schema naming.
4.  **Separation of Concerns**: Keep extraction (Source queries) simple and move complex logic to Transform steps or database views.

---

## 10. Scope and Limitations

**OrionflowETL** is designed for everyday data integration tasks within .NET applications.

*   **Supported Sources**: CSV, SQL Server, PostgreSQL, MySQL.
*   **Supported Sinks**: CSV, SQL Server, PostgreSQL, MySQL.
*   **Execution Model**: Synchronous, in-process, row-by-row processing.

**Limitations**:
*   **No Distributed Computing**: It runs in a single process. Not suitable for Big Data (TB/PB scale).
*   **No Built-in Scheduler**: Use robust external schedulers like Hangfire, Quartz.NET, or OS tools (Cron/Task Scheduler).
*   **No Schema Inference**: You must define mappings explicitly; the framework does not guess types or column matches.
