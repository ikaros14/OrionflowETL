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

## 9. Batch Processing and High Performance (IBatchAware)

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
