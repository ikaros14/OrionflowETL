# OrionflowETL Usage Guide

This document provides a practical guide for developers integrating **OrionflowETL** into their .NET applications. It covers the core concepts, standard workflows, and examples for common ETL scenarios.

**Prerequisites**:
*   Basic knowledge of C# and .NET.
*   An existing .NET project (Console, API, service, etc.).
*   A SQL Server instance (if using SQL adapters).

---

## Core Concepts

OrionflowETL operates on a strictly linear pipeline model:

**Source** &rarr; **Transform Steps** &rarr; **Sink**

1.  **Source (`ISource`)**: Reads data from an external origin (e.g., a CSV file or SQL query) and produces a stream of rows.
2.  **Transform (`IPipelineStep`)**: A sequence of operations that modify, validate, or filter rows one by one. Steps are executed in the order defined.
3.  **Sink (`IDataSink`)**: writes the processed rows to a destination (e.g., a CSV file or SQL table).
4.  **Executor (`PipelineExecutor`)**: The engine that orchestrates the flow, passing data from source to steps and finally to the sink.

---

## 1. Example: CSV to CSV Transformation

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
    // Note: CsvSink requires explicit column definitions for the output file
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

## 2. Example: CSV to SQL Server

This is a common use case for data import tasks. It requires mapping the loosely typed CSV columns to specific database columns.

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

        // Filter out invalid rows (e.g. missing email)
        new FilterRowsStep(row => !string.IsNullOrWhiteSpace(row.Get<string>("Email"))),

        // Validate presence of required data before database insertion
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

## 3. Example: SQL Server to CSV

Extracting data from a database and exporting it to a flat file.

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

    // 2. Steps (Optional transformations)
    var steps = new IPipelineStep[]
    {
        // Example: Ensure no nulls in critical fields
        new ValidateRequiredStep(new[] { "ClientId", "ClientName" })
    };

    // 3. Sink
    var sink = new CsvSink(new CsvSinkOptions
    {
        Path = "active_clients_export.csv",
        Columns = new[] { "ClientId", "ClientName", "TotalOrders" }
    });

    // 4. Execute
    var executor = new PipelineExecutor();
    var result = executor.Execute(source, steps, sink);
}
```

---

## Execution Results

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
    Console.Error.WriteLine("Critical failure occurred.");
}

// 2. Inspect Metrics
Console.WriteLine($"Read: {result.TotalRowsRead}");
Console.WriteLine($"Processed: {result.TotalRowsProcessed}");
Console.WriteLine($"Succeeded: {result.TotalRowsSucceeded}");
Console.WriteLine($"Failed: {result.TotalRowsFailed}");

// 3. Inspect Errors
foreach (var error in result.Errors)
{
    Console.WriteLine($"Error in step '{error.StepName}': {error.Message}");
    // You can also access error.Exception and error.Row
}
```

---

## Best Practices

1.  **Normalization First**: Place `NormalizeHeadersStep` and `TrimStringsStep` at the beginning of your pipeline to ensure consistent data handling in subsequent steps.
2.  **Filter Early**: Use `FilterRowsStep` as early as possible (after normalization) to avoid processing invalid data unnecessarily.
3.  **Validate Late**: Use `ValidateRequiredStep` just before the Sink to ensure the data adheres to the destination's constraints (e.g., non-null columns).
4.  **Rename Clarity**: Use `RenameColumnsStep` to convert external column names (source-specific) to your domain's internal naming convention.

---

## Scope and Limitations

**OrionflowETL** is designed for everyday data integration tasks within .NET applications.

*   **Supported Sources**: CSV, SQL Server.
*   **Supported Sinks**: CSV, SQL Server.
*   **Execution Model**: Synchronous, in-process, row-by-row processing.

**Limitations**:
*   It does not support distributed processing (Spark-like behavior).
*   It does not include a built-in scheduler (use Hangfire, Quartz.NET, or Windows Task Scheduler).
*   It assumes the schema is relatively static during execution.
