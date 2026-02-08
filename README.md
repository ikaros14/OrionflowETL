# OrionflowETL

OrionflowETL is a lightweight, explicit, and embedded ETL (Extract, Transform, Load) framework designed for .NET applications. It provides a structured way to define data pipelines directly in code, offering control and simplicity for developers who need to move and transform data without the overhead of heavy, external orchestration tools.

## Key Features

*   **Linear Pipelines**: Define clear, deterministic flows: Source &rarr; Transforms &rarr; Sink.
*   **Explicit Transformations**: Data manipulation steps are defined as distinct, reusable components.
*   **Decoupled Adapters**: Sources and Sinks are independent of the transformation logic.
*   **Embedded Execution**: Runs entirely within your .NET application process.
*   **Memory Efficient**: Processes data row-by-row, minimizing memory footprint for large datasets.

## What it is NOT

*   **Not a Visual Tool**: There is no drag-and-drop interface; pipelines are defined in C#.
*   **Not a Distributed Orchestrator**: It does not manage cluster resources or distributed compute (like Spark).
*   **Not a Full Platform**: It replaces custom "spaghetti code" scripts, not enterprise platforms like Azure Data Factory or Informatica.

## Installation

OrionflowETL is available directly via NuGet. You can install the core library and specific adapters as needed.

```bash
dotnet add package OrionflowETL.Core
dotnet add package OrionflowETL.Adapters.Csv
dotnet add package OrionflowETL.Adapters.SqlServer
```

*(Note: versioning is currently in early access)*

## Quick Start

The following example demonstrates a simple pipeline that reads a CSV file, cleans the data, and imports it into SQL Server.

```csharp
using OrionflowETL.Adapters.Csv;
using OrionflowETL.Adapters.SqlServer.Sinks;
using OrionflowETL.Core.Execution;
using OrionflowETL.Core.Transforms;

// 1. Configure Source
var source = new CsvSource(new CsvOptions 
{ 
    Path = "input_data.csv",
    HasHeader = true
});

// 2. Configure Transform Steps
var steps = new IPipelineStep[]
{
    // Normalize headers (e.g. " Email " -> "Email")
    new NormalizeHeadersStep(trim: true),

    // Clean data values
    new TrimStringsStep(),

    // Map CSV headers to internal schema
    new RenameColumnsStep(new Dictionary<string, string>
    {
        { "Full Name", "Name" },
        { "Email Address", "Email" }
    }),

    // Filter invalid rows
    new FilterRowsStep(row => !string.IsNullOrWhiteSpace(row.Get<string>("Email"))),

    // Ensure required data is present
    new ValidateRequiredStep(new[] { "Name", "Email" })
};

// 3. Configure Sink
var sink = new SqlServerSink(new SqlServerSinkOptions
{
    ConnectionString = "Server=localhost;Database=DemoDb;Integrated Security=True;Encrypt=True;",
    TableName = "[Sales].[Users]",
    ColumnMapping = new Dictionary<string, string>
    {
        { "Name", "UserName" },
        { "Email", "UserEmail" }
    }
});

// 4. Execute
var executor = new PipelineExecutor();
var result = executor.Execute(source, steps, sink);

Console.WriteLine($"Processed: {result.TotalRowsProcessed}, Failed: {result.TotalRowsFailed}");
```

## Supported Adapters

The following adapters are currently implemented and validated:

| Adapter | Type | support |
| :--- | :--- | :--- |
| **CSV** | Source & Sink | Full Support |
| **SQL Server** | Source & Sink | Full Support |

## Roadmap

The following adapters and features are under consideration for future releases. *This roadmap is indicative and subject to change.*

*   **Planned Adapters**:
    *   PostgreSQL (Source & Sink)
    *   MySQL (Source & Sink)
    *   JSON File (Source & Sink)
*   **Under Consideration**:
    *   REST API Source
    *   Azure Blob Storage / Amazon S3
    *   Parquet Support

## Project Status

**Status: Early Access / Alpha**

The core pipeline architecture is stable. Public APIs are subject to minor changes as we refine the developer experience based on feedback. Use in production with appropriate testing.

## License

See [LICENSE](LICENSE) file for details.
