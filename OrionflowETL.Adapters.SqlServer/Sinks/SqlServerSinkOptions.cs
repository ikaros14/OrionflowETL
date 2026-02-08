namespace OrionflowETL.Adapters.SqlServer.Sinks;

/// <summary>
/// Configuration options for SqlServerSink.
/// </summary>
public sealed class SqlServerSinkOptions
{
    /// <summary>
    /// The connection string to the target SQL Server.
    /// </summary>
    public required string ConnectionString { get; set; }

    /// <summary>
    /// The name of the target table to insert data into.
    /// </summary>
    public required string TableName { get; set; }

    /// <summary>
    /// Explicit mapping between Row column names (Keys) and Database column names (Values).
    /// If null, the Sink will attempt to map Row columns 1:1 to Database columns, 
    /// but since IRow does not expose keys, this likely requires a manual configuration.
    /// </summary>
    public Dictionary<string, string>? ColumnMapping { get; set; }

    /// <summary>
    /// Timeout for the INSERT command execution. Defaults to 30 seconds.
    /// </summary>
    public int CommandTimeoutSeconds { get; set; } = 30;
}
