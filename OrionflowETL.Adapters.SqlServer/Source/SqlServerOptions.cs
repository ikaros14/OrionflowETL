namespace OrionflowETL.Adapters.SqlServer;

public sealed class SqlServerOptions
{
    public required string ConnectionString { get; init; }

    /// <summary>
    /// SELECT query to execute.
    /// </summary>
    public required string Query { get; init; }

    /// <summary>
    /// Optional command timeout in seconds.
    /// </summary>
    public int CommandTimeoutSeconds { get; init; } = 30;
}
