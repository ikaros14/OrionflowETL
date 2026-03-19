namespace OrionflowETL.Adapters.SqlServer;

public sealed class SqlServerOptions
{
    public required string ConnectionString { get; init; }

    /// <summary>
    /// SELECT query, view name, or table name to read from.
    /// Plain names (without spaces) are automatically wrapped in SELECT * FROM [...].
    /// </summary>
    public required string Query { get; init; }

    /// <summary>
    /// Optional command timeout in seconds. Default: 30 for full scans, 120 for windowed batches.
    /// </summary>
    public int CommandTimeoutSeconds { get; init; } = 30;
    
    /// <summary>
    /// Optional row limit for deterministic sampling or Dev Mode.
    /// </summary>
    public int? RowLimit { get; init; }

    /// <summary>
    /// Optional order by clause for deterministic sampling (e.g. "Id ASC").
    /// </summary>
    public string? OrderBy { get; init; }

    // ── Windowed Batching (Incremental Sequence Load) ──────────────────

    /// <summary>
    /// When set, enables windowed (incremental) batching mode.
    /// This column must be indexed and monotonically increasing (e.g. an auto-increment ID).
    /// Each batch reads rows WHERE [WindowColumn] > lastSeen ORDER BY [WindowColumn].
    /// </summary>
    public string? WindowColumn { get; init; }

    /// <summary>
    /// Number of rows per batch in windowed mode. Default: 50,000.
    /// </summary>
    public int BatchSize { get; init; } = 50_000;

    /// <summary>
    /// Seed value for the first windowed batch.
    /// All rows where WindowColumn > InitialValue will be read on the first run.
    /// Default: 0 (reads everything for numeric IDs).
    /// For date columns, pass a date string like "2000-01-01".
    /// </summary>
    public object? InitialValue { get; init; }

    /// <summary>
    /// Returns true when windowed batching is configured.
    /// </summary>
    public bool IsWindowed => WindowColumn is not null;
}
