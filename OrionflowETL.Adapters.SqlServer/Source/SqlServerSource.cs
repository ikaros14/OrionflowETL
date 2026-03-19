using Microsoft.Data.SqlClient;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Adapters.SqlServer;

/// <summary>
/// Reads rows from a SQL Server view, table, or query.
///
/// Supports two modes, configured via <see cref="SqlServerOptions"/>:
///
///   1. Full Scan (default): Reads all rows in a single query.
///      Implements <see cref="ISource"/>.
///
///   2. Windowed Batching (when <see cref="SqlServerOptions.WindowColumn"/> is set):
///      Reads rows using keyset pagination sorted by the window column.
///      Each batch: SELECT * WHERE [col] > lastSeen ORDER BY [col] FETCH NEXT N ROWS ONLY.
///      Implements <see cref="ISource"/> AND <see cref="IBatchedSource"/> for use
///      with BatchedPipelineRunner + checkpoint-based restartability.
///
/// Query auto-detection: if the query string does not contain spaces and does not
/// start with SELECT/WITH/EXEC, it is treated as a table/view name and wrapped in
/// SELECT * FROM [...].
/// </summary>
/// </summary>
public sealed class SqlServerSource : ISource, IBatchedSource, ISchemaDiscovery
{
    private readonly SqlServerOptions _options;
    private readonly string _resolvedQuery;

    public SqlServerSource(SqlServerOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _resolvedQuery = ResolveQuery(options.Query);
    }

    public string Name => _options.IsWindowed
        ? $"SqlServerSource[windowed={_options.WindowColumn}, batch={_options.BatchSize}]"
        : $"SqlServerSource[{_options.Query}]";

    // ── ISource — Full Scan ───────────────────────────────────────────

    /// <summary>
    /// Reads all rows in a single streaming query.
    /// In windowed mode, falls back to a full sequential read across all batches.
    /// </summary>
    public IEnumerable<IRow> Read()
    {
        if (_options.IsWindowed)
        {
            // Flatten batches into a single row stream for SequentialPipelineRunner
            foreach (var batch in ReadBatches())
                foreach (var row in batch.Rows)
                    yield return row;
            yield break;
        }

        foreach (var row in ExecuteFullScan())
            yield return row;
    }

    // ── IBatchedSource — Windowed Batching ───────────────────────────

    /// <summary>
    /// Reads the source as an ordered sequence of batches (windowed mode only).
    /// Each batch opens its own connection and closes it before returning rows,
    /// allowing the sink to commit before the next read begins.
    /// </summary>
    public IEnumerable<ISourceBatch> ReadBatches(object? startFrom = null)
    {
        if (!_options.IsWindowed)
            throw new InvalidOperationException(
                "ReadBatches() requires SqlServerOptions.WindowColumn to be set. " +
                "Use Read() for full scan mode.");

        object lastSeen = startFrom ?? _options.InitialValue ?? 0;
        int batchNumber = 0;

        while (true)
        {
            batchNumber++;

            var sql = $@"
                WITH __source AS ({_resolvedQuery})
                SELECT * FROM __source
                WHERE [{_options.WindowColumn}] > @LastSeen
                ORDER BY [{_options.WindowColumn}]
                OFFSET 0 ROWS FETCH NEXT @BatchSize ROWS ONLY";

            var rows = new List<IRow>();
            object? lastValue = null;

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                connection.Open();
                using var command = new SqlCommand(sql, connection)
                {
                    CommandTimeout = _options.CommandTimeoutSeconds > 30
                        ? _options.CommandTimeoutSeconds
                        : 120   // windowed batches get a generous timeout by default
                };
                command.Parameters.AddWithValue("@LastSeen", lastSeen);
                command.Parameters.AddWithValue("@BatchSize", _options.BatchSize);

                using var reader = command.ExecuteReader();
                if (reader.FieldCount == 0) yield break;

                var fieldCount  = reader.FieldCount;
                var columnNames = ReadColumnNames(reader, fieldCount);
                int windowIdx   = Array.IndexOf(columnNames, _options.WindowColumn);

                while (reader.Read())
                {
                    var data = new Dictionary<string, object?>(fieldCount, StringComparer.OrdinalIgnoreCase);
                    for (int i = 0; i < fieldCount; i++)
                    {
                        var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        data[columnNames[i]] = value?.ToString();
                        if (i == windowIdx) lastValue = value;
                    }
                    rows.Add(new Row(data));
                }
            } // connection closes here — before sink commits

            if (rows.Count == 0) yield break;

            yield return new SqlServerBatch(batchNumber, rows, lastSeen, lastValue ?? lastSeen);

            if (rows.Count < _options.BatchSize) yield break;
            lastSeen = lastValue ?? lastSeen;
        }
    }

    // ── ISchemaDiscovery ───────────────────────────────────────────

    /// <summary>
    /// Executes the query with TOP 0 to fetch only the column metadata from SQL Server.
    /// </summary>
    public IEnumerable<string> DiscoverColumns()
    {
        // Wrap the query to ensure we only get metadata
        var metadataQuery = $@"
            WITH __meta AS ({_resolvedQuery})
            SELECT TOP 0 * FROM __meta";

        using var connection = new SqlConnection(_options.ConnectionString);
        using var command = new SqlCommand(metadataQuery, connection)
        {
            CommandType = System.Data.CommandType.Text,
            CommandTimeout = _options.CommandTimeoutSeconds
        };

        connection.Open();
        using var reader = command.ExecuteReader(System.Data.CommandBehavior.SchemaOnly);

        var names = new string[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++)
            names[i] = reader.GetName(i);

        return names;
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private IEnumerable<IRow> ExecuteFullScan()
    {
        string finalQuery = _resolvedQuery;
        if (_options.RowLimit > 0)
        {
            // Wrap the query to apply TOP and optionally ORDER BY
            var orderBy = !string.IsNullOrWhiteSpace(_options.OrderBy) 
                ? $"ORDER BY {_options.OrderBy}" 
                : "ORDER BY (SELECT NULL)"; // SELECT NULL is a common trick to satisfy ORDER BY requirement for TOP in subqueries if needed

            finalQuery = $@"
                SELECT TOP {_options.RowLimit} * 
                FROM ({_resolvedQuery}) AS __limit_wrapper 
                {orderBy}";
        }

        using var connection = new SqlConnection(_options.ConnectionString);
        using var command = new SqlCommand(finalQuery, connection)
        {
            CommandType    = System.Data.CommandType.Text,
            CommandTimeout = _options.CommandTimeoutSeconds
        };

        connection.Open();
        using var reader = command.ExecuteReader();

        var fieldCount  = reader.FieldCount;
        var columnNames = ReadColumnNames(reader, fieldCount);

        while (reader.Read())
        {
            var dict = new Dictionary<string, object?>(fieldCount, StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < fieldCount; i++)
                dict[columnNames[i]] = reader.IsDBNull(i) ? null : reader.GetValue(i);

            yield return new Row(dict);
        }
    }

    private static string[] ReadColumnNames(SqlDataReader reader, int fieldCount)
    {
        var names = new string[fieldCount];
        for (int i = 0; i < fieldCount; i++)
            names[i] = reader.GetName(i);
        return names;
    }

    /// <summary>
    /// Wraps plain table/view names in SELECT * FROM [...].
    /// Leaves full statements (SELECT, WITH, EXEC) untouched.
    /// </summary>
    private static string ResolveQuery(string raw)
    {
        var trimmed = raw.Trim();

        // Heuristic: if it contains no spaces it's a simple object name (or schema.name)
        if (!trimmed.Contains(' '))
            return $"SELECT * FROM {Bracket(trimmed)}";

        // Also handle names with dots but no keywords
        if (!trimmed.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) &&
            !trimmed.StartsWith("WITH",   StringComparison.OrdinalIgnoreCase) &&
            !trimmed.StartsWith("EXEC",   StringComparison.OrdinalIgnoreCase))
            return $"SELECT * FROM {Bracket(trimmed)}";

        return trimmed.TrimEnd(';', ' ');
    }

    private static string Bracket(string name)
    {
        if (name.Contains('[')) return name;                    // Already bracketed
        var parts = name.Split('.');
        return string.Join(".", Array.ConvertAll(parts, p => $"[{p.Trim('[', ']')}]"));
    }
}

/// <summary>
/// A single materialized batch produced by <see cref="SqlServerSource"/> in windowed mode.
/// Rows are fully in memory so the reader connection is closed before the sink commits.
/// </summary>
internal sealed class SqlServerBatch : ISourceBatch
{
    public int                 BatchNumber     { get; }
    public IReadOnlyList<IRow> Rows            { get; }
    public object?             WindowStart     { get; }
    public object?             LastWindowValue { get; }

    public SqlServerBatch(int batchNumber, IReadOnlyList<IRow> rows,
                          object? windowStart, object? lastWindowValue)
    {
        BatchNumber     = batchNumber;
        Rows            = rows;
        WindowStart     = windowStart;
        LastWindowValue = lastWindowValue;
    }
}
