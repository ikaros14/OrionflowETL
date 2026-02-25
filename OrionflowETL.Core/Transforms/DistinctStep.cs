using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that filters duplicate rows based on one or more key columns,
/// keeping only the first occurrence of each unique key combination.
/// </summary>
/// <remarks>
/// State is maintained across calls — sharing one instance across batches
/// deduplicates globally, not just within a single batch.
/// </remarks>
public sealed class DistinctStep : IPipelineStep
{
    private readonly string[] _columns;
    private readonly HashSet<string> _seen = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="DistinctStep"/> class.
    /// </summary>
    /// <param name="columns">The columns that form the composite key for deduplication.</param>
    public DistinctStep(string[] columns)
    {
        _columns = columns ?? throw new ArgumentNullException(nameof(columns));
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) return null!;

        var keyParts = new List<string>(_columns.Length);
        foreach (var col in _columns)
        {
            keyParts.Add(
                row.Columns.Contains(col)
                    ? row.Get<object?>(col)?.ToString() ?? string.Empty
                    : string.Empty);
        }

        var key = string.Join("|", keyParts);
        if (!_seen.Add(key)) return null!;

        return row;
    }
}
