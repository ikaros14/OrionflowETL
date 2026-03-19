using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// Injects the current UTC date and time into a new (or existing) column.
///
/// Useful for auditing load timestamps:
///   • Dimension rows stamped with when they were last updated
///   • Fact rows stamped with when they were inserted
///
/// The value is written as a <see cref="DateTime"/> (UTC).
/// Downstream sinks that write to SQL Server will store it as DATETIME2.
///
/// Example:
///   new AddTimestampStep("_loaded_at")
/// produces: { "_loaded_at": 2026-03-07T21:00:00Z }
/// </summary>
public sealed class AddTimestampStep : IPipelineStep
{
    private readonly string _targetColumn;
    private readonly Func<DateTime> _clock;  // injectable for testing

    /// <param name="targetColumn">
    ///   Name of the column that will receive the current UTC timestamp.
    ///   If a column with this name already exists, it will be overwritten.
    /// </param>
    public AddTimestampStep(string targetColumn)
        : this(targetColumn, () => DateTime.UtcNow) { }

    /// <summary>Internal constructor for unit testing with a deterministic clock.</summary>
    internal AddTimestampStep(string targetColumn, Func<DateTime> clock)
    {
        if (string.IsNullOrWhiteSpace(targetColumn))
            throw new ArgumentException("targetColumn cannot be null or empty.", nameof(targetColumn));

        _targetColumn = targetColumn;
        _clock        = clock;
    }

    public string Name => $"AddTimestamp[→{_targetColumn}]";

    /// <inheritdoc/>
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var data = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var col in row.Columns)
            data[col] = row.Get<object?>(col);

        data[_targetColumn] = _clock();

        return new Row(data);
    }
}
