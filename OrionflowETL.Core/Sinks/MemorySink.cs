using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Sinks;

/// <summary>
/// A simple sink that stores rows in memory.
/// useful for testing and debugging.
/// </summary>
public class MemorySink : IDataSink
{
    private readonly List<IRow> _rows = new();

    /// <summary>
    /// Gets the rows written to this sink.
    /// </summary>
    public IReadOnlyList<IRow> Rows => _rows;

    /// <inheritdoc />
    public void Write(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));
        _rows.Add(row);
    }
}
