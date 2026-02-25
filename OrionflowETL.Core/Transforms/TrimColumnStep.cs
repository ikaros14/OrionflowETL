using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that trims leading and trailing whitespace from a single column value.
/// If the column does not exist or its value is not a string, the row is returned unchanged.
/// </summary>
public sealed class TrimColumnStep : IPipelineStep
{
    private readonly string _column;

    /// <summary>
    /// Initializes a new instance of the <see cref="TrimColumnStep"/> class.
    /// </summary>
    /// <param name="column">The column whose string value will be trimmed.</param>
    public TrimColumnStep(string column)
    {
        _column = column ?? throw new ArgumentNullException(nameof(column));
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        if (!row.Columns.Contains(_column)) return row;

        var val = row.Get<string?>(_column);
        if (val == null) return row;

        var trimmed = val.Trim();
        if (trimmed == val) return row;

        var newValues = new Dictionary<string, object?>();
        foreach (var col in row.Columns)
            newValues[col] = col == _column ? trimmed : row.Get<object?>(col);

        return new Row(newValues);
    }
}
