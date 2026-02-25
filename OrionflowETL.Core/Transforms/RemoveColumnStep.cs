using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that removes a specific column from the row.
/// If the column does not exist, the row is returned unchanged.
/// </summary>
public sealed class RemoveColumnStep : IPipelineStep
{
    private readonly string _column;

    /// <summary>
    /// Initializes a new instance of the <see cref="RemoveColumnStep"/> class.
    /// </summary>
    /// <param name="column">The column to remove.</param>
    public RemoveColumnStep(string column)
    {
        _column = column ?? throw new ArgumentNullException(nameof(column));
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        if (!row.Columns.Contains(_column)) return row;

        var newValues = new Dictionary<string, object?>();
        foreach (var col in row.Columns)
        {
            if (col != _column)
                newValues[col] = row.Get<object?>(col);
        }

        return new Row(newValues);
    }
}
