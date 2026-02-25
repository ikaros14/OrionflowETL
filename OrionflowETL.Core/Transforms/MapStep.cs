using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that maps (translates) a column value against a dictionary lookup.
/// Values not present in the mapping are passed through unchanged.
/// </summary>
public sealed class MapStep : IPipelineStep
{
    private readonly string _column;
    private readonly Dictionary<string, object> _mapping;

    /// <summary>
    /// Initializes a new instance of the <see cref="MapStep"/> class.
    /// </summary>
    /// <param name="column">The column whose value will be mapped.</param>
    /// <param name="mapping">Dictionary of input value → output value.</param>
    public MapStep(string column, Dictionary<string, object> mapping)
    {
        _column  = column  ?? throw new ArgumentNullException(nameof(column));
        _mapping = mapping ?? throw new ArgumentNullException(nameof(mapping));
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        if (!row.Columns.Contains(_column)) return row;

        var val = row.Get<object?>(_column)?.ToString();
        if (val != null && _mapping.TryGetValue(val, out var mappedVal))
        {
            var newValues = new Dictionary<string, object?>();
            foreach (var col in row.Columns)
                newValues[col] = col == _column ? mappedVal : row.Get<object?>(col);
            return new Row(newValues);
        }

        return row;
    }
}
