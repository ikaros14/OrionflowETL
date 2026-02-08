using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that renames columns based on a provided mapping.
/// </summary>
public sealed class RenameColumnsStep : IPipelineStep
{
    private readonly IReadOnlyDictionary<string, string> _mapping;

    /// <summary>
    /// Initializes a new instance of the RenameColumnsStep class.
    /// </summary>
    /// <param name="mapping">Dictionary where Key is the old column name and Value is the new column name.</param>
    public RenameColumnsStep(IDictionary<string, string> mapping)
    {
        if (mapping == null) throw new ArgumentNullException(nameof(mapping));
        if (mapping.Count == 0) throw new ArgumentException("Mapping cannot be empty", nameof(mapping));
        
        _mapping = new Dictionary<string, string>(mapping, StringComparer.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        // 1. Verify all mapped source columns exist in the row
        // We do this first to "Fail explicitly if mapping column does not exist"
        foreach (var sourceCol in _mapping.Keys)
        {
            if (!ContainsColumn(row, sourceCol))
            {
                throw new InvalidOperationException($"Column '{sourceCol}' specified in the mapping was not found in the row.");
            }
        }

        var newValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        
        // Iterate row columns to preserve data and order.
        foreach (var col in row.Columns)
        {
            string targetName = col;
            
            if (_mapping.TryGetValue(col, out var newName))
            {
                targetName = newName;
            }

            if (newValues.ContainsKey(targetName))
            {
                throw new InvalidOperationException(
                    $"Duplicate column name '{targetName}' generated after rename. " +
                    $"Renaming '{col}' caused a collision with an existing or already renamed column.");
            }

            newValues[targetName] = row[col];
        }

        return new Row(newValues);
    }

    private bool ContainsColumn(IRow row, string colName)
    {
        return row.Columns.Contains(colName, StringComparer.OrdinalIgnoreCase);
    }
}
