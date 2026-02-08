using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that trims leading and trailing whitespace from all string values in a row.
/// </summary>
public sealed class TrimStringsStep : IPipelineStep
{
    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var newValues = new Dictionary<string, object?>();
        bool modified = false;

        foreach (var col in row.Columns)
        {
            var value = row[col];
            if (value is string s)
            {
                var trimmed = s.Trim();
                if (trimmed != s)
                {
                    modified = true;
                    newValues[col] = trimmed;
                }
                else
                {
                    newValues[col] = value;
                }
            }
            else
            {
                newValues[col] = value;
            }
        }

        if (!modified) return row;

        return new Row(newValues);
    }
}
