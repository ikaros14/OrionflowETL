using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that validates that specific columns are present and contain non-null, non-empty values.
/// </summary>
public sealed class ValidateRequiredStep : IPipelineStep
{
    private readonly HashSet<string> _requiredColumns;

    /// <summary>
    /// Initializes a new instance of the ValidateRequiredStep class.
    /// </summary>
    /// <param name="requiredColumns">The list of columns that must be present and have a value.</param>
    public ValidateRequiredStep(IEnumerable<string> requiredColumns)
    {
        if (requiredColumns == null) throw new ArgumentNullException(nameof(requiredColumns));
        
        _requiredColumns = new HashSet<string>(requiredColumns, StringComparer.OrdinalIgnoreCase);
        
        if (_requiredColumns.Count == 0)
            throw new ArgumentException("Required columns list cannot be empty.", nameof(requiredColumns));
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        foreach (var col in _requiredColumns)
        {
            if (!row.Columns.Contains(col, StringComparer.OrdinalIgnoreCase))
            {
                 throw new InvalidOperationException($"Missing required column '{col}'.");
            }

            var value = row[col];

            if (value == null)
            {
                throw new InvalidOperationException($"Column '{col}' is required but contains null.");
            }

            if (value is string s && s.Length == 0)
            {
                 throw new InvalidOperationException($"Column '{col}' is required but contains an empty string.");
            }
        }

        // Return original row (identity) if valid
        return row;
    }
}
