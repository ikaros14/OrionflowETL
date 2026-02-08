using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that normalizes column names by trimming whitespace and optionally converting to lowercase.
/// This is useful when the source produces headers with inconsistent casing or extra spaces.
/// </summary>
public sealed class NormalizeHeadersStep : IPipelineStep
{
    private readonly bool _trim;
    private readonly bool _toLower;

    /// <summary>
    /// Initializes a new instance of the NormalizeHeadersStep class.
    /// </summary>
    /// <param name="trim">If true, trims leading and trailing whitespace from column names. Default is true.</param>
    /// <param name="toLower">If true, converts column names to lowercase using invariant culture. Default is false.</param>
    public NormalizeHeadersStep(bool trim = true, bool toLower = false)
    {
        _trim = trim;
        _toLower = toLower;
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var newValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (var col in row.Columns)
        {
            string newName = col;

            if (_trim)
            {
                newName = newName.Trim();
            }

            if (_toLower)
            {
                newName = newName.ToLowerInvariant();
            }

            if (string.IsNullOrEmpty(newName))
            {
                throw new InvalidOperationException(
                    $"Column name normalization resulted in an empty string for original column '{col}'.");
            }

            if (newValues.ContainsKey(newName))
            {
                throw new InvalidOperationException(
                    $"Duplicate column name '{newName}' generated after normalization. " +
                    $"Original column '{col}' caused a collision.");
            }

            newValues[newName] = row[col];
        }

        return new Row(newValues);
    }
}
