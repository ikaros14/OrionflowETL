using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// Replaces a null (or optionally blank) column value with a specified fallback.
///
/// This is the ETL equivalent of SQL's COALESCE(column, fallback).
///
/// Use cases:
///   • Replace null country codes with a default: NullCoalesce("Country", "MX")
///   • Replace null numeric values with zero: NullCoalesce("Amount", 0)
///   • Replace null strings with a placeholder: NullCoalesce("Notes", "N/A")
///
/// By default, only null values are replaced.
/// Set <c>treatEmptyStringAsNull: true</c> in the constructor to also replace empty strings.
/// </summary>
public sealed class NullCoalesceStep : IPipelineStep
{
    private readonly IReadOnlyDictionary<string, object?> _mappings;
    private readonly bool _treatEmptyStringAsNull;

    /// <param name="mappings">Dictionary of column names and their fallback values.</param>
    /// <param name="treatEmptyStringAsNull">
    ///   When true, empty strings are also replaced with the fallback value.
    ///   Default: false.
    /// </param>
    public NullCoalesceStep(IDictionary<string, object?> mappings, bool treatEmptyStringAsNull = false)
    {
        if (mappings == null) throw new ArgumentNullException(nameof(mappings));
        
        _mappings = new Dictionary<string, object?>(mappings, StringComparer.OrdinalIgnoreCase);
        _treatEmptyStringAsNull = treatEmptyStringAsNull;
    }

    /// <summary>
    /// Legacy constructor for single column support.
    /// </summary>
    public NullCoalesceStep(string column, object? fallback, bool treatEmptyStringAsNull = false)
        : this(new Dictionary<string, object?> { { column, fallback } }, treatEmptyStringAsNull)
    {
    }

    public string Name => $"NullCoalesce[{_mappings.Count} cols]";

    /// <inheritdoc/>
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var data = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        bool modified = false;

        foreach (var col in row.Columns)
        {
            var value = row.Get<object?>(col);

            if (_mappings.TryGetValue(col, out var fallback))
            {
                bool shouldReplace = value is null
                    || (_treatEmptyStringAsNull && value is string s && string.IsNullOrEmpty(s));

                if (shouldReplace)
                {
                    data[col] = fallback;
                    modified = true;
                    continue;
                }
            }

            data[col] = value;
        }

        return modified ? new Row(data) : row;
    }
}
