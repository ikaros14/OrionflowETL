using System.Globalization;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that converts column values to specified types using invariant culture.
/// </summary>
public sealed class CastTypesStep : IPipelineStep
{
    private readonly IReadOnlyDictionary<string, Type> _mapping;
    private static readonly HashSet<Type> _supportedTypes = new()
    {
        typeof(int),
        typeof(long),
        typeof(decimal),
        typeof(double),
        typeof(bool),
        typeof(DateTime),
        typeof(string)
    };

    /// <summary>
    /// Initializes a new instance of the CastTypesStep class.
    /// </summary>
    /// <param name="mapping">Dictionary defining the target type for each column.</param>
    public CastTypesStep(IDictionary<string, Type> mapping)
    {
        if (mapping == null) throw new ArgumentNullException(nameof(mapping));
        if (mapping.Count == 0) throw new ArgumentException("Mapping cannot be empty", nameof(mapping));

        // Validate supported types for V1 to ensure deterministic behavior
        foreach (var type in mapping.Values)
        {
            if (!_supportedTypes.Contains(type))
            {
                throw new NotSupportedException($"Type '{type.Name}' is not supported in CastTypesStep v1.");
            }
        }

        _mapping = new Dictionary<string, Type>(mapping, StringComparer.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var newValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        bool modified = false;

        foreach (var col in row.Columns)
        {
            var value = row[col];
            
            if (_mapping.TryGetValue(col, out var targetType))
            {
                if (value == null)
                {
                    newValues[col] = null;
                }
                else
                {
                    if (value.GetType() == targetType)
                    {
                        newValues[col] = value;
                    }
                    else
                    {
                        try
                        {
                             // Invariant culture used for consistency
                             var newValue = Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
                             newValues[col] = newValue;
                             modified = true;
                        }
                        catch (Exception ex)
                        {
                            throw new InvalidOperationException(
                                $"Failed to cast column '{col}' value '{value}' to type '{targetType.Name}'.", ex);
                        }
                    }
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
