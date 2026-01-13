using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Models;

public sealed class Row : IRow
{
    private readonly IReadOnlyDictionary<string, object?> _values;

    public Row(IDictionary<string, object?> values)
    {
        if (values == null)
            throw new ArgumentNullException(nameof(values));

        _values = new Dictionary<string, object?>(
            values,
            StringComparer.OrdinalIgnoreCase
        );
    }

    public object? this[string columnName]
    {
        get
        {
            if (!_values.TryGetValue(columnName, out var value))
                throw new KeyNotFoundException(
                    $"Column '{columnName}' was not found in the row.");

            return value;
        }
        set
        {

        }
    }
     

    public T Get<T>(string columnName)
    {
        var value = this[columnName];

        if (value is null)
            return default!;

        if (value is T typed)
            return typed;

        throw new InvalidCastException(
            $"Column '{columnName}' contains value of type '{value.GetType().Name}' " +
            $"but was requested as '{typeof(T).Name}'.");
    }
}
