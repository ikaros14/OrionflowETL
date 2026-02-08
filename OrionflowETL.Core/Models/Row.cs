using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Models;

/// <summary>
/// Represents a single row of data with column values accessible by column name.
/// </summary>
/// <remarks>Column names are compared using case-insensitive ordinal comparison. The row provides indexed access
/// to values by column name and supports type-safe retrieval of values using the generic Get method. This class is
/// immutable; once constructed, the set of columns and their values cannot be changed.</remarks>
public sealed class Row : IRow
{
    /// <summary>
    /// Provides a read-only collection of key-value pairs representing stored values.
    /// </summary>
    private readonly IReadOnlyDictionary<string, object?> _values;

    /// <summary>
    /// Gets the names of the columns present in this row.
    /// </summary>
    public IReadOnlyCollection<string> Columns => _values.Keys.ToList().AsReadOnly();

    /// <summary>
    /// Initializes a new instance of the Row class using the specified key-value pairs.
    /// </summary>
    /// <remarks>Column names are treated in a case-insensitive manner. The provided dictionary is copied;
    /// subsequent changes to the original dictionary do not affect the Row instance.</remarks>
    /// <param name="values">A dictionary containing column names as keys and their corresponding values. Keys are compared using
    /// case-insensitive ordinal comparison.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="values"/> is <see langword="null"/>.</exception>
    public Row(IDictionary<string, object?> values)
    {
        if (values == null)
            throw new ArgumentNullException(nameof(values));

        _values = new Dictionary<string, object?>(
            values,
            StringComparer.OrdinalIgnoreCase
        );
    }
    /// <summary>
    /// Gets or sets the value associated with the specified column name in the row.
    /// </summary>
    /// <param name="columnName">The name of the column whose value to get or set. Cannot be null.</param>
    /// <returns>The value associated with the specified column name, or null if the column exists but its value is null.</returns>
    /// <exception cref="KeyNotFoundException">Thrown if the specified column name does not exist in the row.</exception>
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
             throw new NotSupportedException("Row is immutable.");
        }
    }
     
    /// <summary>
    /// Retrieves the value of the specified column and attempts to cast it to the specified type.
    /// </summary>
    /// <typeparam name="T">The type to which the column value should be cast.</typeparam>
    /// <param name="columnName">The name of the column whose value is to be retrieved. Cannot be null.</param>
    /// <returns>The value of the specified column cast to type T, or the default value of T if the column value is null.</returns>
    /// <exception cref="InvalidCastException">Thrown if the value of the specified column cannot be cast to type T.</exception>
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
