namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Describe a Row and it's typed access without magic cast
/// </summary>
public interface IRow
{
    /// <summary>
    /// Gets the names of the columns present in this row.
    /// </summary>
    IReadOnlyCollection<string> Columns { get; }

    /// <summary>
    /// Gets or sets the value associated with the specified column name.
    /// </summary>
    /// <param name="columnName">The name of the column whose value to get or set. Cannot be null.</param>
    /// <returns>The value associated with the specified column name, or null if the column does not exist.</returns>
    object? this[string columnName] { get; set; }

    /// <summary>
    /// Retrieves the value of the specified column and returns it as the requested type.
    /// </summary>
    /// <typeparam name="T">The type to which the column value will be cast and returned.</typeparam>
    /// <param name="columnName">The name of the column whose value is to be retrieved. Cannot be null or empty.</param>
    /// <returns>The value of the specified column cast to type <typeparamref name="T"/>.</returns>
    T Get<T> (string columnName);
}
