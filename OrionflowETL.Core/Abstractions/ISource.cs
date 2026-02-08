namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Responsability: 
///     1. Produce Rows 
///     2. Do not transform them
/// </summary>
public interface ISource
{
    /// <summary>
    /// Returns an enumerable collection of rows from the data source.
    /// </summary>
    /// <returns>An <see cref="IEnumerable{IRow}"/> containing the rows read from the data source. The collection will be empty
    /// if no rows are available.</returns>
    IEnumerable<IRow> Read();
}
