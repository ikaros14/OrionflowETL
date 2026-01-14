using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Models;
/// <summary>
/// Represents a read-only schema definition consisting of one or more columns.
/// </summary>
/// <remarks>A schema defines the structure of tabular data by specifying its columns. Once created, the schema
/// and its columns cannot be modified. This class is typically used to describe the shape of data for validation,
/// serialization, or processing purposes.</remarks>
public sealed class Schema : ISchema
{
    /// <summary>
    /// 
    /// </summary>
    private readonly IReadOnlyList<ISchemaColumn> _columns;

    /// <summary>
    /// Gets the collection of columns defined in the schema.
    /// </summary>
    /// <remarks>The returned list provides read-only access to the schema's columns. The order of columns in
    /// the list reflects their definition within the schema.</remarks>
    public IReadOnlyList<ISchemaColumn> Columns => _columns;

    /// <summary>
    /// Initializes a new instance of the Schema class using the specified collection of schema columns.
    /// </summary>
    /// <param name="columns">A collection of columns that define the schema. Must contain at least one element.</param>
    /// <exception cref="ArgumentNullException">Thrown if the columns parameter is null.</exception>
    /// <exception cref="ArgumentException">Thrown if the columns collection is empty.</exception>
    public Schema(IEnumerable<ISchemaColumn> columns)
    {
        if (columns == null)
            throw new ArgumentNullException(nameof(columns));

        var list = columns.ToList();

        if (list.Count == 0)
            throw new ArgumentException("Schema must contain at least one column");

        _columns = list.AsReadOnly();
    }
}
