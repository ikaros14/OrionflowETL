namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Describe a Schema and it's columns
/// </summary>
public interface ISchema
{
    /// <summary>
    /// Gets the collection of columns defined in the schema.
    /// </summary>
    /// <remarks>The returned list provides read-only access to the schema's columns. The order of columns in
    /// the list reflects their logical sequence within the schema.</remarks>
    IReadOnlyList<ISchemaColumn> Columns { get; }
}
