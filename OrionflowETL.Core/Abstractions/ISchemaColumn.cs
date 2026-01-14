namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Responsability: Describe structure for a SchemaColumn
/// </summary>
public interface ISchemaColumn
{
    /// <summary>
    /// Gets the name associated with the current instance.
    /// </summary>
    string Name { get; }
    /// <summary>
    /// Gets the .NET type of the data represented by this instance.
    /// </summary>
    Type DataType { get; }
    /// <summary>
    /// Define if col is a key column
    /// </summary>
    bool IsKey { get; }
}
