namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Responsability: Describe structure for a SchemaColumn
/// </summary>
public interface ISchemaColumn
{
    string Name { get; }
    Type DataType { get; }
    bool IsKey { get; }
}
