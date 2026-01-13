namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Describe a Schema and it's columns
/// </summary>
public interface ISchema
{
    IReadOnlyList<ISchemaColumn> Columns { get; }
}
