using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Models;

public sealed class SchemaColumn : ISchemaColumn
{
    public string Name { get; }
    public Type DataType { get; }
    public bool IsKey { get; }
     
    public SchemaColumn(string name, Type dataType, bool isKey = false)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be empty", nameof(name));

        Name = name;
        DataType = dataType;
        IsKey = isKey;
    }
}
