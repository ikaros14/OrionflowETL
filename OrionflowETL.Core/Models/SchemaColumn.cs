using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Models;

/// <summary>
/// Represents a column definition within a schema, including its name, data type, and key status.
/// </summary>
/// <remarks>A schema column is typically used to describe the structure of tabular data, such as database tables
/// or data frames. The column name must be a non-empty string. The data type specifies the type of values stored in the
/// column. The key status indicates whether the column is part of the primary key for the schema.</remarks>
public sealed class SchemaColumn : ISchemaColumn
{
    /// <summary>
    /// Gets the name associated with this instance.
    /// </summary>
    public string Name { get; }
    /// <summary>
    /// Gets the .NET type of the data represented by this instance.
    /// </summary>
    public Type DataType { get; }
    /// <summary>
    /// Gets a value indicating whether this property represents a key field in the associated data model.
    /// </summary>
    public bool IsKey { get; }
     
    /// <summary>
    /// Initializes a new instance of the SchemaColumn class with the specified column name, data type, and key
    /// indicator.
    /// </summary>
    /// <param name="name">The name of the column. Cannot be null, empty, or consist only of white-space characters.</param>
    /// <param name="dataType">The data type of the column, represented as a Type instance.</param>
    /// <param name="isKey">Indicates whether the column is a key column. The default value is <see langword="false"/>.</param>
    /// <exception cref="ArgumentException">Thrown if <paramref name="name"/> is null, empty, or consists only of white-space characters.</exception>
    public SchemaColumn(string name, Type dataType, bool isKey = false)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be empty", nameof(name));

        Name = name;
        DataType = dataType;
        IsKey = isKey;
    }
}
