using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Models;

public sealed class Schema : ISchema
{
    private readonly IReadOnlyList<ISchemaColumn> _columns;

    public IReadOnlyList<ISchemaColumn> Columns => _columns;

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
