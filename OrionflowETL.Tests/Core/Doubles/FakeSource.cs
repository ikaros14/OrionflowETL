using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Tests.Core.Doubles;

public sealed class FakeSource : ISource
{
    private readonly IEnumerable<IRow> _rows;

    public FakeSource(IEnumerable<IRow> rows)
    {
        _rows = rows;
    }

    public IEnumerable<IRow> Read() => _rows;
}
