using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Tests.Core.Doubles;

public sealed class FakeSink : IDataSink
{
    public List<IRow> Received { get; } = new();

    public void Write(IRow row)
    {
        Received.Add(row);
    }
}
