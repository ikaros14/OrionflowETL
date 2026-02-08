using OrionflowETL.Core.Models;
using OrionflowETL.Core.Sinks;

namespace OrionflowETL.Tests.Core.Sinks;

public class MemorySinkTest
{
    [Fact]
    public void Write_AddsRowToMemory()
    {
        var sink = new MemorySink();
        var data = new Dictionary<string, object?> { { "Col1", "Val1" } };
        var row = new Row(data);

        sink.Write(row);

        Assert.Single(sink.Rows);
        Assert.Same(row, sink.Rows[0]);
    }

    [Fact]
    public void Write_NullRow_Throws()
    {
        var sink = new MemorySink();
        Assert.Throws<ArgumentNullException>(() => sink.Write(null!));
    }
}
