using OrionflowETL.Core.Execution;
using OrionflowETL.Core.Models;
using OrionflowETL.Tests.Core.Doubles;

namespace OrionflowETL.Tests.Core;

public class PipelineExecutorTests
{
    [Fact]
    public void Executor_Applies_Steps_In_Order()
    {
        var source = new FakeSource(new[]
        {
        new Row(new Dictionary<string, object?> { ["Value"] = 1 })
    });

        var step1 = new FakeStep(r =>
            new Row(new Dictionary<string, object?> { ["Value"] = r.Get<int>("Value") + 1 })
        );

        var step2 = new FakeStep(r =>
            new Row(new Dictionary<string, object?> { ["Value"] = r.Get<int>("Value") * 2 })
        );

        var sink = new FakeSink();
        var executor = new PipelineExecutor();

        executor.Execute(source, new[] { step1, step2 }, sink);

        var result = sink.Received.Single().Get<int>("Value");

        Assert.Equal(4, result);
    }
}
