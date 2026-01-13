using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Tests.Core.Doubles;

public sealed class FakeStep : IPipelineStep
{
    private readonly Func<IRow, IRow> _transform;

    public FakeStep(Func<IRow, IRow> transform)
    {
        _transform = transform;
    }

    public IRow Execute(IRow row) => _transform(row);
}