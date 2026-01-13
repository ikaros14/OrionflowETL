using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Execution;

public sealed class PipelineContext
{
    public ISchema Schema { get; }

    public PipelineContext(ISchema schema)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
    }
}
