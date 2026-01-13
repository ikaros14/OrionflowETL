namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Execute the pipeline
/// </summary>
internal interface IPipelineExecutor
{
    void Execute(
        ISource source,
        IEnumerable<IPipelineStep> steps,
        ISink sink
    );
}
