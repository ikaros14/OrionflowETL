using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Execution;

public sealed class PipelineExecutor : IPipelineExecutor
{
    public void Execute(
        ISource source,
        IEnumerable<IPipelineStep> steps,
        ISink sink)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (steps == null) throw new ArgumentNullException(nameof(steps));
        if (sink == null) throw new ArgumentNullException(nameof(sink));

        foreach (var row in source.Read())
        {
            var current = row;

            foreach (var step in steps)
            {
                current = step.Execute(current);
            }

            sink.Write(current);
        }
    }
}
