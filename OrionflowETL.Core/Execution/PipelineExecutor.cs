using OrionflowETL.Core.Abstractions;
using System;
using System.Collections.Generic;

namespace OrionflowETL.Core.Execution;

/// <summary>
/// Provides functionality to execute a data processing pipeline by reading input from a source, applying a sequence of
/// pipeline steps, and writing the results to a sink.
/// </summary>
/// <remarks>Use this class to coordinate the flow of data through a series of processing steps. The pipeline
/// executes each step in order for every row read from the source, then writes the processed result to the sink. This
/// class is thread-safe only if the provided source, steps, and sink are themselves thread-safe.</remarks>
public sealed class PipelineExecutor : IPipelineExecutor
{
    /// <summary>
    /// Executes a data processing pipeline by reading rows from the specified source, applying each pipeline step in
    /// sequence, and writing the results to the specified sink.
    /// </summary>
    /// <remarks>Each row read from the source is processed by all pipeline steps before being written to the
    /// sink. The method does not guarantee thread safety; concurrent calls should be externally synchronized if
    /// required.</remarks>
    /// <param name="source">The data source to read input rows from. Cannot be null.</param>
    /// <param name="steps">The collection of pipeline steps to apply to each row. Steps are executed in the order provided. Cannot be null.</param>
    /// <param name="sink">The data sink to write processed rows to. Cannot be null.</param>
    /// <param name="errorStrategy">The strategy to handle errors during execution. Defaults to FailFast.</param>
    /// <returns>The result of the execution, including status and metrics.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="source"/>, <paramref name="steps"/>, or <paramref name="sink"/> is null.</exception>
    public ExecutionResult Execute(
        ISource source,
        IEnumerable<IPipelineStep> steps,
        IDataSink sink,
        ErrorStrategy errorStrategy = ErrorStrategy.FailFast)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (steps == null) throw new ArgumentNullException(nameof(steps));
        if (sink == null) throw new ArgumentNullException(nameof(sink));

        long totalRowsRead = 0;
        long totalRowsProcessed = 0;
        long totalRowsSucceeded = 0;
        long totalRowsFailed = 0;
        var errors = new List<ErrorContext>();

        foreach (var row in source.Read())
        {
            totalRowsRead++;
            totalRowsProcessed++;

            try
            {
                var current = row;
                foreach (var step in steps)
                {
                    current = step.Execute(current);
                    if (current == null)
                    {
                        break;
                    }
                }

                if (current != null)
                {
                    sink.Write(current);
                    totalRowsSucceeded++;
                }
            }
            catch (Exception ex)
            {
                totalRowsFailed++;
                var errorContext = new ErrorContext(
                    row,
                    ex.TargetSite?.Name ?? "Unknown", 
                    ex.TargetSite?.DeclaringType ?? typeof(UnknownStep),
                    PipelineStage.Transform, 
                    ex,
                    $"Error processing row: {ex.Message}"
                );
                errors.Add(errorContext);

                if (errorStrategy == ErrorStrategy.FailFast)
                {
                    return new ExecutionResult(ExecutionStatus.Failed, totalRowsRead, totalRowsProcessed, totalRowsSucceeded, totalRowsFailed, errors);
                }
            }
        }

        var status = errors.Count > 0 ? ExecutionStatus.PartialSuccess : ExecutionStatus.Success;
        return new ExecutionResult(status, totalRowsRead, totalRowsProcessed, totalRowsSucceeded, totalRowsFailed, errors);
    }

    // Placeholder type to satisfy non-null requirement if reflection fails
    private class UnknownStep { }
}
