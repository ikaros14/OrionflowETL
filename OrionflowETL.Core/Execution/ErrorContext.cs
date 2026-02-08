using System;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Execution;

/// <summary>
/// Represents context about an error that occurred during pipeline execution.
/// This class is immutable.
/// </summary>
public sealed class ErrorContext
{
    /// <summary>
    /// Gets the row being processed when the error occurred.
    /// </summary>
    public IRow Row { get; }

    /// <summary>
    /// Gets the name of the step or component where the error occurred.
    /// </summary>
    public string StepName { get; }

    /// <summary>
    /// Gets the type of the step or component where the error occurred.
    /// </summary>
    public Type StepType { get; }

    /// <summary>
    /// Gets the stage of the pipeline where the error occurred.
    /// </summary>
    public PipelineStage Stage { get; }

    /// <summary>
    /// Gets the exception that was thrown.
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    /// Gets a human-readable message describing the error context.
    /// </summary>
    public string Message { get; }

    /// <summary>
    /// Gets the timestamp when the error context was created.
    /// </summary>
    public DateTimeOffset Timestamp { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ErrorContext"/> class.
    /// </summary>
    /// <param name="row">The row being processed.</param>
    /// <param name="stepName">The name of the step.</param>
    /// <param name="stepType">The type of the step.</param>
    /// <param name="stage">The pipeline stage.</param>
    /// <param name="exception">The exception thrown.</param>
    /// <param name="message">A descriptive message.</param>
    public ErrorContext(
        IRow row,
        string stepName,
        Type stepType,
        PipelineStage stage,
        Exception exception,
        string message)
    {
        Row = row;
        StepName = stepName;
        StepType = stepType;
        Stage = stage;
        Exception = exception;
        Message = message;
        Timestamp = DateTimeOffset.UtcNow;
    }
}
