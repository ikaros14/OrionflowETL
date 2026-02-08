namespace OrionflowETL.Core.Execution;

/// <summary>
/// Represents the final status of a pipeline execution.
/// </summary>
public enum ExecutionStatus
{
    /// <summary>
    /// The pipeline completed successfully without any errors.
    /// </summary>
    Success,

    /// <summary>
    /// The pipeline completed, but some rows failed to process.
    /// </summary>
    PartialSuccess,

    /// <summary>
    /// The pipeline execution was aborted or failed critically.
    /// </summary>
    Failed
}
