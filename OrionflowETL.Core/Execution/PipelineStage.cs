namespace OrionflowETL.Core.Execution;

/// <summary>
/// Represents the stage of the pipeline where an operation occurs.
/// </summary>
public enum PipelineStage
{
    /// <summary>
    /// The extraction stage (reading from source).
    /// </summary>
    Extract,

    /// <summary>
    /// The transformation stage (processing steps).
    /// </summary>
    Transform,

    /// <summary>
    /// The loading stage (writing to sink).
    /// </summary>
    Load
}
