namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Persists the last successfully processed window value for a named pipeline.
/// Enables restart-from-last-checkpoint without reprocessing committed data.
/// </summary>
public interface ICheckpointStore
{
    /// <summary>
    /// Returns the last committed window value, or null if no checkpoint exists.
    /// </summary>
    string? GetLastProcessedValue(string pipelineName);

    /// <summary>
    /// Persists the last window value AFTER a successful batch commit.
    /// Never call this before the batch transaction commits.
    /// </summary>
    void Save(string pipelineName, object lastWindowValue);

    /// <summary>
    /// Clears the checkpoint, forcing the next run to start from the beginning.
    /// Used for full-reload operations or manual recovery.
    /// </summary>
    void Clear(string pipelineName);
}
