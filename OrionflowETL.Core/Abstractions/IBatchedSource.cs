namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Source that can be consumed in discrete batches, each identified by a window value.
/// Used by BatchedPipelineRunner to support checkpoint-based restartability.
/// </summary>
public interface IBatchedSource
{
    /// <summary>
    /// Reads the source as an ordered sequence of batches.
    /// </summary>
    /// <param name="startFrom">
    ///   Resume value from a checkpoint. Null means start from the beginning.
    /// </param>
    IEnumerable<ISourceBatch> ReadBatches(object? startFrom = null);
}

/// <summary>
/// A single materialized batch returned by an <see cref="IBatchedSource"/>.
/// </summary>
public interface ISourceBatch
{
    /// <summary>1-based batch sequence number.</summary>
    int BatchNumber { get; }

    /// <summary>The window value this batch started from (exclusive lower bound).</summary>
    object? WindowStart { get; }

    /// <summary>The last window value in this batch — used as the checkpoint value.</summary>
    object? LastWindowValue { get; }

    /// <summary>All rows in this batch, fully materialized.</summary>
    IReadOnlyList<IRow> Rows { get; }
}
