namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Optional contract for sinks that support per-batch transaction lifecycle.
///
/// BatchedPipelineRunner detects which of its sinks implement this interface
/// and coordinates batch commits deterministically (e.g., dims before fact).
///
/// Contract:
///   OnBatchBegin   → called once before the first Write() of a batch
///   OnBatchCommit  → called after all rows of a batch have been written; commit tx
///   OnBatchRollback→ called if a batch fails; rollback tx
///
/// The sink is responsible for starting a new transaction after commit/rollback
/// so subsequent batches can continue using the same connection.
/// </summary>
public interface IBatchAware
{
    /// <summary>
    /// Called once before the first Write() of each batch.
    /// </summary>
    /// <param name="batchNumber">1-based batch sequence number.</param>
    /// <param name="isFreshStart">
    ///   True when the pipeline has no prior checkpoint and starts from the
    ///   beginning of the source. False when resuming from a saved checkpoint.
    /// </param>
    void OnBatchBegin(int batchNumber, bool isFreshStart);

    /// <summary>
    /// Called after all rows in a batch have been written. The sink must commit
    /// its transaction and prepare a fresh one for the next batch.
    /// </summary>
    /// <param name="lastWindowValue">The last window value seen in this batch — passed through for sinks that need it.</param>
    void OnBatchCommit(object? lastWindowValue);

    /// <summary>
    /// Called if the batch processing fails. The sink must rollback its transaction.
    /// </summary>
    void OnBatchRollback();
}
