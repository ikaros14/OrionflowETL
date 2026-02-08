using System;
using System.Collections.Generic;
using System.Linq;

namespace OrionflowETL.Core.Execution;

/// <summary>
/// Represents the result of an ETL pipeline execution.
/// This class is immutable and contains metrics about the execution.
/// </summary>
public sealed class ExecutionResult
{
    /// <summary>
    /// Gets the final status of the execution.
    /// </summary>
    public ExecutionStatus Status { get; }

    /// <summary>
    /// Gets the total number of rows read from the source.
    /// </summary>
    public long TotalRowsRead { get; }

    /// <summary>
    /// Gets the total number of rows processed by the pipeline steps.
    /// </summary>
    public long TotalRowsProcessed { get; }

    /// <summary>
    /// Gets the total number of rows successfully written to the sink.
    /// </summary>
    public long TotalRowsSucceeded { get; }

    /// <summary>
    /// Gets the total number of rows that failed processing or writing.
    /// </summary>
    public long TotalRowsFailed { get; }

    /// <summary>
    /// Gets the collection of errors encountered during execution.
    /// </summary>
    public IReadOnlyCollection<ErrorContext> Errors { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ExecutionResult"/> class.
    /// </summary>
    /// <param name="status">The final status of the execution.</param>
    /// <param name="totalRowsRead">Total rows read.</param>
    /// <param name="totalRowsProcessed">Total rows processed.</param>
    /// <param name="totalRowsSucceeded">Total rows succeeded.</param>
    /// <param name="totalRowsFailed">Total rows failed.</param>
    /// <param name="errors">The collection of errors encountered. If null, an empty collection is used.</param>
    public ExecutionResult(
        ExecutionStatus status,
        long totalRowsRead,
        long totalRowsProcessed,
        long totalRowsSucceeded,
        long totalRowsFailed,
        IEnumerable<ErrorContext>? errors = null)
    {
        Status = status;
        TotalRowsRead = totalRowsRead;
        TotalRowsProcessed = totalRowsProcessed;
        TotalRowsSucceeded = totalRowsSucceeded;
        TotalRowsFailed = totalRowsFailed;
        Errors = errors?.ToList().AsReadOnly() ?? new List<ErrorContext>().AsReadOnly();
    }
}
