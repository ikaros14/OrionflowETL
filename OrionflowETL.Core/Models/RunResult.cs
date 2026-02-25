using System;

namespace OrionflowETL.Core.Models
{
    /// <summary>
    /// Represents the result of a batched pipeline execution.
    /// </summary>
    public sealed class RunResult
    {
        /// <summary>Gets a value indicating whether the execution was successful.</summary>
        public bool Success { get; }
        /// <summary>Gets the number of batches processed.</summary>
        public int BatchCount { get; }
        /// <summary>Gets the total number of rows processed.</summary>
        public long RowsTotal { get; }
        /// <summary>Gets the total duration of the execution.</summary>
        public TimeSpan Duration { get; }
        /// <summary>Gets the error message if the execution failed; otherwise, null.</summary>
        public string? ErrorMessage { get; }

        /// <summary>Initializes a new instance of the <see cref="RunResult"/> class.</summary>
        public RunResult(bool success, int batchCount, long rowsTotal, TimeSpan duration, string? errorMessage)
        {
            Success = success;
            BatchCount = batchCount;
            RowsTotal = rowsTotal;
            Duration = duration;
            ErrorMessage = errorMessage;
        }
    }
}
