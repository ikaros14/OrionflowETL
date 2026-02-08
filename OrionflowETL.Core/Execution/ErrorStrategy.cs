namespace OrionflowETL.Core.Execution;

/// <summary>
/// Defines the strategy for handling errors during pipeline execution.
/// </summary>
public enum ErrorStrategy
{
    /// <summary>
    /// Aborts execution immediately upon the first error.
    /// </summary>
    FailFast,

    /// <summary>
    /// Continues execution despite errors, collecting them in the result.
    /// </summary>
    ContinueOnError
}
