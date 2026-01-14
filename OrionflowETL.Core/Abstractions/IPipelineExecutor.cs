namespace OrionflowETL.Core.Abstractions
{
    /// <summary>
    /// Executes an ETL pipeline composed of a source, a sequence of pipeline steps, and a sink.
    /// </summary>
    public interface IPipelineExecutor
    {
        /// <summary>
        /// Executes a data processing pipeline by reading from the specified source, applying the provided pipeline
        /// steps, and writing the results to the specified sink.
        /// </summary>
        /// <remarks>All parameters must be non-null. The pipeline steps are applied sequentially to the
        /// data from the source before writing to the sink. This method does not guarantee thread safety; concurrent
        /// calls should be synchronized externally if required.</remarks>
        /// <param name="source">The data source to read input from. Cannot be null.</param>
        /// <param name="steps">The sequence of pipeline steps to apply to the data. The steps are executed in the order provided. Cannot be
        /// null.</param>
        /// <param name="sink">The sink to write the processed output to. Cannot be null.</param>
        void Execute(
            ISource source,
            IEnumerable<IPipelineStep> steps,
            ISink sink
        );
    }
}
