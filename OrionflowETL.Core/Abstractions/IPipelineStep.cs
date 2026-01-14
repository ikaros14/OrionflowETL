namespace OrionflowETL.Core.Abstractions
{
   /// <summary>
   /// Defines a step in a data processing pipeline that transforms an input row and returns the processed result.
   /// </summary>
   /// <remarks>Implementations of this interface represent individual operations within a pipeline. Each step
   /// receives an input row, applies its transformation or logic, and returns the resulting row. If a step does not
   /// produce a result for a given input, it may return null. This interface is typically used to compose complex data
   /// processing workflows by chaining multiple steps together.</remarks>
    public interface IPipelineStep
    {
        /// <summary>
        /// Processes the specified row and returns the result after applying the operation.
        /// </summary>
        /// <param name="row">The input row to be processed. Cannot be null.</param>
        /// <returns>An <see cref="IRow"/> representing the processed result. Returns null if the operation does not produce a
        /// result.</returns>
        IRow Execute(IRow row);
    }
}

