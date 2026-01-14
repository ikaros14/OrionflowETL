using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Execution;

/// <summary>
/// Provides contextual information and schema metadata for operations within a data processing pipeline.
/// </summary>
/// <remarks>Use PipelineContext to access the schema definition and related metadata required for validating and
/// processing pipeline data. This class is typically instantiated at the start of a pipeline execution and passed to
/// components that require knowledge of the data format or validation rules.</remarks>
public sealed class PipelineContext
{

    /// <summary>
    /// Gets the schema definition associated with the current instance.
    /// </summary>
    /// <remarks>The returned schema provides metadata describing the structure, types, and validation rules
    /// for the data managed by this instance. Use this property to inspect or interact with the schema when performing
    /// operations that depend on the data format.</remarks>
    public ISchema Schema { get; }

    /// <summary>
    /// Initializes a new instance of the PipelineContext class using the specified schema.
    /// </summary>
    /// <param name="schema">The schema that defines the structure and validation rules for the pipeline context. Cannot be null.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="schema"/> is null.</exception>
    public PipelineContext(ISchema schema)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
    }
}
