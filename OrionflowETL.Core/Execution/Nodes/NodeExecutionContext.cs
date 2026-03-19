using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Execution.Nodes;

/// <summary>
/// Provides all context needed for a node to execute within a streaming pipeline.
/// </summary>
public class NodeExecutionContext
{
    /// <summary>Pipeline-level identifier for tracing.</summary>
    public string PipelineId { get; init; } = string.Empty;

    /// <summary>Node-level identifier for tracing and logging.</summary>
    public string NodeId { get; init; } = string.Empty;

    /// <summary>The input schema provided by the previous node or source.</summary>
    public object? InputSchema { get; init; }

    /// <summary>
    /// The stream of rows entering this node.
    /// Source nodes produce this; Transform/Load nodes consume it.
    /// </summary>
    public IAsyncEnumerable<Row>? InputStream { get; init; }

    /// <summary>
    /// For multi-input nodes (e.g. Join), provided as a dictionary keyed by input ID.
    /// </summary>
    public Dictionary<string, IAsyncEnumerable<Row>> InputStreams { get; init; } = new();

    /// <summary>Node-specific configuration dictionary (maps to configSchema from the Designer).</summary>
    public Dictionary<string, object> Configuration { get; init; } = new();

    /// <summary>Global execution metadata (e.g. batch id, run timestamp, environment).</summary>
    public Dictionary<string, object> ExecutionMetadata { get; init; } = new();

    /// <summary>Logger scoped to this pipeline run.</summary>
    public ILogger? Logger { get; init; }
}
