using System.Collections.Generic;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Execution.Nodes;

/// <summary>
/// Represents the complete result of a node's execution.
/// Carry both the output stream and observability data (metrics, errors, warnings).
/// </summary>
public class NodeExecutionResult
{
    public bool Success { get; set; }

    /// <summary>The output schema produced by this node (optional).</summary>
    public object? OutputSchema { get; set; }

    /// <summary>
    /// The lazy stream of rows produced by this node.
    /// The next node in the pipeline will pull from this stream.
    /// Null for terminal Load nodes.
    /// </summary>
    public IAsyncEnumerable<Row>? OutputStream { get; set; }

    public long RowsProcessed { get; set; }

    public List<string> Errors { get; init; } = new();
    public List<string> Warnings { get; init; } = new();

    /// <summary>Optional node-specific metrics (e.g., RowsFiltered, RowsUpdated).</summary>
    public Dictionary<string, string> CustomMetrics { get; init; } = new();

    /// <summary>
    /// Performance and custom metrics captured during execution
    /// (e.g. "db_latency_ms", "bytes_read").
    /// </summary>
    public Dictionary<string, double> Metrics { get; init; } = new();
}
