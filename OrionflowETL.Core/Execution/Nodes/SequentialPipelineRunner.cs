using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace OrionflowETL.Core.Execution.Nodes;

/// <summary>
/// The MVP sequential execution engine for node-based pipelines.
/// Executes nodes one after another, threading the row stream from each
/// node's OutputStream into the next node's InputStream.
///
/// Intentionally kept simple: no parallelism, no scheduling.
/// </summary>
public class SequentialPipelineRunner
{
    private readonly ILogger<SequentialPipelineRunner>? _logger;

    public SequentialPipelineRunner(ILogger<SequentialPipelineRunner>? logger = null)
    {
        _logger = logger;
    }

    /// <summary>
    /// Executes a list of nodes in order, wiring the output stream of each
    /// node as the input stream of the next.
    /// </summary>
    /// <param name="nodes">Ordered list of nodes: [Source, ...Transforms, Load]</param>
    /// <param name="pipelineId">Identifier for tracing.</param>
    /// <param name="configurations">Per-node configuration keyed by NodeId.</param>
    /// <param name="cancellationToken">Cancellation support.</param>
    /// <returns>The collection of results, one per node.</returns>
    public async Task<IReadOnlyList<NodeExecutionResult>> RunAsync(
        IReadOnlyList<INodeExecutionContract> nodes,
        string pipelineId,
        Dictionary<string, Dictionary<string, object>> configurations,
        CancellationToken cancellationToken = default)
    {
        if (nodes == null || nodes.Count == 0)
            throw new ArgumentException("Pipeline must contain at least one node.", nameof(nodes));

        var results = new List<NodeExecutionResult>(nodes.Count);
        IAsyncEnumerable<Models.Row>? currentStream = null;

        for (int i = 0; i < nodes.Count; i++)
        {
            var node = nodes[i];
            var nodeId = $"{node.NodeType}-{i}";

            configurations.TryGetValue(nodeId, out var config);

            var context = new NodeExecutionContext
            {
                PipelineId  = pipelineId,
                NodeId      = nodeId,
                InputStream = currentStream,
                Configuration = config ?? new Dictionary<string, object>(),
                Logger = _logger,
            };

            _logger?.LogInformation("Executing node [{Index}/{Total}] {NodeType}", i + 1, nodes.Count, node.NodeType);

            var result = await node.ExecuteAsync(context, cancellationToken);
            results.Add(result);

            if (!result.Success)
            {
                _logger?.LogError("Node {NodeType} failed. Aborting pipeline.", node.NodeType);
                break;
            }

            // Wire this node's output as the next node's input
            currentStream = result.OutputStream;
        }

        return results.AsReadOnly();
    }
}
