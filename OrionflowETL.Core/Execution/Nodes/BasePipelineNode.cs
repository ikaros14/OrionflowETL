using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace OrionflowETL.Core.Execution.Nodes;

/// <summary>
/// Abstract base class for all pipeline nodes.
/// Provides shared boilerplate for error handling, logging, and configuration/schema validation
/// so concrete nodes can focus purely on their business logic.
/// </summary>
public abstract class BasePipelineNode : INodeExecutionContract
{
    public abstract string NodeType { get; }

    /// <inheritdoc/>
    public async Task<NodeExecutionResult> ExecuteAsync(
        NodeExecutionContext context,
        CancellationToken cancellationToken)
    {
        var result = new NodeExecutionResult { Success = true };

        try
        {
            ValidateConfiguration(context.Configuration);
            ValidateSchema(context.InputSchema);

            await OnExecuteAsync(context, result, cancellationToken);
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.Errors.Add(ex.Message);
            context.Logger?.LogError(ex, "Node {NodeId} ({NodeType}) failed.", context.NodeId, NodeType);
        }

        return result;
    }

    /// <summary>
    /// The core node logic. Implemented by each concrete node type.
    /// Set <see cref="NodeExecutionResult.OutputStream"/> or consume <see cref="NodeExecutionContext.InputStream"/> here.
    /// </summary>
    protected abstract Task OnExecuteAsync(
        NodeExecutionContext context,
        NodeExecutionResult result,
        CancellationToken cancellationToken);

    /// <summary>Writes an informational log entry scoped to this node.</summary>
    protected void LogInfo(NodeExecutionContext context, string message)
        => context.Logger?.LogInformation("[{NodeType}:{NodeId}] {Message}", NodeType, context.NodeId, message);

    /// <summary>
    /// Validates configuration before execution. Override to add required-field checks.
    /// </summary>
    protected virtual void ValidateConfiguration(Dictionary<string, object> config) { }

    /// <summary>
    /// Validates the input schema before execution. Override to add column-compatibility checks.
    /// </summary>
    protected virtual void ValidateSchema(object? inputSchema) { }
}
