using System.Threading;
using System.Threading.Tasks;

namespace OrionflowETL.Core.Execution.Nodes;

/// <summary>
/// The standard contract for all executable nodes in an OrionFlow pipeline.
/// Every Source, Transform, Validate, and Load node must implement this interface.
/// </summary>
public interface INodeExecutionContract
{
    /// <summary>
    /// The unique type identifier for this node class (e.g. "CsvSource", "Trim").
    /// </summary>
    string NodeType { get; }

    /// <summary>
    /// Executes the node logic using the provided context and streaming rows.
    /// </summary>
    Task<NodeExecutionResult> ExecuteAsync(
        NodeExecutionContext context,
        CancellationToken cancellationToken
    );
}
