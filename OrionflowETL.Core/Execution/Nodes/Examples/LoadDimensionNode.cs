using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Execution.Nodes.Examples;

/// <summary>
/// Example Load Node: Consumes the row stream and sinks data into a target store.
/// Demonstrates how to implement a terminal (no OutputStream) Load node.
/// </summary>
public class LoadDimensionNode : BasePipelineNode
{
    public override string NodeType => "LoadDimension";

    protected override async Task OnExecuteAsync(
        NodeExecutionContext context,
        NodeExecutionResult result,
        CancellationToken cancellationToken)
    {
        if (context.InputStream == null)
            throw new InvalidOperationException("LoadDimensionNode requires an InputStream.");

        var tableName = context.Configuration.TryGetValue("TargetTable", out var t) ? t?.ToString() : "unknown";

        LogInfo(context, $"Sinking rows into {tableName}...");

        long count = 0;
        await foreach (var row in context.InputStream.WithCancellation(cancellationToken))
        {
            // Database upsert logic would replace this comment
            count++;

            if (count % 1000 == 0)
                LogInfo(context, $"Processed {count} rows...");
        }

        result.RowsProcessed = count;
        result.Success = true;

        LogInfo(context, $"Successfully loaded {count} rows into {tableName}.");
    }
}
