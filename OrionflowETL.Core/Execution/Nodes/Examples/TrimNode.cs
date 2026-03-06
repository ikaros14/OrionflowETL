using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Execution.Nodes.Examples;

/// <summary>
/// Example Transform Node: Trims leading/trailing whitespace from all string columns.
/// Demonstrates how to implement a stateless row-by-row transformation.
/// </summary>
public class TrimNode : BasePipelineNode
{
    public override string NodeType => "Trim";

    protected override async Task OnExecuteAsync(
        NodeExecutionContext context,
        NodeExecutionResult result,
        CancellationToken cancellationToken)
    {
        if (context.InputStream == null)
            throw new InvalidOperationException("TrimNode requires an InputStream.");

        LogInfo(context, "Initializing Trim transformation...");

        result.OutputStream = TrimStreamAsync(context.InputStream, cancellationToken);
        result.Success = true;

        await Task.CompletedTask; // async contract satisfied
    }

    private async IAsyncEnumerable<Row> TrimStreamAsync(
        IAsyncEnumerable<Row> input,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var row in input.WithCancellation(ct))
        {
            var newValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            foreach (var col in row.Columns)
            {
                var val = row[col];
                newValues[col] = val is string s ? s.Trim() : val;
            }

            yield return new Row(newValues);
        }
    }
}
