using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Execution.Nodes.Examples;

/// <summary>
/// Example Source Node: Reads rows from a CSV file as a lazy async stream.
/// Demonstrates how to implement a Source using IAsyncEnumerable.
/// </summary>
public class CsvSourceNode : BasePipelineNode
{
    public override string NodeType => "CsvSource";

    protected override async Task OnExecuteAsync(
        NodeExecutionContext context,
        NodeExecutionResult result,
        CancellationToken cancellationToken)
    {
        LogInfo(context, "Opening CSV source...");

        var filePath = context.Configuration["FilePath"]?.ToString();
        if (string.IsNullOrEmpty(filePath))
            throw new InvalidOperationException("FilePath is required in node configuration.");

        result.OutputStream = ReadRowsAsync(filePath, cancellationToken);
        result.Success = true;
    }

    private async IAsyncEnumerable<Row> ReadRowsAsync(
        string path,
        [EnumeratorCancellation] CancellationToken ct)
    {
        using var reader = new StreamReader(path);
#if NET7_0_OR_GREATER
        string? header = await reader.ReadLineAsync(ct);
#else
        string? header = await reader.ReadLineAsync();
#endif
        if (header == null) yield break;

        var columns = header.Split(',');

        string? line;
#if NET7_0_OR_GREATER
        while ((line = await reader.ReadLineAsync(ct)) != null)
#else
        while ((line = await reader.ReadLineAsync()) != null)
#endif
        {
            ct.ThrowIfCancellationRequested();
            var values = line.Split(',');
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < columns.Length; i++)
                dict[columns[i]] = i < values.Length ? values[i] : null;

            yield return new Row(dict);
        }
    }
}
