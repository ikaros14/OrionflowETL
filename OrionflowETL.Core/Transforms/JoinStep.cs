using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms
{
    /// <summary>
    /// Performs a join between two data streams (IAsyncEnumerable).
    /// Implements a Hash Join algorithm for efficiency.
    /// </summary>
    public class JoinStep
    {
        public string LeftKey { get; }
        public string RightKey { get; }
        public JoinType JoinType { get; }

        public JoinStep(string leftKey, string rightKey, JoinType joinType = JoinType.Inner)
        {
            LeftKey = leftKey;
            RightKey = rightKey;
            JoinType = joinType;
        }

        public async IAsyncEnumerable<IRow> ExecuteAsync(
            IAsyncEnumerable<IRow> leftStream, 
            IAsyncEnumerable<IRow> rightStream, 
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            // 1. Build Phase: Consume RIGHT stream into memory hash table
            // In a production scenario, we might want to spill to disk or use external sort
            // but for OrionFlow Designer, in-memory hash join is the standard.
            var rightLookup = new Dictionary<object, List<IRow>>();
            var rightColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            await foreach (var row in rightStream.WithCancellation(ct))
            {
                foreach (var col in row.Columns) rightColumns.Add(col);

                var key = row.Get<object?>(RightKey);
                if (key == null) continue;

                if (!rightLookup.TryGetValue(key, out var list))
                {
                    list = new List<IRow>();
                    rightLookup[key] = list;
                }
                list.Add(row);
            }

            // 2. Probe Phase: Iterate THROUGH left stream
            await foreach (var leftRow in leftStream.WithCancellation(ct))
            {
                var key = leftRow.Get<object?>(LeftKey);
                bool matched = false;

                if (key != null && rightLookup.TryGetValue(key, out var rightRows))
                {
                    matched = true;
                    foreach (var rightRow in rightRows)
                    {
                        yield return MergeRows(leftRow, rightRow);
                    }
                }

                if (!matched && JoinType == JoinType.LeftOuter)
                {
                    yield return PadWithNulls(leftRow, rightColumns);
                }
            }
        }

        private IRow PadWithNulls(IRow row, HashSet<string> extraColumns)
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var col in row.Columns) dict[col] = row[col];
            foreach (var col in extraColumns)
            {
                if (!dict.ContainsKey(col)) dict[col] = null;
            }
            return new Row(dict);
        }

        private IRow MergeRows(IRow left, IRow right)
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var col in left.Columns)
            {
                dict[col] = left[col];
            }

            foreach (var col in right.Columns)
            {
                if (!dict.ContainsKey(col))
                {
                    dict[col] = right[col];
                }
            }

            return new Row(dict);
        }
    }

    public enum JoinType
    {
        Inner,
        LeftOuter
    }
}
