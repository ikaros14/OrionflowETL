using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms
{
    public class GroupByStep
    {
        public List<string> GroupByColumns { get; }
        public List<AggregationConfig> Aggregations { get; }

        public GroupByStep(IEnumerable<string> groupByColumns, IEnumerable<AggregationConfig> aggregations)
        {
            GroupByColumns = groupByColumns?.ToList() ?? new List<string>();
            Aggregations = aggregations?.ToList() ?? new List<AggregationConfig>();
        }

        public async IAsyncEnumerable<IRow> ExecuteAsync(
            IAsyncEnumerable<IRow> inputStream,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            var groups = new Dictionary<string, GroupState>();

            await foreach (var row in inputStream.WithCancellation(ct))
            {
                var groupKey = GetGroupKey(row);
                
                if (!groups.TryGetValue(groupKey, out var state))
                {
                    state = new GroupState();
                    // Copy grouping values
                    foreach (var col in GroupByColumns)
                    {
                        state.GroupingValues[col] = row.Get<object?>(col);
                    }
                    // Initialize aggregation states
                    foreach (var agg in Aggregations)
                    {
                        state.AggStates[agg.OutputColumn] = new AggregationValueState(agg.Function);
                    }
                    groups[groupKey] = state;
                }

                foreach (var agg in Aggregations)
                {
                    var val = row.Get<object?>(agg.Column);
                    state.AggStates[agg.OutputColumn].Update(val);
                }
            }

            foreach (var group in groups.Values)
            {
                var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                foreach (var gv in group.GroupingValues) dict[gv.Key] = gv.Value;
                foreach (var av in group.AggStates) dict[av.Key] = av.Value.GetFinalValue();
                
                yield return new Row(dict);
            }
        }

        private string GetGroupKey(IRow row)
        {
            if (GroupByColumns.Count == 0) return "GLOBAL";
            
            var values = GroupByColumns.Select(c => row.Get<object?>(c)?.ToString() ?? "[NULL]");
            return string.Join("|", values);
        }

        public class AggregationConfig
        {
            public string Column { get; set; } = string.Empty;
            public string Function { get; set; } = "Sum"; // Sum, Avg, Count, Min, Max
            public string OutputColumn { get; set; } = string.Empty;
        }

        private class GroupState
        {
            public Dictionary<string, object?> GroupingValues { get; } = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            public Dictionary<string, AggregationValueState> AggStates { get; } = new Dictionary<string, AggregationValueState>(StringComparer.OrdinalIgnoreCase);
        }

        private class AggregationValueState
        {
            private readonly string _function;
            private double _sum = 0;
            private long _count = 0;
            private double? _min = null;
            private double? _max = null;

            public AggregationValueState(string function)
            {
                _function = function;
            }

            public void Update(object? value)
            {
                if (value == null && _function != "Count") return;

                double numValue = 0;
                if (value != null && !double.TryParse(value.ToString(), out numValue))
                {
                    // Ignore non-numeric for math aggregates, but count them if requested? 
                    // Actually, usually GroupBy sum/avg expects numeric.
                }

                _count++;

                switch (_function.ToLower())
                {
                    case "sum":
                        _sum += numValue;
                        break;
                    case "avg":
                        _sum += numValue;
                        break;
                    case "min":
                        if (_min == null || numValue < _min) _min = numValue;
                        break;
                    case "max":
                        if (_max == null || numValue > _max) _max = numValue;
                        break;
                    case "count":
                        // _count incremented already
                        break;
                }
            }

            public object? GetFinalValue()
            {
                return _function.ToLower() switch
                {
                    "sum" => _sum,
                    "avg" => _count == 0 ? 0 : _sum / _count,
                    "count" => _count,
                    "min" => _min,
                    "max" => _max,
                    _ => null
                };
            }
        }
    }
}
