using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;
using System;
using System.Collections.Generic;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that conditionally sets a column value if a condition is met.
/// This works like a CASE WHEN [column] [operator] [value] THEN [newValue] ELSE [original] END.
/// </summary>
public sealed class ConditionalSetStep : IPipelineStep
{
    private readonly string _column;
    private readonly string _operator;
    private readonly object? _conditionValue;
    private readonly string _targetColumn;
    private readonly object? _newValue;

    public ConditionalSetStep(
        string column, 
        string op, 
        object? conditionValue, 
        string targetColumn, 
        object? newValue)
    {
        _column = column ?? throw new ArgumentNullException(nameof(column));
        _operator = op ?? "=";
        _conditionValue = conditionValue;
        _targetColumn = targetColumn ?? throw new ArgumentNullException(nameof(targetColumn));
        _newValue = newValue;
    }

    public string Name => $"ConditionalSet[{_column} {_operator} {_conditionValue} -> {_targetColumn}={_newValue}]";

    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        if (!row.Columns.Contains(_column))
            return row;

        var val = row.Get<object?>(_column);
        bool matches = EvaluateCondition(val);

        if (matches)
        {
            var data = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var col in row.Columns)
                data[col] = row.Get<object?>(col);

            data[_targetColumn] = _newValue;
            return new Row(data);
        }

        return row;
    }

    private bool EvaluateCondition(object? val)
    {
        var sVal = val?.ToString() ?? string.Empty;
        var sTarget = _conditionValue?.ToString() ?? string.Empty;

        return _operator.ToLower() switch
        {
            "=" or "==" => sVal.Equals(sTarget, StringComparison.OrdinalIgnoreCase),
            "!=" or "<>" => !sVal.Equals(sTarget, StringComparison.OrdinalIgnoreCase),
            "contains" => sVal.Contains(sTarget, StringComparison.OrdinalIgnoreCase),
            "startswith" => sVal.StartsWith(sTarget, StringComparison.OrdinalIgnoreCase),
            "endswith" => sVal.EndsWith(sTarget, StringComparison.OrdinalIgnoreCase),
            ">" => Compare(val, _conditionValue) > 0,
            ">=" => Compare(val, _conditionValue) >= 0,
            "<" => Compare(val, _conditionValue) < 0,
            "<=" => Compare(val, _conditionValue) <= 0,
            _ => false
        };
    }

    private int Compare(object? a, object? b)
    {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        if (a is IComparable comp)
        {
            try
            {
                // Try to convert b to a's type if possible
                var bConverted = Convert.ChangeType(b, a.GetType());
                return comp.CompareTo(bConverted);
            }
            catch
            {
                // Fallback to string comparison if types don't match or conversion fails
                return string.Compare(a.ToString(), b.ToString(), StringComparison.OrdinalIgnoreCase);
            }
        }

        return string.Compare(a.ToString(), b.ToString(), StringComparison.OrdinalIgnoreCase);
    }
}
