using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// Sets a column to a fixed constant value, regardless of any existing value.
///
/// Use cases:
///   • Mark all rows with a fixed source system identifier: SetConstant("SourceSystem", "ERP")
///   • Initialize a status column: SetConstant("Status", "Active")
///   • Force a numeric default: SetConstant("Version", 1)
///
/// If the column does not exist in the row, it is added.
/// If it already exists, its value is replaced with the constant.
/// </summary>
public sealed class SetConstantStep : IPipelineStep
{
    private readonly string  _targetColumn;
    private readonly object? _value;

    /// <param name="targetColumn">Column to set.</param>
    /// <param name="value">
    ///   The constant value to assign.
    ///   Pass null to explicitly set the column to null.
    /// </param>
    public SetConstantStep(string targetColumn, object? value)
    {
        if (string.IsNullOrWhiteSpace(targetColumn))
            throw new ArgumentException("targetColumn cannot be null or empty.", nameof(targetColumn));

        _targetColumn = targetColumn;
        _value        = value;
    }

    public string Name => $"SetConstant[{_targetColumn}={_value}]";

    /// <inheritdoc/>
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var data = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var col in row.Columns)
            data[col] = row.Get<object?>(col);

        data[_targetColumn] = _value;

        return new Row(data);
    }
}
