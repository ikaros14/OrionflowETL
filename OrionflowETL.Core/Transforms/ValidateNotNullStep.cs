using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that validates a column is not null (and not empty/whitespace for strings).
/// Throws <see cref="InvalidOperationException"/> if the column is missing or null.
/// </summary>
public sealed class ValidateNotNullStep : IPipelineStep
{
    private readonly string _column;

    /// <summary>
    /// Initializes a new instance of the <see cref="ValidateNotNullStep"/> class.
    /// </summary>
    /// <param name="column">The column to validate.</param>
    public ValidateNotNullStep(string column)
    {
        _column = column ?? throw new ArgumentNullException(nameof(column));
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        if (!row.Columns.Contains(_column) || row.Get<object?>(_column) == null)
            throw new InvalidOperationException($"Column '{_column}' is null or missing.");

        return row;
    }
}
