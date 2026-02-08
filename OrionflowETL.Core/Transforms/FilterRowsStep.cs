using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// A pipeline step that filters rows based on a user-defined predicate.
/// </summary>
public sealed class FilterRowsStep : IPipelineStep
{
    private readonly Func<IRow, bool> _predicate;

    /// <summary>
    /// Initializes a new instance of the FilterRowsStep class.
    /// </summary>
    /// <param name="predicate">A function that evaluates a row. Returns true to keep the row, false to discard it.</param>
    public FilterRowsStep(Func<IRow, bool> predicate)
    {
        _predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
    }

    /// <inheritdoc />
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        if (_predicate(row))
        {
            return row;
        }

        // Return null to signal that the row is discarded.
        // Using null! to suppress CS8603 because IPipelineStep interface defines return type as IRow (non-nullable)
        // despite the documentation stating it can return null.
        return null!;
    }
}
