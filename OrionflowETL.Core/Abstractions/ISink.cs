namespace OrionflowETL.Core.Abstractions;

/// <summary>
///  Responsability: 
///     1. Consume Rows 
///     2. Do not transform them
/// </summary>
public interface ISink
{
    /// <summary>
    /// Writes the specified row to the output destination.
    /// </summary>
    /// <param name="row">The row to write. Cannot be null.</param>
    void Write(IRow row);
}
