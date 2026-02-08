namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Defines the contract for a Data Sink, responsible for persisting or outputting processed rows.
/// </summary>
public interface IDataSink
{
    /// <summary>
    /// Writes a single row to the sink destination.
    /// </summary>
    /// <param name="row">The processed row to write. Cannot be null.</param>
    void Write(IRow row);
}
