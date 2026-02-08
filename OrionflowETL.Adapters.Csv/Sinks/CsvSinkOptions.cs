using System.Text;

namespace OrionflowETL.Adapters.Csv.Sinks;

public sealed class CsvSinkOptions
{
    /// <summary>
    /// Full path to the output CSV file.
    /// </summary>
    public required string Path { get; init; }

    /// <summary>
    /// Character used to delimit values. Defaults to comma (,).
    /// </summary>
    public char Delimiter { get; init; } = ',';

    /// <summary>
    /// Encoding for the output file. Defaults to UTF-8.
    /// </summary>
    public Encoding Encoding { get; init; } = Encoding.UTF8;

    /// <summary>
    /// Explicit ordered list of column names to write.
    /// Information about the schema must be explicit provided by the user.
    /// Mandatory property.
    /// </summary>
    public required IList<string> Columns { get; init; }
}
