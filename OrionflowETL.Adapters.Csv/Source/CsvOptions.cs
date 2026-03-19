using System.Text;

namespace OrionflowETL.Adapters.Csv;

public sealed class CsvOptions
{
    /// <summary>Absolute or relative path to the CSV file.</summary>
    public required string Path { get; init; }

    /// <summary>Column delimiter. Default: comma.</summary>
    public char Delimiter { get; init; } = ',';

    /// <summary>
    /// When true (default), the first row is treated as a header row.
    /// Column names are trimmed of whitespace automatically.
    /// </summary>
    public bool HasHeader { get; init; } = true;

    /// <summary>
    /// When true (default), string values are trimmed of leading/trailing whitespace.
    /// </summary>
    public bool TrimValues { get; init; } = true;

    /// <summary>
    /// When true (default), blank lines in the file are skipped silently.
    /// </summary>
    public bool SkipEmptyLines { get; init; } = true;

    /// <summary>
    /// When true, returns no rows if the file does not exist (instead of throwing).
    /// Useful for optional input files. Default: false (throws FileNotFoundException).
    /// </summary>
    public bool IgnoreMissingFile { get; init; } = false;

    /// <summary>File encoding. Default: UTF-8 with BOM detection.</summary>
    public Encoding Encoding { get; init; } = Encoding.UTF8;
}
