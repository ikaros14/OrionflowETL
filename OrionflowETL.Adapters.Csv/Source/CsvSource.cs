using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Adapters.Csv;

/// <summary>
/// Reads rows from a delimited text file (CSV, TSV, etc.).
///
/// Features:
///   • Configurable delimiter (default: comma)
///   • Optional header row (default: true) — column names are trimmed automatically
///   • Optional value trimming (default: true)
///   • Skips blank lines by default
///   • Handles rows with fewer columns than the header (fills nulls)
///   • Optional silent handling of missing files via IgnoreMissingFile
///   • Configurable encoding (default: UTF-8)
///
/// This is the canonical CsvSource for the OrionflowETL ecosystem.
/// Replace any usage of OrionFlow.Infrastructure.Sources.CsvSource with this class.
/// </summary>
/// </summary>
public sealed class CsvSource : ISource, ISchemaDiscovery
{
    private readonly CsvOptions _options;

    public CsvSource(CsvOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public string Name => $"CsvSource[{System.IO.Path.GetFileName(_options.Path)}]";

    public IEnumerable<IRow> Read()
    {
        if (!File.Exists(_options.Path))
        {
            if (_options.IgnoreMissingFile)
                yield break;

            throw new FileNotFoundException(
                $"CSV source file not found: '{_options.Path}'", _options.Path);
        }

        using var reader = new StreamReader(_options.Path, _options.Encoding);

        string[]? headers = null;

        if (_options.HasHeader)
        {
            var headerLine = reader.ReadLine();
            if (headerLine == null) yield break;

            headers = headerLine
                .Split(_options.Delimiter)
                .Select(h => h.Trim())      // always trim column names
                .ToArray();
        }

        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            if (_options.SkipEmptyLines && string.IsNullOrWhiteSpace(line))
                continue;

            var values = line.Split(_options.Delimiter);

            if (headers == null)
            {
                // No-header mode: generate positional column names (col0, col1, ...)
                headers = Enumerable.Range(0, values.Length)
                                    .Select(i => $"col{i}")
                                    .ToArray();
            }

            var data = new Dictionary<string, object?>(headers.Length, StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < headers.Length; i++)
            {
                if (i < values.Length)
                {
                    var val = values[i];
                    data[headers[i]] = _options.TrimValues ? val.Trim() : val;
                }
                else
                {
                    // Row has fewer columns than header — fill with null
                    data[headers[i]] = null;
                }
            }

            yield return new Row(data);
        }
    }

    public IEnumerable<string> DiscoverColumns()
    {
        if (!File.Exists(_options.Path))
        {
            if (_options.IgnoreMissingFile)
                return Enumerable.Empty<string>();

            throw new FileNotFoundException(
                $"CSV source file not found: '{_options.Path}'", _options.Path);
        }

        using var reader = new StreamReader(_options.Path, _options.Encoding);
        
        if (_options.HasHeader)
        {
            var headerLine = reader.ReadLine();
            if (headerLine == null) return Enumerable.Empty<string>();

            return headerLine
                .Split(_options.Delimiter)
                .Select(h => h.Trim())
                .ToArray();
        }

        // No header: peek first line to count columns
        string? firstLine;
        while ((firstLine = reader.ReadLine()) != null)
        {
            if (_options.SkipEmptyLines && string.IsNullOrWhiteSpace(firstLine))
                continue;

            var values = firstLine.Split(_options.Delimiter);
            return Enumerable.Range(0, values.Length)
                                .Select(i => $"col{i}")
                                .ToArray();
        }

        return Enumerable.Empty<string>();
    }
}
