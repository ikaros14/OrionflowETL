using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Adapters.Csv;

public sealed class CsvSource : ISource
{
    private readonly CsvOptions _options;

    public CsvSource(CsvOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public IEnumerable<IRow> Read()
    {
        using var reader = new StreamReader(_options.Path);

        string[] headers;

        if (_options.HasHeader)
        {
            var headerLine = reader.ReadLine()
                ?? throw new InvalidOperationException("CSV file is empty.");

            headers = headerLine.Split(_options.Delimiter);
        }
        else
        {
            throw new InvalidOperationException(
                "CSV without header is not supported yet.");
        }

        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var values = line.Split(_options.Delimiter);

            var row = new Dictionary<string, object?>();

            for (int i = 0; i < headers.Length && i < values.Length; i++)
            {
                var value = _options.TrimValues
                    ? values[i].Trim()
                    : values[i];

                row[headers[i]] = value;
            }

            yield return new Row(row);
        }
    }
}
