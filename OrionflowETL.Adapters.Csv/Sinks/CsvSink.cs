using System.Text;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Adapters.Csv.Sinks;

public sealed class CsvSink : IDataSink, IDisposable
{
    private readonly CsvSinkOptions _options;
    private StreamWriter? _writer;
    private readonly List<string> _columns;
    private bool _isDisposed;

    public CsvSink(CsvSinkOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        if (string.IsNullOrWhiteSpace(_options.Path))
            throw new ArgumentException("Path cannot be empty", nameof(options));
        
        // Strict: Nothing happens unless explicitly programmed.
        // User MUST provide columns. No inference.
        if (_options.Columns == null || _options.Columns.Count == 0)
        {
            throw new ArgumentException(
                "CsvSink requires explicit 'Columns' configuration. " +
                "Schema inference is not supported to avoid implicit behavior.",
                nameof(options));
        }

        _columns = new List<string>(_options.Columns);
    }

    public void Write(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        EnsureWriterInitialized();

        var values = new List<string>(_columns.Count);

        foreach (var col in _columns)
        {
            object? val;
            try
            {
                val = row[col];
            }
            catch (KeyNotFoundException)
            {
                throw new InvalidOperationException(
                    $"Row schema mismatch: Missing expected column '{col}' defined in the sink schema.");
            }

            values.Add(Escape(val?.ToString()));
        }

        var line = string.Join(_options.Delimiter, values);
        _writer!.WriteLine(line);
    }

    private void EnsureWriterInitialized()
    {
        if (_writer != null) return;

        bool fileExists = File.Exists(_options.Path);
        
        var stream = new FileStream(_options.Path, FileMode.Append, FileAccess.Write, FileShare.Read);
        _writer = new StreamWriter(stream, _options.Encoding)
        {
            AutoFlush = true 
        };

        // Write header only if file didn't exist
        // We do NOT check if the existing file has a header. We assume user configuration is correct.
        if (!fileExists)
        {
            var header = string.Join(_options.Delimiter, _columns.Select(c => Escape(c)));
            _writer.WriteLine(header);
        }
    }

    private string Escape(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        
        // Minimal valid CSV escaping
        if (s.Contains(_options.Delimiter) || s.Contains('"') || s.Contains('\r') || s.Contains('\n'))
        {
            return $"\"{s.Replace("\"", "\"\"")}\"";
        }
        return s;
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _writer?.Dispose();
        _isDisposed = true;
    }
}
