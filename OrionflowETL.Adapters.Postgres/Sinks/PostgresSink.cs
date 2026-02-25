using System.Linq;
using System.Text;
using Npgsql;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Adapters.Postgres.Sinks;

public class PostgresSink : IDataSink, IBatchAware
{
    private readonly PostgresSinkOptions _options;
    private readonly List<IRow> _buffer = new();

    public PostgresSink(PostgresSinkOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            throw new ArgumentException("ConnectionString cannot be empty", nameof(options));
            
        if (string.IsNullOrWhiteSpace(_options.TableName))
            throw new ArgumentException("TableName cannot be empty", nameof(options));

        if (_options.ColumnMapping == null || _options.ColumnMapping.Count == 0)
        {
            throw new ArgumentException("ColumnMapping is required.", nameof(options));
        }
    }

    public void OnBatchBegin(int batchNumber, bool isFreshStart)
    {
        _buffer.Clear();
    }

    public void Write(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        // Validation against mapping
        foreach (var mapping in _options.ColumnMapping)
        {
            if (!row.Columns.Contains(mapping.Key, StringComparer.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Row is missing required column '{mapping.Key}'.");
            }
        }

        _buffer.Add(row);
    }

    public void OnBatchCommit(object? lastWindowValue)
    {
        if (_buffer.Count == 0) return;

        var columns = string.Join(", ", _options.ColumnMapping.Values.Select(v => $"\"{v}\""));
        var copyCommand = $"COPY {_options.TableName} ({columns}) FROM STDIN (FORMAT BINARY)";

        using var connection = new NpgsqlConnection(_options.ConnectionString);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        
        try
        {
            using (var writer = connection.BeginBinaryImport(copyCommand))
            {
                foreach (var row in _buffer)
                {
                    writer.StartRow();
                    foreach (var mapping in _options.ColumnMapping)
                    {
                        var value = row[mapping.Key] ?? DBNull.Value;
                        writer.Write(value);
                    }
                }
                writer.Complete();
            }
            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            _buffer.Clear();
        }
    }

    public void OnBatchRollback()
    {
        _buffer.Clear();
    }
}
