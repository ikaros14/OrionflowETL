using System.Data;
using System.Text;
using MySqlConnector;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Adapters.MySql.Sinks;

public class MySqlSink : IDataSink, IBatchAware
{
    private readonly MySqlSinkOptions _options;
    private DataTable _buffer = new();

    public MySqlSink(MySqlSinkOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(options));

        if (string.IsNullOrWhiteSpace(_options.TableName))
            throw new ArgumentException("TableName is required.", nameof(options));

        if (_options.ColumnMapping == null || _options.ColumnMapping.Count == 0)
            throw new ArgumentException("ColumnMapping is required.", nameof(options));

        InitializeDataTable();
    }

    private void InitializeDataTable()
    {
        _buffer = new DataTable(_options.TableName);
        foreach (var mapping in _options.ColumnMapping)
        {
            // The column name in the DataTable must be the target database column name
            _buffer.Columns.Add(mapping.Value, typeof(object));
        }
    }

    public void OnBatchBegin(int batchNumber, bool isFreshStart)
    {
        _buffer.Clear();
    }

    public void Write(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var dataRow = _buffer.NewRow();
        foreach (var mapping in _options.ColumnMapping)
        {
            var sourceCol = mapping.Key;
            var targetCol = mapping.Value;

            if (!row.Columns.Contains(sourceCol, StringComparer.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Row is missing required column '{sourceCol}'.");
            }

            dataRow[targetCol] = row[sourceCol] ?? DBNull.Value;
        }

        _buffer.Rows.Add(dataRow);
    }

    public void OnBatchCommit(object? lastWindowValue)
    {
        if (_buffer.Rows.Count == 0) return;

        using var connection = new MySqlConnection(_options.ConnectionString);
        connection.Open();

        using var transaction = connection.BeginTransaction();

        try
        {
            // Use MySqlBulkCopy for high performance bulk inserts. 
            // MySqlConnector relies on LOAD DATA LOCAL INFILE internally.
            var bulkCopy = new MySqlBulkCopy(connection, transaction)
            {
                DestinationTableName = _options.TableName
            };

            var bulkResult = bulkCopy.WriteToServer(_buffer);
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
