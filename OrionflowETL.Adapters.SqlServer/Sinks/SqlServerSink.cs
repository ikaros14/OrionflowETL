using System.Data;
using Microsoft.Data.SqlClient;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Adapters.SqlServer.Sinks;

/// <summary>
/// A sink that writes data to a SQL Server table using SqlBulkCopy.
/// Implements IBatchAware to accumulate rows in memory and insert them in batches.
/// </summary>
public sealed class SqlServerSink : IDataSink, IBatchAware
{
    private readonly SqlServerSinkOptions _options;
    private DataTable _buffer = new();

    public SqlServerSink(SqlServerSinkOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            throw new ArgumentException("ConnectionString cannot be empty", nameof(options));

        if (string.IsNullOrWhiteSpace(_options.TableName))
            throw new ArgumentException("TableName cannot be empty", nameof(options));
            
        if (_options.ColumnMapping == null || _options.ColumnMapping.Count == 0)
        {
            throw new ArgumentException(
                "ColumnMapping must be provided to correctly map source rows to the database table.", nameof(options));
        }

        InitializeDataTable();
    }

    private void InitializeDataTable()
    {
        _buffer = new DataTable(_options.TableName);
        foreach (var mapping in _options.ColumnMapping!)
        {
            _buffer.Columns.Add(mapping.Key, typeof(object));
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
        foreach (var mapping in _options.ColumnMapping!)
        {
            var rowColumn = mapping.Key;
            dataRow[rowColumn] = row[rowColumn] ?? DBNull.Value;
        }
        _buffer.Rows.Add(dataRow);
    }

    public void OnBatchCommit(object? lastWindowValue)
    {
        if (_buffer.Rows.Count == 0)
        {
            return;
        }

        using var connection = new SqlConnection(_options.ConnectionString);
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        
        try
        {
            using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = _options.TableName,
                BulkCopyTimeout = _options.CommandTimeoutSeconds
            };

            foreach (var mapping in _options.ColumnMapping!)
            {
                bulkCopy.ColumnMappings.Add(mapping.Key, mapping.Value);
            }

            bulkCopy.WriteToServer(_buffer);
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
