using System.Data;
using Npgsql;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Adapters.Postgres.Sources;

/// <summary>
/// A data source that reads rows from a PostgreSQL database.
/// </summary>
public sealed class PostgresSource : ISource
{
    private readonly PostgresSourceOptions _options;

    public PostgresSource(PostgresSourceOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            throw new ArgumentException("ConnectionString cannot be empty", nameof(options));
            
        if (string.IsNullOrWhiteSpace(_options.Query))
            throw new ArgumentException("Query cannot be empty", nameof(options));
    }

    public IEnumerable<IRow> Read()
    {
        using var connection = new NpgsqlConnection(_options.ConnectionString);
        connection.Open();

        using var command = new NpgsqlCommand(_options.Query, connection);
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            var rowData = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < reader.FieldCount; i++)
            {
                var colName = reader.GetName(i);
                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rowData[colName] = value;
            }

            yield return new Row(rowData);
        }
    }
}
