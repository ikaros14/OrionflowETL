using System.Data;
using MySqlConnector;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Adapters.MySql.Sources;

public class MySqlSource : ISource
{
    private readonly MySqlSourceOptions _options;

    public MySqlSource(MySqlSourceOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(options));

        if (string.IsNullOrWhiteSpace(_options.Query))
            throw new ArgumentException("Query is required.", nameof(options));
    }

    public IEnumerable<IRow> Read()
    {
        using var connection = new MySqlConnection(_options.ConnectionString);
        connection.Open();

        using var command = new MySqlCommand(_options.Query, connection);
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            var rowData = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rowData[name] = value;
            }

            yield return new Row(rowData);
        }
    }
}
