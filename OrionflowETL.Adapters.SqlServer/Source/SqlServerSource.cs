using Microsoft.Data.SqlClient;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Adapters.SqlServer;

public sealed class SqlServerSource : ISource
{
    private readonly SqlServerOptions _options;

    public SqlServerSource(SqlServerOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public IEnumerable<IRow> Read()
    {
        using var connection = new SqlConnection(_options.ConnectionString);
        using var command = new SqlCommand(_options.Query, connection)
        {
            CommandTimeout = _options.CommandTimeoutSeconds
        };

        connection.Open();

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            var row = new Dictionary<string, object?>();

            for (int i = 0; i < reader.FieldCount; i++)
            {
                var value = reader.IsDBNull(i)
                    ? null
                    : reader.GetValue(i);

                row[reader.GetName(i)] = value;
            }

            yield return new Row(row);
        }
    }
}
