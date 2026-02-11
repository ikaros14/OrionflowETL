using System.Text;
using MySqlConnector;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Adapters.MySql.Sinks;

public class MySqlSink : IDataSink
{
    private readonly MySqlSinkOptions _options;

    public MySqlSink(MySqlSinkOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            throw new ArgumentException("ConnectionString is required.", nameof(options));

        if (string.IsNullOrWhiteSpace(_options.TableName))
            throw new ArgumentException("TableName is required.", nameof(options));

        if (_options.ColumnMapping == null || _options.ColumnMapping.Count == 0)
            throw new ArgumentException("ColumnMapping is required.", nameof(options));
    }

    public void Write(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var columns = new StringBuilder();
        var values = new StringBuilder();
        var parameters = new List<MySqlParameter>();

        int index = 0;
        foreach (var mapping in _options.ColumnMapping)
        {
            var sourceCol = mapping.Key;
            var targetCol = mapping.Value;

            if (!row.Columns.Contains(sourceCol, StringComparer.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Row is missing required column '{sourceCol}'.");
            }

            if (index > 0)
            {
                columns.Append(", ");
                values.Append(", ");
            }

            // Using backticks for MySQL identifiers
            columns.Append($"`{targetCol}`");
            
            var paramName = $"@p{index}";
            values.Append(paramName);

            var value = row[sourceCol] ?? DBNull.Value;
            parameters.Add(new MySqlParameter(paramName, value));

            index++;
        }

        var sql = $"INSERT INTO {_options.TableName} ({columns}) VALUES ({values})";

        using var connection = new MySqlConnection(_options.ConnectionString);
        connection.Open();

        using var command = new MySqlCommand(sql, connection);
        command.Parameters.AddRange(parameters.ToArray());

        command.ExecuteNonQuery();
    }
}
