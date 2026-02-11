using System.Linq;
using System.Text;
using Npgsql;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Adapters.Postgres.Sinks;

public class PostgresSink : IDataSink
{
    private readonly PostgresSinkOptions _options;

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

    public void Write(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var columns = new StringBuilder();
        var values = new StringBuilder();
        var parameters = new List<NpgsqlParameter>();
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

            // Quote column names to handle case-sensitivity and special characters safely
            columns.Append($"\"{targetCol}\"");

            var paramName = $"@p{index}";
            values.Append(paramName);

            var value = row[sourceCol] ?? DBNull.Value;
            parameters.Add(new NpgsqlParameter(paramName, value));

            index++;
        }

        using var connection = new NpgsqlConnection(_options.ConnectionString);
        connection.Open();

        using var command = new NpgsqlCommand($"INSERT INTO {_options.TableName} ({columns}) VALUES ({values})", connection);
        command.Parameters.AddRange(parameters.ToArray());
        
        command.ExecuteNonQuery();
    }
}
