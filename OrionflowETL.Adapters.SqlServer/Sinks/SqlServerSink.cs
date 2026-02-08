using System.Text;
using Microsoft.Data.SqlClient;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Adapters.SqlServer.Sinks;

/// <summary>
/// A sink that writes data to a SQL Server table using simple INSERT statements.
/// Maintains no state and uses a new connection per row (v1 simplified model).
/// </summary>
public sealed class SqlServerSink : IDataSink
{
    private readonly SqlServerSinkOptions _options;

    public SqlServerSink(SqlServerSinkOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            throw new ArgumentException("ConnectionString cannot be empty", nameof(options));

        if (string.IsNullOrWhiteSpace(_options.TableName))
            throw new ArgumentException("TableName cannot be empty", nameof(options));
    }

    public void Write(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        if (_options.ColumnMapping == null || _options.ColumnMapping.Count == 0)
        {
            throw new InvalidOperationException(
                "ColumnMapping must be provided because IRow does not expose column names for auto-mapping.");
        }

        var columns = new StringBuilder();
        var values = new StringBuilder();
        var parameters = new List<SqlParameter>();
        int index = 0;

        foreach (var mapping in _options.ColumnMapping)
        {
            var rowColumn = mapping.Key;
            var dbColumn = mapping.Value;

            if (index > 0)
            {
                columns.Append(", ");
                values.Append(", ");
            }

            // Sanitize column name (basic)
            columns.Append($"[{dbColumn}]");
            
            var paramName = $"@p{index}";
            values.Append(paramName);

            // Get value from row (throws if missing)
            var value = row[rowColumn] ?? DBNull.Value;
            parameters.Add(new SqlParameter(paramName, value));

            index++;
        }

        if (parameters.Count == 0)
        {
             return;
        }

        var sql = $"INSERT INTO {_options.TableName} ({columns}) VALUES ({values})";

        using var connection = new SqlConnection(_options.ConnectionString);
        using var command = new SqlCommand(sql, connection);
        
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddRange(parameters.ToArray());

        connection.Open();
        command.ExecuteNonQuery();
    }
}
