
using System.Net.Http.Headers;

namespace OrionflowETL.Adapters.Postgres.Sinks;

public class PostgresSinkOptions
{
    public required string ConnectionString { get; set; }
    public required string TableName { get; set; }
    public IDictionary<string, string> ColumnMapping { get; set; } = new Dictionary<string, string>();
}
