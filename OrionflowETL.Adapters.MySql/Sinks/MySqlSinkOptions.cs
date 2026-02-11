namespace OrionflowETL.Adapters.MySql.Sinks;

public class MySqlSinkOptions
{
    public required string ConnectionString { get; set; }
    public required string TableName { get; set; }
    public required IDictionary<string, string> ColumnMapping { get; set; }
}
