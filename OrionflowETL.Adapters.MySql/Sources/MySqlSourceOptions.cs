namespace OrionflowETL.Adapters.MySql.Sources;

public class MySqlSourceOptions
{
    public required string ConnectionString { get; set; }
    public required string Query { get; set; }
}
