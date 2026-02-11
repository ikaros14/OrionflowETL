namespace OrionflowETL.Adapters.Postgres.Sources;

public class PostgresSourceOptions
{
    public string ConnectionString { get; set; } = string.Empty;
    public string Query { get; set; } = string.Empty;
}
