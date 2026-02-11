using OrionflowETL.Adapters.Postgres.Sources;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Tests.Adapters.Postgres;

public class PostgresSourceTests
{
    [Fact]
    public void Constructor_Throws_When_Options_Null()
    {
        Assert.Throws<ArgumentNullException>(() => new PostgresSource(null!));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_Throws_When_ConnectionString_Missing(string? connectionString)
    {
        var options = new PostgresSourceOptions { ConnectionString = connectionString!, Query = "SELECT 1" };
        Assert.Throws<ArgumentException>(() => new PostgresSource(options));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_Throws_When_Query_Missing(string? query)
    {
        var options = new PostgresSourceOptions { ConnectionString = "Host=dummy;", Query = query! };
        Assert.Throws<ArgumentException>(() => new PostgresSource(options));
    }

    [Fact(Skip = "Integration test requires running Postgres instance")]
    public void Execute_Reads_Rows_Correctly()
    {
        // Setup
        var connectionString = "Host=localhost;Database=testdb;Username=postgres;Password=password";
        var options = new PostgresSourceOptions 
        { 
            ConnectionString = connectionString,
            Query = "SELECT 1 as id, 'test' as name"
        };
        var source = new PostgresSource(options);

        // Act
        var rows = source.Read().ToList();

        // Assert
        Assert.NotNull(rows);
        Assert.NotEmpty(rows);
        Assert.Equal(1, rows[0].Get<int>("id"));
        Assert.Equal("test", rows[0].Get<string>("name"));
    }
}
