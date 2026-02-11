using OrionflowETL.Adapters.Postgres.Sinks;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Tests.Adapters.Postgres;

public class PostgresSinkTests
{
    [Fact]
    public void Constructor_Throws_When_Options_Null()
    {
        Assert.Throws<ArgumentNullException>(() => new PostgresSink(null!));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_Throws_When_ConnectionString_Missing(string? connectionString)
    {
        var options = new PostgresSinkOptions 
        { 
            ConnectionString = connectionString!, 
            TableName = "users",
            ColumnMapping = new Dictionary<string, string> { {"a", "b"} }
        };
        Assert.Throws<ArgumentException>(() => new PostgresSink(options));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_Throws_When_TableName_Missing(string? tableName)
    {
        var options = new PostgresSinkOptions 
        { 
            ConnectionString = "Host=dummy;", 
            TableName = tableName!,
            ColumnMapping = new Dictionary<string, string> { {"a", "b"} }
        };
        Assert.Throws<ArgumentException>(() => new PostgresSink(options));
    }

    [Fact]
    public void Constructor_Throws_When_ColumnMapping_Missing()
    {
        var options = new PostgresSinkOptions 
        { 
            ConnectionString = "Host=dummy;", 
            TableName = "users",
            ColumnMapping = null!
        };
        Assert.Throws<ArgumentException>(() => new PostgresSink(options));
    }

     [Fact]
    public void Constructor_Throws_When_ColumnMapping_Empty()
    {
        var options = new PostgresSinkOptions 
        { 
            ConnectionString = "Host=dummy;", 
            TableName = "users",
            ColumnMapping = new Dictionary<string, string>()
        };
        Assert.Throws<ArgumentException>(() => new PostgresSink(options));
    }

    [Fact]
    public void Write_Throws_When_Row_Null()
    {
        var options = new PostgresSinkOptions 
        { 
             ConnectionString = "Host=dummy;", 
             TableName = "users",
             ColumnMapping = new Dictionary<string, string> { {"id", "id"} }
        };
        var sink = new PostgresSink(options);
        Assert.Throws<ArgumentNullException>(() => sink.Write(null!));
    }

    [Fact]
    public void Write_Throws_When_Row_Missing_Mapped_Column()
    {
        // Arrange
        var options = new PostgresSinkOptions 
        { 
             ConnectionString = "Host=dummy;", 
             TableName = "users",
             ColumnMapping = new Dictionary<string, string> 
             { 
                 { "missing_col", "db_col" } 
             }
        };
        var sink = new PostgresSink(options);
        var row = new Row(new Dictionary<string, object?> { { "other_col", 1 } });

        // Act & Assert
        // We expect InvalidOperationException because the row is missing 'missing_col'
        // and we are failing fast.
        var ex = Assert.Throws<InvalidOperationException>(() => sink.Write(row));
        Assert.Contains("missing_col", ex.Message);
    }

    [Fact(Skip = "Integration test requires running Postgres instance")]
    public void Write_Inserts_Row_Correctly()
    {
        // Requires real DB
    }
}
