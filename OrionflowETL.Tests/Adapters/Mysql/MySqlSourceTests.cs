using System;
using OrionflowETL.Adapters.MySql.Sources;
using Xunit;

namespace OrionflowETL.Tests.Adapters.Mysql;

public class MySqlSourceTests
{
    [Fact]
    public void Constructor_Throws_When_ConnectionString_Missing()
    {
        var options = new MySqlSourceOptions
        {
            ConnectionString = "",
            Query = "SELECT 1"
        };
        
        Assert.Throws<ArgumentException>(() => new MySqlSource(options));
    }

    [Fact]
    public void Constructor_Throws_When_Query_Missing()
    {
        var options = new MySqlSourceOptions
        {
            ConnectionString = "Server=localhost;Uid=root;Pwd=Secret;",
            Query = ""
        };

        Assert.Throws<ArgumentException>(() => new MySqlSource(options));
    }

    [Fact(Skip = "Integration test requires running MySQL instance")]
    public void Execute_Reads_Rows_Correctly()
    {
        // Placeholder for integration test
    }
}
