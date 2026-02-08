using OrionflowETL.Adapters.SqlServer;

namespace OrionflowETL.Tests.Adapters.SqlServer;

public sealed class SqlServerSourceTests
{
    [Fact(Skip = "Requires SQL Server instance")]
    //[Fact]
    public void Reads_rows_from_sql_server_and_produces_iraw()
    {
        // Arrange
        var source = new SqlServerSource(new SqlServerOptions
        {
            ConnectionString =
            "Server=localhost;Database=SupplierPrices;Trusted_Connection=True;",
            Query = """
                SELECT 
                    'P01' AS Code,
                    ' Product A ' AS Name,
                    NULL AS Description
            """
        });

        // Act
        var rows = source.Read().ToList();

        // Assert
        Assert.Single(rows);

        var row = rows[0];
        Assert.Equal("P01", row.Get<string>("Code"));
        Assert.Equal(" Product A ", row.Get<string>("Name"));
        Assert.Null(row.Get<object?>("Description"));
    }
}

