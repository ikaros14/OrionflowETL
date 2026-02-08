using OrionflowETL.Adapters.SqlServer.Sinks;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Tests.Adapters.SqlServer.Sinks;

public class SqlServerSinkTests
{
    [Fact]
    public void Constructor_Throws_If_Options_Null()
    {
        Assert.Throws<ArgumentNullException>(() => new SqlServerSink(null!));
    }

    [Fact]
    public void Constructor_Throws_If_ConnectionString_Empty()
    {
        Assert.Throws<ArgumentException>(() => new SqlServerSink(new SqlServerSinkOptions 
        { 
            ConnectionString = "", 
            TableName = "T" 
        }));
    }

    [Fact]
    public void Write_Throws_If_Mapping_Missing()
    {
        var sink = new SqlServerSink(new SqlServerSinkOptions 
        { 
            ConnectionString = "S", 
            TableName = "T" 
            // No mapping
        });

        var row = new Row(new Dictionary<string, object?> { { "A", 1 } });

        var ex = Assert.Throws<InvalidOperationException>(() => sink.Write(row));
        Assert.Contains("ColumnMapping must be provided", ex.Message);
    }

    [Fact(Skip = "Requires local SQL Server")]
    public void Write_Inserts_Row_Successfully()
    {
        // Integration test setup would go here
        // 1. Create Table
        // 2. Sink.Write
        // 3. Select count(*)
        
        // Arrange
        var connectionString = "Server=localhost;Database=TestDB;Trusted_Connection=True;TrustServerCertificate=True;";
        var tableName = "TestSinkTable";
        
        using (var setupConn = new Microsoft.Data.SqlClient.SqlConnection(connectionString))
        {
            setupConn.Open();
            using var cmd = setupConn.CreateCommand();
            cmd.CommandText = $@"
                IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName};
                CREATE TABLE {tableName} (Id INT, Name NVARCHAR(50));
            ";
            cmd.ExecuteNonQuery();
        }

        var sink = new SqlServerSink(new SqlServerSinkOptions
        {
            ConnectionString = connectionString,
            TableName = tableName,
            ColumnMapping = new Dictionary<string, string>
            {
                { "IdProp", "Id" },
                { "NameProp", "Name" }
            }
        });

        var row = new Row(new Dictionary<string, object?> 
        { 
            { "IdProp", 100 }, 
            { "NameProp", "IntegrationTest" } 
        });

        // Act
        sink.Write(row);

        // Assert
        using (var verifyConn = new Microsoft.Data.SqlClient.SqlConnection(connectionString))
        {
            verifyConn.Open();
            using var cmd = verifyConn.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM {tableName} WHERE Id = 100 AND Name = 'IntegrationTest'";
            var count = (int)cmd.ExecuteScalar();
            Assert.Equal(1, count);
        }
    }
}
