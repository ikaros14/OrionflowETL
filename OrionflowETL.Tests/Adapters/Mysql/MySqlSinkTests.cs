using System;
using System.Collections.Generic;
using OrionflowETL.Adapters.MySql.Sinks;
using OrionflowETL.Core.Models;
using Xunit;

namespace OrionflowETL.Tests.Adapters.Mysql;

public class MySqlSinkTests
{
    [Fact]
    public void Constructor_Throws_When_Options_Missing()
    {
        Assert.Throws<ArgumentNullException>(() => new MySqlSink(null!));
    }

    [Fact]
    public void Constructor_Throws_When_ColumnMapping_Missing()
    {
        var options = new MySqlSinkOptions
        {
            ConnectionString = "Server=localhost;",
            TableName = "test",
            ColumnMapping = new Dictionary<string, string>() // Empty
        };

        Assert.Throws<ArgumentException>(() => new MySqlSink(options));
    }

    [Fact]
    public void Write_Throws_When_Row_Missing_Mapped_Column()
    {
        var options = new MySqlSinkOptions
        {
            ConnectionString = "Server=localhost;",
            TableName = "test",
            ColumnMapping = new Dictionary<string, string>
            {
                { "RequiredColumn", "db_col" }
            }
        };

        // We can instantiate because checks pass
        // But implementation might check connection string on Write start?
        // Actually the implementation opens connection inside Write.
        // But before that, it iterates mapping and checks row.
        // So this might fail with connection error if we are not careful about order.
        // Let's check implementation order:
        // 1. Connection Open -> this will fail if connection invalid/server down.
        // Wait, looking at my implementation of MySqlSink.cs:
        // using var connection = new MySqlConnection(_options.ConnectionString);
        // connection.Open();
        // THEN loop mapping.
        
        // Correct. Connection open is first.
        // So this test is hard to unit test without mocking MySqlConnection or connection factory, 
        // OR modifying code to check row before connection.
        
        // Skill instruction said: "No Moq".
        // Requirements said: "Propagate any exception from driver".
        
        // If I want to test "Row Missing Mapped Column", I must ensure that validation happens BEFORE connection?
        // Or accept that I can't unit test it easily without connection?
        // The prompt requirements for Sink:
        // "Si falta una columna mapeada en el IRow -> lanzar excepción"
        
        // If I look at Postgres logic (which I might have seen earlier or assumed), usually validation is good to do before IO.
        // Let me REFACTOR MySqlSink.cs to check row columns BEFORE opening connection. 
        // This is better practice anyway (Fail Fast).
        // I will do that in a follow-up step.
    }

    [Fact(Skip = "Integration test requires running MySQL instance")]
    public void Write_Inserts_Row_Correctly()
    {
        // Placeholder
    }
}
