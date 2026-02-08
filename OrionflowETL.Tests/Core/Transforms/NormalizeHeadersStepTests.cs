using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;
using OrionflowETL.Core.Transforms;
using Xunit;

namespace OrionflowETL.Tests.Core.Transforms;

public class NormalizeHeadersStepTests
{
    [Fact]
    public void Trims_Whitespace_From_Headers()
    {
        var step = new NormalizeHeadersStep(trim: true);
        var row = new Row(new Dictionary<string, object?> { { " Name ", "Alice" }, { " Email", "test@test.com" } });

        var result = step.Execute(row);

        Assert.True(result.Columns.Contains("Name"));
        Assert.True(result.Columns.Contains("Email"));
        Assert.False(result.Columns.Contains(" Name "));
        Assert.Equal("Alice", result["Name"]);
    }

    [Fact]
    public void Converts_To_Lowercase_If_Enabled()
    {
        var step = new NormalizeHeadersStep(trim: true, toLower: true);
        var row = new Row(new Dictionary<string, object?> { { " Name ", "Alice" } });

        var result = step.Execute(row);

        Assert.True(result.Columns.Contains("name"));
        Assert.False(result.Columns.Contains("Name"));
        Assert.Equal("Alice", result["name"]);
    }

    [Fact]
    public void Default_Is_Trim_Only()
    {
        var step = new NormalizeHeadersStep();
        var row = new Row(new Dictionary<string, object?> { { " Name ", "Alice" } });

        var result = step.Execute(row);

        Assert.True(result.Columns.Contains("Name")); // Case preserved
        Assert.False(result.Columns.Contains(" Name "));
    }

    [Fact]
    public void Throws_On_Duplicate_Resulting_Columns()
    {
        var step = new NormalizeHeadersStep(trim: true);
        // "Name" and " Name " map to same "Name"
        var row = new Row(new Dictionary<string, object?> 
        { 
            { "Name", "Alice" }, 
            { " Name ", "Bob" } 
        });

        var ex = Assert.Throws<InvalidOperationException>(() => step.Execute(row));
        Assert.Contains("Duplicate column name", ex.Message);
    }

    [Fact]
    public void Throws_If_Normalize_Result_Is_Empty()
    {
        var step = new NormalizeHeadersStep(trim: true);
        var row = new Row(new Dictionary<string, object?> { { "   ", "Val" } });

        var ex = Assert.Throws<InvalidOperationException>(() => step.Execute(row));
        Assert.Contains("resulted in an empty string", ex.Message);
    }
}
