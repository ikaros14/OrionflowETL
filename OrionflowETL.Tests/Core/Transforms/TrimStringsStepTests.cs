using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;
using OrionflowETL.Core.Transforms;

namespace OrionflowETL.Tests.Core.Transforms;

public class TrimStringsStepTests
{
    private readonly TrimStringsStep _step = new();

    [Fact]
    public void Trims_Whitespace_From_Strings()
    {
        var input = new Row(new Dictionary<string, object?> { { "Name", "  Alice  " } });
        
        var result = _step.Execute(input);

        Assert.Equal("Alice", result["Name"]);
    }

    [Fact]
    public void Ignores_NonString_Values()
    {
        var input = new Row(new Dictionary<string, object?> { { "Age", 123 }, { "Date", DateTime.Now } });

        var result = _step.Execute(input);

        Assert.Equal(input["Age"], result["Age"]);
        Assert.Equal(input["Date"], result["Date"]);
    }

    [Fact]
    public void Preserves_Nulls()
    {
        var input = new Row(new Dictionary<string, object?> { { "Comment", null } });

        var result = _step.Execute(input);

        Assert.Null(result["Comment"]);
    }

    [Fact]
    public void Preserves_Strings_Without_Whitespace()
    {
        var input = new Row(new Dictionary<string, object?> { { "Name", "Alice" } });

        var result = _step.Execute(input);

        // Optimization check: should return same instance if logic supports it, or at least equal value
        Assert.Equal("Alice", result["Name"]);
    }

    [Fact]
    public void Affects_Multiple_Columns()
    {
        var input = new Row(new Dictionary<string, object?> 
        { 
            { "A", "  A  " }, 
            { "B", "B" }, 
            { "C", " C " } 
        });

        var result = _step.Execute(input);

        Assert.Equal("A", result["A"]);
        Assert.Equal("B", result["B"]);
        Assert.Equal("C", result["C"]);
    }
}
