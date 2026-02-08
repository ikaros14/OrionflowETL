using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;
using OrionflowETL.Core.Transforms;

namespace OrionflowETL.Tests.Core.Transforms;

public class FilterRowsStepTests
{
    [Fact]
    public void Returns_Row_When_Predicate_Is_True()
    {
        var step = new FilterRowsStep(r => true);
        var row = new Row(new Dictionary<string, object?> { { "Id", 1 } });

        var result = step.Execute(row);

        Assert.NotNull(result);
        Assert.Same(row, result);
    }

    [Fact]
    public void Returns_Null_When_Predicate_Is_False()
    {
        var step = new FilterRowsStep(r => false);
        var row = new Row(new Dictionary<string, object?> { { "Id", 1 } });

        var result = step.Execute(row);

        Assert.Null(result);
    }

    [Fact]
    public void Predicate_Can_Check_Values()
    {
        var step = new FilterRowsStep(r => (int)r["Age"] >= 18);
        
        var adult = new Row(new Dictionary<string, object?> { { "Age", 20 } });
        var minor = new Row(new Dictionary<string, object?> { { "Age", 15 } });

        Assert.NotNull(step.Execute(adult));
        Assert.Null(step.Execute(minor));
    }

    [Fact]
    public void Does_Not_Modify_Schema_Or_Values()
    {
        var step = new FilterRowsStep(r => true);
        var row = new Row(new Dictionary<string, object?> { { "Name", "Alice" } });

        var result = step.Execute(row);

        Assert.Equal("Alice", result["Name"]);
        Assert.Equal(row.Columns.Count, result.Columns.Count);
    }

    [Fact]
    public void Throws_ArgumentNullException_If_Row_IsNull()
    {
        var step = new FilterRowsStep(r => true);
        Assert.Throws<ArgumentNullException>(() => step.Execute(null!));
    }
}
