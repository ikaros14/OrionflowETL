using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;
using OrionflowETL.Core.Transforms;

namespace OrionflowETL.Tests.Core.Transforms;

public class ValidateRequiredStepTests
{
    [Fact]
    public void Passes_When_Conditions_Met()
    {
        var required = new[] { "Id", "Name" };
        var step = new ValidateRequiredStep(required);
        var row = new Row(new Dictionary<string, object?> 
        { 
            { "Id", 1 }, 
            { "Name", "Alice" }, 
            { "Optional", null } 
        });

        var result = step.Execute(row);

        Assert.NotNull(result);
        Assert.Same(row, result); // Should return same instance
    }

    [Fact]
    public void Throws_When_Column_Missing()
    {
        var required = new[] { "Id", "Name" };
        var step = new ValidateRequiredStep(required);
        var row = new Row(new Dictionary<string, object?> { { "Id", 1 } }); // Name missing

        var ex = Assert.Throws<InvalidOperationException>(() => step.Execute(row));
        Assert.Contains("Missing required column 'Name'", ex.Message);
    }

    [Fact]
    public void Throws_When_Value_Is_Null()
    {
        var required = new[] { "Name" };
        var step = new ValidateRequiredStep(required);
        var row = new Row(new Dictionary<string, object?> { { "Name", null } });

        var ex = Assert.Throws<InvalidOperationException>(() => step.Execute(row));
        Assert.Contains("contains null", ex.Message);
    }

    [Fact]
    public void Throws_When_String_Is_Empty()
    {
        var required = new[] { "Name" };
        var step = new ValidateRequiredStep(required);
        var row = new Row(new Dictionary<string, object?> { { "Name", "" } });

        var ex = Assert.Throws<InvalidOperationException>(() => step.Execute(row));
        Assert.Contains("contains an empty string", ex.Message);
    }

    [Fact]
    public void Ignores_Whitespace_String()
    {
        // Spec says "no sea vacío" (empty). Whitespace is strictly not empty.
        // TrimStringsStep should be used before if whitespace check is needed.
        var required = new[] { "Name" };
        var step = new ValidateRequiredStep(required);
        var row = new Row(new Dictionary<string, object?> { { "Name", " " } });

        var result = step.Execute(row);
        Assert.Same(row, result);
    }

    [Fact]
    public void Is_Case_Insensitive_For_Column_Name()
    {
        var required = new[] { "name" }; // lowercase
        var step = new ValidateRequiredStep(required);
        var row = new Row(new Dictionary<string, object?> { { "NAME", "Alice" } }); // uppercase

        var result = step.Execute(row);
        Assert.Same(row, result);
    }
}
