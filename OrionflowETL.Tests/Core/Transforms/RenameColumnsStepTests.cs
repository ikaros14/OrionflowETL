using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;
using OrionflowETL.Core.Transforms;

namespace OrionflowETL.Tests.Core.Transforms;

public class RenameColumnsStepTests
{
    [Fact]
    public void Renames_Single_Column()
    {
        var mapping = new Dictionary<string, string> { { "Old", "New" } };
        var step = new RenameColumnsStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "Old", 100 }, { "Other", 200 } });

        var result = step.Execute(row);

        Assert.True(result.Columns.Contains("New"));
        Assert.False(result.Columns.Contains("Old"));
        Assert.Equal(100, result["New"]);
        Assert.Equal(200, result["Other"]);
    }

    [Fact]
    public void Renames_Multiple_Columns()
    {
        var mapping = new Dictionary<string, string> 
        { 
            { "A", "Alpha" }, 
            { "B", "Beta" } 
        };
        var step = new RenameColumnsStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "A", 1 }, { "B", 2 }, { "C", 3 } });

        var result = step.Execute(row);

        Assert.True(result.Columns.Contains("Alpha"));
        Assert.True(result.Columns.Contains("Beta"));
        Assert.True(result.Columns.Contains("C"));
        
        Assert.Equal(1, result["Alpha"]);
        Assert.Equal(2, result["Beta"]);
        Assert.Equal(3, result["C"]);
    }

    [Fact]
    public void Throws_If_Source_Column_Missing()
    {
        var mapping = new Dictionary<string, string> { { "Missing", "Exists" } };
        var step = new RenameColumnsStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "A", 1 } });

        var ex = Assert.Throws<InvalidOperationException>(() => step.Execute(row));
        Assert.Contains("'Missing' specified in the mapping was not found", ex.Message);
    }

    [Fact]
    public void Throws_If_Rename_Causes_Collision_With_Existing_Column()
    {
        // "A" -> "B", but "B" already exists
        var mapping = new Dictionary<string, string> { { "A", "B" } };
        var step = new RenameColumnsStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "A", 1 }, { "B", 2 } });

        var ex = Assert.Throws<InvalidOperationException>(() => step.Execute(row));
        Assert.Contains("Duplicate column name 'B'", ex.Message);
    }

    [Fact]
    public void Throws_If_Two_Renames_Collision()
    {
        // "A" -> "C", "B" -> "C"
        var mapping = new Dictionary<string, string> { { "A", "C" }, { "B", "C" } };
        var step = new RenameColumnsStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "A", 1 }, { "B", 2 } });

        var ex = Assert.Throws<InvalidOperationException>(() => step.Execute(row));
        Assert.Contains("Duplicate column name 'C'", ex.Message);
    }

    [Fact]
    public void Is_Case_Insensitive_On_Source_Column()
    {
        var mapping = new Dictionary<string, string> { { "old", "New" } }; // Lowercase key
        var step = new RenameColumnsStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "OLD", 100 } }); // Uppercase in row

        var result = step.Execute(row);

        Assert.True(result.Columns.Contains("New"));
        Assert.Equal(100, result["New"]);
    }
}
