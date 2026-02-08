using System.Globalization;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;
using OrionflowETL.Core.Transforms;

namespace OrionflowETL.Tests.Core.Transforms;

public class CastTypesStepTests
{
    [Fact]
    public void Casts_String_To_Int()
    {
        var mapping = new Dictionary<string, Type> { { "Age", typeof(int) } };
        var step = new CastTypesStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "Age", "25" } });

        var result = step.Execute(row);

        Assert.IsType<int>(result["Age"]);
        Assert.Equal(25, result["Age"]);
    }

    [Fact]
    public void Casts_String_To_Double_Invariant()
    {
        var mapping = new Dictionary<string, Type> { { "Price", typeof(double) } };
        var step = new CastTypesStep(mapping);
        // "12.50" with dot is invariant/US
        var row = new Row(new Dictionary<string, object?> { { "Price", "12.50" } });

        var result = step.Execute(row);

        Assert.IsType<double>(result["Price"]);
        Assert.Equal(12.50, result["Price"]);
    }

    [Fact]
    public void Casts_String_To_Bool()
    {
        var mapping = new Dictionary<string, Type> { { "IsActive", typeof(bool) } };
        var step = new CastTypesStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "IsActive", "True" } });

        var result = step.Execute(row);

        Assert.IsType<bool>(result["IsActive"]);
        Assert.True((bool)result["IsActive"]);
    }

    [Fact]
    public void Casts_String_To_DateTime_Invariant()
    {
        var mapping = new Dictionary<string, Type> { { "Date", typeof(DateTime) } };
        var step = new CastTypesStep(mapping);
        var dateStr = "2023-10-01T12:00:00";
        var row = new Row(new Dictionary<string, object?> { { "Date", dateStr } });

        var result = step.Execute(row);

        Assert.IsType<DateTime>(result["Date"]);
        Assert.Equal(DateTime.Parse(dateStr, CultureInfo.InvariantCulture), result["Date"]);
    }

    [Fact]
    public void Preserves_Nulls()
    {
        var mapping = new Dictionary<string, Type> { { "Age", typeof(int) } };
        var step = new CastTypesStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "Age", null } });

        var result = step.Execute(row);

        Assert.Null(result["Age"]);
    }

    [Fact]
    public void Ignores_Unmapped_Columns()
    {
        var mapping = new Dictionary<string, Type> { { "Age", typeof(int) } };
        var step = new CastTypesStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "Age", "30" }, { "Name", "Bob" } });

        var result = step.Execute(row);

        Assert.IsType<int>(result["Age"]);
        Assert.IsType<string>(result["Name"]); // Unchanged
    }

    [Fact]
    public void Throws_On_Invalid_Format()
    {
        var mapping = new Dictionary<string, Type> { { "Age", typeof(int) } };
        var step = new CastTypesStep(mapping);
        var row = new Row(new Dictionary<string, object?> { { "Age", "NotAnNumber" } });

        Assert.Throws<InvalidOperationException>(() => step.Execute(row));
    }

    [Fact]
    public void Throws_On_Unsupported_Type_Configuration()
    {
        // Guid is not in the V1 allowed list
        var mapping = new Dictionary<string, Type> { { "Id", typeof(Guid) } };
        
        Assert.Throws<NotSupportedException>(() => new CastTypesStep(mapping));
    }
}
