using System.Security.Cryptography;
using System.Text;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;
using OrionflowETL.Core.Transforms;

namespace OrionflowETL.Tests.Transforms;

public class HashRowStepTests
{
    // ── Helpers ─────────────────────────────────────────────────────

    private static IRow MakeRow(params (string col, object? val)[] columns)
    {
        var dict = columns.ToDictionary(c => c.col, c => c.val);
        return new Row(dict);
    }

    private static string ExpectedSha256(string input)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }

    private static string ExpectedMd5(string input)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        return Convert.ToHexString(MD5.HashData(bytes)).ToLowerInvariant();
    }

    // ── Constructor validation ───────────────────────────────────────

    [Fact]
    public void Constructor_ThrowsArgumentException_WhenTargetColumnIsEmpty()
    {
        Assert.Throws<ArgumentException>(() =>
            new HashRowStep(string.Empty));
    }

    [Fact]
    public void Constructor_ThrowsArgumentException_WhenTargetColumnIsWhitespace()
    {
        Assert.Throws<ArgumentException>(() =>
            new HashRowStep("   "));
    }

    // ── SHA256 (default) ────────────────────────────────────────────

    [Fact]
    public void Execute_AddsHashColumn_WithSha256ByDefault()
    {
        var step = new HashRowStep("__hash", ["Name", "City"]);
        var row  = MakeRow(("Name", "Alice"), ("City", "CDMX"));

        var result = step.Execute(row);

        Assert.True(result.Columns.Contains("__hash"));
        var hash = result.Get<string>("__hash")!;
        Assert.Equal(64, hash.Length);  // SHA256 hex = 64 chars
        Assert.Equal(ExpectedSha256("Alice|CDMX"), hash);
    }

    [Fact]
    public void Execute_HashIsStable_SameInputSameHash()
    {
        var step = new HashRowStep("__hash", ["A", "B"]);
        var row1 = MakeRow(("A", "foo"), ("B", "bar"));
        var row2 = MakeRow(("A", "foo"), ("B", "bar"));

        var h1 = step.Execute(row1).Get<string>("__hash");
        var h2 = step.Execute(row2).Get<string>("__hash");

        Assert.Equal(h1, h2);
    }

    [Fact]
    public void Execute_HashDiffers_WhenOneColumnChanges()
    {
        var step = new HashRowStep("__hash", ["Name", "City"]);
        var row1 = MakeRow(("Name", "Alice"), ("City", "CDMX"));
        var row2 = MakeRow(("Name", "Alice"), ("City", "MTY"));

        var h1 = step.Execute(row1).Get<string>("__hash");
        var h2 = step.Execute(row2).Get<string>("__hash");

        Assert.NotEqual(h1, h2);
    }

    // ── MD5 ──────────────────────────────────────────────────────────

    [Fact]
    public void Execute_AddsHashColumn_WithMd5Algorithm()
    {
        var step = new HashRowStep("__hash", ["Name"], HashAlgorithmType.MD5);
        var row  = MakeRow(("Name", "Alice"));

        var result = step.Execute(row);
        var hash   = result.Get<string>("__hash")!;

        Assert.Equal(32, hash.Length); // MD5 hex = 32 chars
        Assert.Equal(ExpectedMd5("Alice"), hash);
    }

    // ── Null handling ────────────────────────────────────────────────

    [Fact]
    public void Execute_TreatsNullAsSentinel_DifferentFromEmptyString()
    {
        var step    = new HashRowStep("__hash", ["Value"]);
        var rowNull = MakeRow(("Value", null));
        var rowEmpty = MakeRow(("Value", ""));

        var hashNull  = step.Execute(rowNull).Get<string>("__hash");
        var hashEmpty = step.Execute(rowEmpty).Get<string>("__hash");

        Assert.NotEqual(hashNull, hashEmpty);
    }

    [Fact]
    public void Execute_HandlesNullValueWithNullSentinel()
    {
        var step   = new HashRowStep("__hash", ["Value"]);
        var rowNull = MakeRow(("Value", null));

        var result = step.Execute(rowNull);
        var hash   = result.Get<string>("__hash")!;

        Assert.Equal(ExpectedSha256("∅"), hash);
    }

    // ── Column selection ─────────────────────────────────────────────

    [Fact]
    public void Execute_HashesAllColumns_WhenColumnListIsEmpty()
    {
        var step = new HashRowStep("__hash");  // no columns specified
        var row  = MakeRow(("A", "1"), ("B", "2"));

        var result = step.Execute(row);

        Assert.True(result.Columns.Contains("__hash"));
        // Result should be non-empty
        Assert.NotEmpty(result.Get<string>("__hash")!);
    }

    [Fact]
    public void Execute_MissingColumnInRow_TreatsAsNull()
    {
        // "Z" is not in the row — should be treated as null (∅)
        var step = new HashRowStep("__hash", ["A", "Z"]);
        var row  = MakeRow(("A", "hello"));

        var result = step.Execute(row);
        var hash   = result.Get<string>("__hash")!;

        Assert.Equal(ExpectedSha256("hello|∅"), hash);
    }

    // ── Row immutability ─────────────────────────────────────────────

    [Fact]
    public void Execute_PreservesAllExistingColumns()
    {
        var step = new HashRowStep("__hash", ["Name"]);
        var row  = MakeRow(("Name", "Alice"), ("Age", "30"), ("City", "CDMX"));

        var result = step.Execute(row);

        Assert.Equal("Alice", result.Get<object>("Name")?.ToString());
        Assert.Equal("30",    result.Get<object>("Age")?.ToString());
        Assert.Equal("CDMX",  result.Get<object>("City")?.ToString());
        Assert.True(result.Columns.Contains("__hash"));
    }

    [Fact]
    public void Execute_OverwritesExistingHashColumn()
    {
        var step = new HashRowStep("__hash", ["Name"]);
        var row  = MakeRow(("Name", "Alice"), ("__hash", "old_hash"));

        var result = step.Execute(row);
        var hash   = result.Get<string>("__hash")!;

        Assert.NotEqual("old_hash", hash);
        Assert.Equal(64, hash.Length); // SHA256
    }

    // ── Name property ────────────────────────────────────────────────

    [Fact]
    public void Name_IncludesAlgorithmAndTargetColumn()
    {
        var step = new HashRowStep("__hash", algorithm: HashAlgorithmType.MD5);
        Assert.Contains("MD5", step.Name);
        Assert.Contains("__hash", step.Name);
    }
}
