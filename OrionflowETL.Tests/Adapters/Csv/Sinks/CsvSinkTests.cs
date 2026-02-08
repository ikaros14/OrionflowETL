using OrionflowETL.Adapters.Csv.Sinks;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Tests.Adapters.Csv.Sinks;

public class CsvSinkTests : IDisposable
{
    private readonly List<string> _tempFiles = new();

    private string GetTempFile()
    {
        var path = Path.GetTempFileName();
        _tempFiles.Add(path);
        return path;
    }

    public void Dispose()
    {
        foreach (var file in _tempFiles)
        {
            if (File.Exists(file)) 
            {
                try 
                {
                    File.Delete(file);
                } 
                catch 
                {
                    // Ignore cleanup errors
                }
            }
        }
    }

    [Fact]
    public void Write_NewFile_WritesHeaderAndRows()
    {
        // Arrange
        var file = GetTempFile();
        File.Delete(file); // Ensure it doesn't exist

        var options = new CsvSinkOptions
        {
            Path = file,
            Columns = new[] { "Id", "Name" }
        };

        using var sink = new CsvSink(options);
        var row1 = new Row(new Dictionary<string, object?> { { "Id", 1 }, { "Name", "Alice" } });
        var row2 = new Row(new Dictionary<string, object?> { { "Id", 2 }, { "Name", "Bob" } });

        // Act
        sink.Write(row1);
        sink.Write(row2);
        sink.Dispose();

        // Assert
        var lines = File.ReadAllLines(file);
        Assert.Equal(3, lines.Length);
        Assert.Equal("Id,Name", lines[0]);
        Assert.Equal("1,Alice", lines[1]);
        Assert.Equal("2,Bob", lines[2]);
    }

    [Fact]
    public void Write_ExistingFile_Appends_AndAssumesHeaderExists()
    {
         // Arrange
        var file = GetTempFile();
        // Simulate existing file with header and one row
        File.WriteAllLines(file, new[] { "Id,Name", "1,Alice" });

        var options = new CsvSinkOptions
        {
            Path = file,
            Columns = new[] { "Id", "Name" }
        };

        using var sink = new CsvSink(options);
        var row2 = new Row(new Dictionary<string, object?> { { "Id", 2 }, { "Name", "Bob" } });

        // Act
        sink.Write(row2);
        sink.Dispose();

        // Assert
        var lines = File.ReadAllLines(file);
        Assert.Equal(3, lines.Length);
        Assert.Equal("Id,Name", lines[0]); // Original header preserved
        Assert.Equal("1,Alice", lines[1]);
        Assert.Equal("2,Bob", lines[2]); // Appended
    }
    
    // REMOVED: Write_ExistingFile_InfersSchema_IfColumnsNull
    // Logic is now separate: Columns are required.

    [Fact]
    public void Constructor_WithoutColumns_Throws()
    {
        var file = GetTempFile();
        
        // C\# required property validation happens at compile time technically if using 'required', 
        // but let's test if we pass null to the constructor logic if we were bypassing initiators or just plain object creation.
        // Actually, with 'required' modifier, we can't easily compile 'new CsvSinkOptions { Columns = null }' without warnings/errors depending on context.
        // But let's assume we pass a list that is null (ignoring nullable warning) or empty.
        
        var options = new CsvSinkOptions
        {
            Path = file,
            Columns = new List<string>() // Empty list
        };

        Assert.Throws<ArgumentException>(() => new CsvSink(options));
    }

    [Fact]
    public void Write_MissingColumn_Throws()
    {
        var file = GetTempFile();
        File.Delete(file);

        var options = new CsvSinkOptions
        {
            Path = file,
            Columns = new[] { "Id", "Name" }
        };

        using var sink = new CsvSink(options);
        // "Name" is missing
        var row = new Row(new Dictionary<string, object?> { { "Id", 1 } }); 

        Assert.Throws<InvalidOperationException>(() => sink.Write(row));
    }

    [Fact]
    public void Write_EscapesSpecialCharacters()
    {
        var file = GetTempFile();
        File.Delete(file);

        var options = new CsvSinkOptions
        {
            Path = file,
            Columns = new[] { "Text" }
        };

        using var sink = new CsvSink(options);
        var row = new Row(new Dictionary<string, object?> { { "Text", "Hello, \"World\"" } });

        sink.Write(row);
        sink.Dispose();

        var lines = File.ReadAllLines(file);
        Assert.Equal("\"Hello, \"\"World\"\"\"", lines[1]);
    }
}
