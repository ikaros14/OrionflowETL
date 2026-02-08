using OrionflowETL.Adapters.Csv;

namespace OrionflowETL.Tests.Adapters.Csv;
public sealed class CsvSourceTests
{
    [Fact]
    public void Reads_csv_file_and_produces_rows()
    {
        // Arrange
        var path = Path.GetTempFileName();

        File.WriteAllText(path,
                        @"Code,Name
                        P01, Product A
                        P02, Product B");

        var source = new CsvSource(new CsvOptions
        {
            Path = path
        });

        // Act
        var rows = source.Read().ToList();

        // Assert
        Assert.Equal(2, rows.Count);
        Assert.Equal("P01", rows[0].Get<string>("Code"));
        Assert.Equal("Product A", rows[0].Get<string>("Name"));
    }
}
