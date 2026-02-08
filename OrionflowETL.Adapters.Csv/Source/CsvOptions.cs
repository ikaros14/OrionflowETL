namespace OrionflowETL.Adapters.Csv;

public sealed class CsvOptions
{
    public required string Path { get; init; }

    public char Delimiter { get; init; } = ',';

    public bool HasHeader { get; init; } = true;

    public bool TrimValues { get; init; } = true;
}