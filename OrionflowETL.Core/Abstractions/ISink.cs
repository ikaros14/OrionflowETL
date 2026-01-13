namespace OrionflowETL.Core.Abstractions;

/// <summary>
///  Responsability: 
///     1. Consume Rows 
///     2. Do not transform them
/// </summary>
public interface ISink
{
    void Write(IRow row);
}
