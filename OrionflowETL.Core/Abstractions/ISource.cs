namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Responsability: 
///     1. Produce Rows 
///     2. Do not transform them
/// </sumary>
public interface ISource
{
    IEnumerable<IRow> Read();
}
