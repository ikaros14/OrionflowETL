namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Describe a Row and it's typed access without magic cast
/// </summary>
public interface IRow
{
    object? this[string columnName] { get; set; }
    T Get<T> (string columnName);
}
