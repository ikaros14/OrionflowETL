namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Transform
/// </summary>
public interface IPipelineStep
{
    IRow Execute(IRow row);
}

