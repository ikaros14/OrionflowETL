using System.Collections.Generic;

namespace OrionflowETL.Core.Abstractions;

/// <summary>
/// Optional interface for data sources that supports schema inspection
/// without reading the entire dataset.
/// </summary>
public interface ISchemaDiscovery
{
    /// <summary>
    /// Discovers the columns available in the data source.
    /// </summary>
    /// <returns>A collection of column names.</returns>
    IEnumerable<string> DiscoverColumns();
}
