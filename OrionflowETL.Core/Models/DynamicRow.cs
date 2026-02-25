using System.Collections.Generic;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Models
{
    /// <summary>
    /// A generic, dictionary-backed implementation of <see cref="IRow"/>.
    /// Used by many sources and transforms to hold row data dynamically.
    /// </summary>
    public class DynamicRow : IRow
    {
        private readonly Dictionary<string, object?> _data;

        /// <summary>Initializes a new instance of the <see cref="DynamicRow"/> class.</summary>
        public DynamicRow()
        {
            _data = new Dictionary<string, object?>();
        }

        /// <summary>Initializes a new instance of the <see cref="DynamicRow"/> class with existing data.</summary>
        public DynamicRow(Dictionary<string, object?> data)
        {
            _data = data;
        }

        /// <summary>Gets or sets the value associated with the specified column.</summary>
        public object? this[string column]
        {
            get => _data.ContainsKey(column) ? _data[column] : null;
            set => _data[column] = value;
        }

        /// <summary>Retrieves the value of the specified column cast to type T.</summary>
        public T Get<T>(string column)
        {
            if (!_data.TryGetValue(column, out var val))
                return default!;
            
            if (val == null) return default!;
            return (T)val;
        }

        /// <summary>Gets the names of the columns present in this row.</summary>
        public IReadOnlyCollection<string> Columns => _data.Keys;
        
        /// <summary>Checks if the row contains the specified column.</summary>
        public bool ContainsColumn(string column) => _data.ContainsKey(column);
    }
}
