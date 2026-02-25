using System;
using System.Collections.Generic;
using System.Linq;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Sources
{
    /// <summary>
    /// An <see cref="ISource"/> that deduplicates rows from an inner source based on business key columns.
    /// In case of duplicates, the last row processed wins.
    /// </summary>
    public sealed class DeduplicatingSource : ISource
    {
        private readonly ISource _innerSource;
        private readonly string[] _businessKeyColumns;

        /// <summary>Initializes a new instance of the <see cref="DeduplicatingSource"/> class.</summary>
        public DeduplicatingSource(ISource innerSource, IEnumerable<string> businessKeyColumns)
        {
            _innerSource = innerSource ?? throw new ArgumentNullException(nameof(innerSource));
            
            if (businessKeyColumns == null)
                throw new ArgumentNullException(nameof(businessKeyColumns));

            _businessKeyColumns = businessKeyColumns.ToArray();

            if (_businessKeyColumns.Length == 0)
                throw new ArgumentException("Business key columns cannot be empty.", nameof(businessKeyColumns));
        }

        /// <summary>Reads deduplicated rows from the inner source.</summary>
        public IEnumerable<IRow> Read()
        {
            var buffer = new Dictionary<object?[], IRow>(BusinessKeyComparer.Instance);
            foreach (var row in _innerSource.Read())
            {
                if (row == null) continue;
                var key = new object?[_businessKeyColumns.Length];
                for (int i = 0; i < _businessKeyColumns.Length; i++)
                {
                    key[i] = row.Get<object?>(_businessKeyColumns[i]);
                }

                // Last-write-wins: overwrite existing key
                buffer[key] = row;
            }

            return buffer.Values;
        }

        private sealed class BusinessKeyComparer : IEqualityComparer<object?[]>
        {
            public static readonly BusinessKeyComparer Instance = new();

            public bool Equals(object?[]? x, object?[]? y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (x is null || y is null) return false;
                if (x.Length != y.Length) return false;

                for (int i = 0; i < x.Length; i++)
                {
                    if (!Equals(x[i], y[i])) return false;
                }
                return true;
            }

            public int GetHashCode(object?[] obj)
            {
                if (obj is null) return 0;
                var hash = new HashCode();
                foreach (var item in obj)
                {
                    hash.Add(item);
                }
                return hash.ToHashCode();
            }
        }
    }
}
