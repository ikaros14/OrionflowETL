using System;
using System.Collections.Generic;
using System.Linq;
using OrionflowETL.Core.Abstractions;

namespace OrionflowETL.Core.Sinks
{
    /// <summary>
    /// Multiplexes a single pipeline execution across multiple sinks.
    /// </summary>
    public sealed class CompositeSink : IDataSink, IDisposable
    {
        private readonly IReadOnlyList<IDataSink> _sinks;
        private readonly bool _stopOnFirstError;
        private bool _disposed;

        /// <summary>Exposes inner sinks for coordination.</summary>
        public IReadOnlyList<IDataSink> InnerSinks => _sinks;

        /// <summary>Initializes a new instance of the <see cref="CompositeSink"/> class.</summary>
        public CompositeSink(IEnumerable<IDataSink> sinks, bool stopOnFirstError = true)
        {
            if (sinks == null) throw new ArgumentNullException(nameof(sinks));
            _sinks = sinks.ToList().AsReadOnly();
            if (_sinks.Count == 0)
                throw new ArgumentException("CompositeSink requires at least one inner sink.", nameof(sinks));
            _stopOnFirstError = stopOnFirstError;
        }

        /// <summary>Initializes a new instance of the <see cref="CompositeSink"/> class.</summary>
        public CompositeSink(params IDataSink[] sinks)
            : this((IEnumerable<IDataSink>)sinks)
        { }

        /// <summary>Writes a row to all inner sinks.</summary>
        public void Write(IRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            if (_stopOnFirstError)
            {
                foreach (var sink in _sinks)
                    sink.Write(row);
            }
            else
            {
                List<Exception>? errors = null;
                foreach (var sink in _sinks)
                {
                    try { sink.Write(row); }
                    catch (Exception ex)
                    {
                        (errors ??= new List<Exception>()).Add(ex);
                    }
                }
                if (errors != null)
                    throw new AggregateException("One or more sinks failed to process the row.", errors);
            }
        }

        /// <summary>Disposes all inner sinks.</summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            List<Exception>? errors = null;
            foreach (var sink in _sinks)
            {
                if (sink is IDisposable d)
                {
                    try { d.Dispose(); }
                    catch (Exception ex)
                    {
                        (errors ??= new List<Exception>()).Add(ex);
                    }
                }
            }

            if (errors != null)
                throw new AggregateException(
                    "One or more sinks failed during disposal.", errors);
        }
    }
}
