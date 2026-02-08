using Xunit;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Execution;
using System.Collections.Generic;
using System;
using System.Linq;

namespace OrionflowETL.Tests.Core.Execution;

public class PipelineExecutorTest
{
    private class FakeRow : IRow
    {
        public IReadOnlyCollection<string> Columns => Array.Empty<string>();
        public object? this[string columnName] { get => null; set { } }
        public T Get<T>(string columnName) => default!;
    }

    private class FakeSource : ISource
    {
        private readonly IEnumerable<IRow> _rows;
        public IReadOnlyCollection<string> Columns => Array.Empty<string>();

        public FakeSource(IEnumerable<IRow> rows)
        {
            _rows = rows;
        }

        public IEnumerable<IRow> Read() => _rows;
        
        // Satisfy potential interface requirements if any specific props exist
    }

    private class FakeStep : IPipelineStep
    {
        private readonly Func<IRow, IRow> _action;
        public FakeStep(Func<IRow, IRow> action) { _action = action; }
        public IRow Execute(IRow row) => _action(row);
    }

    private class FakeSink : IDataSink
    {
        public void Write(IRow row) { }
    }
    
    // Step that throws exception
    private class FailingStep : IPipelineStep
    {
        public IRow Execute(IRow row) => throw new Exception("BAM");
    }

    [Fact]
    public void Execute_ShouldSucceed_WhenNoErrors()
    {
        // Arrange
        var row = new FakeRow();
        var source = new FakeSource(new[] { row }); // 1 row
        var step = new FakeStep(r => r); // Identity step
        var sink = new FakeSink();

        var executor = new PipelineExecutor();

        // Act
        var result = executor.Execute(source, new[] { step }, sink);

        // Assert
        Assert.Equal(ExecutionStatus.Success, result.Status);
        Assert.Equal(1, result.TotalRowsRead);
        Assert.Equal(1, result.TotalRowsSucceeded);
    }

    [Fact]
    public void Execute_FailFast_ShouldAbortOnFirstError()
    {
        // Arrange
        var row = new FakeRow();
        var source = new FakeSource(new[] { row, row }); // 2 rows
        var step = new FailingStep();
        var sink = new FakeSink();
        var executor = new PipelineExecutor();

        // Act
        var result = executor.Execute(
            source, 
            new[] { step }, 
            sink, 
            ErrorStrategy.FailFast
        );

        // Assert
        Assert.Equal(ExecutionStatus.Failed, result.Status);
        Assert.Equal(1, result.TotalRowsRead); // Only read 1 because it aborted
        Assert.Equal(1, result.TotalRowsFailed);
        Assert.Single(result.Errors);
    }

    [Fact]
    public void Execute_ContinueOnError_ShouldProcessAllRows()
    {
        // Arrange
        var row = new FakeRow();
        var source = new FakeSource(new[] { row, row, row }); // 3 rows
        var step = new FailingStep();
        var sink = new FakeSink();
        var executor = new PipelineExecutor();

        // Act
        var result = executor.Execute(
            source, 
            new[] { step }, 
            sink, 
            ErrorStrategy.ContinueOnError
        );

        // Assert
        Assert.Equal(ExecutionStatus.PartialSuccess, result.Status);
        // Requirement said: PartialSuccess if errors but execution continued. 
        Assert.Equal(3, result.TotalRowsRead);
        Assert.Equal(3, result.TotalRowsFailed);
        Assert.Equal(0, result.TotalRowsSucceeded);
        Assert.Equal(3, result.Errors.Count);
    }
}
