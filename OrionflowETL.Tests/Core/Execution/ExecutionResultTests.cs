using Xunit;
using System;
using System.Collections.Generic;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Execution;

namespace OrionflowETL.Tests.Core.Execution;

public class ExecutionResultTests
{
    private class FakeRow : IRow
    {
        public IReadOnlyCollection<string> Columns => Array.Empty<string>();
        public object? this[string columnName] { get => null; set { } }
        public T Get<T>(string columnName) => default!;
    }

    [Fact]
    public void Constructor_ShouldInitializeProperties_WhenStatusIsSuccess()
    {
        // Arrange
        var status = ExecutionStatus.Success;
        long read = 100;
        long processed = 100;
        long succeeded = 100;
        long failed = 0;

        // Act
        var result = new ExecutionResult(status, read, processed, succeeded, failed);

        // Assert
        Assert.Equal(ExecutionStatus.Success, result.Status);
        Assert.Equal(100, result.TotalRowsRead);
        Assert.Equal(100, result.TotalRowsProcessed);
        Assert.Equal(100, result.TotalRowsSucceeded);
        Assert.Equal(0, result.TotalRowsFailed);
        Assert.NotNull(result.Errors);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Constructor_ShouldInitializeProperties_WhenStatusIsPartialSuccess()
    {
        // Arrange
        var status = ExecutionStatus.PartialSuccess;
        long read = 100;
        long processed = 100;
        long succeeded = 90;
        long failed = 10;

        // Act
        var result = new ExecutionResult(status, read, processed, succeeded, failed);

        // Assert
        Assert.Equal(ExecutionStatus.PartialSuccess, result.Status);
        Assert.Equal(100, result.TotalRowsRead);
        Assert.Equal(100, result.TotalRowsProcessed);
        Assert.Equal(90, result.TotalRowsSucceeded);
        Assert.Equal(10, result.TotalRowsFailed);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Constructor_ShouldInitializeProperties_WhenStatusIsFailed()
    {
        // Arrange
        var status = ExecutionStatus.Failed;
        long read = 50;
        long processed = 10;
        long succeeded = 5;
        long failed = 5;

        // Act
        var result = new ExecutionResult(status, read, processed, succeeded, failed);

        // Assert
        Assert.Equal(ExecutionStatus.Failed, result.Status);
        Assert.Equal(50, result.TotalRowsRead);
        Assert.Equal(10, result.TotalRowsProcessed);
        Assert.Equal(5, result.TotalRowsSucceeded);
        Assert.Equal(5, result.TotalRowsFailed);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Constructor_ShouldInitializeErrors_WhenProvided()
    {
        // Arrange
        var err1 = new ErrorContext(new FakeRow(), "S1", typeof(string), PipelineStage.Transform, new Exception(), "E1");
        var err2 = new ErrorContext(new FakeRow(), "S2", typeof(string), PipelineStage.Load, new Exception(), "E2");
        var errors = new List<ErrorContext> { err1, err2 };

        // Act
        var result = new ExecutionResult(ExecutionStatus.PartialSuccess, 10, 10, 9, 1, errors);

        // Assert
        Assert.NotNull(result.Errors);
        Assert.Equal(2, result.Errors.Count);
        Assert.Contains(err1, result.Errors);
        Assert.Contains(err2, result.Errors);
    }

    [Fact]
    public void Constructor_ShouldMakeErrorsReadOnly()
    {
        // Arrange
        var errors = new List<ErrorContext>();
        
        // Act
        var result = new ExecutionResult(ExecutionStatus.Success, 0, 0, 0, 0, errors);

        // Assert
        Assert.IsAssignableFrom<IReadOnlyCollection<ErrorContext>>(result.Errors);
        // Additional check: modifying the original list should not affect the result if we copied it (which we did by ToList)
        errors.Add(new ErrorContext(new FakeRow(), "S", typeof(string), PipelineStage.Extract, new Exception(), "E"));
        Assert.Empty(result.Errors);
    }
}
