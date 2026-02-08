using System;
using System.Collections.Generic;
using Xunit;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Execution;

namespace OrionflowETL.Tests.Core.Execution;

public class ErrorContextTests
{
    private class FakeRow : IRow
    {
        public IReadOnlyCollection<string> Columns => Array.Empty<string>();

        public object? this[string columnName]
        {
            get => null;
            set { }
        }

        public T Get<T>(string columnName) => default!;
    }

    [Fact]
    public void Constructor_ShouldInitializeProperties_Correctly()
    {
        // Arrange
        var row = new FakeRow();
        var stepName = "TestStep";
        var stepType = typeof(string);
        var stage = PipelineStage.Transform;
        var exception = new Exception("Test exception");
        var message = "Something went wrong";

        // Act
        var errorContext = new ErrorContext(
            row,
            stepName,
            stepType,
            stage,
            exception,
            message
        );

        // Assert
        Assert.Same(row, errorContext.Row);
        Assert.Equal(stepName, errorContext.StepName);
        Assert.Equal(stepType, errorContext.StepType);
        Assert.Equal(stage, errorContext.Stage);
        Assert.Same(exception, errorContext.Exception);
        Assert.Equal(message, errorContext.Message);
        Assert.True(errorContext.Timestamp <= DateTimeOffset.UtcNow);
        Assert.True(errorContext.Timestamp > DateTimeOffset.UtcNow.AddSeconds(-1));
    }
}
