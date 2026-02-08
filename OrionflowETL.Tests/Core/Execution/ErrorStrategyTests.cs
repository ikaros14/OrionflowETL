using Xunit;
using OrionflowETL.Core.Execution;

namespace OrionflowETL.Tests.Core.Execution;

public class ErrorStrategyTests
{
    [Fact]
    public void Strategies_ShouldHaveExpectedValues()
    {
        // Assert
        Assert.True(Enum.IsDefined(typeof(ErrorStrategy), "FailFast"));
        Assert.True(Enum.IsDefined(typeof(ErrorStrategy), "ContinueOnError"));
    }

    [Fact]
    public void FailFast_ShouldBeDefault_IfZero()
    {
        // Arrange
        ErrorStrategy strategy = default;

        // Assert
        Assert.Equal(ErrorStrategy.FailFast, strategy);
    }
}
