using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Tests.Core
{
    public class RowTest
    {
        [Fact]
        public void Get_Returns_Typed_Value()
        {
            var row = new Row(new Dictionary<string, object?>
            {
                ["Id"] = 10
            });

            var value = row.Get<int>("Id");

            Assert.Equal(10, value);
        }

        [Fact]
        public void Get_Throws_When_Type_Does_Not_Match()
        {
            var row = new Row(new Dictionary<string, object?>
            {
                ["Id"] = 10
            });

            Assert.Throws<InvalidCastException>(() =>
                row.Get<string>("Id")
            );
        }
    }

}

