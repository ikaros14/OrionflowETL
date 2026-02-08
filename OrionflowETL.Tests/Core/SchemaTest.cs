using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Tests.Core
{
    public class SchemaTest
    {
        [Fact]
        public void Schema_Preserves_Column_Order()
        {
            var columns = new ISchemaColumn[]
            {
                new SchemaColumn("Id", typeof(int)),
                new SchemaColumn("Name", typeof(string))
            };

            var schema = new Schema(columns);

            Assert.Equal("Id", schema.Columns[0].Name);
            Assert.Equal("Name", schema.Columns[1].Name);
        }

        [Fact]
        public void Schema_Throws_When_Empty()
        {
            Assert.Throws<ArgumentException>(() =>
                new Schema(Array.Empty<ISchemaColumn>())
            );
        }
    }

}
