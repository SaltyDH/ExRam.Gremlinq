using System;
using System.Threading.Tasks;
using ExRam.Gremlinq.Core;
using ExRam.Gremlinq.Tests.Entities;
using Xunit;
using Xunit.Abstractions;

namespace ExRam.Gremlinq.Providers.CosmosDb.Tests
{
    public class CosmosDbQuerySerializationTest : CosmosDbQuerySerializationTestBase
    {
        public CosmosDbQuerySerializationTest(ITestOutputHelper testOutputHelper)
            : base(
                GremlinQuerySource.g
                    .ConfigureEnvironment(env => env
                        .UseCosmosDb(builder => builder
                            .At(new Uri("wss://localhost"), "database", "graph")
                            .AuthenticateBy("authKey"))), testOutputHelper)
        {
        }

        [Fact]
        public async Task GuidDeserialization_Succeeds()
        {
            await _g
                .AddV(new VertexWithGuid
                {
                    Id = Guid.NewGuid(),
                    Label = "Test",
                    TestGuidProperty = new Guid("d74f367c15da423ca5c1f08ad4ab42fd")
                });

            await Verify(this);
        }
    }
}
