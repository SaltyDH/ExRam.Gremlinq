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
                            .At(new Uri("https://localhost:8081"), "Issue103", "Default")
                            .AuthenticateBy(
                                "authKey")
                            .ConfigureWebSocket(builder => builder
                                .At(new Uri("ws://localhost:8901")))))
                , testOutputHelper)
        {
        }

        [Fact]
        public async Task GuidDeserialization_Succeeds()
        {
            var g = GremlinQuerySource.g
                .ConfigureEnvironment(env => env
                    .UseCosmosDb(builder => builder
                        .At(new Uri("https://localhost:8081"), "Issue103", "Default")
                        .AuthenticateBy(
                            "authKey")
                        .ConfigureWebSocket(builder => builder
                            .At(new Uri("ws://localhost:8901")))));

            await g
                .AddV(new VertexWithGuid
                {
                    Id = Guid.NewGuid(),
                    Label = "Test",
                    TestGuidProperty = new Guid("d74f367c15da423ca5c1f08ad4ab42fd")
                })
                .FirstOrDefaultAsync();
        }
    }
}
