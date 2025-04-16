using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Numerics.Tensors;
using System.Text;
using System.Threading.Tasks;
using FastTests.Voron.FixedSize;
using Hl7.Fhir.Model;
using Lucene.Net.Search;
using Lucene.Net.Support;
using NetTopologySuite.Operation.Distance;
using Sparrow.Server.Tensors;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class TensorsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const float Eps = 1e-6f;

        // Test that for identical vectors, we get maximum similarity (and so a distance of zero).
        [RavenFact(RavenTestCategory.Core)]
        public void IdenticalVectors_ReturnsMaxSimilarity_ZeroDistance()
        {
            float[] vector = [1f, 2f, 3f, 4f];
            var span = new ReadOnlySpan<float>(vector);

            float similarity = Functions.CosineSimilarity(span, span);
            float distance = Functions.CosineDistance(span, span);

            float expectedSim = TensorPrimitives.CosineSimilarity(span, span);
            float expectedDistance = 1.0f - expectedSim;

            // Expected behavior: identical vectors yield similarity==1.0 so distance==0.0.
            // (If your implementation were really computing similarity, this is what you’d expect.)
            Assert.InRange(similarity, (float)(expectedSim - Eps), (float)(expectedSim + Eps));
            Assert.InRange(distance, (float)(expectedDistance - Eps), (float)(expectedDistance + Eps));
        }

        // Test for orthogonal vectors.
        // Conventionally, for orthogonal vectors, cosine similarity should be 0 and so distance should be 1.
        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(2)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(17)]
        [InlineData(32)]
        [InlineData(256)]
        [InlineData(1000)]
        public void OrthogonalVectors_ShouldYieldLowSimilarity_HighDistance(int size)
        {
            float[] a = new float[size];
            float[] b = new float[size];

            a[size-1] = 1f;
            b[0] = 1f;

            var similarity = Functions.CosineSimilarity<float>(a, b);
            var distance = Functions.CosineDistance<float>(a, b);

            float expectedSim = TensorPrimitives.CosineSimilarity(a, b);
            float expectedDistance = 1.0f - expectedSim;

            // Expected behavior: identical vectors yield similarity==1.0 so distance==0.0.
            // (If your implementation were really computing similarity, this is what you’d expect.)
            Assert.InRange(similarity, (float)(expectedSim - Eps), (float)(expectedSim + Eps));
            Assert.InRange(distance, (float)(expectedDistance - Eps), (float)(expectedDistance + Eps));
        }

        // Test for vectors that are both zero.
        // Many definitions choose to define the similarity of two zero vectors as 1 (so that distance is 0).
        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(2)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(17)]
        [InlineData(32)]
        [InlineData(33)]
        [InlineData(256)]
        [InlineData(1000)]
        public void BothZeroVectors_ProducesDefinedSimilarityAndDistance(int size)
        {
            float[] a = new float[size];
            float[] b = new float[size];

            var similarity = Functions.CosineSimilarity<float>(a, b);
            var distance = Functions.CosineDistance<float>(a, b);

            // Compute reference similarity (using conventional cosine similarity).
            float expectedSim = TensorPrimitives.CosineSimilarity(a, b);
            float expectedDistance = 1.0f - expectedSim;

            // Verify they match within a small tolerance.

            Assert.InRange(similarity, (float)(expectedSim - Eps), (float)(expectedSim + Eps));
            Assert.InRange(distance, (float)(expectedDistance - Eps), (float)(expectedDistance + Eps));
        }

        // A randomized test to compare your implementation with a reference implementation.
        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void RandomVectors_ReferenceComparison(int seed)
        {
            var rnd = new Random(seed);
            int size = rnd.Next(1024) + 1;

            // Generate two random vectors of the same size.
            float[] vector1 = new float[size];
            float[] vector2 = new float[size];
            for (int i = 0; i < size; i++)
            {
                vector1[i] = (float)rnd.NextDouble();
                vector2[i] = (float)rnd.NextDouble();
            }

            // Use your functions.
            float sim = Functions.CosineSimilarity<float>(vector1, vector2);
            float distance = Functions.CosineDistance<float>(vector1, vector2);

            // Compute reference similarity (using conventional cosine similarity).
            float expectedSim = TensorPrimitives.CosineSimilarity(vector1, vector2);
            float expectedDistance = 1.0f - expectedSim;

            // Verify they match within a small tolerance.

            Assert.InRange(sim, (float)(expectedSim - Eps), (float)(expectedSim + Eps));
            Assert.InRange(distance, (float)(expectedDistance - Eps), (float)(expectedDistance + Eps));
        }
    }
}
