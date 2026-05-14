using System;
using System.Runtime.InteropServices;
using System.Threading;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;

namespace SlowTests.Voron.Graphs;

public class HnswDescentCoverDiagnostic(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Voron)]
    public void MeasureDescentCover_OnDefaultHeuristic_ProducesPlausibleBaseline()
    {
        const int vectorSize = 32;
        const int vectorSizeInBytes = vectorSize * sizeof(float);
        const int numberOfEntries = 1000;
        const int numberOfQueries = 100;
        const float rho = 0.5f;

        var random = new Random(42);
        var vectors = new float[numberOfEntries][];
        for (int i = 0; i < numberOfEntries; i++)
            vectors[i] = RandomUnitVector(random, vectorSize);

        var queries = new float[numberOfQueries][];
        for (int i = 0; i < numberOfQueries; i++)
            queries[i] = RandomUnitVector(random, vectorSize);

        using var _ = Slice.From(Allocator, nameof(MeasureDescentCover_OnDefaultHeuristic_ProducesPlausibleBaseline), out var treeName);
        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, treeName, vectorSizeInBytes, numberOfEdges: 12, numberOfCandidates: 16, VectorEmbeddingType.Single);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, treeName, new Random(42)))
            {
                for (int i = 0; i < numberOfEntries; i++)
                    registration.Register(i + 1, MemoryMarshal.Cast<float, byte>(vectors[i]));
                registration.Commit(CancellationToken.None);
            }
            wTx.Commit();
        }

        Hnsw.DescentCoverReport report;
        using (var rTx = Env.ReadTransaction())
        {
            var queryBuffer = new byte[numberOfQueries * vectorSizeInBytes];
            for (int i = 0; i < numberOfQueries; i++)
                MemoryMarshal.Cast<float, byte>(queries[i]).CopyTo(queryBuffer.AsSpan(i * vectorSizeInBytes));

            report = Hnsw.MeasureDescentCover(rTx.LowLevelTransaction, treeName, queryBuffer, numberOfQueries, rho);
        }

        Output.WriteLine(report.ToString());

        Assert.Equal(numberOfQueries, report.QueriesSampled);
        Assert.True(report.MeanPathLength > 0, $"MeanPathLength should be > 0, was {report.MeanPathLength}");
        Assert.True(report.MeanPathLength <= 50, $"MeanPathLength should be <= 50 (log slack), was {report.MeanPathLength}");
        Assert.InRange(report.FractionUncovered, 0.0, 1.0);
        Assert.True(report.MeanWitnessesWhenCovered >= 0);
    }

    private static float[] RandomUnitVector(Random random, int dim)
    {
        var v = new float[dim];
        double norm2 = 0;
        for (int i = 0; i < dim; i++)
        {
            v[i] = (float)(random.NextDouble() * 2 - 1);
            norm2 += v[i] * v[i];
        }
        var inv = (float)(1.0 / Math.Sqrt(norm2));
        for (int i = 0; i < dim; i++)
            v[i] *= inv;
        return v;
    }
}
