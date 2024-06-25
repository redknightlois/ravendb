using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Voron;
using Microsoft.Coyote;
using Microsoft.Coyote.SystematicTesting;
using Raven.Server.Documents;
using Sparrow;
using Voron.Data.RawData;
using Xunit;
using Xunit.Abstractions;

namespace ConcurrencyTests.Voron
{
    public class ConcurrentSmallDataSection(ITestOutputHelper output) : StorageTest(output)
    {
        [Fact]
        public void CanReadAndWriteFromSection_AfterFlush()
        {
            var configuration = Configuration.Create().WithTestingIterations(10);
            var engine = TestingEngine.Create(configuration, CanReadAndWriteFromSection_AfterFlushAsync);
            engine.Run();
        }

        protected async Task CanReadAndWriteFromSection_AfterFlushAsync()
        {
            Options.ManualFlushing = true;
            
            long id = -1;
            long pageNumber = -1;
            
            var t1 = Task.Run(() =>
            {
                using (var tx = Env.WriteTransaction())
                {
                    var section = ActiveRawDataSmallSection.Create(tx, "test", (byte)TableType.None);
                    pageNumber = section.PageNumber;

                    Assert.True(section.TryAllocate(15, out id));
                    WriteValue(section, id, "Hello There");
                    tx.Commit();
                }
            });
            var t2 = Task.Run(Env.FlushLogToDataFile);

            // Join all.
            await Task.WhenAll(t1, t2);

            Env.FlushLogToDataFile();

            Assert.NotEqual(-1, pageNumber);
            Assert.NotEqual(-1, id);
            
            using (var tx = Env.ReadTransaction())
            {
                var section = new ActiveRawDataSmallSection(tx, pageNumber);

                AssertValueMatches(section, id, "Hello There");
            }
        }

        private static unsafe void WriteValue(ActiveRawDataSmallSection section, long id, string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            fixed (byte* p = bytes)
            {
                Assert.True(section.TryWrite(id, p, bytes.Length, false));
            }
        }

        private static unsafe void AssertValueMatches(ActiveRawDataSmallSection section, long id, string expected)
        {
            var p = section.DirectRead(id, out int size, out bool _);
            var buffer = new byte[size];
            fixed (byte* bp = buffer)
            {
                Memory.Copy(bp, p, size);
            }
            var actual = Encoding.UTF8.GetString(buffer, 0, size);
            Assert.Equal(expected, actual);
        }
    }
}
