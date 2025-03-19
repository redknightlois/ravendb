using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public class RavenDB_23886(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [Fact]
        public void SingleControlCharacter()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("\u0001");
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal("\u0001", reader["Single"].ToString());
                }
            }
        }

        [Fact]
        public void SingleControlCharacterAsLast()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("Cool\u0001");
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal("Cool\u0001", reader["Single"].ToString());
                }
            }
        }

        [Fact]
        public void SingleControlCharacterInTheMiddle()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("Cool\u0001Cool");
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal("Cool\u0001Cool", reader["Single"].ToString());
                }
            }
        }

        [Fact]
        public void SingleControlCharacterAsFirst()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("\u0002Cool");
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal("\u0002Cool", reader["Single"].ToString());
                }
            }
        }

        [Fact]
        public void MultipleControlCharacter()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Single");
                        builder.WriteValue("\u0002Cool\u0002Cool\u0002");
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(1, reader.Count);
                    Assert.Equal("\u0002Cool\u0002Cool\u0002", reader["Single"].ToString());
                }
            }
        }
    }
}
