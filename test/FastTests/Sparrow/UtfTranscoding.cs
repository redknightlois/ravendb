using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server.Utf8;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class UtfTranscodingTests : NoDisposalNeeded
    {

        public UtfTranscodingTests(ITestOutputHelper output) : base(output)
        {
            
        }

        public static IEnumerable<object> Utf16Strings
        {
            get 
            {
                var assembly = Assembly.GetExecutingAssembly();
                var resourceName = "FastTests.Sparrow.utftranscoder.txt";

                using (Stream stream = assembly.GetManifestResourceStream(resourceName))
                using (StreamReader reader = new StreamReader(stream))
                {
                    var values = reader.ReadToEnd().Split('\n');

                    List<object> strings = new List<object>();
                    foreach (var value in values)
                        strings.Add(new object[] { value });
                    return strings;
                }
            }
        }

        [Theory]
        [MemberData(nameof(Utf16Strings))]
        public void TranscodingBackAndForth(string text)
        {
            ReadOnlySpan<char> textSpan = text;

            Span<byte> byteSpan = new byte[text.Length * 4];
            UtfTranscoder.ScalarFromUtf16(textSpan, ref byteSpan);
            Assert.Equal(text, Encoding.UTF8.GetString(byteSpan));

            Span<char> outputSpan = new char[text.Length * 2];
            UtfTranscoder.ScalarToUtf16(byteSpan, ref outputSpan);

            Assert.True(textSpan.SequenceEqual(outputSpan));
        }
    }
}
