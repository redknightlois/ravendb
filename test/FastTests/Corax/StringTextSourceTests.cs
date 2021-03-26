using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Tokenizers;
using Lucene.Net.Analysis;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{

    public class StringTextSourceTests : NoDisposalNeeded
    {
        public StringTextSourceTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SimpleTokenization()
        {
            var context = new TokenSpanStorageContext();
            var source = new StringTextSource(context, "This is a good string.");

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);
            tokenizer.SetSource(source);

            int[] tokenSizes = { 4, 2, 1, 4, 7 };

            int tokenCount = 0;
            foreach (var token in tokenizer)
            {
                Assert.Equal(tokenSizes[tokenCount], token.Length);
                tokenCount++;
            }

            Assert.Equal(5, tokenCount);
        }

        [Fact]
        public void ResetSource()
        {
            var context = new TokenSpanStorageContext();
            var source1 = new StringTextSource(context, "This is a good string.");
            var source2 = new StringTextSource(context, "This is a another string.");

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);
            tokenizer.SetSource(source1);

            // Iterate the first source.
            foreach (var token in tokenizer) { }

            tokenizer.SetSource(source2);

            int[] tokenSizes = { 4, 2, 1, 7, 7 };

            int tokenCount = 0;
            foreach (var token in tokenizer)
            {
                Assert.Equal(tokenSizes[tokenCount], token.Length);
                tokenCount++;
            }

            Assert.Equal(5, tokenCount);
        }
    }
}
