using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Tokenizers;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class WhitespaceTokenizerTests : NoDisposalNeeded
    {
        public WhitespaceTokenizerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("      This is a leading whitespace", new[] { 4, 2, 1, 7, 10 })]
        [InlineData("This is a trailing whitespace     ", new[] { 4, 2, 1, 8, 10 })]
        [InlineData("No_whitespaces", new[] { 14 })]
        public void ParseWhitespaces(string value, int[] tokenSizes)
        {
            var context = new TokenSpanStorageContext();
            var source = new StringTextSource(context, value);

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);
            tokenizer.SetSource(source);

            int tokenCount = 0;
            foreach (var token in tokenizer)
            {
                Assert.Equal(tokenSizes[tokenCount], token.Length);
                tokenCount++;
            }

            Assert.Equal(tokenSizes.Length, tokenCount);
        }
    }

}
