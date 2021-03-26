using System;
using System.Text;
using Corax;
using Corax.Filters;
using Corax.Tokenizers;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class LowerCaseFilterTests : NoDisposalNeeded
    {
        public LowerCaseFilterTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("This iS A leaDIng whitespaCE.", new[] { 4, 2, 1, 7, 11 })]
        [InlineData("This IS a trailing whitespace     ", new[] { 4, 2, 1, 8, 10 })]
        [InlineData("No_Whitespaces", new[] { 14 })]
        public void ExecuteLowercase(string value, int[] tokenSizes)
        {
            var context = new TokenSpanStorageContext();
            var source = new StringTextSource(context, value);

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);
            tokenizer.SetSource(source);

            var filter = new LowerCaseFilter<WhitespaceTokenizer<StringTextSource>>(context, tokenizer);
            // filter.SetTokenizer(tokenizer);

            int tokenCount = 0;
            foreach (var token in filter)
            {
                Assert.Equal(tokenSizes[tokenCount], token.Length);
                var tokenString = new string(Encoding.UTF8.GetChars(context.RequestReadAccess(token).ToArray()));
                Assert.Equal(tokenString.ToLower(), tokenString);

                tokenCount++;
            }

            Assert.Equal(tokenSizes.Length, tokenCount);
        }
    }
}
