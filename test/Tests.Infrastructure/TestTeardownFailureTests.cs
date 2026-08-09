using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Utils;
using Xunit;

namespace Tests.Infrastructure
{
    public class TestTeardownFailureTests
    {
        private readonly ITestOutputHelper _output;

        public TestTeardownFailureTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task TeardownFailureIsReportedApartFromTheBodyFailure()
        {
            // Real disposal failure: the harness samples the process stacks and attaches the stacks file path.
            InvalidOperationException disposalFailure = null;
            try
            {
                await TestBase.ThrowCouldNotDisposeServerExceptionAsync(
                    "http://127.0.0.1:12345", "teardown-harness-check", TimeSpan.FromSeconds(5));
            }
            catch (InvalidOperationException e)
            {
                disposalFailure = e;
            }

            Assert.NotNull(disposalFailure);

            var stacksFilePath = GetStacksFilePath(disposalFailure.Message);
            Assert.True(File.Exists(stacksFilePath), $"stacks file '{stacksFilePath}' should exist");

            // The body is still running when this test invokes teardown, so the report must not claim
            // the body failed. Whether the outcome reads as unknown or passed depends on the runner.
            var harness = new HarnessUnderTest(_output, disposalFailure);
            var teardownFailure = await Assert.ThrowsAsync<TestTeardownException>(() => harness.DisposeAsync().AsTask());
            Assert.Null(teardownFailure.BodyFailure);
            Assert.NotEqual(true, teardownFailure.BodyFailed);

            var teardownErrors = Assert.IsType<AggregateException>(teardownFailure.InnerException);
            Assert.Contains(teardownErrors.InnerExceptions, e => ReferenceEquals(e, disposalFailure));

            // Both the body and the teardown failed.
            var bodyFailure = new InvalidOperationException("simulated test body failure");
            var teardownAggregator = new ExceptionAggregator("simulated teardown errors");
            teardownAggregator.Execute(() => throw disposalFailure);
            teardownAggregator.Execute(() => throw new IOException("simulated path deletion failure"));

            var bothFailed = TestBase.BuildTeardownFailure(teardownAggregator, bodyFailure, bodyFailed: true);
            var bothFailedReport = Assert.IsType<TestTeardownException>(bothFailed);
            Assert.Same(bodyFailure, bothFailedReport.BodyFailure);
            Assert.True(bothFailedReport.BodyFailed);

            var bothErrors = Assert.IsType<AggregateException>(bothFailedReport.InnerException);
            Assert.Equal(2, bothErrors.InnerExceptions.Count);
            Assert.Contains(bothErrors.InnerExceptions, e => ReferenceEquals(e, disposalFailure));
            Assert.Contains(bothErrors.InnerExceptions, e => e is IOException);
            Assert.DoesNotContain(bothErrors.InnerExceptions, e => ReferenceEquals(e, bodyFailure));

            // The body failure renders before the teardown failure.
            var runnerAggregator = new Xunit.v3.ExceptionAggregator();
            runnerAggregator.Add(bodyFailure);
            runnerAggregator.Add(bothFailedReport);
            var renderedOrder = Assert.IsType<AggregateException>(runnerAggregator.ToException());
            Assert.Same(bodyFailure, renderedOrder.InnerExceptions[0]);
            Assert.Same(bothFailedReport, renderedOrder.InnerExceptions[1]);

            // Only the teardown failed, and the body outcome was readable.
            var onlyTeardown = Assert.IsType<TestTeardownException>(
                TestBase.BuildTeardownFailure(teardownAggregator, bodyFailure: null, bodyFailed: false));
            Assert.Null(onlyTeardown.BodyFailure);
            Assert.False(onlyTeardown.BodyFailed);

            // A clean teardown reports nothing.
            Assert.Null(TestBase.BuildTeardownFailure(new ExceptionAggregator("no errors"), bodyFailure: null, bodyFailed: false));
        }

        private static string GetStacksFilePath(string message)
        {
            const string prefix = "StackTraces available at: '";
            var start = message.IndexOf(prefix, StringComparison.Ordinal);
            Assert.True(start >= 0, $"the disposal failure must carry the stacks file path; message was: {message}");
            start += prefix.Length;

            var end = message.IndexOf('\'', start);
            Assert.True(end > start, $"the stacks file path must be quoted; message was: {message}");

            return message.Substring(start, end - start);
        }

        private sealed class HarnessUnderTest : TestBase
        {
            private readonly Exception _teardownError;

            public HarnessUnderTest(ITestOutputHelper output, Exception teardownError) : base(output)
            {
                _teardownError = teardownError;
            }

            protected override void Dispose(ExceptionAggregator exceptionAggregator)
            {
                exceptionAggregator.Execute(() => throw _teardownError);
            }
        }
    }
}
