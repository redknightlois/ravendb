using System;
using System.Text;

namespace Tests.Infrastructure
{
    /// <summary>
    /// Thrown by the harness when test teardown collects errors. The type marks the
    /// failure as teardown, distinct from a failure of the test body; the body failure,
    /// when one exists, is carried separately on <see cref="BodyFailure"/>.
    /// </summary>
    public sealed class TestTeardownException : Exception
    {
        public TestTeardownException(AggregateException teardownErrors, Exception bodyFailure, bool? bodyFailed)
            : base(CreateMessage(bodyFailed, teardownErrors), teardownErrors)
        {
            BodyFailure = bodyFailure;
            BodyFailed = bodyFailed;
        }

        /// <summary>
        /// The test body failure, when the body failed and its outcome could be read; null otherwise.
        /// </summary>
        public Exception BodyFailure { get; }

        /// <summary>
        /// True when the test body failed, false when it passed, null when its outcome could not be read.
        /// </summary>
        public bool? BodyFailed { get; }

        // First line only of each collected error, so a truncated CI view still shows the body
        // outcome and the disposal error's server and stacks file path.
        private static string CreateMessage(bool? bodyFailed, AggregateException teardownErrors)
        {
            var sb = new StringBuilder("The test teardown failed. ");

            if (bodyFailed == true)
                sb.Append("The test body also failed.");
            else if (bodyFailed == false)
                sb.Append("The test body did not fail; only the teardown failed.");
            else
                sb.Append("Whether the test body failed could not be determined.");

            if (teardownErrors != null && teardownErrors.InnerExceptions.Count > 0)
            {
                sb.AppendLine().Append("Collected teardown errors:");

                foreach (var error in teardownErrors.InnerExceptions)
                {
                    var message = error.Message;
                    var lineBreak = message.IndexOfAny(new[] { '\r', '\n' });
                    if (lineBreak >= 0)
                        message = message.Substring(0, lineBreak);

                    sb.AppendLine().Append("  - ").Append(error.GetType().Name).Append(": ").Append(message);
                }
            }

            return sb.ToString();
        }
    }
}
