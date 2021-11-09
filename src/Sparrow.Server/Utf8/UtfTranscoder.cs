using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Server.Utf8
{
    public unsafe static partial class UtfTranscoder
    {
        private static delegate*<ReadOnlySpan<byte>, ref Span<char>, bool> _convertUtf8ToUtf16;
        private static delegate*<ReadOnlySpan<char>, ref Span<byte>, bool> _convertUtf16ToUtf8;

        static UtfTranscoder()
        {
            _convertUtf8ToUtf16 = &ScalarToUtf16;
            _convertUtf16ToUtf8 = &ScalarFromUtf16;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool FromUtf16(ReadOnlySpan<char> source, ref Span<byte> dest)
        {
            return _convertUtf16ToUtf8(source, ref dest);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ToUtf16(ReadOnlySpan<byte> source, ref Span<char> dest)
        {
            return _convertUtf8ToUtf16(source, ref dest);
        }

    }
}
