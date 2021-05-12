using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sparrow.Server.Binary
{
    public ref struct BinaryWriter
    {
        private Span<byte> _data;

        public BinaryWriter(Span<byte> data)
        {
            _data = data;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(T v) where T : struct
        {
            MemoryMarshal.Write(_data, ref v);
            _data = _data.Slice(Unsafe.SizeOf<T>());
        }
    }
}
